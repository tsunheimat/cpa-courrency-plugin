package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultLimit        = 2
	defaultWait         = 50 * time.Millisecond
	defaultRetryAfter   = 1
	defaultPollInterval = 5 * time.Millisecond
	leaseTTL            = 30 * time.Second
	leaseRenewInterval  = 10 * time.Second
)

type requestClass uint8

const (
	classCold requestClass = iota
	classWarm
)

type Lease struct {
	Token string
	Key   string
	Class requestClass
}

type Usage struct {
	Limit      int
	Reserved   int
	InFlight   int
	WarmFlight int
}

type AccountUsage struct {
	Key        string
	Limit      int
	Reserved   int
	InFlight   int
	WarmFlight int
}

// AggregateSnapshot returns process-local usage without exposing authority keys.
// It is intentionally a separate read path so management inspection never
// participates in admission or mutates leases.
func (a *localAuthority) AggregateSnapshot(_ context.Context, limit, reserved int) (Usage, int, error) {
	if a == nil {
		return Usage{}, 0, ErrAuthorityUnavailable
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	var total Usage
	accounts := 0
	for _, u := range a.usage {
		if u.InFlight > 0 {
			accounts++
		}
		total.InFlight += u.InFlight
		total.WarmFlight += u.WarmFlight
	}
	total.Limit, total.Reserved = limit, reserved
	return total, accounts, nil
}

func (a *localAuthority) AccountSnapshots(_ context.Context, limit, reserved int) ([]AccountUsage, error) {
	if a == nil {
		return nil, ErrAuthorityUnavailable
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	accounts := make([]AccountUsage, 0, len(a.usage))
	for key, u := range a.usage {
		if u.InFlight == 0 {
			continue
		}
		accounts = append(accounts, AccountUsage{Key: key, Limit: limit, Reserved: reserved, InFlight: u.InFlight, WarmFlight: u.WarmFlight})
	}
	sort.Slice(accounts, func(i, j int) bool { return accounts[i].Key < accounts[j].Key })
	return accounts, nil
}

type Authority interface {
	Snapshot(context.Context, string, int, int) (Usage, error)
	Acquire(context.Context, string, int, int, requestClass) (Lease, error)
	Release(context.Context, Lease) error
	Renew(context.Context, Lease) error
}

var ErrAuthorityUnavailable = errors.New("concurrency authority unavailable")

type AdmissionError struct {
	Code       string
	HTTPStatus int
	RetryAfter int
	Message    string
	Authority  bool
}

func (e *AdmissionError) Error() string {
	if e == nil || e.Message == "" {
		return "account concurrency admission failed"
	}
	return e.Message
}
func (e *AdmissionError) StatusCode() int {
	if e == nil {
		return 0
	}
	return e.HTTPStatus
}
func (e *AdmissionError) Retryable() bool { return e != nil && e.RetryAfter > 0 }

type localAuthority struct {
	mu     sync.Mutex
	active map[string]Lease
	usage  map[string]Usage
}

func newLocalAuthority() *localAuthority {
	return &localAuthority{active: make(map[string]Lease), usage: make(map[string]Usage)}
}

func (a *localAuthority) Snapshot(_ context.Context, key string, limit, reserved int) (Usage, error) {
	if a == nil {
		return Usage{}, ErrAuthorityUnavailable
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	u := a.usage[key]
	u.Limit, u.Reserved = limit, reserved
	return u, nil
}

func (a *localAuthority) Acquire(ctx context.Context, key string, limit, reserved int, class requestClass) (Lease, error) {
	if a == nil {
		return Lease{}, ErrAuthorityUnavailable
	}
	if limit < 1 {
		return Lease{}, ErrAuthorityUnavailable
	}
	for {
		a.mu.Lock()
		u := a.usage[key]
		allowed := u.InFlight < limit
		if class == classCold {
			general := limit - reserved
			if general < 1 {
				general = 1
			}
			allowed = u.InFlight < general
		}
		if allowed {
			token := fmt.Sprintf("%x", sha256.Sum256([]byte(key+":"+strconv.FormatInt(time.Now().UnixNano(), 10))))
			lease := Lease{Token: token, Key: key, Class: class}
			a.active[token] = lease
			u.Limit, u.Reserved, u.InFlight = limit, reserved, u.InFlight+1
			if class == classWarm {
				u.WarmFlight++
			}
			a.usage[key] = u
			a.mu.Unlock()
			return lease, nil
		}
		a.mu.Unlock()
		if ctx == nil {
			return Lease{}, context.Canceled
		}
		select {
		case <-ctx.Done():
			return Lease{}, ctx.Err()
		case <-time.After(defaultPollInterval):
		}
	}
}

func (a *localAuthority) Release(_ context.Context, lease Lease) error {
	if a == nil {
		return ErrAuthorityUnavailable
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	stored, ok := a.active[lease.Token]
	if !ok {
		return nil
	}
	delete(a.active, lease.Token)
	u := a.usage[stored.Key]
	if u.InFlight > 0 {
		u.InFlight--
	}
	if stored.Class == classWarm && u.WarmFlight > 0 {
		u.WarmFlight--
	}
	a.usage[stored.Key] = u
	return nil
}

func (a *localAuthority) Renew(_ context.Context, lease Lease) error {
	if a == nil {
		return ErrAuthorityUnavailable
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.active[lease.Token]; !ok {
		return ErrAuthorityUnavailable
	}
	return nil
}

// RedisScriptClient is the narrow interface needed by Redis-compatible authorities.
// Implementations should execute the supplied Lua script atomically (Eval semantics).
type RedisScriptClient interface {
	Eval(context.Context, string, []string, ...any) (any, error)
}

// redisNetClient is a dependency-free Redis RESP2 client for EVAL commands.
// It is intentionally small: the authority only needs atomic scripts and does
// not expose arbitrary Redis operations.
type redisNetClient struct {
	addr, password string
	db             int
}

func (c redisNetClient) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	if c.addr == "" {
		return nil, ErrAuthorityUnavailable
	}
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	r := bufio.NewReader(conn)
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	writeCmd := func(parts ...string) error {
		var b strings.Builder
		b.WriteString("*")
		b.WriteString(strconv.Itoa(len(parts)))
		b.WriteString("\r\n")
		for _, p := range parts {
			b.WriteString("$")
			b.WriteString(strconv.Itoa(len(p)))
			b.WriteString("\r\n")
			b.WriteString(p)
			b.WriteString("\r\n")
		}
		_, err := io.WriteString(conn, b.String())
		return err
	}
	if c.password != "" {
		if err := writeCmd("AUTH", c.password); err != nil {
			return nil, err
		}
		if _, err := readRESP(r); err != nil {
			return nil, err
		}
	}
	if c.db != 0 {
		if err := writeCmd("SELECT", strconv.Itoa(c.db)); err != nil {
			return nil, err
		}
		if _, err := readRESP(r); err != nil {
			return nil, err
		}
	}
	parts := []string{"EVAL", script, strconv.Itoa(len(keys))}
	parts = append(parts, keys...)
	for _, arg := range args {
		parts = append(parts, fmt.Sprint(arg))
	}
	if err := writeCmd(parts...); err != nil {
		return nil, err
	}
	return readRESP(r)
}
func readRESP(r *bufio.Reader) (any, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	switch prefix {
	case '+':
		return line, nil
	case '-':
		return nil, errors.New(line)
	case ':':
		return strconv.ParseInt(line, 10, 64)
	case '$':
		n, e := strconv.Atoi(line)
		if e != nil {
			return nil, e
		}
		if n < 0 {
			return nil, nil
		}
		data := make([]byte, n+2)
		if _, e = io.ReadFull(r, data); e != nil {
			return nil, e
		}
		return string(data[:n]), nil
	case '*':
		n, e := strconv.Atoi(line)
		if e != nil {
			return nil, e
		}
		out := make([]any, n)
		for i := range out {
			out[i], e = readRESP(r)
			if e != nil {
				return nil, e
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported redis response")
	}
}

type redisAuthority struct {
	client RedisScriptClient
	prefix string
}

func newRedisAuthority(client RedisScriptClient, prefix string) *redisAuthority {
	return &redisAuthority{client: client, prefix: prefix}
}

func (a *redisAuthority) key(account string) string {
	return strings.TrimSuffix(a.prefix, ":") + ":" + account
}

func (a *redisAuthority) Snapshot(ctx context.Context, account string, limit, reserved int) (Usage, error) {
	if a == nil || a.client == nil {
		return Usage{}, ErrAuthorityUnavailable
	}
	ctx, cancel := ensureAuthorityContext(ctx)
	defer cancel()
	result, err := a.client.Eval(ctx, redisSnapshotScript, []string{a.key(account)}, time.Now().UnixMilli(), leaseTTL.Milliseconds())
	if err != nil {
		return Usage{}, fmt.Errorf("%w: %v", ErrAuthorityUnavailable, err)
	}
	vals, ok := result.([]any)
	if !ok || len(vals) < 2 {
		return Usage{}, ErrAuthorityUnavailable
	}
	inflight, _ := toInt(vals[0])
	warm, _ := toInt(vals[1])
	if len(vals) >= 3 {
		if fenced, ok := toInt(vals[2]); ok && fenced != 0 {
			return Usage{}, ErrAuthorityUnavailable
		}
	}
	return Usage{Limit: limit, Reserved: reserved, InFlight: inflight, WarmFlight: warm}, nil
}

func (a *redisAuthority) Acquire(ctx context.Context, account string, limit, reserved int, class requestClass) (Lease, error) {
	if a == nil || a.client == nil {
		return Lease{}, ErrAuthorityUnavailable
	}
	ctx, cancel := ensureAuthorityContext(ctx)
	defer cancel()
	token := fmt.Sprintf("%x", sha256.Sum256([]byte(account+":"+strconv.FormatInt(time.Now().UnixNano(), 10))))
	result, err := a.client.Eval(ctx, redisAcquireScript, []string{a.key(account)}, limit, reserved, int(class), token, time.Now().UnixMilli(), leaseTTL.Milliseconds())
	if err != nil {
		return Lease{}, fmt.Errorf("%w: %v", ErrAuthorityUnavailable, err)
	}
	n, ok := toInt(result)
	if !ok {
		return Lease{}, ErrAuthorityUnavailable
	}
	switch n {
	case 0:
		return Lease{}, &AdmissionError{Code: "account_concurrency_limit", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "account concurrency limit reached"}
	case 1:
		return Lease{Token: token, Key: account, Class: class}, nil
	default:
		// redisAcquireScript reserves negative values for fail-closed
		// authority state (currently -1 for a fenced account). Treat every
		// unexpected result as unavailable too; only the explicit success
		// sentinel may create a lease.
		return Lease{}, ErrAuthorityUnavailable
	}
}

func (a *redisAuthority) Release(ctx context.Context, lease Lease) error {
	if a == nil || a.client == nil {
		return ErrAuthorityUnavailable
	}
	ctx, cancel := ensureAuthorityContext(ctx)
	defer cancel()
	if _, err := a.client.Eval(ctx, redisReleaseScript, []string{a.key(lease.Key)}, lease.Token, int(lease.Class), leaseTTL.Milliseconds()); err != nil {
		return fmt.Errorf("%w: %v", ErrAuthorityUnavailable, err)
	}
	return nil
}

func (a *redisAuthority) Renew(ctx context.Context, lease Lease) error {
	if a == nil || a.client == nil {
		return ErrAuthorityUnavailable
	}
	ctx, cancel := ensureAuthorityContext(ctx)
	defer cancel()
	now := time.Now().UnixMilli()
	result, err := a.client.Eval(ctx, redisRenewScript, []string{a.key(lease.Key)}, lease.Token, now, leaseTTL.Milliseconds())
	if err != nil {
		return fmt.Errorf("%w: %v", ErrAuthorityUnavailable, err)
	}
	if n, ok := toInt(result); !ok || n == 0 {
		return ErrAuthorityUnavailable
	}
	return nil
}

// Fence marks an account as unsafe for further admission. The marker lives in
// Redis, so it constrains every CPA instance that shares the authority rather
// than relying on a process-local flag. A fenced account remains unavailable
// until the stale lease is explicitly released (fail-closed recovery).
func (a *redisAuthority) Fence(ctx context.Context, lease Lease) error {
	if a == nil || a.client == nil {
		return ErrAuthorityUnavailable
	}
	ctx, cancel := ensureAuthorityContext(ctx)
	defer cancel()
	if _, err := a.client.Eval(ctx, redisFenceScript, []string{a.key(lease.Key)}, lease.Token); err != nil {
		return fmt.Errorf("%w: %v", ErrAuthorityUnavailable, err)
	}
	return nil
}

func ensureAuthorityContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		return context.WithTimeout(context.Background(), authorityCallTimeout)
	}
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) <= authorityCallTimeout {
			return ctx, func() {}
		}
		return context.WithTimeout(ctx, authorityCallTimeout)
	}
	return context.WithTimeout(ctx, authorityCallTimeout)
}

func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case string:
		i, e := strconv.Atoi(n)
		return i, e == nil
	case []byte:
		i, e := strconv.Atoi(string(n))
		return i, e == nil
	default:
		return 0, false
	}
}

const redisSnapshotScript = `local now=tonumber(ARGV[1] or '0'); local fields=redis.call('HGETALL',KEYS[1]); local i=tonumber(redis.call('HGET',KEYS[1],'inflight') or '0'); local w=tonumber(redis.call('HGET',KEYS[1],'warm') or '0'); local fenced=redis.call('HEXISTS',KEYS[1],'__fenced'); for n=1,#fields,2 do local f=fields[n]; local v=fields[n+1]; if f~='inflight' and f~='warm' and f~='__fenced' then local c,e=string.match(v,'^(%d+):(%d+)$'); if c and tonumber(e)<=now then redis.call('HSET',KEYS[1],'__fenced','1'); fenced=1 end end end; if i<0 then i=0 end; if w<0 then w=0 end; redis.call('HSET',KEYS[1],'inflight',i,'warm',w); if i>0 or fenced==1 then redis.call('PERSIST',KEYS[1]); return {i,w,fenced} else redis.call('DEL',KEYS[1]); return {0,0,0} end`
const redisAcquireScript = `local now=tonumber(ARGV[5]); local ttl=tonumber(ARGV[6]); local fields=redis.call('HGETALL',KEYS[1]); local i=tonumber(redis.call('HGET',KEYS[1],'inflight') or '0'); local w=tonumber(redis.call('HGET',KEYS[1],'warm') or '0'); local fenced=redis.call('HEXISTS',KEYS[1],'__fenced'); for n=1,#fields,2 do local f=fields[n]; local v=fields[n+1]; if f~='inflight' and f~='warm' and f~='__fenced' then local c,e=string.match(v,'^(%d+):(%d+)$'); if c and tonumber(e)<=now then redis.call('HSET',KEYS[1],'__fenced','1'); fenced=1 end end end; if fenced==1 then redis.call('PERSIST',KEYS[1]); return -1 end; if i<0 then i=0 end; if w<0 then w=0 end; local limit=tonumber(ARGV[1]); local reserved=tonumber(ARGV[2]); local class=tonumber(ARGV[3]); local allowed=limit; if class==0 then allowed=limit-reserved; if allowed<1 then allowed=1 end end; if i>=allowed then redis.call('HSET',KEYS[1],'inflight',i,'warm',w); redis.call('PERSIST',KEYS[1]); return 0 end; i=i+1; if class==1 then w=w+1 end; redis.call('HSET',KEYS[1],'inflight',i,'warm',w,ARGV[4],class..':'..(now+ttl)); redis.call('PERSIST',KEYS[1]); return 1`
const redisReleaseScript = `local v=redis.call('HGET',KEYS[1],ARGV[1]); if not v then return 0 end; local c=string.match(v,'^(%d+):'); if not c then c=v end; redis.call('HDEL',KEYS[1],ARGV[1]); local i=tonumber(redis.call('HGET',KEYS[1],'inflight') or '0')-1; local w=tonumber(redis.call('HGET',KEYS[1],'warm') or '0'); if tonumber(c)==1 then w=w-1 end; if i<=0 then redis.call('DEL',KEYS[1]); else if w<0 then w=0 end; redis.call('HSET',KEYS[1],'inflight',i,'warm',w); end; return 1`
const redisRenewScript = `local v=redis.call('HGET',KEYS[1],ARGV[1]); if not v then return 0 end; local c=string.match(v,'^(%d+):'); if not c then return 0 end; local now=tonumber(ARGV[2]); local ttl=tonumber(ARGV[3]); local _,e=string.match(v,'^(%d+):(%d+)$'); if not e or tonumber(e)<=now then return 0 end; redis.call('HSET',KEYS[1],ARGV[1],c..':'..(now+ttl)); redis.call('PERSIST',KEYS[1]); return 1`
const redisFenceScript = `if redis.call('HEXISTS',KEYS[1],ARGV[1])==1 then redis.call('HSET',KEYS[1],'__fenced','1'); return 1 end; return 0`

type candidateScore struct {
	id       string
	inFlight int
	index    int
}

func chooseCandidate(ctx context.Context, authority Authority, candidates []string, limit, reserved int, class requestClass) (string, error) {
	if authority == nil {
		return "", ErrAuthorityUnavailable
	}
	var scores []candidateScore
	for i, id := range candidates {
		u, err := authority.Snapshot(ctx, accountKey("cpa", id), limit, reserved)
		if err != nil {
			return "", err
		}
		general := limit - reserved
		if general < 1 {
			general = 1
		}
		if class == classCold && u.InFlight >= general {
			continue
		}
		if class == classWarm && u.InFlight >= limit {
			continue
		}
		scores = append(scores, candidateScore{id: id, inFlight: u.InFlight, index: i})
	}
	if len(scores) == 0 {
		return "", &AdmissionError{Code: "account_concurrency_limit", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "account concurrency limit reached"}
	}
	sort.SliceStable(scores, func(i, j int) bool {
		if scores[i].inFlight != scores[j].inFlight {
			return scores[i].inFlight < scores[j].inFlight
		}
		return scores[i].index < scores[j].index
	})
	return scores[0].id, nil
}

func accountKey(namespace, authID string) string {
	sum := sha256.Sum256([]byte(namespace + "\x00" + authID))
	return hex.EncodeToString(sum[:])
}

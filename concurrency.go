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

type Authority interface {
	Snapshot(context.Context, string, int, int) (Usage, error)
	Acquire(context.Context, string, int, int, requestClass) (Lease, error)
	Release(context.Context, Lease) error
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
	result, err := a.client.Eval(ctx, redisSnapshotScript, []string{a.key(account)})
	if err != nil {
		return Usage{}, fmt.Errorf("%w: %v", ErrAuthorityUnavailable, err)
	}
	vals, ok := result.([]any)
	if !ok || len(vals) < 2 {
		return Usage{}, ErrAuthorityUnavailable
	}
	inflight, _ := toInt(vals[0])
	warm, _ := toInt(vals[1])
	return Usage{Limit: limit, Reserved: reserved, InFlight: inflight, WarmFlight: warm}, nil
}

func (a *redisAuthority) Acquire(ctx context.Context, account string, limit, reserved int, class requestClass) (Lease, error) {
	if a == nil || a.client == nil {
		return Lease{}, ErrAuthorityUnavailable
	}
	token := fmt.Sprintf("%x", sha256.Sum256([]byte(account+":"+strconv.FormatInt(time.Now().UnixNano(), 10))))
	result, err := a.client.Eval(ctx, redisAcquireScript, []string{a.key(account)}, limit, reserved, int(class), token)
	if err != nil {
		return Lease{}, fmt.Errorf("%w: %v", ErrAuthorityUnavailable, err)
	}
	if n, ok := toInt(result); !ok {
		return Lease{}, ErrAuthorityUnavailable
	} else if n == 0 {
		return Lease{}, &AdmissionError{Code: "account_concurrency_limit", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "account concurrency limit reached"}
	}
	return Lease{Token: token, Key: account, Class: class}, nil
}

func (a *redisAuthority) Release(ctx context.Context, lease Lease) error {
	if a == nil || a.client == nil {
		return ErrAuthorityUnavailable
	}
	if _, err := a.client.Eval(ctx, redisReleaseScript, []string{a.key(lease.Key)}, lease.Token, int(lease.Class)); err != nil {
		return fmt.Errorf("%w: %v", ErrAuthorityUnavailable, err)
	}
	return nil
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

const redisSnapshotScript = `local i=tonumber(redis.call('HGET',KEYS[1],'inflight') or '0'); local w=tonumber(redis.call('HGET',KEYS[1],'warm') or '0'); return {i,w}`
const redisAcquireScript = `local i=tonumber(redis.call('HGET',KEYS[1],'inflight') or '0'); local limit=tonumber(ARGV[1]); local reserved=tonumber(ARGV[2]); local class=tonumber(ARGV[3]); local allowed=limit; if class==0 then allowed=limit-reserved; if allowed<1 then allowed=1 end end; if i>=allowed then return 0 end; redis.call('HINCRBY',KEYS[1],'inflight',1); if class==1 then redis.call('HINCRBY',KEYS[1],'warm',1) end; redis.call('HSET',KEYS[1],ARGV[4],class); return 1`
const redisReleaseScript = `local class=redis.call('HGET',KEYS[1],ARGV[1]); if not class then return 0 end; redis.call('HDEL',KEYS[1],ARGV[1]); redis.call('HINCRBY',KEYS[1],'inflight',-1); if tonumber(class)==1 then redis.call('HINCRBY',KEYS[1],'warm',-1) end; return 1`

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

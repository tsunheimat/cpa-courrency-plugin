package main

/*
#include <stdint.h>
#include <stdlib.h>
typedef struct { void* ptr; size_t len; } cliproxy_buffer;
typedef struct { uint32_t abi_version; void* host_ctx; void* call; void* free_buffer; } cliproxy_host_api;
typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);
typedef struct { uint32_t abi_version; cliproxy_plugin_call_fn call; cliproxy_plugin_free_fn free_buffer; cliproxy_plugin_shutdown_fn shutdown; } cliproxy_plugin_api;
extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"gopkg.in/yaml.v3"
)

const pluginID = "cpa-account-concurrency"

// authorityCallTimeout bounds release/renew/snapshot calls that are not
// already covered by the request admission wait timeout.  A lost authority
// must never leave a callback blocked forever.
const authorityCallTimeout = 2 * time.Second

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}
type envelopeError struct {
	Code       string `json:"code"`
	Class      string `json:"class,omitempty"`
	Message    string `json:"message"`
	Retryable  bool   `json:"retryable,omitempty"`
	HTTPStatus int    `json:"http_status,omitempty"`
	RetryAfter int    `json:"retry_after,omitempty"`
}
type lifecycleRequest struct {
	ConfigYAML    []byte `json:"config_yaml"`
	SchemaVersion uint32 `json:"schema_version"`
}

type quiesceRequest struct {
	DeadlineUnixNano int64 `json:"deadline_unix_nano,omitempty"`
}
type registration struct {
	SchemaVersion uint32                   `json:"schema_version"`
	Metadata      pluginapi.Metadata       `json:"metadata"`
	Capabilities  registrationCapabilities `json:"capabilities"`
}
type registrationCapabilities struct {
	Scheduler                           bool `json:"scheduler"`
	RequestInterceptor                  bool `json:"request_interceptor"`
	RequestInterceptorEnforcesAdmission bool `json:"request_interceptor_enforces_admission"`
	RequestLifecyclePlugin              bool `json:"request_lifecycle_plugin"`
}

type pluginConfig struct {
	Enabled           bool          `yaml:"enabled"`
	MaxConcurrency    int           `yaml:"max_concurrency"`
	WarmReservedSlots int           `yaml:"warm_reserved_slots"`
	WaitTimeout       time.Duration `yaml:"wait_timeout"`
	Authority         string        `yaml:"authority"`
	RedisPrefix       string        `yaml:"redis_prefix"`
	RedisAddr         string        `yaml:"redis_addr"`
	RedisPassword     string        `yaml:"redis_password"`
	RedisDB           int           `yaml:"redis_db"`
}

type pluginState struct {
	mu          sync.Mutex
	gate        sync.RWMutex // serializes shutdown/reconfigure against callbacks
	cfg         pluginConfig
	authority   Authority
	leases      map[string]Lease
	bound       map[string]string
	requests    map[string]*requestLifecycle
	stopping    bool
	uncertain   bool
	reloadFence bool
}

type requestLifecycle struct {
	mu        sync.Mutex
	terminal  bool
	fenced    bool // lease renewal failed; do not admit another request
	completed bool
	lease     Lease
	bound     string
	stopBeat  chan struct{}
}

var localAuthorityShared = newLocalAuthority()
var state = pluginState{cfg: defaultConfig(), authority: localAuthorityShared, leases: make(map[string]Lease), bound: make(map[string]string), requests: make(map[string]*requestLifecycle)}

func defaultConfig() pluginConfig {
	return pluginConfig{Enabled: true, MaxConcurrency: defaultLimit, WarmReservedSlots: 0, WaitTimeout: defaultWait, Authority: "local", RedisPrefix: "cpa:concurrency"}
}

func main() {}

//export cliproxy_plugin_init
func cliproxy_plugin_init(_ *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if plugin == nil {
		return 1
	}
	plugin.abi_version = C.uint32_t(pluginabi.ABIVersion)
	plugin.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	plugin.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	plugin.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response != nil {
		response.ptr = nil
		response.len = 0
	}
	if method == nil {
		writeResponse(response, errorEnvelope("invalid_method", "method is required"))
		return 1
	}
	var raw []byte
	if request != nil && requestLen > 0 {
		raw = C.GoBytes(unsafe.Pointer(request), C.int(requestLen))
	}
	out, err := handleMethod(C.GoString(method), raw)
	if err != nil {
		var admission *AdmissionError
		if errors.As(err, &admission) {
			writeResponse(response, typedErrorEnvelope(admission.Code, admission.Message, admission.Retryable(), admission.StatusCode(), admission.RetryAfter))
		} else {
			writeResponse(response, errorEnvelope("plugin_error", err.Error()))
		}
		return 1
	}
	writeResponse(response, out)
	return 0
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, _ C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() {
	state.gate.Lock()
	defer state.gate.Unlock()
	state.mu.Lock()
	leases := make([]Lease, 0, len(state.leases))
	for _, lease := range state.leases {
		leases = append(leases, lease)
	}
	authority := state.authority
	state.stopping = true
	if len(leases) > 0 {
		state.uncertain = true
		state.reloadFence = true
	}
	requests := make([]*requestLifecycle, 0, len(state.requests))
	for _, rs := range state.requests {
		requests = append(requests, rs)
	}
	state.mu.Unlock()
	for _, rs := range requests {
		rs.mu.Lock()
		if rs.stopBeat != nil {
			close(rs.stopBeat)
			rs.stopBeat = nil
		}
		rs.mu.Unlock()
	}
	for _, lease := range leases {
		if authority != nil {
			ctx, cancel := boundedAuthorityContext()
			_ = authority.Release(ctx, lease)
			cancel()
		}
	}
	// Authority release above is the single ownership transition. Keep request
	// records so late completion callbacks can clear the reload fence, but do
	// not ask the authority to release the same token a second time.
	for _, rs := range requests {
		rs.mu.Lock()
		rs.lease = Lease{}
		rs.bound = ""
		rs.mu.Unlock()
	}
}

func handleMethod(method string, raw []byte) ([]byte, error) {
	switch method {
	case pluginabi.MethodPluginRegister, pluginabi.MethodPluginReconfigure:
		if err := configure(raw); err != nil {
			return nil, err
		}
		return okEnvelope(pluginRegistration())
	case pluginabi.MethodPluginQuiesce:
		if err := quiesce(raw); err != nil {
			return nil, err
		}
		return okEnvelope(struct{}{})
	case pluginabi.MethodSchedulerPick:
		return schedulerPick(raw)
	case pluginabi.MethodRequestInterceptBefore:
		return interceptBefore(raw)
	case pluginabi.MethodRequestInterceptAfter:
		return interceptAfter(raw)
	case pluginabi.MethodRequestComplete:
		return complete(raw)
	default:
		return errorEnvelope("unknown_method", "unknown method: "+method), nil
	}
}

// quiesce fences new admission on this instance and waits for every lease it
// owns to complete. It intentionally does not take state.gate: completion
// callbacks must remain able to drain existing leases while new callbacks see
// state.stopping and fail closed. A timeout leaves the instance fenced and
// authoritative; the host must not replace it.
func quiesce(raw []byte) error {
	deadline := time.Now().Add(authorityCallTimeout)
	if len(raw) > 0 {
		var req quiesceRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			return err
		}
		if req.DeadlineUnixNano > 0 {
			deadline = time.Unix(0, req.DeadlineUnixNano)
		}
	}
	state.mu.Lock()
	state.stopping = true
	if len(state.leases) > 0 {
		state.reloadFence = true
	}
	state.mu.Unlock()
	for {
		state.mu.Lock()
		remaining := len(state.leases)
		state.mu.Unlock()
		if remaining == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			state.mu.Lock()
			state.reloadFence = true
			state.mu.Unlock()
			return context.DeadlineExceeded
		}
		time.Sleep(defaultPollInterval)
	}
}

func configure(raw []byte) error {
	state.gate.Lock()
	defer state.gate.Unlock()
	var req lifecycleRequest
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			return err
		}
	}
	if req.SchemaVersion != 0 && req.SchemaVersion < 2 {
		return fmt.Errorf("CPA schema version 2 or newer is required")
	}
	cfg := defaultConfig()
	if len(req.ConfigYAML) > 0 {
		if err := yaml.Unmarshal(req.ConfigYAML, &cfg); err != nil {
			return err
		}
	}
	if cfg.MaxConcurrency < 1 {
		return fmt.Errorf("max_concurrency must be greater than zero")
	}
	if cfg.WarmReservedSlots < 0 {
		return fmt.Errorf("warm_reserved_slots must not be negative")
	}
	if cfg.MaxConcurrency <= 1 {
		cfg.WarmReservedSlots = 0
	} else if cfg.WarmReservedSlots == 0 {
		cfg.WarmReservedSlots = minInt(cfg.MaxConcurrency-1, maxInt(1, (cfg.MaxConcurrency+4)/5))
	}
	if cfg.WarmReservedSlots >= cfg.MaxConcurrency {
		cfg.WarmReservedSlots = cfg.MaxConcurrency - 1
	}
	if cfg.WaitTimeout < 0 {
		return fmt.Errorf("wait_timeout must not be negative")
	}
	if cfg.WaitTimeout == 0 {
		cfg.WaitTimeout = defaultWait
	}
	cfg.Authority = strings.ToLower(strings.TrimSpace(cfg.Authority))
	if cfg.Authority == "" {
		cfg.Authority = "local"
	}
	if cfg.Authority != "local" && cfg.Authority != "redis" {
		return fmt.Errorf("authority must be local or redis")
	}
	if cfg.Authority == "redis" && strings.TrimSpace(cfg.RedisAddr) == "" {
		return fmt.Errorf("redis_addr is required when authority is redis")
	}
	state.mu.Lock()
	state.stopping = false
	// Do not clear an uncertainty while leases remain.  In particular, a
	// renewal failure may have left a running request without a fenced lease;
	// admitting new work before that request completes would permit oversell.
	if len(state.leases) == 0 {
		state.uncertain = false
	}
	if len(state.leases) > 0 && state.cfg.Authority != "" && state.cfg.Authority != cfg.Authority {
		state.mu.Unlock()
		return fmt.Errorf("cannot change concurrency authority while requests are in flight")
	}
	previousAuthority := state.cfg.Authority
	materialRedisChange := previousAuthority == "redis" && cfg.Authority == "redis" &&
		(state.cfg.RedisAddr != cfg.RedisAddr || state.cfg.RedisPassword != cfg.RedisPassword || state.cfg.RedisDB != cfg.RedisDB || state.cfg.RedisPrefix != cfg.RedisPrefix)
	if materialRedisChange && len(state.leases) > 0 {
		state.mu.Unlock()
		return fmt.Errorf("cannot change Redis authority configuration while requests are in flight")
	}
	state.cfg = cfg
	if cfg.Authority == "local" {
		if previousAuthority != "local" || state.authority == nil {
			state.authority = localAuthorityShared
		}
	} else if previousAuthority != "redis" || state.authority == nil || materialRedisChange {
		state.authority = newRedisAuthority(redisNetClient{addr: cfg.RedisAddr, password: cfg.RedisPassword, db: cfg.RedisDB}, cfg.RedisPrefix)
	}
	state.mu.Unlock()
	return nil
}

func pluginRegistration() registration {
	return registration{SchemaVersion: pluginabi.SchemaVersion, Metadata: pluginapi.Metadata{Name: pluginID, Version: "0.1.0", Author: "CPA concurrency plugin", GitHubRepository: "https://github.com/router-for-me/CLIProxyAPI", ConfigFields: []pluginapi.ConfigField{
		{Name: "max_concurrency", Type: pluginapi.ConfigFieldTypeInteger, Description: "Hard per-account in-flight limit."},
		{Name: "warm_reserved_slots", Type: pluginapi.ConfigFieldTypeInteger, Description: "Reserved slots for verified warm/strict affinity."},
		{Name: "wait_timeout", Type: pluginapi.ConfigFieldTypeString, Description: "Bounded admission wait (Go duration, for example 50ms)."},
		{Name: "authority", Type: pluginapi.ConfigFieldTypeEnum, EnumValues: []string{"local", "redis"}, Description: "Lease authority; Redis requires redis_addr."},
		{Name: "redis_prefix", Type: pluginapi.ConfigFieldTypeString, Description: "Redis key prefix; account identities are hashed."},
		{Name: "redis_addr", Type: pluginapi.ConfigFieldTypeString, Description: "Redis address when authority is redis."},
		{Name: "redis_password", Type: pluginapi.ConfigFieldTypeString, Description: "Redis password when authority is redis."},
		{Name: "redis_db", Type: pluginapi.ConfigFieldTypeInteger, Description: "Redis database number."},
	}}, Capabilities: registrationCapabilities{Scheduler: true, RequestInterceptor: true, RequestInterceptorEnforcesAdmission: true, RequestLifecyclePlugin: true}}
}

func schedulerPick(raw []byte) ([]byte, error) {
	state.gate.RLock()
	defer state.gate.RUnlock()
	var req pluginapi.SchedulerPickRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	state.mu.Lock()
	cfg, authority, stopping, uncertain := state.cfg, state.authority, state.stopping, state.uncertain
	state.mu.Unlock()
	if !cfg.Enabled {
		return okEnvelope(pluginapi.SchedulerPickResponse{Handled: false})
	}
	if authority == nil || stopping || uncertain {
		return nil, &AdmissionError{Code: "account_concurrency_authority_unavailable", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "concurrency authority unavailable", Authority: true}
	}
	ids := make([]string, 0, len(req.Candidates))
	seen := make(map[string]struct{}, len(req.Candidates))
	for _, c := range req.Candidates {
		if id := canonicalAuthID(c.ID); id != "" {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil, &AdmissionError{Code: "account_concurrency_limit", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "no protected account is available"}
	}
	hint, strict, warm := affinityHint(req.Options.Metadata)
	if strict && hint != "" {
		for _, id := range ids {
			if id == hint {
				ctx, cancel := boundedAuthorityContext()
				u, err := authority.Snapshot(ctx, accountKey("cpa", id), cfg.MaxConcurrency, cfg.WarmReservedSlots)
				cancel()
				if err != nil {
					return nil, authorityError(err)
				}
				if u.InFlight >= cfg.MaxConcurrency {
					return nil, &AdmissionError{Code: "account_concurrency_limit", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "account concurrency limit reached"}
				}
				return okEnvelope(pluginapi.SchedulerPickResponse{Handled: true, AuthID: id})
			}
		}
	}
	if warm {
		ids = preferHint(ids, hint)
	}
	ctx, cancel := boundedAuthorityContext()
	defer cancel()
	selected, err := chooseCandidate(ctx, authority, ids, cfg.MaxConcurrency, cfg.WarmReservedSlots, func() requestClass {
		if warm {
			return classWarm
		}
		return classCold
	}())
	if err != nil {
		return nil, authorityError(err)
	}
	return okEnvelope(pluginapi.SchedulerPickResponse{Handled: true, AuthID: selected})
}

func authorityError(err error) error {
	if err == nil {
		return nil
	}
	var ae *AdmissionError
	if errors.As(err, &ae) {
		return err
	}
	markAuthorityUncertain()
	return &AdmissionError{Code: "account_concurrency_authority_unavailable", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "concurrency authority unavailable", Authority: true}
}

func interceptBefore(raw []byte) ([]byte, error) {
	var req pluginapi.RequestInterceptRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	return okEnvelope(pluginapi.RequestInterceptResponse{Headers: req.Headers, Body: req.Body})
}

func interceptAfter(raw []byte) ([]byte, error) {
	var req pluginapi.RequestInterceptRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	req.RequestID = strings.TrimSpace(req.RequestID)
	req.AuthID = canonicalAuthID(req.AuthID)
	if req.RequestID == "" {
		return admissionResponse(&AdmissionError{Code: "invalid_request_id", HTTPStatus: http.StatusBadRequest, Message: "request_id is required"})
	}
	state.gate.RLock()
	defer state.gate.RUnlock()
	state.mu.Lock()
	cfg, authority := state.cfg, state.authority
	if state.stopping || state.uncertain {
		state.mu.Unlock()
		return admissionResponse(&AdmissionError{Code: "account_concurrency_authority_unavailable", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "concurrency authority unavailable", Authority: true})
	}
	rs := state.requests[req.RequestID]
	if rs == nil {
		rs = &requestLifecycle{}
		state.requests[req.RequestID] = rs
	}
	state.mu.Unlock()
	if !cfg.Enabled || req.AuthID == "" {
		return okEnvelope(pluginapi.RequestInterceptResponse{Headers: req.Headers, Body: req.Body})
	}
	class := classCold
	hint, _, warm := affinityHint(req.Metadata)
	// A verified binding reserves warm capacity only while executing on the
	// bound original auth. Retries/failovers selected onto another auth are
	// cold and must use general capacity.
	if warm && hint != "" && hint != req.AuthID {
		warm = false
	}
	if warm {
		class = classWarm
	}
	key := accountKey("cpa", req.AuthID)
	if authority == nil {
		return admissionResponse(&AdmissionError{Code: "account_concurrency_authority_unavailable", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "concurrency authority unavailable", Authority: true})
	}
	ctx := context.Background()
	if cfg.WaitTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.WaitTimeout)
		defer cancel()
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.fenced {
		return admissionResponse(&AdmissionError{Code: "account_concurrency_authority_unavailable", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "concurrency authority unavailable", Authority: true})
	}
	if rs.terminal {
		return okEnvelope(pluginapi.RequestInterceptResponse{Headers: req.Headers, Body: req.Body})
	}
	old, bound := rs.lease, rs.bound
	if bound == req.AuthID && old.Token != "" {
		return okEnvelope(pluginapi.RequestInterceptResponse{Headers: req.Headers, Body: req.Body})
	}
	if old.Token != "" {
		ctxRelease, cancelRelease := boundedAuthorityContext()
		errRelease := authority.Release(ctxRelease, old)
		cancelRelease()
		if errRelease != nil {
			markAuthorityUncertain()
			return admissionResponse(&AdmissionError{Code: "account_concurrency_authority_unavailable", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "concurrency authority unavailable", Authority: true})
		}
		state.mu.Lock()
		delete(state.leases, req.RequestID)
		delete(state.bound, req.RequestID)
		state.mu.Unlock()
		rs.lease = Lease{}
		rs.bound = ""
		stopHeartbeat(rs)
	}
	lease, err := authority.Acquire(ctx, key, cfg.MaxConcurrency, cfg.WarmReservedSlots, class)
	if err != nil {
		var capacity *AdmissionError
		isCapacity := errors.As(err, &capacity) && capacity.Code == "account_concurrency_limit"
		if !isCapacity && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			markAuthorityUncertain()
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			err = &AdmissionError{Code: "account_concurrency_limit", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "account concurrency limit reached"}
		}
		var ae *AdmissionError
		if !errors.As(err, &ae) {
			ae = &AdmissionError{Code: "account_concurrency_authority_unavailable", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "concurrency authority unavailable", Authority: true}
		}
		return admissionResponse(ae)
	}
	state.mu.Lock()
	state.leases[req.RequestID] = lease
	state.bound[req.RequestID] = req.AuthID
	state.mu.Unlock()
	rs.lease, rs.bound = lease, req.AuthID
	startHeartbeat(rs, authority, lease)
	return okEnvelope(pluginapi.RequestInterceptResponse{Headers: req.Headers, Body: req.Body})
}

func complete(raw []byte) ([]byte, error) {
	var req pluginapi.RequestCompletion
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	req.RequestID = strings.TrimSpace(req.RequestID)
	if req.RequestID == "" {
		return okEnvelope(struct{}{})
	}
	state.gate.RLock()
	defer state.gate.RUnlock()
	state.mu.Lock()
	rs := state.requests[req.RequestID]
	if rs == nil {
		rs = &requestLifecycle{}
		state.requests[req.RequestID] = rs
	}
	state.mu.Unlock()
	rs.mu.Lock()
	if rs.completed {
		rs.mu.Unlock()
		return okEnvelope(struct{}{})
	}
	rs.terminal = true
	rs.completed = true
	lease := rs.lease
	ok := lease.Token != ""
	stopHeartbeat(rs)
	state.mu.Lock()
	authority := state.authority
	state.mu.Unlock()
	// Keep the lease visible to quiesce until its authority release succeeds.
	// Otherwise quiesce could report a safe drain while a slow or failed Redis
	// release still leaves the retiring instance authoritative.
	if ok && authority != nil {
		ctx, cancel := boundedAuthorityContext()
		err := authority.Release(ctx, lease)
		cancel()
		if err != nil {
			markAuthorityUncertain()
			rs.fenced = true
			rs.mu.Unlock()
			return okEnvelope(struct{}{})
		}
	}
	rs.lease = Lease{}
	rs.bound = ""
	state.mu.Lock()
	delete(state.leases, req.RequestID)
	delete(state.bound, req.RequestID)
	if len(state.leases) == 0 && state.reloadFence {
		state.uncertain = false
		state.reloadFence = false
	}
	state.mu.Unlock()
	rs.mu.Unlock()
	return okEnvelope(struct{}{})
}

func markAuthorityUncertain() {
	state.mu.Lock()
	state.uncertain = true
	state.mu.Unlock()
}

func startHeartbeat(rs *requestLifecycle, authority Authority, lease Lease) {
	startHeartbeatWithInterval(rs, authority, lease, leaseRenewInterval)
}

func startHeartbeatWithInterval(rs *requestLifecycle, authority Authority, lease Lease, interval time.Duration) {
	if authority == nil || lease.Token == "" {
		return
	}
	stopHeartbeat(rs)
	stop := make(chan struct{})
	rs.stopBeat = stop
	go func() {
		if interval <= 0 {
			interval = leaseRenewInterval
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				rs.mu.Lock()
				if rs.terminal || rs.lease.Token != lease.Token {
					rs.mu.Unlock()
					return
				}
				ctx, cancel := boundedAuthorityContext()
				err := authority.Renew(ctx, lease)
				cancel()
				if err != nil {
					// Best-effort distributed fencing. Redis authorities persist this
					// marker so another instance cannot acquire the account after the
					// lease expires. If the partition also prevents Fence, the Redis
					// acquire script fences on observing expiry.
					if fencer, ok := authority.(interface {
						Fence(context.Context, Lease) error
					}); ok {
						ctxFence, cancelFence := boundedAuthorityContext()
						_ = fencer.Fence(ctxFence, lease)
						cancelFence()
					}
					rs.terminal = true
					rs.fenced = true
					rs.mu.Unlock()
					// Renewal uncertainty fences this request and fails closed for all
					// future admissions. Reconfigure cannot clear uncertainty while the
					// lease remains in state.leases.
					markAuthorityUncertain()
					return
				}
				rs.mu.Unlock()
			case <-stop:
				return
			}
		}
	}()
}

func stopHeartbeat(rs *requestLifecycle) {
	if rs.stopBeat != nil {
		close(rs.stopBeat)
		rs.stopBeat = nil
	}
}

func admissionResponse(err *AdmissionError) ([]byte, error) {
	if err == nil {
		err = &AdmissionError{Code: "account_concurrency_authority_unavailable", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "concurrency authority unavailable", Authority: true}
	}
	body, _ := json.Marshal(map[string]any{"error": map[string]any{"type": err.Code, "code": err.Code, "message": err.Message, "retryable": err.Retryable()}})
	status := err.HTTPStatus
	if status == 0 {
		status = http.StatusServiceUnavailable
	}
	headers := http.Header{"Content-Type": []string{"application/json"}}
	if err.RetryAfter > 0 {
		headers.Set("Retry-After", strconv.Itoa(err.RetryAfter))
	}
	return okEnvelope(pluginapi.RequestInterceptResponse{Terminate: true, StatusCode: status, ResponseHeaders: headers, ResponseBody: body})
}

func boundedAuthorityContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), authorityCallTimeout)
}

// Account IDs are canonicalized by trimming surrounding whitespace only.
// Case and all interior characters remain significant; distinct IDs therefore
// cannot silently alias except for this documented whitespace normalization.
func canonicalAuthID(id string) string { return strings.TrimSpace(id) }

func affinityHint(metadata map[string]any) (hint string, strict, warm bool) {
	if metadata == nil {
		return
	}
	// Explicit pins/session bindings are warm/strict. selected_auth_id is only
	// scheduler selection state and is deliberately never sufficient for warm
	// classification (CPA publishes it on every retry).
	explicit := false
	for _, key := range []string{"pinned_auth_id", "session_auth_id", "affinity_auth_id"} {
		if v, ok := metadata[key].(string); ok && canonicalAuthID(v) != "" {
			hint = canonicalAuthID(v)
			strict, warm = true, true
			explicit = true
			break
		}
	}
	if hint == "" {
		if v, ok := metadata["selected_auth_id"].(string); ok {
			hint = canonicalAuthID(v)
		}
	}
	if !explicit {
		if v, ok := metadata["cache_auth_id"].(string); ok && canonicalAuthID(v) != "" {
			if verified, _ := metadata["cache_verified"].(bool); verified {
				hint = canonicalAuthID(v)
				warm, strict = true, true
			}
		}
	}
	return
}
func preferHint(ids []string, hint string) []string {
	if hint == "" {
		return ids
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == hint {
			out = append(out, id)
		}
	}
	for _, id := range ids {
		if id != hint {
			out = append(out, id)
		}
	}
	return out
}
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func okEnvelope(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope{OK: true, Result: raw})
}
func errorEnvelope(code, msg string) []byte {
	return typedErrorEnvelope(code, msg, true, http.StatusServiceUnavailable)
}

func typedErrorEnvelope(code, msg string, retryable bool, status int, retryAfter ...int) []byte {
	value := 0
	if len(retryAfter) > 0 {
		value = retryAfter[0]
	}
	raw, _ := json.Marshal(envelope{OK: false, Error: &envelopeError{Code: code, Class: code, Message: msg, Retryable: retryable, HTTPStatus: status, RetryAfter: value}})
	return raw
}
func writeResponse(response *C.cliproxy_buffer, raw []byte) {
	if response == nil || len(raw) == 0 {
		return
	}
	ptr := C.CBytes(raw)
	if ptr == nil {
		return
	}
	response.ptr = ptr
	response.len = C.size_t(len(raw))
}

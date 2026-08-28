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
}
type lifecycleRequest struct {
	ConfigYAML    []byte `json:"config_yaml"`
	SchemaVersion uint32 `json:"schema_version"`
}
type registration struct {
	SchemaVersion uint32                   `json:"schema_version"`
	Metadata      pluginapi.Metadata       `json:"metadata"`
	Capabilities  registrationCapabilities `json:"capabilities"`
}
type registrationCapabilities struct {
	Scheduler              bool `json:"scheduler"`
	RequestInterceptor     bool `json:"request_interceptor"`
	RequestLifecyclePlugin bool `json:"request_lifecycle_plugin"`
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
	mu        sync.Mutex
	cfg       pluginConfig
	authority Authority
	leases    map[string]Lease
	bound     map[string]string
}

var state = pluginState{cfg: defaultConfig(), authority: newLocalAuthority(), leases: make(map[string]Lease), bound: make(map[string]string)}

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
			writeResponse(response, typedErrorEnvelope(admission.Code, admission.Message, admission.Retryable(), admission.StatusCode()))
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
	state.mu.Lock()
	leases := make([]Lease, 0, len(state.leases))
	for _, lease := range state.leases {
		leases = append(leases, lease)
	}
	authority := state.authority
	state.leases = make(map[string]Lease)
	state.bound = make(map[string]string)
	state.mu.Unlock()
	for _, lease := range leases {
		if authority != nil {
			_ = authority.Release(context.Background(), lease)
		}
	}
}

func handleMethod(method string, raw []byte) ([]byte, error) {
	switch method {
	case pluginabi.MethodPluginRegister, pluginabi.MethodPluginReconfigure:
		if err := configure(raw); err != nil {
			return nil, err
		}
		return okEnvelope(pluginRegistration())
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

func configure(raw []byte) error {
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
	if len(state.leases) > 0 && state.cfg.Authority != "" && state.cfg.Authority != cfg.Authority {
		state.mu.Unlock()
		return fmt.Errorf("cannot change concurrency authority while requests are in flight")
	}
	previousAuthority := state.cfg.Authority
	state.cfg = cfg
	if cfg.Authority == "local" {
		if previousAuthority != "local" || state.authority == nil {
			state.authority = newLocalAuthority()
		}
	} else if previousAuthority != "redis" || state.authority == nil {
		state.authority = newRedisAuthority(redisNetClient{addr: cfg.RedisAddr, password: cfg.RedisPassword, db: cfg.RedisDB}, cfg.RedisPrefix)
	}
	state.mu.Unlock()
	return nil
}

func pluginRegistration() registration {
	return registration{SchemaVersion: pluginabi.SchemaVersion, Metadata: pluginapi.Metadata{Name: pluginID, Version: "0.1.0", Author: "CPA concurrency plugin", ConfigFields: []pluginapi.ConfigField{
		{Name: "max_concurrency", Type: pluginapi.ConfigFieldTypeInteger, Description: "Hard per-account in-flight limit."},
		{Name: "warm_reserved_slots", Type: pluginapi.ConfigFieldTypeInteger, Description: "Reserved slots for verified warm/strict affinity."},
		{Name: "wait_timeout", Type: pluginapi.ConfigFieldTypeString, Description: "Bounded admission wait (Go duration, for example 50ms)."},
		{Name: "authority", Type: pluginapi.ConfigFieldTypeEnum, EnumValues: []string{"local", "redis"}, Description: "Lease authority; Redis requires redis_addr."},
		{Name: "redis_prefix", Type: pluginapi.ConfigFieldTypeString, Description: "Redis key prefix; account identities are hashed."},
		{Name: "redis_addr", Type: pluginapi.ConfigFieldTypeString, Description: "Redis address when authority is redis."},
		{Name: "redis_password", Type: pluginapi.ConfigFieldTypeString, Description: "Redis password when authority is redis."},
		{Name: "redis_db", Type: pluginapi.ConfigFieldTypeInteger, Description: "Redis database number."},
	}}, Capabilities: registrationCapabilities{Scheduler: true, RequestInterceptor: true, RequestLifecyclePlugin: true}}
}

func schedulerPick(raw []byte) ([]byte, error) {
	var req pluginapi.SchedulerPickRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	state.mu.Lock()
	cfg, authority := state.cfg, state.authority
	state.mu.Unlock()
	if !cfg.Enabled {
		return okEnvelope(pluginapi.SchedulerPickResponse{Handled: false})
	}
	if authority == nil {
		return nil, &AdmissionError{Code: "account_concurrency_authority_unavailable", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "concurrency authority unavailable", Authority: true}
	}
	ids := make([]string, 0, len(req.Candidates))
	for _, c := range req.Candidates {
		if strings.TrimSpace(c.ID) != "" {
			ids = append(ids, c.ID)
		}
	}
	if len(ids) == 0 {
		return nil, &AdmissionError{Code: "account_concurrency_limit", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "no protected account is available"}
	}
	hint, strict, warm := affinityHint(req.Options.Metadata)
	if strict && hint != "" {
		for _, id := range ids {
			if id == hint {
				u, err := authority.Snapshot(context.Background(), accountKey("cpa", id), cfg.MaxConcurrency, cfg.WarmReservedSlots)
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
	selected, err := chooseCandidate(context.Background(), authority, ids, cfg.MaxConcurrency, cfg.WarmReservedSlots, func() requestClass {
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
	state.mu.Lock()
	cfg, authority := state.cfg, state.authority
	state.mu.Unlock()
	if !cfg.Enabled || req.AuthID == "" {
		return okEnvelope(pluginapi.RequestInterceptResponse{Headers: req.Headers, Body: req.Body})
	}
	class := classCold
	_, _, warm := affinityHint(req.Metadata)
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
	state.mu.Lock()
	old, bound := state.leases[req.RequestID], state.bound[req.RequestID]
	state.mu.Unlock()
	if bound == req.AuthID && old.Token != "" {
		return okEnvelope(pluginapi.RequestInterceptResponse{Headers: req.Headers, Body: req.Body})
	}
	if old.Token != "" {
		if errRelease := authority.Release(context.Background(), old); errRelease != nil {
			return admissionResponse(&AdmissionError{Code: "account_concurrency_authority_unavailable", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: defaultRetryAfter, Message: "concurrency authority unavailable", Authority: true})
		}
		state.mu.Lock()
		delete(state.leases, req.RequestID)
		delete(state.bound, req.RequestID)
		state.mu.Unlock()
	}
	lease, err := authority.Acquire(ctx, key, cfg.MaxConcurrency, cfg.WarmReservedSlots, class)
	if err != nil {
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
	return okEnvelope(pluginapi.RequestInterceptResponse{Headers: req.Headers, Body: req.Body})
}

func complete(raw []byte) ([]byte, error) {
	var req pluginapi.RequestCompletion
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	state.mu.Lock()
	lease, ok := state.leases[req.RequestID]
	delete(state.leases, req.RequestID)
	delete(state.bound, req.RequestID)
	authority := state.authority
	state.mu.Unlock()
	if ok && authority != nil {
		_ = authority.Release(context.Background(), lease)
	}
	return okEnvelope(struct{}{})
}

func admissionResponse(err *AdmissionError) ([]byte, error) {
	body, _ := json.Marshal(map[string]any{"error": map[string]any{"type": err.Code, "code": err.Code, "message": err.Message, "retryable": true}})
	headers := http.Header{"Content-Type": []string{"application/json"}, "Retry-After": []string{strconv.Itoa(err.RetryAfter)}}
	return okEnvelope(pluginapi.RequestInterceptResponse{Terminate: true, StatusCode: http.StatusServiceUnavailable, ResponseHeaders: headers, ResponseBody: body})
}

func affinityHint(metadata map[string]any) (hint string, strict, warm bool) {
	if metadata == nil {
		return
	}
	for _, key := range []string{"pinned_auth_id", "selected_auth_id", "session_auth_id", "affinity_auth_id", "cache_auth_id"} {
		if v, ok := metadata[key].(string); ok && strings.TrimSpace(v) != "" {
			hint = strings.TrimSpace(v)
			strict = key != "cache_auth_id"
			warm = strict
			break
		}
	}
	if v, ok := metadata["cache_verified"].(bool); ok && v {
		warm = hint != ""
		strict = hint != ""
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

func typedErrorEnvelope(code, msg string, retryable bool, status int) []byte {
	raw, _ := json.Marshal(envelope{OK: false, Error: &envelopeError{Code: code, Class: code, Message: msg, Retryable: retryable, HTTPStatus: status}})
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

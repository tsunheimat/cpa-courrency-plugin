package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func resetTestState() {
	state.gate.Lock()
	defer state.gate.Unlock()
	state.mu.Lock()
	oldRequests := make([]*requestLifecycle, 0, len(state.requests))
	for _, rs := range state.requests {
		oldRequests = append(oldRequests, rs)
	}
	state.mu.Unlock()
	for _, rs := range oldRequests {
		rs.mu.Lock()
		rs.terminal = true
		stopHeartbeat(rs)
		rs.mu.Unlock()
	}
	state.mu.Lock()
	state.cfg = pluginConfig{Enabled: true, MaxConcurrency: 2, WarmReservedSlots: 1, WaitTimeout: 2 * time.Millisecond, Authority: "local"}
	state.authority = newLocalAuthority()
	state.leases = make(map[string]Lease)
	state.bound = make(map[string]string)
	state.requests = make(map[string]*requestLifecycle)
	state.stopping = false
	state.uncertain = false
	state.reloadFence = false
	state.mu.Unlock()
}

func TestPluginRegistrationIncludesRequiredRepositoryMetadata(t *testing.T) {
	reg := pluginRegistration()
	if reg.Metadata.GitHubRepository == "" {
		t.Fatal("plugin registration omitted GitHubRepository")
	}
	if reg.Metadata.Name != pluginID || !reg.Capabilities.Scheduler || !reg.Capabilities.RequestInterceptorEnforcesAdmission {
		t.Fatalf("registration = %#v", reg)
	}
	if !reg.Capabilities.ManagementAPI {
		t.Fatal("registration omitted management_api capability")
	}
}

func TestManagementRegistrationAndLiveSnapshotAreReadOnlyAndRedacted(t *testing.T) {
	resetTestState()
	reg, err := handleMethod(pluginabi.MethodManagementRegister, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(reg), managementUsagePath) || !strings.Contains(string(reg), managementUIPath) {
		t.Fatalf("registration = %s", reg)
	}
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "mgmt", AuthID: "account-secret@example.com"})
	if _, err := interceptAfter(raw); err != nil {
		t.Fatal(err)
	}
	before := len(state.leases)
	request, _ := json.Marshal(managementRPCRequest{ManagementRequest: pluginapi.ManagementRequest{Method: http.MethodGet, Path: managementUsagePath}})
	out, err := handleMethod(pluginabi.MethodManagementHandle, request)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "account-secret") || strings.Contains(string(out), "@example.com") {
		t.Fatalf("snapshot leaked account identity: %s", out)
	}
	var env envelope
	if err := json.Unmarshal(out, &env); err != nil {
		t.Fatal(err)
	}
	var managementResp pluginapi.ManagementResponse
	if err := json.Unmarshal(env.Result, &managementResp); err != nil {
		t.Fatal(err)
	}
	var snapshot concurrencySnapshot
	if err := json.Unmarshal(managementResp.Body, &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.InFlight != 1 || snapshot.ConfiguredLimit != 2 || snapshot.WarmReserved != 1 || snapshot.AvailableCapacity != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if got := len(state.leases); got != before {
		t.Fatalf("snapshot mutated leases: before=%d after=%d", before, got)
	}
}

func TestManagementUIContainsAuthenticatedRefreshAndFailureStates(t *testing.T) {
	resp, err := (managementHandler{}).HandleManagement(context.Background(), pluginapi.ManagementRequest{Method: http.MethodGet, Path: "/v0/resource/plugins/" + pluginID + "/ui"})
	if err != nil {
		t.Fatal(err)
	}
	body := string(resp.Body)
	if strings.Contains(body, `"in_flight":`) || strings.Contains(body, `"accounts_in_use":`) {
		t.Fatal("unauthenticated resource embeds live allocation values")
	}
	for _, want := range []string{"/v0/management/plugins/cpa-account-concurrency/usage", "credentials:'same-origin'", "Loading live usage", "Unable to load live usage", "stale", "no active accounts", "aria-live"} {
		if !strings.Contains(body, want) {
			t.Errorf("UI missing %q", want)
		}
	}
}

func TestManagementSnapshotReportsAuthorityUnavailableWithoutSecrets(t *testing.T) {
	resetTestState()
	state.mu.Lock()
	state.authority = nil
	state.cfg.RedisPassword = "do-not-leak"
	state.cfg.Authority = "redis"
	state.uncertain = true
	state.mu.Unlock()
	request, _ := json.Marshal(managementRPCRequest{ManagementRequest: pluginapi.ManagementRequest{Method: http.MethodGet, Path: managementUsagePath}})
	out, err := handleMethod(pluginabi.MethodManagementHandle, request)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "do-not-leak") {
		t.Fatalf("snapshot leaked configuration secret: %s", out)
	}
	var env envelope
	if err := json.Unmarshal(out, &env); err != nil {
		t.Fatal(err)
	}
	var managementResp pluginapi.ManagementResponse
	if err := json.Unmarshal(env.Result, &managementResp); err != nil {
		t.Fatal(err)
	}
	var snapshot concurrencySnapshot
	if err := json.Unmarshal(managementResp.Body, &snapshot); err != nil {
		t.Fatal(err)
	}
	if !snapshot.Stale || snapshot.AuthorityState != "unavailable" || snapshot.Error == "" {
		t.Fatalf("unavailable snapshot = %#v", snapshot)
	}
}

func TestManagementSnapshotRaceSafe(t *testing.T) {
	resetTestState()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = readConcurrencySnapshot(context.Background())
			}
		}()
	}
	wg.Wait()
}

func TestHotReloadFailsClosedUntilInflightLeaseCompletes(t *testing.T) {
	resetTestState()
	first, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "reload", AuthID: "acct"})
	if _, err := interceptAfter(first); err != nil {
		t.Fatal(err)
	}
	cliproxyPluginShutdown()
	config, _ := json.Marshal(lifecycleRequest{SchemaVersion: 4, ConfigYAML: []byte("authority: local\nmax_concurrency: 1\nwait_timeout: 1ms\n")})
	if err := configure(config); err != nil {
		t.Fatal(err)
	}
	second, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "new", AuthID: "acct"})
	out, err := interceptAfter(second)
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	_ = json.Unmarshal(out, &env)
	var rejected pluginapi.RequestInterceptResponse
	if env.OK {
		_ = json.Unmarshal(env.Result, &rejected)
	}
	if !rejected.Terminate {
		t.Fatal("reload admitted overlapping lease")
	}
	if _, err := complete([]byte(`{"request_id":"reload"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := interceptAfter(second); err != nil {
		t.Fatal(err)
	}
}

func TestPluginQuiesceDrainsInflightLeaseAndFencesAdmission(t *testing.T) {
	resetTestState()
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "drain", AuthID: "acct"})
	if _, err := interceptAfter(raw); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		deadline := time.Now().Add(500 * time.Millisecond).UnixNano()
		payload, _ := json.Marshal(quiesceRequest{DeadlineUnixNano: deadline})
		_, err := handleMethod(pluginabi.MethodPluginQuiesce, payload)
		result <- err
	}()
	// Quiescing must stop new work while allowing the old request to complete.
	time.Sleep(10 * time.Millisecond)
	newRaw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "new", AuthID: "acct"})
	out, err := interceptAfter(newRaw)
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	_ = json.Unmarshal(out, &env)
	var rejected pluginapi.RequestInterceptResponse
	_ = json.Unmarshal(env.Result, &rejected)
	if !rejected.Terminate {
		t.Fatal("admission succeeded while plugin was quiescing")
	}
	completion, _ := json.Marshal(pluginapi.RequestCompletion{RequestID: "drain"})
	if _, err := complete(completion); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	remaining := len(state.leases)
	state.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("leases after completion = %d", remaining)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("quiesce error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("quiesce did not complete after lease drain")
	}
}

func TestPluginQuiesceTimeoutLeavesAuthorityFenced(t *testing.T) {
	resetTestState()
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "stuck", AuthID: "acct"})
	if _, err := interceptAfter(raw); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(quiesceRequest{DeadlineUnixNano: time.Now().Add(10 * time.Millisecond).UnixNano()})
	if _, err := handleMethod(pluginabi.MethodPluginQuiesce, payload); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("quiesce error = %v, want deadline exceeded", err)
	}
	newRaw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "after-timeout", AuthID: "acct"})
	out, err := interceptAfter(newRaw)
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	_ = json.Unmarshal(out, &env)
	var rejected pluginapi.RequestInterceptResponse
	_ = json.Unmarshal(env.Result, &rejected)
	if !rejected.Terminate {
		t.Fatal("admission succeeded after quiesce timeout")
	}
}

func TestRedisReconfigureSwapsMaterialClientConfigWhenIdle(t *testing.T) {
	resetTestState()
	first, _ := json.Marshal(lifecycleRequest{SchemaVersion: 4, ConfigYAML: []byte("authority: redis\nredis_addr: 127.0.0.1:6379\nredis_password: one\nredis_db: 1\nredis_prefix: first\n")})
	if err := configure(first); err != nil {
		t.Fatal(err)
	}
	second, _ := json.Marshal(lifecycleRequest{SchemaVersion: 4, ConfigYAML: []byte("authority: redis\nredis_addr: 127.0.0.1:6380\nredis_password: two\nredis_db: 2\nredis_prefix: second\n")})
	if err := configure(second); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	a, ok := state.authority.(*redisAuthority)
	state.mu.Unlock()
	if !ok {
		t.Fatalf("authority type = %T", state.authority)
	}
	client, ok := a.client.(redisNetClient)
	if !ok || client.addr != "127.0.0.1:6380" || client.password != "two" || client.db != 2 || a.prefix != "second" {
		t.Fatalf("redis authority = %#v client=%#v", a, a.client)
	}
}

type countingAuthority struct {
	*localAuthority
	mu       sync.Mutex
	acquires int
	releases int
	renews   int
}

func (a *countingAuthority) Acquire(ctx context.Context, key string, limit, reserved int, class requestClass) (Lease, error) {
	a.mu.Lock()
	a.acquires++
	a.mu.Unlock()
	return a.localAuthority.Acquire(ctx, key, limit, reserved, class)
}
func (a *countingAuthority) Release(ctx context.Context, l Lease) error {
	a.mu.Lock()
	a.releases++
	a.mu.Unlock()
	return a.localAuthority.Release(ctx, l)
}
func (a *countingAuthority) Renew(ctx context.Context, l Lease) error {
	a.mu.Lock()
	a.renews++
	a.mu.Unlock()
	return a.localAuthority.Renew(ctx, l)
}

func TestLocalAuthorityHardCapAndExactlyOnceRelease(t *testing.T) {
	a := newLocalAuthority()
	first, err := a.Acquire(context.Background(), "a", 2, 1, classWarm)
	if err != nil {
		t.Fatal(err)
	}
	second, err := a.Acquire(context.Background(), "a", 2, 1, classWarm)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err = a.Acquire(ctx, "a", 2, 1, classWarm); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("third lease error = %v", err)
	}
	if err = a.Release(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err = a.Release(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err = a.Release(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	u, err := a.Snapshot(context.Background(), "a", 2, 1)
	if err != nil || u.InFlight != 0 {
		t.Fatalf("usage = %#v, err=%v", u, err)
	}
}

func TestWarmReservationAndColdFairSelection(t *testing.T) {
	a := newLocalAuthority()
	warm, err := a.Acquire(context.Background(), accountKey("cpa", "a"), 4, 1, classWarm)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err = a.Acquire(context.Background(), accountKey("cpa", "a"), 4, 1, classCold); err != nil {
			t.Fatal(err)
		}
	}
	chosen, err := chooseCandidate(context.Background(), a, []string{"a", "b"}, 4, 1, classCold)
	if err != nil {
		t.Fatal(err)
	}
	if chosen != "b" {
		t.Fatalf("cold selection = %q, want b", chosen)
	}
	if err = a.Release(context.Background(), warm); err != nil {
		t.Fatal(err)
	}
}

func TestSchedulerRejectsFullCapacityAndPreservesStrictAffinity(t *testing.T) {
	resetTestState()
	a := state.authority
	for i := 0; i < 2; i++ {
		if _, err := a.Acquire(context.Background(), accountKey("cpa", "strict"), 2, 1, classWarm); err != nil {
			t.Fatal(err)
		}
	}
	raw, _ := json.Marshal(pluginapi.SchedulerPickRequest{Options: pluginapi.SchedulerOptions{Metadata: map[string]any{"pinned_auth_id": "strict"}}, Candidates: []pluginapi.SchedulerAuthCandidate{{ID: "strict"}, {ID: "other"}}})
	if _, err := schedulerPick(raw); err == nil {
		t.Fatal("strict full scheduler unexpectedly succeeded")
	}
	resetTestState()
	raw, _ = json.Marshal(pluginapi.SchedulerPickRequest{Options: pluginapi.SchedulerOptions{Metadata: map[string]any{"pinned_auth_id": "strict"}}, Candidates: []pluginapi.SchedulerAuthCandidate{{ID: "strict"}, {ID: "other"}}})
	out, err := schedulerPick(raw)
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	_ = json.Unmarshal(out, &env)
	var picked pluginapi.SchedulerPickResponse
	_ = json.Unmarshal(env.Result, &picked)
	if picked.AuthID != "strict" {
		t.Fatalf("picked auth = %q", picked.AuthID)
	}
}

func TestSelectedAccountBindingTransferAndCompletionRelease(t *testing.T) {
	resetTestState()
	before, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "r1", AuthID: "a"})
	if _, err := interceptAfter(before); err != nil {
		t.Fatal(err)
	}
	same, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "r1", AuthID: "a"})
	if _, err := interceptAfter(same); err != nil {
		t.Fatal(err)
	}
	transfer, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "r1", AuthID: "b"})
	if _, err := interceptAfter(transfer); err != nil {
		t.Fatal(err)
	}
	completion, _ := json.Marshal(pluginapi.RequestCompletion{RequestID: "r1", Outcome: pluginapi.RequestCompletionSucceeded})
	if _, err := complete(completion); err != nil {
		t.Fatal(err)
	}
	if _, err := complete(completion); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.leases) != 0 || len(state.bound) != 0 {
		t.Fatalf("leases after duplicate completion = %#v/%#v", state.leases, state.bound)
	}
}

func TestFailedFailoverDoesNotLeaveStaleLeaseBinding(t *testing.T) {
	resetTestState()
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "r2", AuthID: "a"})
	if _, err := interceptAfter(raw); err != nil {
		t.Fatal(err)
	}
	// Fill account b so the failover attempt is rejected.
	state.mu.Lock()
	authority := state.authority
	state.mu.Unlock()
	for i := 0; i < 2; i++ {
		if _, err := authority.Acquire(context.Background(), accountKey("cpa", "b"), 2, 1, classWarm); err != nil {
			t.Fatal(err)
		}
	}
	raw, _ = json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "r2", AuthID: "b"})
	out, err := interceptAfter(raw)
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	_ = json.Unmarshal(out, &env)
	var rejected pluginapi.RequestInterceptResponse
	_ = json.Unmarshal(env.Result, &rejected)
	if !rejected.Terminate {
		t.Fatal("full failover account was not rejected")
	}
	raw, _ = json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "r2", AuthID: "a"})
	out, err = interceptAfter(raw)
	if err != nil {
		t.Fatal(err)
	}
	_ = json.Unmarshal(out, &env)
	var resumed pluginapi.RequestInterceptResponse
	_ = json.Unmarshal(env.Result, &resumed)
	if resumed.Terminate {
		t.Fatal("retry on original account retained stale released binding")
	}
}

func TestAdmissionResponseIsMachineReadable503(t *testing.T) {
	raw, err := admissionResponse(&AdmissionError{Code: "account_concurrency_limit", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: 1, Message: "account concurrency limit reached"})
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	_ = json.Unmarshal(raw, &env)
	var resp pluginapi.RequestInterceptResponse
	_ = json.Unmarshal(env.Result, &resp)
	if !resp.Terminate || resp.StatusCode != http.StatusServiceUnavailable || resp.ResponseHeaders.Get("Retry-After") != "1" {
		t.Fatalf("response = %#v", resp)
	}
	var body map[string]map[string]any
	if err = json.Unmarshal(resp.ResponseBody, &body); err != nil {
		t.Fatal(err)
	}
	if body["error"]["code"] != "account_concurrency_limit" {
		t.Fatalf("body = %s", resp.ResponseBody)
	}
}

func TestAuthorityFailureFailsClosed(t *testing.T) {
	resetTestState()
	state.mu.Lock()
	state.authority = nil
	state.cfg.Authority = "redis"
	state.mu.Unlock()
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "r", AuthID: "a"})
	out, err := interceptAfter(raw)
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	_ = json.Unmarshal(out, &env)
	var resp pluginapi.RequestInterceptResponse
	_ = json.Unmarshal(env.Result, &resp)
	if !resp.Terminate || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("resp = %#v", resp)
	}
}

func TestSelectedAuthMetadataIsColdUnlessVerifiedBinding(t *testing.T) {
	resetTestState()
	selected, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "cold", AuthID: "acct", Metadata: map[string]any{"selected_auth_id": "acct"}})
	if _, err := interceptAfter(selected); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	if got := state.requests["cold"].lease.Class; got != classCold {
		state.mu.Unlock()
		t.Fatalf("selected-auth lease class = %v, want cold", got)
	}
	state.mu.Unlock()
	verified, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "warm", AuthID: "acct", Metadata: map[string]any{"cache_auth_id": "acct", "cache_verified": true}})
	if _, err := interceptAfter(verified); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if got := state.requests["warm"].lease.Class; got != classWarm {
		t.Fatalf("verified cache lease class = %v, want warm", got)
	}
}

func TestPinnedAuthIsWarm(t *testing.T) {
	resetTestState()
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "pinned", AuthID: "acct", Metadata: map[string]any{"pinned_auth_id": "acct"}})
	if _, err := interceptAfter(raw); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if got := state.requests["pinned"].lease.Class; got != classWarm {
		t.Fatalf("pinned lease class = %v, want warm", got)
	}
}

func TestVerifiedBindingDoesNotMakeColdFailoverWarm(t *testing.T) {
	resetTestState()
	first, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "retry", AuthID: "a", Metadata: map[string]any{"cache_auth_id": "a", "cache_verified": true}})
	if _, err := interceptAfter(first); err != nil {
		t.Fatal(err)
	}
	retry, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "retry", AuthID: "b", Metadata: map[string]any{"cache_auth_id": "a", "cache_verified": true, "selected_auth_id": "b"}})
	if _, err := interceptAfter(retry); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if got := state.requests["retry"].lease.Class; got != classCold {
		t.Fatalf("failover lease class = %v, want cold", got)
	}
}

type typedCapacityAuthority struct {
	*localAuthority
	mu   sync.Mutex
	full bool
}

type renewalFailureAuthority struct {
	*countingAuthority
}

func (a *renewalFailureAuthority) Renew(context.Context, Lease) error {
	a.mu.Lock()
	a.renews++
	a.mu.Unlock()
	return errors.New("renew transport failed")
}

func (a *typedCapacityAuthority) Acquire(ctx context.Context, key string, limit, reserved int, class requestClass) (Lease, error) {
	a.mu.Lock()
	full := a.full
	a.mu.Unlock()
	if full {
		return Lease{}, &AdmissionError{Code: "account_concurrency_limit", HTTPStatus: http.StatusServiceUnavailable, RetryAfter: 1, Message: "account concurrency limit reached"}
	}
	return a.localAuthority.Acquire(ctx, key, limit, reserved, class)
}

func TestCapacityRejectionDoesNotPoisonAuthorityAndRecoversAfterRelease(t *testing.T) {
	resetTestState()
	base := newLocalAuthority()
	hold, err := base.Acquire(context.Background(), accountKey("cpa", "acct"), 2, 1, classWarm)
	if err != nil {
		t.Fatal(err)
	}
	a := &typedCapacityAuthority{localAuthority: base, full: true}
	state.mu.Lock()
	state.authority = a
	state.mu.Unlock()
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "capacity-1", AuthID: "acct"})
	for i := 0; i < 2; i++ {
		out, err := interceptAfter(raw)
		if err != nil {
			t.Fatal(err)
		}
		var env envelope
		_ = json.Unmarshal(out, &env)
		var resp pluginapi.RequestInterceptResponse
		_ = json.Unmarshal(env.Result, &resp)
		if !resp.Terminate || resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("capacity response = %#v", resp)
		}
		state.mu.Lock()
		uncertain := state.uncertain
		state.mu.Unlock()
		if uncertain {
			t.Fatal("typed capacity rejection poisoned authority")
		}
	}
	a.mu.Lock()
	a.full = false
	a.mu.Unlock()
	if err := base.Release(context.Background(), hold); err != nil {
		t.Fatal(err)
	}
	out, err := interceptAfter(raw)
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	_ = json.Unmarshal(out, &env)
	var resp pluginapi.RequestInterceptResponse
	_ = json.Unmarshal(env.Result, &resp)
	if resp.Terminate {
		t.Fatalf("admission did not recover after capacity release: %#v", resp)
	}
}

func TestEmptyRequestIDIsRejectedWithoutLease(t *testing.T) {
	resetTestState()
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{AuthID: "acct"})
	out, err := interceptAfter(raw)
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	_ = json.Unmarshal(out, &env)
	var resp pluginapi.RequestInterceptResponse
	_ = json.Unmarshal(env.Result, &resp)
	if !resp.Terminate || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty request id response = %#v", resp)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.leases) != 0 || len(state.requests) != 0 {
		t.Fatalf("empty request id created state: leases=%d requests=%d", len(state.leases), len(state.requests))
	}
}

func TestAuthIDWhitespaceAliasIsStable(t *testing.T) {
	resetTestState()
	first, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "alias", AuthID: " acct "})
	if _, err := interceptAfter(first); err != nil {
		t.Fatal(err)
	}
	second, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "alias", AuthID: "acct"})
	if _, err := interceptAfter(second); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if got := state.bound["alias"]; got != "acct" {
		t.Fatalf("canonical bound auth = %q", got)
	}
}

func TestRenewalFailureFencesUntilCompletion(t *testing.T) {
	resetTestState()
	base := &countingAuthority{localAuthority: newLocalAuthority()}
	a := &renewalFailureAuthority{countingAuthority: base}
	state.mu.Lock()
	state.authority = a
	state.mu.Unlock()
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "fenced", AuthID: "acct"})
	if _, err := interceptAfter(raw); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	rs := state.requests["fenced"]
	state.mu.Unlock()
	startHeartbeatWithInterval(rs, a, rs.lease, time.Millisecond)
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		rs.mu.Lock()
		fenced := rs.fenced
		rs.mu.Unlock()
		if fenced {
			break
		}
		time.Sleep(time.Millisecond)
	}
	rs.mu.Lock()
	fenced := rs.fenced
	rs.mu.Unlock()
	if !fenced {
		t.Fatal("renewal failure did not fence request")
	}
	state.mu.Lock()
	uncertain, leaseCount := state.uncertain, len(state.leases)
	if !uncertain || leaseCount != 1 {
		state.mu.Unlock()
		t.Fatalf("fenced state uncertain=%v leases=%d", uncertain, leaseCount)
	}
	state.mu.Unlock()
	completion, _ := json.Marshal(pluginapi.RequestCompletion{RequestID: "fenced"})
	if _, err := complete(completion); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.leases) != 0 {
		t.Fatalf("lease retained after fenced completion: %d", len(state.leases))
	}
}

type renewalFenceAuthority struct {
	*renewalFailureAuthority
	fenced chan Lease
}

func (a *renewalFenceAuthority) Fence(_ context.Context, lease Lease) error {
	select {
	case a.fenced <- lease:
	default:
	}
	return nil
}

func TestRenewalLossAttemptsDistributedFence(t *testing.T) {
	resetTestState()
	base := &countingAuthority{localAuthority: newLocalAuthority()}
	a := &renewalFenceAuthority{renewalFailureAuthority: &renewalFailureAuthority{countingAuthority: base}, fenced: make(chan Lease, 1)}
	state.mu.Lock()
	state.authority = a
	state.mu.Unlock()
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "fence-distributed", AuthID: "acct"})
	if _, err := interceptAfter(raw); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	rs := state.requests["fence-distributed"]
	lease := rs.lease
	state.mu.Unlock()
	startHeartbeatWithInterval(rs, a, lease, time.Millisecond)
	select {
	case got := <-a.fenced:
		if got.Token != lease.Token {
			t.Fatalf("fenced token = %q, want %q", got.Token, lease.Token)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("renewal loss did not invoke distributed fence")
	}
}

type deadlineAuthority struct {
	deadlineSeen bool
}

func (a *deadlineAuthority) Snapshot(ctx context.Context, _ string, _, _ int) (Usage, error) {
	_, a.deadlineSeen = ctx.Deadline()
	return Usage{}, ErrAuthorityUnavailable
}
func (a *deadlineAuthority) Acquire(context.Context, string, int, int, requestClass) (Lease, error) {
	return Lease{}, ErrAuthorityUnavailable
}
func (a *deadlineAuthority) Release(ctx context.Context, _ Lease) error {
	_, a.deadlineSeen = ctx.Deadline()
	return ErrAuthorityUnavailable
}
func (a *deadlineAuthority) Renew(ctx context.Context, _ Lease) error {
	_, a.deadlineSeen = ctx.Deadline()
	return ErrAuthorityUnavailable
}

func TestSchedulerAuthorityCallHasDeadline(t *testing.T) {
	resetTestState()
	a := &deadlineAuthority{}
	state.mu.Lock()
	state.authority = a
	state.mu.Unlock()
	raw, _ := json.Marshal(pluginapi.SchedulerPickRequest{Candidates: []pluginapi.SchedulerAuthCandidate{{ID: "acct"}}})
	if _, err := schedulerPick(raw); err == nil {
		t.Fatal("scheduler unexpectedly admitted with unavailable authority")
	}
	if !a.deadlineSeen {
		t.Fatal("scheduler authority call had no deadline")
	}
}

func TestReconfigureDoesNotClearUncertaintyWithTrackedLease(t *testing.T) {
	resetTestState()
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "uncertain", AuthID: "acct"})
	if _, err := interceptAfter(raw); err != nil {
		t.Fatal(err)
	}
	markAuthorityUncertain()
	config, _ := json.Marshal(lifecycleRequest{SchemaVersion: 4, ConfigYAML: []byte("max_concurrency: 2\nwait_timeout: 1ms\nauthority: local\n")})
	if err := configure(config); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.uncertain {
		t.Fatal("reconfigure cleared uncertainty while lease remained tracked")
	}
}

func TestReconfigureShutdownAndLateCallbacksRaceSafely(t *testing.T) {
	resetTestState()
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "race", AuthID: "acct"})
	config, _ := json.Marshal(lifecycleRequest{SchemaVersion: 4, ConfigYAML: []byte("max_concurrency: 2\nwait_timeout: 1ms\nauthority: local\n")})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			_, _ = interceptAfter(raw)
		}()
		go func() {
			defer wg.Done()
			_, _ = complete([]byte(`{"request_id":"race"}`))
		}()
		go func() {
			defer wg.Done()
			_ = configure(config)
		}()
	}
	shutdownDone := make(chan struct{})
	go func() {
		cliproxyPluginShutdown()
		close(shutdownDone)
	}()
	wg.Wait()
	<-shutdownDone
}

func TestDefaultReservationFormula(t *testing.T) {
	for limit, want := range map[int]int{1: 0, 2: 1, 5: 1, 6: 2, 10: 2} {
		resetTestState()
		raw, _ := json.Marshal(lifecycleRequest{SchemaVersion: 4, ConfigYAML: []byte("max_concurrency: " + fmt.Sprint(limit) + "\nwait_timeout: 10ms\n")})
		if err := configure(raw); err != nil {
			t.Fatalf("limit %d configure: %v", limit, err)
		}
		state.mu.Lock()
		got := state.cfg.WarmReservedSlots
		state.mu.Unlock()
		if got != want {
			t.Fatalf("limit %d reservation = %d, want %d", limit, got, want)
		}
	}
}

func TestConcurrentCallbacksSerializePerRequest(t *testing.T) {
	resetTestState()
	a := &countingAuthority{localAuthority: newLocalAuthority()}
	state.mu.Lock()
	state.authority = a
	state.mu.Unlock()
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "concurrent", AuthID: "acct"})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = interceptAfter(raw) }()
	}
	wg.Wait()
	a.mu.Lock()
	gotAcquire, gotRelease := a.acquires, a.releases
	a.mu.Unlock()
	if gotAcquire != 1 || gotRelease != 0 {
		t.Fatalf("duplicate callbacks acquire/release = %d/%d, want 1/0", gotAcquire, gotRelease)
	}
	completion, _ := json.Marshal(pluginapi.RequestCompletion{RequestID: "concurrent"})
	_, _ = complete(completion)
	a.mu.Lock()
	gotRelease = a.releases
	a.mu.Unlock()
	if gotRelease != 1 {
		t.Fatalf("completion releases = %d, want 1", gotRelease)
	}
}

func TestCompletionTombstonePreventsLateAcquire(t *testing.T) {
	resetTestState()
	a := &countingAuthority{localAuthority: newLocalAuthority()}
	state.mu.Lock()
	state.authority = a
	state.mu.Unlock()
	completion, _ := json.Marshal(pluginapi.RequestCompletion{RequestID: "late"})
	if _, err := complete(completion); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "late", AuthID: "acct"})
	if _, err := interceptAfter(raw); err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	got := a.acquires
	a.mu.Unlock()
	if got != 0 {
		t.Fatalf("late callback acquired %d leases", got)
	}
}

func TestShutdownMarksRequestsTerminalAndRejectsAdmission(t *testing.T) {
	resetTestState()
	a := &countingAuthority{localAuthority: newLocalAuthority()}
	state.mu.Lock()
	state.authority = a
	state.mu.Unlock()
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "shutdown", AuthID: "acct"})
	if _, err := interceptAfter(raw); err != nil {
		t.Fatal(err)
	}
	cliproxyPluginShutdown()
	if _, err := interceptAfter(raw); err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	acquires, releases := a.acquires, a.releases
	a.mu.Unlock()
	if acquires != 1 || releases != 1 {
		t.Fatalf("shutdown acquire/release = %d/%d, want 1/1", acquires, releases)
	}
}

func TestRedisLeaseLifecycleArgumentsAndIdempotentRelease(t *testing.T) {
	f := &recordingRedis{}
	a := newRedisAuthority(f, "cpa:test")
	l, err := a.Acquire(context.Background(), "acct", 2, 1, classWarm)
	if err != nil {
		t.Fatal(err)
	}
	if f.acquireTTL <= 0 {
		t.Fatalf("acquire ttl = %d", f.acquireTTL)
	}
	if err := a.Renew(context.Background(), l); err != nil {
		t.Fatal(err)
	}
	if f.renewTTL <= 0 {
		t.Fatalf("renew ttl = %d", f.renewTTL)
	}
	if err := a.Release(context.Background(), l); err != nil {
		t.Fatal(err)
	}
	if err := a.Release(context.Background(), l); err != nil {
		t.Fatal(err)
	}
}

func TestRedisFakeExpiryRenewalAndCrashRecovery(t *testing.T) {
	f := newLeaseFakeRedis()
	a := newRedisAuthority(f, "cpa:test")
	one, err := a.Acquire(context.Background(), "acct", 1, 0, classWarm)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Acquire(context.Background(), "acct", 1, 0, classWarm); err == nil {
		t.Fatal("second acquire exceeded fake hard cap")
	}
	f.expire(one.Token)
	if _, err = a.Acquire(context.Background(), "acct", 1, 0, classWarm); !errors.Is(err, ErrAuthorityUnavailable) {
		t.Fatalf("expired lease acquire error = %v, want authority unavailable", err)
	}
	if err = a.Release(context.Background(), one); err != nil {
		t.Fatal(err)
	}
	// A live lease is extended by renewal and remains occupied.
	live, err := a.Acquire(context.Background(), "other", 2, 0, classWarm)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Renew(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	f.expire(live.Token)
	if err = a.Renew(context.Background(), live); err == nil {
		t.Fatal("renewal unexpectedly revived an expired lease")
	}
	if err = a.Release(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	if err = a.Release(context.Background(), live); err != nil {
		t.Fatal(err)
	}
}

type recordingRedis struct {
	acquireTTL, renewTTL int64
	released             int
}

type fencedRedis struct{}

func (fencedRedis) Eval(_ context.Context, script string, _ []string, _ ...any) (any, error) {
	if script == redisAcquireScript {
		return int64(-1), nil
	}
	return []any{int64(0), int64(0), int64(0)}, nil
}

func TestRedisAcquireAfterExpiryFenceFailsClosedForAnotherInstance(t *testing.T) {
	a := newRedisAuthority(fencedRedis{}, "cpa:test")
	lease, err := a.Acquire(context.Background(), "acct", 1, 0, classWarm)
	if lease.Token != "" {
		t.Fatalf("fenced acquire returned lease %#v", lease)
	}
	if !errors.Is(err, ErrAuthorityUnavailable) {
		t.Fatalf("fenced acquire error = %v, want authority unavailable", err)
	}
}

func TestRedisAcquireFenceProducesTypedAuthorityUnavailable(t *testing.T) {
	resetTestState()
	state.mu.Lock()
	state.authority = newRedisAuthority(fencedRedis{}, "cpa:test")
	state.mu.Unlock()

	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: "fenced", AuthID: "acct"})
	out, err := interceptAfter(raw)
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	if err := json.Unmarshal(out, &env); err != nil {
		t.Fatal(err)
	}
	var resp pluginapi.RequestInterceptResponse
	if err := json.Unmarshal(env.Result, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.Terminate || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("fenced response = %#v", resp)
	}
	var body map[string]map[string]any
	if err := json.Unmarshal(resp.ResponseBody, &body); err != nil {
		t.Fatal(err)
	}
	if got := body["error"]["type"]; got != "account_concurrency_authority_unavailable" {
		t.Fatalf("fenced error type = %v", got)
	}
	if got := body["error"]["code"]; got != "account_concurrency_authority_unavailable" {
		t.Fatalf("fenced error code = %v", got)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.leases) != 0 {
		t.Fatalf("fenced admission created leases: %#v", state.leases)
	}
	if got := state.requests["fenced"].lease; got.Token != "" {
		t.Fatalf("fenced request retained lease %#v", got)
	}
	if !state.uncertain {
		t.Fatal("fenced authority failure did not preserve fail-closed uncertainty")
	}
}

func (f *recordingRedis) Eval(_ context.Context, script string, _ []string, args ...any) (any, error) {
	switch script {
	case redisAcquireScript:
		f.acquireTTL, _ = toInt64(args[5])
		return int64(1), nil
	case redisRenewScript:
		f.renewTTL, _ = toInt64(args[2])
		return int64(1), nil
	case redisReleaseScript:
		f.released++
		return int64(0), nil
	default:
		return []any{int64(0), int64(0)}, nil
	}
}
func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}

type fakeRedisEntry struct {
	class  int
	expiry int64
}
type leaseFakeRedis struct {
	mu      sync.Mutex
	entries map[string]fakeRedisEntry
	fenced  bool
}

func newLeaseFakeRedis() *leaseFakeRedis {
	return &leaseFakeRedis{entries: make(map[string]fakeRedisEntry)}
}
func (f *leaseFakeRedis) expire(token string) {
	f.mu.Lock()
	if e, ok := f.entries[token]; ok {
		e.expiry = 0
		f.entries[token] = e
	}
	f.mu.Unlock()
}
func (f *leaseFakeRedis) clean(now int64) {
	for token, e := range f.entries {
		if e.expiry <= now {
			f.fenced = true
			_ = token
		}
	}
}
func (f *leaseFakeRedis) Eval(_ context.Context, script string, keys []string, args ...any) (any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now().UnixMilli()
	if len(args) > 4 {
		if n, ok := toInt64(args[4]); ok {
			now = n
		}
	}
	f.clean(now)
	switch script {
	case redisAcquireScript:
		if f.fenced {
			return int64(-1), nil
		}
		limit, _ := toInt64(args[0])
		reserved, _ := toInt64(args[1])
		class, _ := toInt64(args[2])
		token, _ := args[3].(string)
		ttl, _ := toInt64(args[5])
		allowed := limit
		if class == 0 && limit-reserved > 0 {
			allowed = limit - reserved
		}
		if int64(len(f.entries)) >= allowed {
			return int64(0), nil
		}
		f.entries[token] = fakeRedisEntry{class: int(class), expiry: now + ttl}
		return int64(1), nil
	case redisRenewScript:
		token, _ := args[0].(string)
		now, _ := toInt64(args[1])
		ttl, _ := toInt64(args[2])
		e, ok := f.entries[token]
		if !ok || e.expiry <= now {
			return int64(0), nil
		}
		e.expiry = now + ttl
		f.entries[token] = e
		return int64(1), nil
	case redisReleaseScript:
		token, _ := args[0].(string)
		if _, ok := f.entries[token]; !ok {
			return int64(0), nil
		}
		delete(f.entries, token)
		if len(f.entries) == 0 {
			f.fenced = false
		}
		return int64(1), nil
	case redisSnapshotScript:
		warm := 0
		for _, e := range f.entries {
			if e.class == 1 {
				warm++
			}
		}
		fenced := int64(0)
		if f.fenced {
			fenced = 1
		}
		return []any{int64(len(f.entries)), int64(warm), fenced}, nil
	default:
		_ = keys
		return nil, errors.New("unknown script")
	}
}

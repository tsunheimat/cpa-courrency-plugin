package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func resetTestState() {
	state.mu.Lock()
	state.cfg = pluginConfig{Enabled: true, MaxConcurrency: 2, WarmReservedSlots: 1, WaitTimeout: 2 * time.Millisecond, Authority: "local"}
	state.authority = newLocalAuthority()
	state.leases = make(map[string]Lease)
	state.bound = make(map[string]string)
	state.mu.Unlock()
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

package pipeline

import (
	"context"
	"testing"
)

// Case: a cooperatively paused segment persists source resume state plus queued work and Resume continues from that state.
func TestEngineSpec_PausePersistsAndResumeContinues(t *testing.T) {
	t.Parallel()

	store := &pauseRecoveryStore{}
	segment1 := &pausingSegment{started: make(chan struct{}), store: store}
	plan1 := buildSingleStagePlan(t, segment1)
	runtime := NewInMemoryRuntime()
	source1 := &stringSource{records: []string{"paused"}, blockIndex: -1}
	sink1 := &recordingSink{}
	engine1, err := NewPullEngine(source1, sink1, plan1, runtime)
	if err != nil {
		t.Fatalf("new first engine failed: %v", err)
	}

	runErr := make(chan error, 1)
	go func() {
		runErr <- engine1.Run(context.Background())
	}()

	<-segment1.started
	if err := engine1.Pause(context.Background()); err != nil {
		t.Fatalf("pause failed: %v", err)
	}
	if err := <-runErr; err != nil {
		t.Fatalf("run after pause failed: %v", err)
	}

	segment2 := &pausingSegment{store: store}
	plan2 := buildSingleStagePlan(t, segment2)
	source2 := &stringSource{records: []string{"paused"}, blockIndex: -1}
	sink2 := &recordingSink{}
	engine2, err := NewPullEngine(source2, sink2, plan2, runtime)
	if err != nil {
		t.Fatalf("new resumed engine failed: %v", err)
	}
	if err := engine2.Resume(context.Background()); err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	if len(sink2.payloads) != 1 || sink2.payloads[0] != "paused" {
		t.Fatalf("expected resumed output, got %v", sink2.payloads)
	}
	if len(store.reasons) != 1 || store.reasons[0] != ResumeAfterPause {
		t.Fatalf("expected one pause recovery, got %v", store.reasons)
	}
	if len(store.attempts) != 1 || store.attempts[0] != 2 {
		t.Fatalf("expected resumed pause attempt id 2, got %v", store.attempts)
	}
	if len(sink2.items) != 1 || sink2.items[0].AttemptID != 2 {
		t.Fatalf("expected resumed sink attempt id 2, got %+v", sink2.items)
	}
}

// Case: crash recovery compensates a non-idempotent segment before replaying the interrupted work.
func TestEngineSpec_CrashRecoveryCompensatesNonIdempotentSegment(t *testing.T) {
	t.Parallel()

	state := &crashSegmentState{}
	started := make(chan struct{})
	plan1 := buildSingleStagePlan(t, &blockingNonIdempotentSegment{
		started: started,
		release: make(chan struct{}),
		state:   state,
	})
	runtime := NewInMemoryRuntime()
	source1 := &stringSource{records: []string{"src-42"}, blockIndex: -1}
	sink1 := &recordingSink{}
	engine1, err := NewPullEngine(source1, sink1, plan1, runtime)
	if err != nil {
		t.Fatalf("new first engine failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() {
		runErr <- engine1.Run(ctx)
	}()

	<-started
	cancel()
	if err := <-runErr; err == nil {
		t.Fatalf("expected interrupted run to leave crash-recovery work")
	}

	release := make(chan struct{})
	close(release)
	plan2 := buildSingleStagePlan(t, &blockingNonIdempotentSegment{
		release: release,
		state:   state,
	})
	source2 := &stringSource{records: []string{"src-42"}, blockIndex: -1}
	sink2 := &recordingSink{}
	engine2, err := NewPullEngine(source2, sink2, plan2, runtime)
	if err != nil {
		t.Fatalf("new recovery engine failed: %v", err)
	}
	if err := engine2.Run(context.Background()); err != nil {
		t.Fatalf("recovery run failed: %v", err)
	}

	state.mu.Lock()
	compensations := state.compensations
	recoveries := state.recoveries
	events := append([]string(nil), state.events...)
	reasons := append([]ResumeReason(nil), state.reasons...)
	recoverAttempts := append([]AttemptID(nil), state.recoverAttempts...)
	compensateAttempts := append([]AttemptID(nil), state.compensateAttempts...)
	state.mu.Unlock()
	if compensations != 1 {
		t.Fatalf("expected one compensation during crash recovery, got %d", compensations)
	}
	if recoveries != 1 {
		t.Fatalf("expected one recover call during crash recovery, got %d", recoveries)
	}
	if len(reasons) != 1 || reasons[0] != ResumeAfterCrash {
		t.Fatalf("expected crash recovery reason, got %v", reasons)
	}
	if len(recoverAttempts) != 1 || recoverAttempts[0] != 2 {
		t.Fatalf("expected recover to see attempt id 2, got %v", recoverAttempts)
	}
	if len(compensateAttempts) != 1 || compensateAttempts[0] != 1 {
		t.Fatalf("expected compensate to target prior attempt 1, got %v", compensateAttempts)
	}
	wantEvents := []string{"process", "compensate", "recover", "process"}
	if len(events) != len(wantEvents) {
		t.Fatalf("unexpected event sequence length: %v", events)
	}
	for i := range wantEvents {
		if events[i] != wantEvents[i] {
			t.Fatalf("expected events %v, got %v", wantEvents, events)
		}
	}
}

// Case: tracing a source record returns zero outputs when a completed pipeline branch legitimately emits nothing.
func TestEngineSpec_TraceSourceToZeroOrManyOutputs(t *testing.T) {
	t.Parallel()

	plan := buildSingleStagePlan(t, filterSegment{})
	runtime := NewInMemoryRuntime()
	source := &stringSource{records: []string{"src-123"}, blockIndex: -1}
	sink := &recordingSink{}
	engine, err := NewPullEngine(source, sink, plan, runtime)
	if err != nil {
		t.Fatalf("new engine failed: %v", err)
	}
	if err := engine.Run(context.Background()); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	trace, err := engine.Trace(context.Background(), "src-123")
	if err != nil {
		t.Fatalf("trace failed: %v", err)
	}
	if len(trace) != 0 {
		t.Fatalf("expected zero-output trace to be empty, got %d items", len(trace))
	}
}

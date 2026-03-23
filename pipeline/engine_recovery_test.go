package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	goruntime "runtime"
	"sync"
	"testing"
)

type stringSource struct {
	mu           sync.Mutex
	records      []string
	index        int
	blockIndex   int
	blockUntil   <-chan struct{}
	blockEntered chan struct{}
	blockOnce    sync.Once
}

type sourceItem struct {
	recordID RecordID
	payload  string
	metadata map[string]string
}

type recordSource struct {
	mu      sync.Mutex
	records []sourceItem
	index   int
}

func (s *recordSource) Next(context.Context) (SourceRecord[string], bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.index >= len(s.records) {
		return SourceRecord[string]{}, false, nil
	}
	item := s.records[s.index]
	s.index++
	return SourceRecord[string]{
		RecordID: item.recordID,
		Payload:  item.payload,
		Metadata: cloneMetadata(item.metadata),
	}, true, nil
}

func (s *recordSource) SnapshotCursor(context.Context) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(struct {
		Index int `json:"index"`
	}{Index: s.index})
}

func (s *recordSource) RestoreCursor(_ context.Context, cursor []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(cursor) == 0 {
		s.index = 0
		return nil
	}
	var state struct {
		Index int `json:"index"`
	}
	if err := json.Unmarshal(cursor, &state); err != nil {
		return err
	}
	s.index = state.Index
	return nil
}

func (s *stringSource) Next(ctx context.Context) (SourceRecord[string], bool, error) {
	s.mu.Lock()
	index := s.index
	shouldBlock := s.blockUntil != nil && s.blockIndex >= 0 && index == s.blockIndex
	if shouldBlock {
		s.blockOnce.Do(func() {
			if s.blockEntered != nil {
				close(s.blockEntered)
			}
		})
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return SourceRecord[string]{}, false, ctx.Err()
		case <-s.blockUntil:
		}
		s.mu.Lock()
		index = s.index
	}
	if index >= len(s.records) {
		s.mu.Unlock()
		return SourceRecord[string]{}, false, nil
	}
	payload := s.records[index]
	s.index++
	s.mu.Unlock()
	return SourceRecord[string]{
		RecordID: RecordID(payload),
		Payload:  payload,
	}, true, nil
}

func (s *stringSource) SnapshotCursor(context.Context) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(struct {
		Index int `json:"index"`
	}{Index: s.index})
}

func (s *stringSource) RestoreCursor(_ context.Context, cursor []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(cursor) == 0 {
		s.index = 0
		return nil
	}
	var state struct {
		Index int `json:"index"`
	}
	if err := json.Unmarshal(cursor, &state); err != nil {
		return err
	}
	s.index = state.Index
	return nil
}

type recordingSink struct {
	mu       sync.Mutex
	payloads []string
	items    []Envelope[string]
	notify   chan string
}

func (s *recordingSink) Consume(_ context.Context, item Envelope[string]) error {
	s.mu.Lock()
	s.payloads = append(s.payloads, item.Payload)
	s.items = append(s.items, item)
	s.mu.Unlock()
	if s.notify != nil {
		select {
		case s.notify <- item.Payload:
		default:
		}
	}
	return nil
}

type passthroughSegment struct{}

func (passthroughSegment) Descriptor() SegmentDescriptor {
	return SegmentDescriptor{ID: "segment", Idempotency: Idempotent, CompatibilityVersion: "v1"}
}

func (passthroughSegment) Process(_ ProcessContext, in SegmentInput[string], out func(SegmentOutput[string]) error) (ProcessResult, error) {
	if err := out(SegmentOutput[string]{Payload: in.Payload, Metadata: in.Metadata}); err != nil {
		return ProcessResult{}, err
	}
	return ProcessResult{Status: ProcessCompleted}, nil
}

func (passthroughSegment) Done(context.Context) error { return nil }

type pausingSegment struct {
	started      chan struct{}
	startOnce    sync.Once
	store        *pauseRecoveryStore
	recoveredFrom string
}

type pauseRecoveryStore struct {
	mu      sync.Mutex
	payload map[RecordID]string
	reasons []ResumeReason
	attempts []AttemptID
}

func (s *pauseRecoveryStore) save(recordID RecordID, payload string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.payload == nil {
		s.payload = map[RecordID]string{}
	}
	s.payload[recordID] = payload
}

func (s *pauseRecoveryStore) load(recordID RecordID) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.payload[recordID]
}

func (s *pauseRecoveryStore) recordRecovery(reason ResumeReason, attemptID AttemptID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reasons = append(s.reasons, reason)
	s.attempts = append(s.attempts, attemptID)
}

func (s *pausingSegment) Descriptor() SegmentDescriptor {
	return SegmentDescriptor{ID: "segment", Idempotency: Idempotent, CompatibilityVersion: "v1"}
}

func (s *pausingSegment) Process(ctx ProcessContext, in SegmentInput[string], out func(SegmentOutput[string]) error) (ProcessResult, error) {
	if s.recoveredFrom != "" {
		payload := s.recoveredFrom
		s.recoveredFrom = ""
		if err := out(SegmentOutput[string]{Payload: payload, Metadata: in.Metadata}); err != nil {
			return ProcessResult{}, err
		}
		return ProcessResult{Status: ProcessCompleted}, nil
	}
	s.startOnce.Do(func() {
		if s.started != nil {
			close(s.started)
		}
	})
	for !ctx.PauseRequested() {
		select {
		case <-ctx.Done():
			return ProcessResult{}, ctx.Err()
		default:
			goruntime.Gosched()
		}
	}
	if s.store != nil {
		s.store.save(in.SourceRecordID, in.Payload)
	}
	return ProcessResult{
		Status:   ProcessPaused,
	}, nil
}

func (s *pausingSegment) Recover(_ context.Context, in SegmentInput[string], info ResumeInfo) error {
	if s.store != nil {
		s.store.recordRecovery(info.Reason, info.AttemptID)
		s.recoveredFrom = s.store.load(in.SourceRecordID)
	}
	return nil
}

func (s *pausingSegment) Done(context.Context) error { return nil }

type crashSegmentState struct {
	mu            sync.Mutex
	processes     int
	compensations int
	recoveries    int
	reasons       []ResumeReason
	recoverAttempts []AttemptID
	compensateAttempts []AttemptID
	events        []string
}

type blockingNonIdempotentSegment struct {
	started   chan struct{}
	startOnce sync.Once
	release   <-chan struct{}
	state     *crashSegmentState
}

func (s *blockingNonIdempotentSegment) Descriptor() SegmentDescriptor {
	return SegmentDescriptor{ID: "segment", Idempotency: NonIdempotent, CompatibilityVersion: "v1"}
}

func (s *blockingNonIdempotentSegment) Process(ctx ProcessContext, in SegmentInput[string], out func(SegmentOutput[string]) error) (ProcessResult, error) {
	s.state.mu.Lock()
	s.state.processes++
	s.state.events = append(s.state.events, "process")
	s.state.mu.Unlock()
	s.startOnce.Do(func() {
		if s.started != nil {
			close(s.started)
		}
	})
	select {
	case <-ctx.Done():
		return ProcessResult{}, ctx.Err()
	case <-s.release:
	}
	if err := out(SegmentOutput[string]{Payload: in.Payload, Metadata: in.Metadata}); err != nil {
		return ProcessResult{}, err
	}
	return ProcessResult{Status: ProcessCompleted}, nil
}

func (s *blockingNonIdempotentSegment) Recover(_ context.Context, _ SegmentInput[string], info ResumeInfo) error {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	s.state.recoveries++
	s.state.reasons = append(s.state.reasons, info.Reason)
	s.state.recoverAttempts = append(s.state.recoverAttempts, info.AttemptID)
	s.state.events = append(s.state.events, "recover")
	return nil
}

func (s *blockingNonIdempotentSegment) Done(context.Context) error { return nil }
func (s *blockingNonIdempotentSegment) Compensator() Compensator {
	return crashCompensator{state: s.state}
}

type crashCompensator struct {
	state *crashSegmentState
}

func (c crashCompensator) Compensate(_ context.Context, _ RecordID, attemptID AttemptID, _ error) error {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.compensations++
	c.state.compensateAttempts = append(c.state.compensateAttempts, attemptID)
	c.state.events = append(c.state.events, "compensate")
	return nil
}

type filterSegment struct{}

func (filterSegment) Descriptor() SegmentDescriptor {
	return SegmentDescriptor{ID: "segment", Idempotency: Idempotent, CompatibilityVersion: "v1"}
}

func (filterSegment) Process(ProcessContext, SegmentInput[string], func(SegmentOutput[string]) error) (ProcessResult, error) {
	return ProcessResult{Status: ProcessCompleted}, nil
}

func (filterSegment) Done(context.Context) error { return nil }

type countingDeterministicSegment struct {
	mu        sync.Mutex
	processes int
	recovers  int
}

func (s *countingDeterministicSegment) Descriptor() SegmentDescriptor {
	return SegmentDescriptor{ID: "segment", Idempotency: Idempotent, CompatibilityVersion: "v1"}
}

func (s *countingDeterministicSegment) Process(_ ProcessContext, in SegmentInput[string], out func(SegmentOutput[string]) error) (ProcessResult, error) {
	s.mu.Lock()
	s.processes++
	s.mu.Unlock()
	if err := out(SegmentOutput[string]{Payload: in.Payload, Metadata: in.Metadata}); err != nil {
		return ProcessResult{}, err
	}
	return ProcessResult{Status: ProcessCompleted}, nil
}

func (s *countingDeterministicSegment) Recover(context.Context, SegmentInput[string], ResumeInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recovers++
	return nil
}

func (s *countingDeterministicSegment) Done(context.Context) error { return nil }

type countyRecoveryStore struct {
	mu        sync.Mutex
	nextPart  map[RecordID]int
	reasons   []ResumeReason
	attempts  []AttemptID
	processed []string
}

func (s *countyRecoveryStore) load(recordID RecordID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextPart[recordID]
}

func (s *countyRecoveryStore) save(recordID RecordID, nextPart int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nextPart == nil {
		s.nextPart = map[RecordID]int{}
	}
	s.nextPart[recordID] = nextPart
}

func (s *countyRecoveryStore) appendRecovery(reason ResumeReason, attemptID AttemptID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reasons = append(s.reasons, reason)
	s.attempts = append(s.attempts, attemptID)
}

func (s *countyRecoveryStore) appendProcessed(part string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.processed = append(s.processed, part)
}

type countyBatchSegment struct {
	store      *countyRecoveryStore
	resumePart int
	waitAfter  int
}

func (s *countyBatchSegment) Descriptor() SegmentDescriptor {
	return SegmentDescriptor{ID: "segment", Idempotency: Idempotent, CompatibilityVersion: "v1"}
}

func (s *countyBatchSegment) Recover(_ context.Context, in SegmentInput[string], info ResumeInfo) error {
	if s.store != nil {
		s.resumePart = s.store.load(in.SourceRecordID)
		s.store.appendRecovery(info.Reason, info.AttemptID)
	}
	return nil
}

func (s *countyBatchSegment) Process(ctx ProcessContext, in SegmentInput[string], out func(SegmentOutput[string]) error) (ProcessResult, error) {
	start := s.resumePart
	s.resumePart = 0
	for part := start; part < 4; part++ {
		if s.waitAfter > 0 && part == s.waitAfter {
			<-ctx.Done()
			return ProcessResult{}, ctx.Err()
		}
		payload := fmt.Sprintf("%s-part-%d", in.Payload, part+1)
		if err := out(SegmentOutput[string]{Payload: payload, Metadata: in.Metadata}); err != nil {
			return ProcessResult{}, err
		}
		if s.store != nil {
			s.store.save(in.SourceRecordID, part+1)
			s.store.appendProcessed(payload)
		}
	}
	return ProcessResult{Status: ProcessCompleted}, nil
}

func (*countyBatchSegment) Done(context.Context) error { return nil }

type namedPassthroughSegment struct {
	id string
}

func (s namedPassthroughSegment) Descriptor() SegmentDescriptor {
	return SegmentDescriptor{ID: SegmentID(s.id), Idempotency: Idempotent, CompatibilityVersion: "v1"}
}

func (s namedPassthroughSegment) Process(_ ProcessContext, in SegmentInput[string], out func(SegmentOutput[string]) error) (ProcessResult, error) {
	if err := out(SegmentOutput[string]{Payload: in.Payload, Metadata: in.Metadata}); err != nil {
		return ProcessResult{}, err
	}
	return ProcessResult{Status: ProcessCompleted}, nil
}

func (namedPassthroughSegment) Done(context.Context) error { return nil }

func buildSingleStagePlan(t *testing.T, segment any) *Plan {
	t.Helper()
	plan, err := NewBuilder().Through(segment).Build()
	if err != nil {
		t.Fatalf("build plan failed: %v", err)
	}
	return plan
}

func buildPlan(t *testing.T, segments ...any) *Plan {
	t.Helper()
	builder := NewBuilder()
	for _, segment := range segments {
		builder = builder.Through(segment)
	}
	plan, err := builder.Build()
	if err != nil {
		t.Fatalf("build plan failed: %v", err)
	}
	return plan
}

func cursorForIndex(t *testing.T, index int) []byte {
	t.Helper()
	cursor, err := json.Marshal(struct {
		Index int `json:"index"`
	}{Index: index})
	if err != nil {
		t.Fatalf("marshal cursor failed: %v", err)
	}
	return cursor
}

// Case: Run resumes from the latest durable source resume state instead of restarting from source zero.
func TestRunResumesFromDurableSourceResumeStateByDefault(t *testing.T) {
	t.Parallel()

	plan := buildSingleStagePlan(t, passthroughSegment{})
	pipelineID, err := planPipelineID(plan)
	if err != nil {
		t.Fatalf("plan pipeline id failed: %v", err)
	}

	runtime := NewInMemoryRuntime()
	if err := runtime.SaveSourceResumeState(context.Background(), SourceResumeState{
		PipelineID:   pipelineID,
		SourceCursor: cursorForIndex(t, 1),
		Paused:       false,
	}); err != nil {
		t.Fatalf("save source resume state failed: %v", err)
	}

	source := &stringSource{records: []string{"first", "second"}, blockIndex: -1}
	sink := &recordingSink{}
	engine, err := NewPullEngine[string, string](source, sink, plan, runtime)
	if err != nil {
		t.Fatalf("new pull engine failed: %v", err)
	}

	if err := engine.Run(context.Background()); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if len(sink.payloads) != 1 || sink.payloads[0] != "second" {
		t.Fatalf("expected only the durable tail record, got %v", sink.payloads)
	}
}

// Case: graceful cancellation persists paused source resume state and a later Run continues from the remaining source work.
func TestRunCancellationPersistsPausedSourceResumeStateAndContinues(t *testing.T) {
	t.Parallel()

	plan := buildSingleStagePlan(t, passthroughSegment{})
	pipelineID, err := planPipelineID(plan)
	if err != nil {
		t.Fatalf("plan pipeline id failed: %v", err)
	}

	runtime := NewInMemoryRuntime()
	block := make(chan struct{})
	blockEntered := make(chan struct{})
	source1 := &stringSource{
		records:      []string{"first", "second"},
		blockIndex:   1,
		blockUntil:   block,
		blockEntered: blockEntered,
	}
	sink1 := &recordingSink{notify: make(chan string, 1)}
	engine1, err := NewPullEngine[string, string](source1, sink1, plan, runtime)
	if err != nil {
		t.Fatalf("new first engine failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() {
		runErr <- engine1.Run(ctx)
	}()

	<-sink1.notify
	<-blockEntered
	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("expected graceful pause on cancellation, got %v", err)
	}

	resumeState, ok, err := runtime.LoadSourceResumeState(context.Background(), pipelineID)
	if err != nil {
		t.Fatalf("load source resume state failed: %v", err)
	}
	if !ok || !resumeState.Paused {
		t.Fatalf("expected paused source resume state, got ok=%v state=%+v", ok, resumeState)
	}

	source2 := &stringSource{records: []string{"first", "second"}, blockIndex: -1}
	sink2 := &recordingSink{}
	engine2, err := NewPullEngine[string, string](source2, sink2, plan, runtime)
	if err != nil {
		t.Fatalf("new resumed engine failed: %v", err)
	}
	if err := engine2.Run(context.Background()); err != nil {
		t.Fatalf("resume-by-run failed: %v", err)
	}
	if len(sink2.payloads) != 1 || sink2.payloads[0] != "second" {
		t.Fatalf("expected resumed tail record only, got %v", sink2.payloads)
	}
	close(block)
}

// Case: crash replay compensates a recovered non-idempotent stage and then reprocesses the interrupted record.
func TestCrashRecoveryCompensatesRecoveredNonIdempotentStage(t *testing.T) {
	t.Parallel()

	state := &crashSegmentState{}
	started := make(chan struct{})
	plan1 := buildSingleStagePlan(t, &blockingNonIdempotentSegment{
		started: started,
		release: make(chan struct{}),
		state:   state,
	})
	runtime := NewInMemoryRuntime()
	source1 := &stringSource{records: []string{"only"}, blockIndex: -1}
	sink1 := &recordingSink{}
	engine1, err := NewPullEngine[string, string](source1, sink1, plan1, runtime)
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
		t.Fatalf("expected crash-style cancellation during processing")
	}

	release := make(chan struct{})
	close(release)
	plan2 := buildSingleStagePlan(t, &blockingNonIdempotentSegment{
		release: release,
		state:   state,
	})
	source2 := &stringSource{records: []string{"only"}, blockIndex: -1}
	sink2 := &recordingSink{}
	engine2, err := NewPullEngine[string, string](source2, sink2, plan2, runtime)
	if err != nil {
		t.Fatalf("new recovery engine failed: %v", err)
	}
	if err := engine2.Run(context.Background()); err != nil {
		t.Fatalf("recovery run failed: %v", err)
	}

	state.mu.Lock()
	compensations := state.compensations
	processes := state.processes
	recoveries := state.recoveries
	reasons := append([]ResumeReason(nil), state.reasons...)
	recoverAttempts := append([]AttemptID(nil), state.recoverAttempts...)
	compensateAttempts := append([]AttemptID(nil), state.compensateAttempts...)
	events := append([]string(nil), state.events...)
	state.mu.Unlock()
	if compensations != 1 {
		t.Fatalf("expected one compensation on crash recovery, got %d", compensations)
	}
	if recoveries != 1 {
		t.Fatalf("expected one recover call on crash recovery, got %d", recoveries)
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
	if processes < 2 {
		t.Fatalf("expected recovered stage to be processed again, got %d processes", processes)
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
	if len(sink2.payloads) != 1 || sink2.payloads[0] != "only" {
		t.Fatalf("expected recovered output, got %v", sink2.payloads)
	}
	if len(sink2.items) != 1 || sink2.items[0].AttemptID != 2 {
		t.Fatalf("expected recovered sink attempt id 2, got %+v", sink2.items)
	}
}

// Case: Restart clears framework-owned runtime state and replays the pipeline from the source beginning.
func TestRestartClearsRuntimeStateAndStartsFromBeginning(t *testing.T) {
	t.Parallel()

	plan := buildSingleStagePlan(t, passthroughSegment{})
	runtime := NewInMemoryRuntime()
	source := &stringSource{records: []string{"a", "b"}, blockIndex: -1}
	sink := &recordingSink{}
	engine, err := NewPullEngine[string, string](source, sink, plan, runtime)
	if err != nil {
		t.Fatalf("new engine failed: %v", err)
	}

	if err := engine.Run(context.Background()); err != nil {
		t.Fatalf("initial run failed: %v", err)
	}
	if err := engine.Restart(context.Background()); err != nil {
		t.Fatalf("restart failed: %v", err)
	}
	if len(sink.payloads) != 4 {
		t.Fatalf("expected restart to replay from source zero, got %v", sink.payloads)
	}
	want := []string{"a", "b", "a", "b"}
	for i := range want {
		if sink.payloads[i] != want[i] {
			t.Fatalf("unexpected restart payload order: %v", sink.payloads)
		}
	}
}

// Case: Restart runs the configured restart hook after runtime reset and before replay from source zero.
func TestRestartInvokesHookAfterResetBeforeReplay(t *testing.T) {
	t.Parallel()

	plan := buildSingleStagePlan(t, passthroughSegment{})
	runtime := NewInMemoryRuntime()
	source := &stringSource{records: []string{"a", "b"}, blockIndex: -1}
	sink := &recordingSink{}

	engine, err := NewPullEngine[string, string](
		source,
		sink,
		plan,
		runtime,
		WithOnRestart(func(ctx context.Context, pipelineID string) error {
			resumeState, ok, err := runtime.LoadSourceResumeState(ctx, pipelineID)
			if err != nil {
				t.Fatalf("load source resume state in restart hook failed: %v", err)
			}
			if ok {
				t.Fatalf("expected source resume state to be cleared before restart hook, got %+v", resumeState)
			}

			started, err := runtime.SourceRecords(ctx, pipelineID)
			if err != nil {
				t.Fatalf("load source records in restart hook failed: %v", err)
			}
			if len(started) != 0 {
				t.Fatalf("expected source records to be cleared before restart hook, got %+v", started)
			}

			if len(sink.payloads) != 2 {
				t.Fatalf("expected replay not to have started before restart hook, got sink payloads %v", sink.payloads)
			}
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("new engine failed: %v", err)
	}

	if err := engine.Run(context.Background()); err != nil {
		t.Fatalf("initial run failed: %v", err)
	}
	if err := engine.Restart(context.Background()); err != nil {
		t.Fatalf("restart failed: %v", err)
	}

	want := []string{"a", "b", "a", "b"}
	if len(sink.payloads) != len(want) {
		t.Fatalf("expected %d sink payloads, got %v", len(want), sink.payloads)
	}
	for i := range want {
		if sink.payloads[i] != want[i] {
			t.Fatalf("unexpected restart payload order: %v", sink.payloads)
		}
	}
}

// Case: a restart hook failure aborts replay after runtime reset and returns the hook error.
func TestRestartHookErrorAbortsReplay(t *testing.T) {
	t.Parallel()

	plan := buildSingleStagePlan(t, passthroughSegment{})
	runtime := NewInMemoryRuntime()
	source := &stringSource{records: []string{"a", "b"}, blockIndex: -1}
	sink := &recordingSink{}
	restartErr := errors.New("restart hook failed")

	engine, err := NewPullEngine[string, string](
		source,
		sink,
		plan,
		runtime,
		WithOnRestart(func(context.Context, string) error {
			return restartErr
		}),
	)
	if err != nil {
		t.Fatalf("new engine failed: %v", err)
	}

	if err := engine.Run(context.Background()); err != nil {
		t.Fatalf("initial run failed: %v", err)
	}
	if err := engine.Restart(context.Background()); !errors.Is(err, restartErr) {
		t.Fatalf("expected restart hook error %v, got %v", restartErr, err)
	}

	if len(sink.payloads) != 2 {
		t.Fatalf("expected replay to be aborted after hook failure, got %v", sink.payloads)
	}

	pipelineID, err := planPipelineID(plan)
	if err != nil {
		t.Fatalf("plan pipeline id failed: %v", err)
	}
	if resumeState, ok, err := runtime.LoadSourceResumeState(context.Background(), pipelineID); err != nil {
		t.Fatalf("load source resume state after hook failure failed: %v", err)
	} else if ok {
		t.Fatalf("expected source resume state to remain cleared after failed restart hook, got %+v", resumeState)
	}
}

// Case: deterministic cache reuse skips repeated execution while still producing normal downstream outputs.
func TestDeterministicCacheSkipsRepeatedExecution(t *testing.T) {
	t.Parallel()

	segment := &countingDeterministicSegment{}
	plan := buildSingleStagePlan(t, segment)
	source := &recordSource{
		records: []sourceItem{
			{recordID: "record-1", payload: "same"},
			{recordID: "record-2", payload: "same"},
		},
	}
	sink := &recordingSink{}
	runtime := NewInMemoryRuntime()

	engine, err := NewPullEngine[string, string](source, sink, plan, runtime)
	if err != nil {
		t.Fatalf("new pull engine failed: %v", err)
	}
	if err := engine.Run(context.Background()); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if got := len(sink.payloads); got != 2 {
		t.Fatalf("expected 2 sink payloads, got %d", got)
	}
	if sink.payloads[0] != "same" || sink.payloads[1] != "same" {
		t.Fatalf("unexpected sink payloads: %+v", sink.payloads)
	}
	segment.mu.Lock()
	processes := segment.processes
	recovers := segment.recovers
	segment.mu.Unlock()
	if processes != 1 {
		t.Fatalf("expected deterministic segment to execute once, got %d", processes)
	}
	if recovers != 0 {
		t.Fatalf("expected deterministic cache path not to invoke recover, got %d", recovers)
	}
}

// Case: a cooperatively paused segment receives Recover with a pause reason before processing resumes.
func TestPauseResumeInvokesRecoverHook(t *testing.T) {
	t.Parallel()

	store := &pauseRecoveryStore{}
	segment1 := &pausingSegment{started: make(chan struct{}), store: store}
	plan := buildSingleStagePlan(t, segment1)
	runtime := NewInMemoryRuntime()
	source1 := &stringSource{records: []string{"county-a"}, blockIndex: -1}
	sink1 := &recordingSink{}
	engine1, err := NewPullEngine(source1, sink1, plan, runtime)
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
	source2 := &stringSource{records: []string{"county-a"}, blockIndex: -1}
	sink2 := &recordingSink{}
	engine2, err := NewPullEngine(source2, sink2, buildSingleStagePlan(t, segment2), runtime)
	if err != nil {
		t.Fatalf("new resumed engine failed: %v", err)
	}
	if err := engine2.Resume(context.Background()); err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	if len(sink2.payloads) != 1 || sink2.payloads[0] != "county-a" {
		t.Fatalf("expected resumed payload, got %v", sink2.payloads)
	}
	if len(store.reasons) != 1 || store.reasons[0] != ResumeAfterPause {
		t.Fatalf("expected one pause recovery reason, got %v", store.reasons)
	}
	if len(store.attempts) != 1 || store.attempts[0] != 2 {
		t.Fatalf("expected pause recovery attempt id 2, got %v", store.attempts)
	}
	if len(sink2.items) != 1 || sink2.items[0].AttemptID != 2 {
		t.Fatalf("expected resumed sink attempt id 2, got %+v", sink2.items)
	}
}

// Case: a resumed segment can use application-owned state to continue partial work from the next unfinished part.
func TestRecoverHookResumesCountyBatchFromAppState(t *testing.T) {
	t.Parallel()

	store := &countyRecoveryStore{}
	segment1 := &countyBatchSegment{store: store, waitAfter: 2}
	plan1 := buildSingleStagePlan(t, segment1)
	runtime := NewInMemoryRuntime()
	source1 := &stringSource{records: []string{"county-1"}, blockIndex: -1}
	sink1 := &recordingSink{notify: make(chan string, 4)}
	engine1, err := NewPullEngine(source1, sink1, plan1, runtime)
	if err != nil {
		t.Fatalf("new first engine failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() {
		runErr <- engine1.Run(ctx)
	}()

	<-sink1.notify
	<-sink1.notify
	cancel()
	if err := <-runErr; err == nil {
		t.Fatalf("expected interrupted run after partial batch emission")
	}

	segment2 := &countyBatchSegment{store: store}
	plan2 := buildSingleStagePlan(t, segment2)
	source2 := &stringSource{records: []string{"county-1"}, blockIndex: -1}
	sink2 := &recordingSink{}
	engine2, err := NewPullEngine(source2, sink2, plan2, runtime)
	if err != nil {
		t.Fatalf("new recovery engine failed: %v", err)
	}
	if err := engine2.Run(context.Background()); err != nil {
		t.Fatalf("recovery run failed: %v", err)
	}

	want := []string{"county-1-part-3", "county-1-part-4"}
	if len(sink2.payloads) != len(want) {
		t.Fatalf("expected only remaining parts on recovery, got %v", sink2.payloads)
	}
	for i := range want {
		if sink2.payloads[i] != want[i] {
			t.Fatalf("expected remaining parts %v, got %v", want, sink2.payloads)
		}
	}
	if got := store.load("county-1"); got != 4 {
		t.Fatalf("expected app state to advance to part 4, got %d", got)
	}
	if len(store.reasons) != 1 || store.reasons[0] != ResumeAfterCrash {
		t.Fatalf("expected one crash recovery reason, got %v", store.reasons)
	}
	if len(store.attempts) != 1 || store.attempts[0] != 2 {
		t.Fatalf("expected county recovery attempt id 2, got %v", store.attempts)
	}
	if len(sink2.items) != len(want) {
		t.Fatalf("expected sink items for remaining parts, got %+v", sink2.items)
	}
	for _, item := range sink2.items {
		if item.AttemptID != 2 {
			t.Fatalf("expected recovered county outputs to use attempt id 2, got %+v", sink2.items)
		}
	}
}

// Case: resume rejects queued work whose next segment no longer matches the current plan order.
func TestResumeRejectsQueuedWorkThatDoesNotMatchCurrentPlan(t *testing.T) {
	t.Parallel()

	plan := buildPlan(t,
		namedPassthroughSegment{id: "segment-a"},
		namedPassthroughSegment{id: "segment-c"},
		namedPassthroughSegment{id: "segment-b"},
	)
	pipelineID, err := planPipelineID(plan)
	if err != nil {
		t.Fatalf("plan pipeline id failed: %v", err)
	}

	runtime := NewInMemoryRuntime()
	item := Envelope[json.RawMessage]{
		OriginRecordID: "rec-1",
		RecordID:       "rec-1/segment-a/1",
		AttemptID:      1,
		ParentIDs:      []RecordID{"rec-1"},
		SegmentPath:    []SegmentID{"segment-a"},
		Payload:        json.RawMessage(`"payload"`),
	}
	if err := runtime.CommitSourceProgress(context.Background(), SourceProgress{
		ResumeState: SourceResumeState{
			PipelineID:   pipelineID,
			SourceCursor: cursorForIndex(t, 1),
			Paused:       true,
		},
		Started: StartedRecord{
			PipelineID: pipelineID,
			Item:       item,
		},
		PendingWork: PendingSegmentWork{
			PipelineID:    pipelineID,
			NextSegmentID: "segment-b",
			Item:          item,
		},
	}); err != nil {
		t.Fatalf("commit source progress failed: %v", err)
	}

	source := &stringSource{records: []string{"unused"}, blockIndex: -1}
	sink := &recordingSink{}
	engine, err := NewPullEngine[string, string](source, sink, plan, runtime)
	if err != nil {
		t.Fatalf("new engine failed: %v", err)
	}
	if err := engine.Resume(context.Background()); !errors.Is(err, ErrBuilderResumePlanMismatch) {
		t.Fatalf("expected resume plan mismatch, got %v", err)
	}
}

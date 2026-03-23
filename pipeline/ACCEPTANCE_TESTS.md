# Pipeline Acceptance Tests

This file is a living checklist of the externally meaningful guarantees we expect from the Manifold pipeline engine.

The goal is to keep these cases easy to review with product and architecture discussions, and easy to map back to executable tests. Add new cases here whenever we introduce a new invariant, lifecycle guarantee, or recovery rule.

## Lifecycle And Recovery

- `AT-001 Run Resumes By Default`
  Scenario: durable source progress already exists for a pipeline, then `Run()` is called.
  Expectation: execution resumes from the latest durable source cursor instead of restarting from source zero.
  Current automated coverage: `TestRunResumesFromDurableSourceResumeStateByDefault`.

- `AT-002 Resume Requires Durable State`
  Scenario: `Resume()` is called for a pipeline with no durable recovery state.
  Expectation: the engine rejects the call instead of silently starting a fresh run.
  Current automated coverage: not called out by a dedicated acceptance-style test yet.

- `AT-003 Graceful Pause Persists Resumable Work`
  Scenario: a segment cooperatively pauses after a pause request.
  Expectation: the engine durably stores the source resume state plus queued segment work and emitted-progress count needed for a later resume, and resumed segment work can reload application-owned state through the recovery hook.
  Current automated coverage: `TestEngineSpec_PausePersistsAndResumeContinues`, `TestPauseResumeInvokesRecoverHook`.

- `AT-004 Cancellation During Source Pull Becomes Paused Work`
  Scenario: the process is cancelled while the engine is between committed source items.
  Expectation: the engine persists paused source resume state and a later `Run()` continues with the remaining source work only.
  Current automated coverage: `TestRunCancellationPersistsPausedSourceResumeStateAndContinues`.

- `AT-005 Abrupt Termination Is Recoverable`
  Scenario: the process stops during segment work without a graceful pause.
  Expectation: a later `Run()` reloads durable pending work and per-segment emitted progress, classifies the work as crash recovery, and resumes the interrupted work without losing already emitted successors.
  Current automated coverage: `TestCrashRecoveryCompensatesRecoveredNonIdempotentStage`, `TestRecoverHookResumesCountyBatchFromAppState`.

- `AT-005A Resume Rejects Incompatible Plan Order`
  Scenario: queued work was persisted for a different stage ordering than the current plan.
  Expectation: resume fails fast instead of routing the queued work to the wrong segment.
  Current automated coverage: `TestResumeRejectsQueuedWorkThatDoesNotMatchCurrentPlan`.

- `AT-006 Restart Replays From Source Zero`
  Scenario: a pipeline has durable runtime state and `Restart()` is called.
  Expectation: framework-owned runtime state is cleared and the pipeline replays from the source beginning.
  Current automated coverage: `TestRestartClearsRuntimeStateAndStartsFromBeginning`.

- `AT-006A Restart Hook Runs After Reset Before Replay`
  Scenario: a pipeline configures a restart hook and `Restart()` is called.
  Expectation: the hook runs after manifold-owned runtime state is cleared and before source replay begins.
  Current automated coverage: `TestRestartInvokesHookAfterResetBeforeReplay`.

- `AT-006B Restart Hook Failure Aborts Replay`
  Scenario: a configured restart hook returns an error during `Restart()`.
  Expectation: replay does not begin and the hook error is returned to the caller.
  Current automated coverage: `TestRestartHookErrorAbortsReplay`.

## Compensation And Idempotency

- `AT-007 Non-Idempotent Crash Replay Compensates Before Reprocessing`
  Scenario: a non-idempotent segment is interrupted and later recovered.
  Expectation: its compensator is invoked before the recovered input is reloaded through the recovery hook and processed again.
  Current automated coverage: `TestCrashRecoveryCompensatesRecoveredNonIdempotentStage`, `TestEngineSpec_CrashRecoveryCompensatesNonIdempotentSegment`.

- `AT-008 Non-Idempotent Segments Must Provide Compensation`
  Scenario: a non-idempotent segment is added to a plan without a compensator.
  Expectation: plan validation rejects the segment shape.
  Current automated coverage: `TestValidateSegmentContract`.

## Durable Invariants

- `AT-009 No Segment Appears Durably Completed Without Successor-Recovery State`
  Scenario: a segment completes and emits outputs, then the process crashes before the successor runs.
  Expectation: every durably accepted output already has successor work durably queued before the segment can appear completed.
  Current automated coverage: partially covered by crash-recovery tests; should gain a more explicit acceptance-style test.

- `AT-010 Source Progress And Started Record Advance Atomically`
  Scenario: a source item is durably admitted into the pipeline.
  Expectation: the source cursor and started-record lineage are committed together so recovery never loses admitted work.
  Current automated coverage: exercised through `TestRuntimeContractShape`; should gain a focused engine-level acceptance test.

- `AT-011 Progress And Queue Writes Are Atomic`
  Scenario: a segment emit or completion updates progress, persists outputs, queues successor work, or writes a terminal record.
  Expectation: each emit/completion boundary succeeds or fails atomically so recovery never observes output progress without the matching durable queue state.
  Current automated coverage: exercised through `TestRuntimeContractShape`; should gain a focused adapter-level acceptance test.

## Deterministic Reuse And Compatibility

- `AT-012 Deterministic Reuse Skips Repeat Execution`
  Scenario: the same deterministic segment sees the same logical input more than once.
  Expectation: the engine reuses the durable deterministic result instead of calling `Process()` again.
  Current automated coverage: `TestDeterministicCacheSkipsRepeatedExecution`.

- `AT-013 Cache Hits Still Produce Normal Lineage`
  Scenario: a deterministic cache hit occurs.
  Expectation: the engine durably re-materializes normal downstream work and final completed progress for the current lineage rather than a fake in-memory-only state, without invoking segment recovery when no real execution occurs.
  Current automated coverage: indirectly covered by `TestDeterministicCacheSkipsRepeatedExecution`; should gain a more explicit lineage-focused assertion.

- `AT-014 CompatibilityVersion Prevents Unsafe Reuse`
  Scenario: a segment's `CompatibilityVersion` changes after prior runs have persisted progress or deterministic outputs.
  Expectation: old durable progress and cached outputs are not reused for the new version.
  Current automated coverage: not covered by a dedicated test yet.

- `AT-015 Input Checksum Prevents Reusing Results For Different Logical Input`
  Scenario: a deterministic segment sees a changed payload or metadata.
  Expectation: previously cached outputs are not reused.
  Current automated coverage: not covered by a dedicated test yet.

## Traceability

- `AT-016 Trace Returns Zero To Many Terminal Outputs`
  Scenario: a source record fans out to zero, one, or many terminal outputs.
  Expectation: `Trace()` returns every terminal output associated with the source identity, including the zero-output case.
  Current automated coverage: `TestEngineSpec_TraceSourceToZeroOrManyOutputs`, `TestEnvelopeTraceabilityFanOutShape`.

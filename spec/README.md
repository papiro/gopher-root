# Manifold Spec

`manifold` defines its cross-language contract in versioned Protobuf files under `spec/manifold/v1/`.
The Go codebase is the reference implementation of that spec, and public Go pipeline contracts
should remain aligned with the schema and semantic notes here.

## Layout

- `spec/manifold/v1/types.proto`: shared identifiers, payload carriers, descriptors, and enums.
- `spec/manifold/v1/topology.proto`: compiled topology plus a provisional builder-level plan model
  for future topology features.
- `spec/manifold/v1/runtime.proto`: source resume state, durable queued work, per-segment progress, persisted outputs, and trace records.

## Versioning

The initial package is `manifold.v1`. Additive changes should remain backward-compatible within
`v1`; breaking contract changes should move to a new versioned package.

## Encoding Conventions

- Business payloads use `google.protobuf.Value` because the current Go contracts already treat
  segment boundaries and stored outputs as JSON-shaped data.
- Source cursors use `bytes` because they are opaque implementation-owned resume tokens rather than
  shared business payloads.
- Metadata remains `map<string, string>` to match the existing Go contracts.

## Normative Semantics

- `SourceRecord.record_id` is the stable source identity for one source-emitted business record.
- `SegmentInput.source_record_id` is that same stable source identity propagated unchanged across
  segments.
- `SegmentOutput` carries only business output data. The engine, not segment user code, owns
  lineage-node identity and source-identity propagation for emitted outputs.
- `Envelope.record_id` identifies a specific lineage node and is distinct from
  `Envelope.origin_record_id`, which preserves the stable source identity end-to-end.
- `attempt_id` starts at 1 for a new source lineage and increases when that lineage is retried or replayed.
- `Trace` is part of the contract: implementations must support zero-to-many terminal outputs per
  source identity and preserve lineage metadata needed for explainability.
- `Run` resumes from the latest durable recovery boundary when one exists. `Resume` remains an
  explicit lifecycle operation that requires durable runtime state. `Restart` is the explicit
  same-plan replay path that first clears framework-owned runtime state and may run an
  implementation-configured restart hook before replay begins.
- Pause and resume are cooperative. Source resume state captures source position; queued work rows plus
  per-segment emitted progress capture the remaining framework-owned work.
- The runtime contract does not define framework-managed segment snapshots or a `Restore(...)` continuation
  hook. Mid-segment recovery is expressed through durable emitted progress plus segment-owned app state
  reloaded in `Recover(...)`.
- When queued work is resumed, the framework classifies the cause as pause-resume or crash-recovery
  and may call a segment recovery hook before `Process` so the segment can reload any
  application-owned cursor or progress it needs to continue.
- Queued work names the next segment to run and must still match the current plan order when
  resumed; incompatible queued work is rejected instead of being routed to the wrong stage.
- Source records, pending work rows, segment progress, segment outputs, and terminal outputs must be
  sufficient to resume every started lineage record after graceful pause or crash recovery.
- Non-idempotent crash recovery must compensate before the resumed segment input is recovered and
  processed again.
- No segment may appear durably completed unless all state required to resume from its successor is
  already durable.
- `CompatibilityVersion` is segment-owned and changes only when previously persisted progress or
  outputs are no longer safe to reuse.
- Deterministic reuse is keyed by segment identity, `CompatibilityVersion`, and a checksum of the
  logical input payload plus metadata. Cache hits must still materialize normal durable successor
  work and completed progress for the current lineage.
- `AckStatus`, `IdempotencyKind`, and `ProcessStatus` are normative because retry, replay, and
  compensation behavior depend on them.

## Provisional Future Model

`PipelinePlan`, `StageOperation`, `TopologyModifier`, `Broadcast`, `Parallelism`, and `Partition`
are included to reserve a shared vocabulary for the broader future model. The current Go runtime
does not yet execute those features end-to-end, so these messages are provisional schema surface,
not evidence of full runtime support.

## Crosswalk

- `types.proto` maps to `pipeline/contract.go`, `pipeline/types/identity.go`,
  `pipeline/types/source_record.go`,
  `pipeline/types/envelope.go`, `pipeline/types/descriptor.go`,
  `pipeline/types/idempotency.go`, and `pipeline/types/acks.go`.
- `topology.proto` maps to `pipeline/types/topology.go`, `pipeline/builder_api.go`,
  `pipeline/golden_example/doc.go`, and `pipeline/builder_contract_test.go`.
- `runtime.proto` maps to `pipeline/interfaces/runtime.go`, `pipeline/interfaces/engine.go`,
  `pipeline/interfaces/io.go`, `pipeline/interfaces/segment.go`,
  `pipeline/spec_engine_test.go`, and `pipeline/contract_test.go`.

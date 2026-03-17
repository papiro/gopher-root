# Manifold Spec

`manifold` defines its cross-language contract in versioned Protobuf files under `spec/manifold/v1/`.
The Go codebase is the reference implementation of that spec, and public Go pipeline contracts
should remain aligned with the schema and semantic notes here.

## Layout

- `spec/manifold/v1/types.proto`: shared identifiers, payload carriers, descriptors, and enums.
- `spec/manifold/v1/topology.proto`: compiled topology plus a provisional builder-level plan model
  for future topology features.
- `spec/manifold/v1/runtime.proto`: checkpoints, durable acks, persisted outputs, and trace records.

## Versioning

The initial package is `manifold.v1`. Additive changes should remain backward-compatible within
`v1`; breaking contract changes should move to a new versioned package.

## Encoding Conventions

- Business payloads use `google.protobuf.Value` because the current Go contracts already treat
  segment boundaries and stored outputs as JSON-shaped data.
- Source cursors and segment snapshots use `bytes` because they are opaque, implementation-owned
  resume tokens rather than shared business payloads.
- Metadata remains `map<string, string>` to match the existing Go contracts.

## Normative Semantics

- `record_id` on `SourceRecord` and `SegmentRecord` is the stable source identity propagated
  unchanged across segments.
- `Envelope.record_id` identifies a specific lineage node and is distinct from
  `Envelope.origin_record_id`, which preserves the stable source identity end-to-end.
- `attempt_id` increases when a logical record lineage is retried or replayed.
- `Trace` is part of the contract: implementations must support zero-to-many terminal outputs per
  source identity and preserve lineage metadata needed for explainability.
- Pause and resume are cooperative. Checkpoints capture source position and in-flight frontier
  state; segment snapshots capture only remaining uncommitted work.
- `AckStatus`, `IdempotencyKind`, and `ProcessStatus` are normative because retry, replay, and
  compensation behavior depend on them.

## Provisional Future Model

`PipelinePlan`, `StageOperation`, `TopologyModifier`, `Broadcast`, `Parallelism`, and `Partition`
are included to reserve a shared vocabulary for the broader future model. The current Go runtime
does not yet execute those features end-to-end, so these messages are provisional schema surface,
not evidence of full runtime support.

## Crosswalk

- `types.proto` maps to `pipeline/contract.go`, `pipeline/types/identity.go`,
  `pipeline/types/source_record.go`, `pipeline/types/segment_record.go`,
  `pipeline/types/envelope.go`, `pipeline/types/descriptor.go`,
  `pipeline/types/idempotency.go`, and `pipeline/types/acks.go`.
- `topology.proto` maps to `pipeline/types/topology.go`, `pipeline/builder_api.go`,
  `pipeline/golden_example/doc.go`, and `pipeline/builder_contract_test.go`.
- `runtime.proto` maps to `pipeline/interfaces/runtime.go`, `pipeline/interfaces/engine.go`,
  `pipeline/interfaces/io.go`, `pipeline/interfaces/segment.go`,
  `pipeline/spec_engine_test.go`, and `pipeline/contract_test.go`.

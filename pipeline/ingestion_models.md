# Ingestion Models: Pull vs Push

This package supports two ingress styles:

- `Source[T]` (pull): engine asks for the next item.
- `StreamSource[T]` (push): source emits items on a channel.

Both can be correct and efficient. The difference is who owns pacing at ingress.

## Pull (`Source`)

Use pull when the engine should control demand explicitly.

```go
type Source[T any] interface {
    Next(ctx context.Context) (SourceRecord[T], bool, error)
}

for {
    record, ok, err := source.Next(ctx)
    if err != nil { return err }
    if !ok { break } // end-of-stream
    // segments see only the stable source identity plus business payload
    in := SegmentInput[T]{
        SourceRecordID: record.RecordID,
        Payload:        record.Payload,
        Metadata:       record.Metadata,
    }
    _ = in // process one item
}
```

### Pull best practices

- Implement `Next(ctx)` as a blocking call when no data is available.
- Always honor `ctx.Done()` and return quickly on cancellation.
- Return `(zero, false, nil)` only for true end-of-stream.
- Avoid busy polling loops in `Next` implementations.
- If backed by polling systems, use backoff/timers and context-aware waits.

## Push (`StreamSource`)

Use push when input is naturally event-driven and emits as events arrive.

```go
type StreamSource[T any] interface {
    Stream(ctx context.Context) <-chan SourceRecord[T]
}

for {
    select {
    case <-ctx.Done():
        return ctx.Err()
    case record, ok := <-source.Stream(ctx):
        if !ok { return nil } // source completed
        // engine converts source records into segment input records before processing
        _ = record
    }
}
```

### Push best practices

- Close the stream channel exactly once when the source is complete.
- Select on `ctx.Done()` in producer and consumer loops.
- Use bounded channels to apply explicit back-pressure.
- Do not silently drop messages; if dropping is policy, make it explicit and observable.
- Avoid "one goroutine per message" send patterns under load.

## Minimal comparison

- Pull: `engine -> source` request (`Next`) drives intake.
- Push: `source -> engine` emission (`Stream`) drives intake.
- Both can block under downstream pressure when implemented with bounded buffers.
- Idle CPU should be near zero in both models if operations block instead of poll.

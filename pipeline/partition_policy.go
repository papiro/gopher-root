package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
)

var (
	ErrPartitionPolicyPartitionCountPositive = errors.New("partition policy requires partitions greater than zero")
	ErrPartitionMetadataKeyRequired          = errors.New("partition metadata key is required")
	ErrPartitionMetadataValueMissing         = errors.New("partition metadata key is missing")
	ErrPartitionKeyFuncRequired              = errors.New("partition key function is required")
)

// PartitionPolicy routes one normalized runtime record to exactly one partition.
//
// Policies should return an index in the range [0, partitions). Deterministic policies
// should route equivalent logical inputs to the same partition across retries/resume.
type PartitionPolicy interface {
	Partition(record Envelope[json.RawMessage], partitions int) (int, error)
}

type roundRobinPartitionPolicy struct {
	mu   sync.Mutex
	next uint64
}

// RoundRobin returns a stateful policy that rotates records across partitions in order.
//
// This policy is useful for worker distribution but is not deterministic across retries
// or resume boundaries, so it is a better fit for `Parallelism` than keyed partitioning.
func RoundRobin() PartitionPolicy {
	return &roundRobinPartitionPolicy{}
}

func (p *roundRobinPartitionPolicy) Partition(_ Envelope[json.RawMessage], partitions int) (int, error) {
	if partitions < 1 {
		return 0, ErrPartitionPolicyPartitionCountPositive
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	idx := int(p.next % uint64(partitions))
	p.next++
	return idx, nil
}

type byHashPartitionPolicy struct {
	key func(Envelope[json.RawMessage]) (string, error)
}

// ByRecordID deterministically maps records to partitions using `RecordID`.
func ByRecordID() PartitionPolicy {
	return byHashPartitionPolicy{
		key: func(record Envelope[json.RawMessage]) (string, error) {
			return string(record.RecordID), nil
		},
	}
}

// ByMetadataKey deterministically maps records to partitions using one metadata value.
func ByMetadataKey(key string) PartitionPolicy {
	return byHashPartitionPolicy{
		key: func(record Envelope[json.RawMessage]) (string, error) {
			if key == "" {
				return "", ErrPartitionMetadataKeyRequired
			}
			if record.Metadata == nil {
				return "", fmt.Errorf("%w: %q", ErrPartitionMetadataValueMissing, key)
			}
			value, ok := record.Metadata[key]
			if !ok {
				return "", fmt.Errorf("%w: %q", ErrPartitionMetadataValueMissing, key)
			}
			return value, nil
		},
	}
}

// ByKey deterministically maps records to partitions by decoding payload JSON into `T`
// and hashing the key returned by `fn`.
func ByKey[T any](fn func(T) string) PartitionPolicy {
	return byHashPartitionPolicy{
		key: func(record Envelope[json.RawMessage]) (string, error) {
			if fn == nil {
				return "", ErrPartitionKeyFuncRequired
			}

			var payload T
			if err := json.Unmarshal(record.Payload, &payload); err != nil {
				return "", err
			}
			return fn(payload), nil
		},
	}
}

func (p byHashPartitionPolicy) Partition(record Envelope[json.RawMessage], partitions int) (int, error) {
	if partitions < 1 {
		return 0, ErrPartitionPolicyPartitionCountPositive
	}

	key, err := p.key(record)
	if err != nil {
		return 0, err
	}

	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(key))
	return int(hasher.Sum64() % uint64(partitions)), nil
}

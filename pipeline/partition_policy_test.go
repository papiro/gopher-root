package pipeline_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/pierre/manifold/pipeline"
)

func TestRoundRobinPolicyRotatesAcrossPartitions(t *testing.T) {
	t.Parallel()

	policy := pipeline.RoundRobin()
	record := pipeline.Envelope[json.RawMessage]{}

	got := make([]int, 4)
	for i := range got {
		idx, err := policy.Partition(record, 3)
		if err != nil {
			t.Fatalf("partition failed: %v", err)
		}
		got[i] = idx
	}

	want := []int{0, 1, 2, 0}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected round robin sequence: got=%v want=%v", got, want)
		}
	}
}

func TestByRecordIDPolicyIsStable(t *testing.T) {
	t.Parallel()

	policy := pipeline.ByRecordID()
	record := pipeline.Envelope[json.RawMessage]{RecordID: "rec-42"}

	left, err := policy.Partition(record, 8)
	if err != nil {
		t.Fatalf("first partition failed: %v", err)
	}
	right, err := policy.Partition(record, 8)
	if err != nil {
		t.Fatalf("second partition failed: %v", err)
	}

	if left != right {
		t.Fatalf("expected stable partition, got %d and %d", left, right)
	}
	if left < 0 || left >= 8 {
		t.Fatalf("partition out of range: %d", left)
	}
}

func TestByMetadataKeyPolicyUsesMetadataValue(t *testing.T) {
	t.Parallel()

	policy := pipeline.ByMetadataKey("customer_id")
	record := pipeline.Envelope[json.RawMessage]{
		Metadata: map[string]string{"customer_id": "cust-7"},
	}

	left, err := policy.Partition(record, 4)
	if err != nil {
		t.Fatalf("first partition failed: %v", err)
	}
	right, err := policy.Partition(record, 4)
	if err != nil {
		t.Fatalf("second partition failed: %v", err)
	}

	if left != right {
		t.Fatalf("expected stable partition, got %d and %d", left, right)
	}
	if left < 0 || left >= 4 {
		t.Fatalf("partition out of range: %d", left)
	}
}

func TestByMetadataKeyPolicyRejectsMissingMetadata(t *testing.T) {
	t.Parallel()

	policy := pipeline.ByMetadataKey("customer_id")
	_, err := policy.Partition(pipeline.Envelope[json.RawMessage]{}, 4)
	if !errors.Is(err, pipeline.ErrPartitionMetadataValueMissing) {
		t.Fatalf("expected ErrPartitionMetadataValueMissing, got %v", err)
	}
}

func TestByKeyPolicyDecodesPayload(t *testing.T) {
	t.Parallel()

	type customerEvent struct {
		CustomerID string `json:"customer_id"`
	}

	policy := pipeline.ByKey(func(in customerEvent) string {
		return in.CustomerID
	})

	record := pipeline.Envelope[json.RawMessage]{
		Payload: json.RawMessage(`{"customer_id":"cust-9"}`),
	}

	left, err := policy.Partition(record, 16)
	if err != nil {
		t.Fatalf("first partition failed: %v", err)
	}
	right, err := policy.Partition(record, 16)
	if err != nil {
		t.Fatalf("second partition failed: %v", err)
	}

	if left != right {
		t.Fatalf("expected stable partition, got %d and %d", left, right)
	}
	if left < 0 || left >= 16 {
		t.Fatalf("partition out of range: %d", left)
	}
}

func TestByKeyPolicyRejectsInvalidJSON(t *testing.T) {
	t.Parallel()

	type customerEvent struct {
		CustomerID string `json:"customer_id"`
	}

	policy := pipeline.ByKey(func(in customerEvent) string {
		return in.CustomerID
	})

	_, err := policy.Partition(pipeline.Envelope[json.RawMessage]{
		Payload: json.RawMessage(`{"customer_id"`),
	}, 4)
	if err == nil {
		t.Fatalf("expected invalid JSON error")
	}
}

func ExampleRoundRobin() {
	policy := pipeline.RoundRobin()

	for i := 0; i < 4; i++ {
		idx, _ := policy.Partition(pipeline.Envelope[json.RawMessage]{}, 3)
		fmt.Println(idx)
	}

	// Output:
	// 0
	// 1
	// 2
	// 0
}

func ExampleByRecordID() {
	policy := pipeline.ByRecordID()
	record := pipeline.Envelope[json.RawMessage]{RecordID: "rec-1"}

	left, _ := policy.Partition(record, 4)
	right, _ := policy.Partition(record, 4)

	fmt.Println(left == right)
	fmt.Println(left >= 0 && left < 4)

	// Output:
	// true
	// true
}

func ExampleByMetadataKey() {
	policy := pipeline.ByMetadataKey("customer_id")
	record := pipeline.Envelope[json.RawMessage]{
		Metadata: map[string]string{"customer_id": "cust-1"},
	}

	left, _ := policy.Partition(record, 4)
	right, _ := policy.Partition(record, 4)

	fmt.Println(left == right)
	fmt.Println(left >= 0 && left < 4)

	// Output:
	// true
	// true
}

func ExampleByKey() {
	type customerEvent struct {
		CustomerID string `json:"customer_id"`
	}

	policy := pipeline.ByKey(func(in customerEvent) string {
		return in.CustomerID
	})
	record := pipeline.Envelope[json.RawMessage]{
		Payload: json.RawMessage(`{"customer_id":"cust-1"}`),
	}

	left, _ := policy.Partition(record, 4)
	right, _ := policy.Partition(record, 4)

	fmt.Println(left == right)
	fmt.Println(left >= 0 && left < 4)

	// Output:
	// true
	// true
}

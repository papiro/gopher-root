package pipeline_test

import (
	"encoding/json"
	"fmt"

	"github.com/pierre/manifold/pipeline"
)

// messageToTextCoupling demonstrates a minimal stateless transform artifact.
type messageToTextCoupling struct{}

func (messageToTextCoupling) Couple(segmentOutput json.RawMessage) (json.RawMessage, error) {
	var in struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(segmentOutput, &in); err != nil {
		return nil, err
	}
	out := struct {
		Text string `json:"text"`
	}{
		Text: in.Message,
	}
	return json.Marshal(out)
}

func Example_helloWorldContracts() {
	cfg := pipeline.EngineConfig{
		Segments: []pipeline.SegmentDescriptor{
			{ID: "source", Idempotency: pipeline.Idempotent, CompatibilityVersion: "v1"},
			{ID: "sink", Idempotency: pipeline.Idempotent, CompatibilityVersion: "v1"},
		},
		Couplings: []pipeline.CouplingDescriptor{
			{ID: "source-to-sink", FromSegment: "source", ToSegment: "sink"},
		},
		Topology: pipeline.Topology{
			Connections: []pipeline.Connection{
				{From: "source", To: "sink", CouplingID: "source-to-sink"},
			},
			AllowAmbiguousOrder: false,
		},
	}

	err := pipeline.ValidateTopology(cfg)
	fmt.Println("topology valid:", err == nil)

	c := messageToTextCoupling{}
	out, err := pipeline.ApplyCoupling(c, json.RawMessage(`{"message":"hello world"}`))
	fmt.Println("coupling valid:", err == nil)
	fmt.Println("coupled payload:", string(out))

	// Output:
	// topology valid: true
	// coupling valid: true
	// coupled payload: {"text":"hello world"}
}

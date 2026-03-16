package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

func planPipelineID(plan *Plan) (string, error) {
	payload, err := json.Marshal(plan.config)
	if err != nil {
		return "", fmt.Errorf("pipeline config encode failed: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

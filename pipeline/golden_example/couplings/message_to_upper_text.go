package couplings

import (
	"encoding/json"
	"strings"
)

type MessageToUpperText struct{}

func (MessageToUpperText) Couple(segmentOutput json.RawMessage) (json.RawMessage, error) {
	var in struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(segmentOutput, &in); err != nil {
		return nil, err
	}

	out := struct {
		Text string `json:"text"`
	}{
		Text: strings.ToUpper(in.Message),
	}
	return json.Marshal(out)
}

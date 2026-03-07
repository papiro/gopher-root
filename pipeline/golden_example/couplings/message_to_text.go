package couplings

import "encoding/json"

type MessageToText struct{}

func (MessageToText) Couple(segmentOutput json.RawMessage) (json.RawMessage, error) {
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

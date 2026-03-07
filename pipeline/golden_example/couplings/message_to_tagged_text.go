package couplings

import "encoding/json"

type MessageToTaggedText struct{}

func (MessageToTaggedText) Couple(segmentOutput json.RawMessage) (json.RawMessage, error) {
	var in struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(segmentOutput, &in); err != nil {
		return nil, err
	}

	out := struct {
		Text string `json:"text"`
	}{
		Text: "[golden] " + in.Message,
	}
	return json.Marshal(out)
}

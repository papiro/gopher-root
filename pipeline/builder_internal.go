package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

func asSinkWithDone[T any](sink Sink[T]) SinkWithDone[T] {
	if withDone, ok := any(sink).(SinkWithDone[T]); ok {
		return withDone
	}
	return nil
}

func cloneMetadata(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func setMetadataField(field reflect.Value, metadata map[string]string) {
	if !field.IsValid() {
		return
	}
	if metadata == nil {
		field.Set(reflect.Zero(field.Type()))
		return
	}
	field.Set(reflect.ValueOf(metadata))
}

func appendSegmentPath(path []SegmentID, next SegmentID) []SegmentID {
	out := make([]SegmentID, len(path)+1)
	copy(out, path)
	out[len(path)] = next
	return out
}

func childRecordID(parent RecordID, segment SegmentID, idx, total int) RecordID {
	base := string(parent) + "/" + string(segment)
	_ = total
	return RecordID(fmt.Sprintf("%s/%d", base, idx+1))
}

func syntheticCouplingID(index int, coupling Coupling) CouplingID {
	name := sanitizeTypeName(reflect.TypeOf(coupling))
	return CouplingID(fmt.Sprintf("builder-coupling-%02d-%s", index+1, name))
}

type identityCoupling struct{}

func (identityCoupling) Couple(segmentOutput json.RawMessage) (json.RawMessage, error) {
	return append(json.RawMessage(nil), segmentOutput...), nil
}

func sanitizeTypeName(t reflect.Type) string {
	if t == nil {
		return "coupling"
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	name := t.Name()
	if name == "" {
		name = t.String()
	}
	name = strings.ToLower(name)
	replacer := strings.NewReplacer("[", "-", "]", "", "*", "", ".", "-", "/", "-", " ", "-")
	return replacer.Replace(name)
}

func jsonValueForPayloadType(target reflect.Type, payload json.RawMessage) (reflect.Value, error) {
	decoded := reflect.New(target)
	if err := json.Unmarshal(payload, decoded.Interface()); err != nil {
		return reflect.Value{}, err
	}
	return decoded.Elem(), nil
}

func isSegmentInputShape(t reflect.Type) bool {
	if t.Kind() != reflect.Struct {
		return false
	}
	if _, ok := t.FieldByName("SourceRecordID"); !ok {
		return false
	}
	if _, ok := t.FieldByName("Payload"); !ok {
		return false
	}
	if _, ok := t.FieldByName("Metadata"); !ok {
		return false
	}
	return true
}

func isSegmentOutputShape(t reflect.Type) bool {
	if t.Kind() != reflect.Struct {
		return false
	}
	if _, ok := t.FieldByName("Payload"); !ok {
		return false
	}
	if _, ok := t.FieldByName("Metadata"); !ok {
		return false
	}
	return true
}

func isNilBuilderValue(v any) bool {
	if v == nil {
		return true
	}
	return isNilReflectValue(reflect.ValueOf(v))
}

func isNilReflectValue(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

var (
	contextType        = reflect.TypeOf((*context.Context)(nil)).Elem()
	processContextType = reflect.TypeOf((*ProcessContext)(nil)).Elem()
	errorType          = reflect.TypeOf((*error)(nil)).Elem()
	processResultType  = reflect.TypeOf(ProcessResult{})
	resumeInfoType     = reflect.TypeOf(ResumeInfo{})
)

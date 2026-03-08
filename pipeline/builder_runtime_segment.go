package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

type runtimeSegment struct {
	value             reflect.Value
	desc              SegmentDescriptor
	processMethod     reflect.Value
	doneMethod        reflect.Value
	compensatorMethod reflect.Value
	inputRecordType   reflect.Type
	outputRecordType  reflect.Type
	outputCallback    reflect.Type
}

type runtimeSegmentInput struct {
	originRecordID RecordID
	payload        json.RawMessage
	metadata       map[string]string
}

type runtimeSegmentOutput struct {
	payload  json.RawMessage
	metadata map[string]string
}

func newRuntimeSegment(segment any) (runtimeSegment, error) {
	value := reflect.ValueOf(segment)
	if !value.IsValid() || isNilReflectValue(value) {
		return runtimeSegment{}, ErrBuilderSegmentRequired
	}

	descMethod := value.MethodByName("Descriptor")
	processMethod := value.MethodByName("Process")
	doneMethod := value.MethodByName("Done")
	compensatorMethod := value.MethodByName("Compensator")
	if !descMethod.IsValid() || !processMethod.IsValid() || !doneMethod.IsValid() {
		return runtimeSegment{}, ErrBuilderUnsupportedSegmentShape
	}

	descValue := descMethod.Call(nil)
	if len(descValue) != 1 {
		return runtimeSegment{}, ErrBuilderUnsupportedSegmentShape
	}
	desc, ok := descValue[0].Interface().(SegmentDescriptor)
	if !ok {
		return runtimeSegment{}, ErrBuilderUnsupportedSegmentShape
	}

	// Require Process(context.Context, SegmentRecord[TIn], func(SegmentRecord[TOut]) error) error.
	processType := processMethod.Type()
	if processType.NumIn() != 3 || processType.NumOut() != 1 {
		return runtimeSegment{}, ErrBuilderUnsupportedSegmentShape
	}
	if processType.In(0) != contextType {
		return runtimeSegment{}, ErrBuilderUnsupportedSegmentShape
	}
	if !processType.Out(0).Implements(errorType) {
		return runtimeSegment{}, ErrBuilderUnsupportedSegmentShape
	}

	inputRecordType := processType.In(1)
	if !isRecordShape(inputRecordType) {
		return runtimeSegment{}, ErrBuilderUnsupportedSegmentShape
	}

	callbackType := processType.In(2)
	if callbackType.Kind() != reflect.Func || callbackType.NumIn() != 1 || callbackType.NumOut() != 1 {
		return runtimeSegment{}, ErrBuilderUnsupportedSegmentShape
	}
	if !callbackType.Out(0).Implements(errorType) {
		return runtimeSegment{}, ErrBuilderUnsupportedSegmentShape
	}
	outputRecordType := callbackType.In(0)
	if !isRecordShape(outputRecordType) {
		return runtimeSegment{}, ErrBuilderUnsupportedSegmentShape
	}

	return runtimeSegment{
		value:             value,
		desc:              desc,
		processMethod:     processMethod,
		doneMethod:        doneMethod,
		compensatorMethod: compensatorMethod,
		inputRecordType:   inputRecordType,
		outputRecordType:  outputRecordType,
		outputCallback:    callbackType,
	}, nil
}

func validateRuntimeSegment(segment runtimeSegment) error {
	if segment.desc.ID == "" {
		return ErrSegmentIDRequired
	}
	if segment.desc.Idempotency == NonIdempotent && segment.compensatorNil() {
		return ErrCompensatorRequired
	}
	return nil
}

func (s runtimeSegment) compensatorNil() bool {
	if !s.compensatorMethod.IsValid() {
		return true
	}
	values := s.compensatorMethod.Call(nil)
	if len(values) != 1 {
		return true
	}
	return isNilReflectValue(values[0])
}

// processJSON adapts the runtime's JSON envelope into the segment's typed Process call and captures typed outputs back as JSON.
func (s runtimeSegment) processJSON(ctx context.Context, in runtimeSegmentInput) ([]runtimeSegmentOutput, error) {
	record := reflect.New(s.inputRecordType).Elem()
	record.FieldByName("RecordID").Set(reflect.ValueOf(in.originRecordID))
	setMetadataField(record.FieldByName("Metadata"), cloneMetadata(in.metadata))

	payloadField, _ := s.inputRecordType.FieldByName("Payload")
	inputPayload, err := jsonValueForPayloadType(payloadField.Type, in.payload)
	if err != nil {
		return nil, fmt.Errorf("segment %q input decode failed: %w", s.desc.ID, err)
	}
	record.FieldByName("Payload").Set(inputPayload)

	outputs := make([]runtimeSegmentOutput, 0, 1)
	callback := reflect.MakeFunc(s.outputCallback, func(args []reflect.Value) []reflect.Value {
		outRecord := args[0]

		if recordIDField := outRecord.FieldByName("RecordID"); recordIDField.IsValid() {
			if outRecordID, ok := recordIDField.Interface().(RecordID); ok && outRecordID != "" && outRecordID != in.originRecordID {
				return []reflect.Value{reflect.ValueOf(fmt.Errorf("%w: segment=%q got=%q want=%q", ErrBuilderSegmentRecordIDMismatch, s.desc.ID, outRecordID, in.originRecordID))}
			}
		}

		payload, marshalErr := json.Marshal(outRecord.FieldByName("Payload").Interface())
		if marshalErr != nil {
			return []reflect.Value{reflect.ValueOf(marshalErr)}
		}

		var metadata map[string]string
		if metadataField := outRecord.FieldByName("Metadata"); metadataField.IsValid() && !metadataField.IsZero() {
			metadata = cloneMetadata(metadataField.Interface().(map[string]string))
		}

		outputs = append(outputs, runtimeSegmentOutput{
			payload:  payload,
			metadata: metadata,
		})
		return []reflect.Value{reflect.Zero(errorType)}
	})

	results := s.processMethod.Call([]reflect.Value{
		reflect.ValueOf(ctx),
		record,
		callback,
	})
	if len(results) != 1 {
		return nil, ErrBuilderUnsupportedSegmentShape
	}
	if errValue := results[0].Interface(); errValue != nil {
		return nil, errValue.(error)
	}
	return outputs, nil
}

func (s runtimeSegment) done(ctx context.Context) error {
	results := s.doneMethod.Call([]reflect.Value{reflect.ValueOf(ctx)})
	if len(results) != 1 {
		return nil
	}
	if errValue := results[0].Interface(); errValue != nil {
		return errValue.(error)
	}
	return nil
}

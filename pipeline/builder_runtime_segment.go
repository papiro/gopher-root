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
	recoverMethod     reflect.Value
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
	pauseRequested func() bool
}

type runtimeSegmentOutput struct {
	payload  json.RawMessage
	metadata map[string]string
}

type runtimeProcessContext struct {
	context.Context
	pauseRequested func() bool
}

func (c runtimeProcessContext) PauseRequested() bool {
	if c.pauseRequested == nil {
		return false
	}
	return c.pauseRequested()
}

func newRuntimeSegment(segment any) (runtimeSegment, error) {
	value := reflect.ValueOf(segment)
	if !value.IsValid() || isNilReflectValue(value) {
		return runtimeSegment{}, ErrBuilderSegmentRequired
	}

	descMethod := value.MethodByName("Descriptor")
	processMethod := value.MethodByName("Process")
	recoverMethod := value.MethodByName("Recover")
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

	// Require Process(ProcessContext, SegmentInput[TIn], func(SegmentOutput[TOut]) error) (ProcessResult, error).
	processType := processMethod.Type()
	if processType.NumIn() != 3 || processType.NumOut() != 2 {
		return runtimeSegment{}, ErrBuilderUnsupportedSegmentShape
	}
	if processType.In(0) != processContextType {
		return runtimeSegment{}, ErrBuilderUnsupportedSegmentShape
	}
	if processType.Out(0) != processResultType || !processType.Out(1).Implements(errorType) {
		return runtimeSegment{}, ErrBuilderUnsupportedSegmentShape
	}

	inputRecordType := processType.In(1)
	if !isSegmentInputShape(inputRecordType) {
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
	if !isSegmentOutputShape(outputRecordType) {
		return runtimeSegment{}, ErrBuilderUnsupportedSegmentShape
	}

	if recoverMethod.IsValid() {
		recoverType := recoverMethod.Type()
		if recoverType.NumIn() != 3 || recoverType.In(0) != contextType || recoverType.In(1) != inputRecordType || recoverType.In(2) != resumeInfoType || recoverType.NumOut() != 1 {
			return runtimeSegment{}, ErrBuilderUnsupportedSegmentShape
		}
		if !recoverType.Out(0).Implements(errorType) {
			return runtimeSegment{}, ErrBuilderUnsupportedSegmentShape
		}
	}

	return runtimeSegment{
		value:             value,
		desc:              desc,
		processMethod:     processMethod,
		recoverMethod:     recoverMethod,
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

func (s runtimeSegment) inputPayloadType() reflect.Type {
	field, _ := s.inputRecordType.FieldByName("Payload")
	return field.Type
}

func (s runtimeSegment) outputPayloadType() reflect.Type {
	field, _ := s.outputRecordType.FieldByName("Payload")
	return field.Type
}

func (s runtimeSegment) canRecover() bool {
	return s.recoverMethod.IsValid()
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

func (s runtimeSegment) compensate(ctx context.Context, recordID RecordID, attemptID AttemptID, reason error) error {
	if s.compensatorNil() {
		return nil
	}
	values := s.compensatorMethod.Call(nil)
	if len(values) != 1 {
		return ErrBuilderUnsupportedSegmentShape
	}
	compensator, ok := values[0].Interface().(Compensator)
	if !ok || compensator == nil {
		return ErrBuilderUnsupportedSegmentShape
	}
	return compensator.Compensate(ctx, recordID, attemptID, reason)
}

func (s runtimeSegment) recover(ctx context.Context, in runtimeSegmentInput, info ResumeInfo) error {
	if !s.canRecover() {
		return nil
	}
	input, err := s.buildInputValue(in)
	if err != nil {
		return err
	}
	results := s.recoverMethod.Call([]reflect.Value{
		reflect.ValueOf(ctx),
		input,
		reflect.ValueOf(info),
	})
	if len(results) != 1 {
		return ErrBuilderUnsupportedSegmentShape
	}
	if errValue := results[0].Interface(); errValue != nil {
		return errValue.(error)
	}
	return nil
}

// processJSON adapts the runtime's JSON envelope into the segment's typed Process call.
func (s runtimeSegment) processJSON(ctx context.Context, in runtimeSegmentInput, emit func(runtimeSegmentOutput) error) (ProcessResult, error) {
	input, err := s.buildInputValue(in)
	if err != nil {
		return ProcessResult{}, err
	}

	callback := reflect.MakeFunc(s.outputCallback, func(args []reflect.Value) []reflect.Value {
		outRecord := args[0]

		payload, marshalErr := json.Marshal(outRecord.FieldByName("Payload").Interface())
		if marshalErr != nil {
			return []reflect.Value{reflect.ValueOf(marshalErr)}
		}

		var metadata map[string]string
		if metadataField := outRecord.FieldByName("Metadata"); metadataField.IsValid() && !metadataField.IsZero() {
			metadata = cloneMetadata(metadataField.Interface().(map[string]string))
		}

		if err := emit(runtimeSegmentOutput{
			payload:  payload,
			metadata: metadata,
		}); err != nil {
			return []reflect.Value{reflect.ValueOf(err)}
		}
		return []reflect.Value{reflect.Zero(errorType)}
	})

	results := s.processMethod.Call([]reflect.Value{
		reflect.ValueOf(runtimeProcessContext{
			Context:        ctx,
			pauseRequested: in.pauseRequested,
		}),
		input,
		callback,
	})
	if len(results) != 2 {
		return ProcessResult{}, ErrBuilderUnsupportedSegmentShape
	}
	result, ok := results[0].Interface().(ProcessResult)
	if !ok {
		return ProcessResult{}, ErrBuilderUnsupportedSegmentShape
	}
	if errValue := results[1].Interface(); errValue != nil {
		return ProcessResult{}, errValue.(error)
	}
	return result, nil
}

func (s runtimeSegment) buildInputValue(in runtimeSegmentInput) (reflect.Value, error) {
	input := reflect.New(s.inputRecordType).Elem()
	input.FieldByName("SourceRecordID").Set(reflect.ValueOf(in.originRecordID))
	setMetadataField(input.FieldByName("Metadata"), cloneMetadata(in.metadata))

	payloadField, _ := s.inputRecordType.FieldByName("Payload")
	inputPayload, err := jsonValueForPayloadType(payloadField.Type, in.payload)
	if err != nil {
		return reflect.Value{}, fmt.Errorf("segment %q input decode failed: %w", s.desc.ID, err)
	}
	input.FieldByName("Payload").Set(inputPayload)
	return input, nil
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

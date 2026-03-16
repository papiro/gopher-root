package golden_example

import "context"

type noPauseProcessContext struct {
	context.Context
}

func (noPauseProcessContext) PauseRequested() bool { return false }

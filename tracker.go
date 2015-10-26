package singleply

import "sync"

type StatusCallback interface {
	SetStatus(status string)
}

type State struct {
	operation string
	latest    string
}

type Tracker struct {
	lock   sync.Mutex
	states map[*State]*State
}

func (s *Tracker) AddOperation(operation string) *State {
	panic("unimp")
}

func (s *Tracker) OperationComplete(state *State) {
	panic("unimp")
}

func (s *Tracker) GetState() []*State {
	panic("unimp")
}

func (s *State) SetStatus(status string) {
	panic("unimp")
}

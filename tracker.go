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
	s.lock.Lock()
	defer s.lock.Unlock()

	state := &State{operation: operation, latest: ""}
	s.states[state] = state

	return state
}

func (s *Tracker) OperationComplete(state *State) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.states, state)
}

func (s *Tracker) GetState() []*State {
	s.lock.Lock()
	defer s.lock.Unlock()

	states := make([]*State, 0, len(s.states))
	for _, st := range states {
		states = append(states, &State{operation: st.operation, latest: st.latest})
	}

	return states
}

func (s *State) SetStatus(status string) {
	s.latest = status
}

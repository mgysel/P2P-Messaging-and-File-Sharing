package impl

import (
	"sync"

	"go.dedis.ch/cs438/types"
)

// ConcurrentRumorSequenceTable keeps track of the last rumor that is received
type ConcurrentRumorSequenceTable struct {
	sequences map[string]uint32
	rumors    map[string][]*types.Rumor

	sync.RWMutex
}

func (t *ConcurrentRumorSequenceTable) Init() {
	t.sequences = make(map[string]uint32)
	t.rumors = make(map[string][]*types.Rumor)
}

// if the result doesn't exist, -1 is returned
func (t *ConcurrentRumorSequenceTable) getSequence(origin string) int {
	t.RLock()
	defer t.RUnlock()

	if val, ok := t.sequences[origin]; ok {
		return int(val)
	}
	return -1
}

// no checks are performed
// it's a direct set operation to the map
func (t *ConcurrentRumorSequenceTable) set(origin string, rumor *types.Rumor) {
	t.Lock()
	defer t.Unlock()

	newMsgCopy := rumor.Msg.Copy()
	newRumor := types.Rumor{
		Origin:   rumor.Origin,
		Sequence: rumor.Sequence,
		Msg:      &newMsgCopy,
	}

	t.sequences[origin] = uint32(newRumor.Sequence)

	tmp := t.rumors[origin]
	if tmp == nil {
		tmp = make([]*types.Rumor, 0)
	}
	tmp = append(tmp, &newRumor)
	t.rumors[origin] = tmp
}

func (t *ConcurrentRumorSequenceTable) generateStatusMessage() types.StatusMessage {
	t.RLock()
	defer t.RUnlock()

	ret := make(types.StatusMessage)
	for k, v := range t.sequences {
		ret[k] = uint(v)
	}

	return ret
}

// this function is only for generating the catup-up rumors for the status message, case 2
func (t *ConcurrentRumorSequenceTable) generateRumorMessage(currentDiff types.StatusMessage) types.RumorsMessage {
	t.RLock()
	defer t.RUnlock()

	ret := types.RumorsMessage{
		Rumors: make([]types.Rumor, 0),
	}

	for k, v := range currentDiff {
		if seq, ok := t.sequences[k]; ok {
			for i := v + 1; uint32(i) <= seq; i++ {
				tmp := t.rumors[k][i-1]
				msgCopy := tmp.Msg.Copy()
				newRumor := types.Rumor{
					Origin:   tmp.Origin,
					Sequence: tmp.Sequence,
					Msg:      &msgCopy,
				}
				ret.Rumors = append(ret.Rumors, newRumor)
			}
		} else {
			panic("inconsistency")
		}
	}

	return ret
}

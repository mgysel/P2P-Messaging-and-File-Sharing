package impl

import (
	"errors"
	"math/rand"
	"sync"

	"go.dedis.ch/cs438/peer"
)

var (
	ErrNoNextHopToRelayError    = errors.New("no next hop available to relay")
	ErrNoRandomNeighborToReturn = errors.New("no random neighbor available to return")
)

// RoutingTable defines a simple next-hop routing table. The key is the origin
// and the value the relay address. The routing table must always have an entry
// to itself as follow:
//
//   Table[myAddr] = myAddr.
//
// Table[C] = B means that to reach C, message must be sent to B, the relay.
type ConcurrentRoutingTable struct {
	routes map[string]string
	lock   sync.RWMutex
}

func (crt *ConcurrentRoutingTable) Init(origin string) {
	crt.routes = make(map[string]string)

	crt.SetRoutingEntry(origin, origin)
}

// SetRoutingEntry sets the routing entry.
// Overwrites it if the entry already exists.
// If the origin is equal to the relayAddr, then the node has a new neighbor (the notion of neighboors is not needed in HW0).
// If relayAddr is empty then the record must be deleted (and the peer has potentially lost a neighbor).
func (crt *ConcurrentRoutingTable) SetRoutingEntry(origin, relayAddr string) {
	crt.lock.Lock()
	defer crt.lock.Unlock()

	// log.Debug().Msgf("Add Peer %v %v", origin, relayAddr)
	// defer log.Debug().Msgf("After adding %v", crt.routes)

	if relayAddr == "" {
		delete(crt.routes, origin)
		return
	}

	if relay, ok := crt.routes[origin]; ok {
		if relay == origin {
			// this is a neighboor already, don't update the route
			// (because you might change the A->B, a neighbor, into something that needs a relay)
			return
		}
	}

	crt.routes[origin] = relayAddr
}

func (crt *ConcurrentRoutingTable) AddPeers(addresses []string) {
	for _, address := range addresses {
		crt.SetRoutingEntry(address, address)
	}
}

func (crt *ConcurrentRoutingTable) GetRoutingTable() peer.RoutingTable {
	crt.lock.RLock()
	defer crt.lock.RUnlock()

	// deep copy the routing table
	ret := make(peer.RoutingTable)
	for key, value := range crt.routes {
		ret[key] = value
	}

	return ret
}

func (crt *ConcurrentRoutingTable) IsRouteAvailable(dest string) bool {
	crt.lock.RLock()
	defer crt.lock.RUnlock()

	if _, ok := crt.routes[dest]; ok {
		return true
	}
	return false
}

func (crt *ConcurrentRoutingTable) GetNextHop(dest string) (string, error) {
	crt.lock.RLock()
	defer crt.lock.RUnlock()

	if nextHopAddr, ok := crt.routes[dest]; ok {
		return nextHopAddr, nil
	}
	return "", ErrNoNextHopToRelayError
}

func (crt *ConcurrentRoutingTable) GetRandomNeighbor(bannedAddrs ...string) (string, error) {
	crt.lock.RLock()
	defer crt.lock.RUnlock()

	// log.Debug().Msgf("GetRandomNeighbor %v; %v", crt.routes, bannedAddrs)
	neighbors := make([]string, 0)
	for k, v := range crt.routes {
		if k == v { // neighbor
			ok := true
			for _, banned := range bannedAddrs {
				if k == banned {
					ok = false
					break
				}
			}

			if ok {
				neighbors = append(neighbors, k)
			}
		}
	}

	if len(neighbors) == 0 {
		return "", ErrNoRandomNeighborToReturn
	}

	randIdx := rand.Uint32() % uint32(len(neighbors))

	return neighbors[randIdx], nil
}

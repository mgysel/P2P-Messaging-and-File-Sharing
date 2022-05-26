package impl

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

const RecvTimeoutValue = 10 * time.Millisecond
const SendTimeoutValue = 500 * time.Millisecond

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.
	concurrentRoutingTable := ConcurrentRoutingTable{}
	concurrentRoutingTable.Init(conf.Socket.GetAddress())

	concurrentRumorSequenceTable := ConcurrentRumorSequenceTable{}
	concurrentRumorSequenceTable.Init()

	pendingAckTracker := PendingAckTracker{}

	ret := &node{
		conf:                         conf,
		shouldStop:                   0,
		concurrentRoutingTable:       &concurrentRoutingTable,
		concurrentRumorSequenceTable: &concurrentRumorSequenceTable,
		pendingAckTracker:            &pendingAckTracker,
		rumorSequence:                0,
	}

	pendingAckTracker.Init(ret)

	return ret
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer

	// You probably want to keep the peer.Configuration on this struct:
	conf peer.Configuration

	// for signaling to goroutines to stop
	shouldStop int32
	wg         sync.WaitGroup

	// routing table
	concurrentRoutingTable *ConcurrentRoutingTable

	// rumor sequence tracing table
	concurrentRumorSequenceTable *ConcurrentRumorSequenceTable

	pendingAckTracker *PendingAckTracker

	// the rumor sequence is a 1-based numbering system
	rumorSequence uint32
}

// Start implements peer.Service
// Start starts the node. It should, among other things, start listening on
// its address using the socket.
// The Start function should not block
func (n *node) Start() error {
	// Socket is initialized already, with the UDP connection inside

	// The part where you register the handler. Must be done when you initialize
	// your peer with every type of message expected.
	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.ExecChatMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, n.ExecRumorsMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, n.ExecAckMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, n.ExecStatusMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, n.ExecEmptyMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, n.ExecPrivateMessage)

	/* update closeSignalCount */
	n.wg.Add(1)
	go n.startListener()
	n.wg.Add(1)
	go n.startAntiEntropyMechanism()
	n.wg.Add(1)
	go n.startHeartBeatMechanism()
	n.wg.Add(1)
	go n.pendingAckTracker.StartRunner()

	return nil
}

// Stop implements peer.Service
// Stop stops the node. This function must block until all goroutines are
// done.
func (n *node) Stop() error {
	// log.Debug().Msg("stop request received")

	atomic.AddInt32(&n.shouldStop, 1)

	// log.Debug().Msg("Waiting for all goroutines to stop")
	n.wg.Wait()
	// log.Debug().Msg("All go routines have stopped")

	return nil
}

// Unicast implements peer.Messaging
// Unicast sends a packet to a given destination. If the destination is the
// same as the node's address, then the message must still be sent to the
// node via its socket. Use transport.NewHeader to build the packet's
// header.
func (n *node) Unicast(dest string, msg transport.Message) error {
	if n.concurrentRoutingTable.IsRouteAvailable(dest) {
		nextHopAddr, err := n.concurrentRoutingTable.GetNextHop(dest)
		if err != nil {
			log.Err(err).Msg("can't get next hop")
			return err
		}

		header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest, 0)

		return n.conf.Socket.Send(nextHopAddr, transport.Packet{
			Header: &header,
			Msg:    &msg,
		}, SendTimeoutValue)
	}

	return errors.New("no route available to dest")
}

// Broadcast sends a packet to all know destinations. This function must ony
// be used by clients and not called from code. The node must not send the
// message to itself (to its socket), but still process it.
//
// 1. the skeleton specifies, in "transport/mod.go:143",
// that "Destination is empty in the case of a broadcast, otherwise contains the destination address.".
// 2. the pdf mentions in Task 1 (page 6) that broadcasting a message means to create Rumor
// and send it to a random neighbor.
// Thus, the pdf implies, and the tests expect that a broadcast has a destination
// in the rumor message it generates.
//
// The inconsistency comes from the spec of a Broadcast call vs a Broadcast implementation.
// If a client/node calls Broadcast, the *Broadcast call* does not have a destination,
// because it implicitly means "send to all". But, the *Broadcast implementation*
// consists of sending Rumor messages to some chosen destinations.
func (n *node) Broadcast(msg transport.Message) error {
	// create a rumor message containing 1 rumor (embeds the msg)
	newRumorSequence := atomic.AddUint32(&n.rumorSequence, 1)
	// log.Debug().Msgf("newRumorSequence %v", newRumorSequence)
	newRumor := types.Rumor{
		Origin:   n.conf.Socket.GetAddress(),
		Sequence: uint(newRumorSequence),
		Msg:      &msg,
	}

	newRumorMessage := types.RumorsMessage{}
	newRumorMessage.Rumors = make([]types.Rumor, 1)
	newRumorMessage.Rumors[0] = newRumor

	newRumorTransportMessage, err := n.conf.MessageRegistry.MarshalMessage(newRumorMessage)
	if err != nil {
		log.Err(err).Msg("can't marshal rumor message for broadcasting")
		return err
	}

	// process the message locally
	// We need to process it locally first, because even if we have no neighbor, we still need to make the packet and queue it locally
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), "", 0)
	pkt := transport.Packet{
		Header: &header,
		Msg:    &newRumorTransportMessage,
	}

	err = n.conf.MessageRegistry.ProcessPacket(pkt)
	if err != nil {
		log.Err(err).Msg("can't process the packet for broadcast locally")
		return err
	}

	// send to 1 random neighboor
	randomNeighbor, err := n.concurrentRoutingTable.GetRandomNeighbor(n.conf.Socket.GetAddress())
	if err != nil {
		log.Err(err).Msg("can't get a random neighbor for broadcasting")
		if err != ErrNoRandomNeighborToReturn {
			return err
		}

		return nil
	}
	header.Destination = randomNeighbor

	// send it
	err = n.conf.Socket.Send(randomNeighbor, pkt, SendTimeoutValue)
	if err != nil {
		log.Err(err).Msg("can't send the packet for broadcast")
		return err
	}

	// register the pending ack
	n.pendingAckTracker.AddPendingAck([]string{n.conf.Socket.GetAddress(), randomNeighbor}, pkt, time.Now().Add(n.conf.AckTimeout).Unix())

	return nil
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	n.concurrentRoutingTable.AddPeers(addr)
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	return n.concurrentRoutingTable.GetRoutingTable()
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	n.concurrentRoutingTable.SetRoutingEntry(origin, relayAddr)
}

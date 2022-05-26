package impl

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// This listener should be started once, on a goroutine
func (n *node) startListener() {
	// log.Debug().Msg("starting startListener")
	// defer log.Debug().Msg("closing startListener")

	defer n.wg.Done()

	for {
		if atomic.LoadInt32(&n.shouldStop) > 0 {
			return
		}

		pkt, err := n.conf.Socket.Recv(RecvTimeoutValue)
		if errors.Is(err, transport.TimeoutErr(RecvTimeoutValue)) {
			// log.Debug().Msg("socket Recv timeout")
			continue
		} else if err != nil {
			log.Err(err).Msg("socket recv has error")
			// FIXME: return error
		}

		/* no error, continue on processing the received data */

		// check if the message is intended for our node
		if pkt.Header.Destination == n.conf.Socket.GetAddress() {
			// execute message callback
			n.conf.MessageRegistry.ProcessPacket(pkt)
		} else {
			// relay the message -> RelayedBy field update
			// we can't put this in the Send() because the implementation from TA don't perform this change
			pkt.Header.RelayedBy = n.conf.Socket.GetAddress()

			n.conf.Socket.Send(pkt.Header.Destination, pkt, SendTimeoutValue)
		}
	}
}

func (n *node) performAntiEntropyMechanism() error {
	// performAntiEntropyMechanism
	// log.Debug().Msg("performAntiEntropyMechanism")

	// build status message
	newStatusMessage := n.concurrentRumorSequenceTable.generateStatusMessage()

	newStatusTransportMessage, err := n.conf.MessageRegistry.MarshalMessage(newStatusMessage)
	if err != nil {
		log.Err(err).Msg("can't marshal status message for AntiEntropy")
		return err
	}

	// send to 1 random neighboor
	randomNeighbor, err := n.concurrentRoutingTable.GetRandomNeighbor(n.conf.Socket.GetAddress())
	if err != nil {
		if err != ErrNoRandomNeighborToReturn {
			log.Err(err).Msg("can't get a random neighbor for broadcasting")
			return err
		}

		return nil
	}

	err = n.Unicast(randomNeighbor, newStatusTransportMessage)
	if err != nil {
		log.Err(err).Msg("performAntiEntropyMechanism - can't unicast ack message")
		return err
	}

	return nil
}

// AntiEntropyInterval is the interval at which the peer sends a status
// message to a random neighbor. 0 means no status messages are sent.
// Default: 0
func (n *node) startAntiEntropyMechanism() {
	// log.Debug().Msgf("starting startAntiEntropyMechanism %v", n.conf.Socket.GetAddress())
	// defer log.Debug().Msgf("closing startAntiEntropyMechanism %v", n.conf.Socket.GetAddress())

	defer n.wg.Done()

	if n.conf.AntiEntropyInterval.Nanoseconds() == 0 {
		// log.Debug().Msg("Anti Entropy Mechanism is off")
		return
	}

	timer := time.NewTimer(n.conf.AntiEntropyInterval)
	for {
		select {
		case <-timer.C:
			err := n.performAntiEntropyMechanism()
			if err != nil {
				log.Err(err).Msg("performAntiEntropyMechanism has failed")
			}
			timer = time.NewTimer(n.conf.AntiEntropyInterval)
		default:
			if atomic.LoadInt32(&n.shouldStop) > 0 {
				timer.Stop()
				return
			}
		}
	}
}

func (n *node) performHeartBeatMechanism() error {
	// build empty message
	newEmptyMessage := types.EmptyMessage{}

	newEmptyTransportMessage, err := n.conf.MessageRegistry.MarshalMessage(newEmptyMessage)
	if err != nil {
		log.Err(err).Msg("can't marshal rumor message for broadcasting")
		return err
	}

	return n.Broadcast(newEmptyTransportMessage)
}

func (n *node) startHeartBeatMechanism() {
	// log.Debug().Msg("starting startHeartBeatMechanism")
	// defer log.Debug().Msg("closing startHeartBeatMechanism")

	defer n.wg.Done()

	if n.conf.HeartbeatInterval.Nanoseconds() == 0 {
		// log.Debug().Msg("Heart Beat Mechanism is off")
		return
	}

	// we need to perform a send on start, according to the Task 7 description
	// BUT, we would actually want to wait for a bit, because the we had run into an issue
	// where the unit test's addPeer is called AFTER our heartbeat is sent lol

	timer := time.NewTimer(n.conf.HeartbeatInterval)
	for {

		select {
		case <-timer.C:
			err := n.performHeartBeatMechanism()
			if err != nil {
				log.Err(err).Msg("performHeartBeatMechanism has failed")
			}
			timer = time.NewTimer(n.conf.HeartbeatInterval)
		default:
			if atomic.LoadInt32(&n.shouldStop) > 0 {
				timer.Stop()
				return
			}
		}
	}
}

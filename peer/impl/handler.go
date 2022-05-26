package impl

import (
	"math/rand"
	"time"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// The handler. This function will be called when a ChatMessage is received.
func (n *node) ExecChatMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	// chatMsg, ok := msg.(*types.ChatMessage)
	_, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	// log.Debug().Msg(chatMsg.Message)

	return nil
}

// The handler. This function will be called when a RumorsMessage is received.
func (n *node) ExecRumorsMessage(msg types.Message, pkt transport.Packet) error {
	// decode the message into a RumorsMessage
	// cast the message to its actual type. You assume it is the right type.
	rumorMsg, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	// log.Debug().Msg(rumorMsg.String())

	// process the rumor
	hasExpected := false
	for _, rumor := range rumorMsg.Rumors {
		// check if the rumor is expected
		expected := false

		// log.Debug().Msgf("analyzing %v", rumor)

		seq := n.concurrentRumorSequenceTable.getSequence(rumor.Origin)
		if rumor.Sequence == 1 { // a new one
			if seq == -1 {
				// expected, it's indeed a new one
				n.concurrentRumorSequenceTable.set(rumor.Origin, &rumor)

				// update the routing table (check HW1 Task 6)
				// log.Debug().Msgf("Current %v: attempt to add from %v relayed by %v", n.conf.Socket.GetAddress(), pkt.Header.Source, pkt.Header.RelayedBy)
				n.concurrentRoutingTable.SetRoutingEntry(rumor.Origin, pkt.Header.RelayedBy)

				expected = true
			} /* else {
				// not expected, because we already have an entry in the table
			} */
		} else {
			if seq == -1 {
				// not expected, as the origin doesn't exist and it's not the seq == 1 (first one)
			} else if seq+1 != int(n.rumorSequence) {
				// not expected
			} else {
				// expected
				n.concurrentRumorSequenceTable.set(rumor.Origin, &rumor)

				// update the routing table (check HW1 Task 6)
				// log.Debug().Msgf("Current %v: attempt to add from %v relayed by %v", n.conf.Socket.GetAddress(), pkt.Header.Source, pkt.Header.RelayedBy)
				n.concurrentRoutingTable.SetRoutingEntry(rumor.Origin, pkt.Header.RelayedBy)

				expected = true
			}
		}

		if expected {
			hasExpected = true

			// process the packet respectively
			newHeader := pkt.Header.Copy()
			newPkt := transport.Packet{
				Header: &newHeader,
				Msg:    rumor.Msg,
			}

			err := n.conf.MessageRegistry.ProcessPacket(newPkt)
			if err != nil {
				log.Err(err).Msg("can't process a rumor message for rumors array")
				return err
			}
		}
	}

	// if it's processed locally, don't send the ack message
	if pkt.Header.Source != n.conf.Socket.GetAddress() {
		// send an ack message back to the source
		ackMessage := types.AckMessage{
			AckedPacketID: pkt.Header.PacketID,
			Status:        n.concurrentRumorSequenceTable.generateStatusMessage(),
		}

		newAckTransportMessage, err := n.conf.MessageRegistry.MarshalMessage(ackMessage)
		if err != nil {
			log.Err(err).Msg("can't marshal ack message for broadcasting")
			return err
		}

		header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), pkt.Header.Source, 0)
		err = n.conf.Socket.Send(pkt.Header.Source, transport.Packet{
			Header: &header,
			Msg:    &newAckTransportMessage,
		}, SendTimeoutValue)
		if err != nil {
			log.Err(err).Msg("ExecRumorsMessage - can't send ack message")
			return err
		}
	}

	// if it's processed locally, don't send the rumor out to a random neighbor
	if pkt.Header.Source != n.conf.Socket.GetAddress() {
		// send the rumor out to a random neighbor IF the rumor data is new
		if hasExpected {
			// send to 1 random neighboor, excluding myself and the origin
			randomNeighbor, err := n.concurrentRoutingTable.GetRandomNeighbor(n.conf.Socket.GetAddress(), pkt.Header.Source)
			if err != nil {
				if err != ErrNoRandomNeighborToReturn {
					log.Err(err).Msg("can't get a random neighbor for broadcasting")
					return err
				} else {
					return nil
				}
			}

			// keep the origin
			newHeader := transport.NewHeader(pkt.Header.Source, n.conf.Socket.GetAddress(), randomNeighbor, 0)
			newPkt := transport.Packet{
				Header: &newHeader,
				Msg:    pkt.Msg,
			}

			err = n.conf.Socket.Send(randomNeighbor, newPkt, SendTimeoutValue)
			if err != nil {
				log.Err(err).Msg("can't get a random neighbor to end the rumor message")
				return err
			}

			// register the pending ack
			n.pendingAckTracker.AddPendingAck([]string{n.conf.Socket.GetAddress(), pkt.Header.Source, randomNeighbor}, pkt, time.Now().Add(n.conf.AckTimeout).Unix())
		}
	}

	return nil
}

// The handler. This function will be called when a AckMessage is received.
func (n *node) ExecAckMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	ackMsg, ok := msg.(*types.AckMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	// log.Debug().Msg(ackMsg.String())

	// remove the pending timeout
	n.pendingAckTracker.RemovePendingAck(pkt.Header.PacketID)

	// process it
	newHeader := pkt.Header.Copy()
	newStatusTransportMessage, err := n.conf.MessageRegistry.MarshalMessage(ackMsg.Status)
	if err != nil {
		log.Err(err).Msg("can't marshal status message for ExecAckMessage")
		return err
	}
	newPkt := transport.Packet{
		Header: &newHeader,
		Msg:    &newStatusTransportMessage,
	}
	err = n.conf.MessageRegistry.ProcessPacket(newPkt)
	if err != nil {
		log.Err(err).Msg("can't process status message packet")
		return err
	}

	return nil
}

// The handler. This function will be called when a StatusMessage is received.
func (n *node) ExecStatusMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	statusMsg, ok := msg.(*types.StatusMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	// log.Debug().Msg(statusMsg.String())

	if pkt.Header.Destination == pkt.Header.Source {
		// this is locally called, don't need to process it
		log.Debug().Msg("this is locally called, don't need to process it")
		return nil
	}

	// 1. The remote peer has Rumors that the peer P doesn’t have.
	//   - peer P send a status message to the remote peer
	// 2. The peer P has Rumors that the remote peer doesn’t have.
	//   - The peer P must send all the missing Rumors, in order of increasing sequence number
	//     and in a single RumorsMessage, to the remote peer. In this case peer P is not
	//     expecting an ack message in return. However, the remote peer could send back one,
	//     as the remote peer does not need to differentiate between “catch up”
	//     and “broadcast” RumorsMessages.
	// 3. Both peers have new messages.
	//   - reduce to case 1 and 2
	// 4. Both peers have the same view.
	//   - ContinueMongering

	localStatus := n.concurrentRumorSequenceTable.generateStatusMessage()
	ourExtras := make(types.StatusMessage)   // save the smaller sequence (0 for none)
	ourMissings := make(types.StatusMessage) // save the smaller sequence (0 for none)

	for k, v := range localStatus {
		if seq, ok := (*statusMsg)[k]; ok {
			// we both have it
			if seq == v {
				continue
			} else if seq > v {
				// they have newer data
				ourMissings[k] = v
			} else {
				// we have newer data
				ourExtras[k] = seq
			}
		} else {
			// remote peer don't have it
			ourExtras[k] = 0
		}
	}

	for k := range *statusMsg {
		// the case of we both have it is processed in the prior loop
		if _, ok := localStatus[k]; !ok {
			// we don't have it
			ourMissings[k] = 0
		}
	}

	if len(ourMissings) > 0 { // case 1
		// log.Debug().Msg("Case 1")

		// build status message
		newStatusMessage := n.concurrentRumorSequenceTable.generateStatusMessage()

		newStatusTransportMessage, err := n.conf.MessageRegistry.MarshalMessage(newStatusMessage)
		if err != nil {
			log.Err(err).Msg("can't marshal status message for AntiEntropy")
			return err
		}

		header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), pkt.Header.Source, 0)

		err = n.conf.Socket.Send(pkt.Header.Source, transport.Packet{
			Header: &header,
			Msg:    &newStatusTransportMessage,
		}, SendTimeoutValue)
		if err != nil {
			log.Err(err).Msg("ExecStatusMessage case 1 - can't send status message for AntiEntropy")
			return err
		}
	}

	if len(ourExtras) > 0 { // case 2
		// log.Debug().Msg("Case 2")

		// send the diff (extra that we have comparing to it) using Rumor
		// back to the remote peer

		newRumorMessage := n.concurrentRumorSequenceTable.generateRumorMessage(ourExtras)

		newStatusTransportMessage, err := n.conf.MessageRegistry.MarshalMessage(newRumorMessage)
		if err != nil {
			log.Err(err).Msg("can't marshal status message for AntiEntropy")
			return err
		}

		header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), pkt.Header.Source, 0)

		err = n.conf.Socket.Send(pkt.Header.Source, transport.Packet{
			Header: &header,
			Msg:    &newStatusTransportMessage,
		}, SendTimeoutValue)
		if err != nil {
			log.Err(err).Msg("ExecStatusMessage case 2 - can't send status message for AntiEntropy")
			return err
		}
	}

	// case 3 is split into case 1 and case 2

	if len(ourMissings) == 0 && len(ourExtras) == 0 && n.conf.ContinueMongering > 0 { // case 4
		// log.Debug().Msg("Case 4")

		if rand.Float64() <= n.conf.ContinueMongering {
			// log.Debug().Msg("Case 4 - running")

			// v
			// build status message
			newStatusMessage := n.concurrentRumorSequenceTable.generateStatusMessage()

			newStatusTransportMessage, err := n.conf.MessageRegistry.MarshalMessage(newStatusMessage)
			if err != nil {
				log.Err(err).Msg("can't marshal status message for AntiEntropy")
				return err
			}

			// send to 1 random neighboor
			randomNeighbor, err := n.concurrentRoutingTable.GetRandomNeighbor(n.conf.Socket.GetAddress(), pkt.Header.Source)
			if err != nil {
				if err != ErrNoRandomNeighborToReturn {
					log.Err(err).Msg("can't get a random neighbor for broadcasting")
					return err
				}

				return nil
			}

			err = n.Unicast(randomNeighbor, newStatusTransportMessage)
			if err != nil {
				log.Err(err).Msg("ExecStatusMessage case 4 - can't unicast ContinueMongering message")
				return err
			}

			// register the pending ack
			// n.pendingAckTracker.AddPendingAck([]string{n.conf.Socket.GetAddress(), pkt.Header.Source, randomNeighbor}, pkt, time.Now().Add(n.conf.AckTimeout).Unix())
		} /* else {
			// x
		} */
	}

	return nil
}

// The handler. This function will be called when a EmptyMessage is received.
func (n *node) ExecEmptyMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	emptyMsg, ok := msg.(*types.EmptyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	log.Debug().Msg(emptyMsg.String())

	// do nothing, it's just for the heartbeat embedded in the rumor

	return nil
}

// The handler. This function will be called when a PrivateMessage is received.
func (n *node) ExecPrivateMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	privateMsg, ok := msg.(*types.PrivateMessage)
	// privateMsg, ok := msg.(*types.PrivateMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	// log.Debug().Msg(privateMsg.String())

	if _, ok := privateMsg.Recipients[n.conf.Socket.GetAddress()]; ok {
		// process it
		newHeader := pkt.Header.Copy()
		newPkt := transport.Packet{
			Header: &newHeader,
			// basically what this does is unwraps one layer out of this thing
			// from PrivateMsg (which is a type of something - kind of the concept of generic) to the type it really embeded
			Msg: privateMsg.Msg,
		}
		err := n.conf.MessageRegistry.ProcessPacket(newPkt)
		if err != nil {
			log.Err(err).Msg("can't process private message packet")
			return err
		}
	} /* else {
		// skip it, not intended for us
	}*/

	return nil
}

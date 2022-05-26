package impl

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
)

const AckTrackerInterval = time.Millisecond * 10

// TODO: use priority queue
type PendingAckTracker struct {
	pending map[string]*PendingAckTrackerData
	n       *node

	sync.RWMutex
}

type PendingAckTrackerData struct {
	excludeAddr []string

	header *transport.Header
	msg    *transport.Message

	timeout int64
}

func (p *PendingAckTracker) Init(n *node) {
	p.pending = make(map[string]*PendingAckTrackerData)
	p.n = n
}

// Upon a new node being created, start this runner
func (p *PendingAckTracker) StartRunner() {
	// log.Debug().Msg("starting PendingAckTracker StartRunner")
	// defer log.Debug().Msg("closing PendingAckTracker StartRunner")

	defer p.n.wg.Done()

	for {
		time.Sleep(AckTrackerInterval)

		if atomic.LoadInt32(&p.n.shouldStop) > 0 {
			return
		}

		resendList := make([]*PendingAckTrackerData, 0)
		{
			currentUnix := time.Now().Unix()
			resendID := make([]string, 0)

			p.RLock()
			for k, v := range p.pending {
				if v.timeout <= currentUnix {
					// timed out
					resendID = append(resendID, k)
					resendList = append(resendList, v)
				}
			}
			p.RUnlock()

			for _, k := range resendID {
				p.RemovePendingAck(k)
			}
		}

		for _, v := range resendList {
			// send the packet again

			// send to 1 random neighboor, excluding the list
			randomNeighbor, err := p.n.concurrentRoutingTable.GetRandomNeighbor(v.excludeAddr...)
			if err != nil {
				if err != ErrNoRandomNeighborToReturn {
					log.Err(err).Msg("can't get a random neighbor for broadcasting")
				} else {
					continue
				}
			}

			// keep the origin
			newHeader := transport.NewHeader(v.header.Source, v.header.RelayedBy, randomNeighbor, 0)
			newPkt := transport.Packet{
				Header: &newHeader,
				Msg:    v.msg,
			}

			err = p.n.conf.Socket.Send(randomNeighbor, newPkt, SendTimeoutValue)
			if err != nil {
				log.Err(err).Msg("can't resend the packet")
			}

			// register the pending ack
			v.excludeAddr = append(v.excludeAddr, randomNeighbor)
			p.n.pendingAckTracker.AddPendingAck(v.excludeAddr, newPkt, time.Now().Add(p.n.conf.AckTimeout).Unix())
		}
	}
}

// When the broadcast is sent, use this function to track the pending ack
func (p *PendingAckTracker) AddPendingAck(excludeAddr []string, pkt transport.Packet, timeout int64) {
	p.Lock()
	defer p.Unlock()

	if p.n.conf.AckTimeout.Nanoseconds() == 0 { // we don't care about the timeout
		return
	}

	pktCopy := pkt.Copy()

	p.pending[pkt.Header.PacketID] = &PendingAckTrackerData{
		excludeAddr: excludeAddr,

		header: pktCopy.Header,
		msg:    pktCopy.Msg,

		timeout: timeout,
	}
}

func (p *PendingAckTracker) RemovePendingAck(packetID string) {
	p.Lock()
	defer p.Unlock()

	delete(p.pending, packetID)
}

package udp

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
)

const bufSize = 65000

var nextPortCandidate uint32 = 1024

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	// Using port 0 makes the system randomly choose a free port.
	// `127.0.0.1:0` is extensively used in tests
	// TODO: retry if the port is taken so the test won't fail randomly
	if strings.HasSuffix(address, ":0") {
		address = address[:len(address)-2]
		port := atomic.AddUint32(&nextPortCandidate, 1)
		address = fmt.Sprintf("%s:%d", address, port /*1024+rand.Int()%10000*/)
	}

	newUDPSocket, err := net.ListenPacket("udp", address)
	if err != nil {
		log.Err(err).Msg("failed to create a new UDP socket")
		return nil, err
	}

	return &Socket{
		socket: newUDPSocket,
		ins:    make([]transport.Packet, 0),
		outs:   make([]transport.Packet, 0),
	}, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	socket net.PacketConn

	ins  []transport.Packet
	outs []transport.Packet

	sync.Mutex
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	s.Lock()
	defer s.Unlock()

	return s.socket.Close()
}

// if the error is a timeout error, we transform it to transport.TimeoutErr
func transformTimeoutError(errDescription string, err error) error {
	if oe, ok := err.(net.Error); ok {
		if oe.Timeout() {
			// log.Err(err).Msgf(errDescription + "(timeout)")
			return transport.TimeoutErr(0)
		}
	}

	log.Err(err).Msgf(errDescription)
	return err
}

// Send implements transport.Socket
// A timeout value of 0 means no timeout.
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	s.Lock()
	defer s.Unlock()

	if timeout.Nanoseconds() != 0 {
		err := s.socket.SetWriteDeadline(time.Now().Add(timeout))
		if err != nil {
			log.Err(err).Msgf("failed to SetWriteDeadline")
			return err
		}
	} else {
		err := s.socket.SetWriteDeadline(time.Time{})
		if err != nil {
			log.Err(err).Msgf("failed to SetWriteDeadline")
			return err
		}
	}

	// https://pkg.go.dev/net#UDPConn.WriteTo
	dst, err := net.ResolveUDPAddr("udp", dest)
	if err != nil {
		log.Err(err).Msgf("failed to resolve UDP addr %s", dest)
		return err
	}

	pktMsg, err := pkt.Marshal()
	if err != nil {
		log.Err(err).Msgf("failed to marshal pkt data")
		return err
	}

	_, err = s.socket.WriteTo(pktMsg, dst)
	if err != nil {
		return transformTimeoutError("failed to send to UDP socket", err)
	}

	pktCopy := pkt.Copy() // the pkt object might be shared
	s.outs = append(s.outs, pktCopy)

	return nil
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
// A timeout value of 0 means no timeout.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	s.Lock()
	defer s.Unlock()

	pkt := transport.Packet{}

	if timeout.Nanoseconds() != 0 {
		err := s.socket.SetReadDeadline(time.Now().Add(timeout))
		if err != nil {
			log.Err(err).Msgf("failed to SetReadDeadline")
			return pkt, err
		}
	} else {
		err := s.socket.SetReadDeadline(time.Time{})
		if err != nil {
			log.Err(err).Msgf("failed to SetReadDeadline")
			return pkt, err
		}
	}

	buf := make([]byte, bufSize)
	n, addr, err := s.socket.ReadFrom(buf)
	if err != nil {
		return pkt, transformTimeoutError("failed to receive from UDP socket", err)
	}

	buf = buf[:n]
	err = pkt.Unmarshal(buf)
	if err != nil {
		log.Err(err).Msgf("failed to unmarshal UDP packet from %s", addr)
		return pkt, err
	}

	pktCopy := pkt.Copy() // the pkt object is shared esp. if it's forwarded!
	s.ins = append(s.ins, pktCopy)

	return pkt, nil
}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	s.Lock()
	defer s.Unlock()

	return s.socket.LocalAddr().String()
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	s.Lock()
	defer s.Unlock()

	ret := make([]transport.Packet, len(s.ins))

	for i, pkt := range s.ins {
		ret[i] = pkt.Copy()
	}

	return ret
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	s.Lock()
	defer s.Unlock()

	ret := make([]transport.Packet, len(s.outs))

	for i, pkt := range s.outs {
		ret[i] = pkt.Copy()
	}

	return ret
}

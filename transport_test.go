// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"bytes"
	"io"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTransport_Join(t *testing.T) {
	net := &MockNetwork{}

	t1 := net.NewTransport("node1")

	c1 := DefaultLANConfig()
	c1.Name = "node1"
	c1.Transport = t1
	m1, err := Create(c1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m1.setAlive(); err != nil {
		t.Fatalf("err: %v", err)
	}
	m1.schedule()
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}()

	c2 := DefaultLANConfig()
	c2.Name = "node2"
	c2.Transport = net.NewTransport("node2")
	m2, err := Create(c2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m2.setAlive(); err != nil {
		t.Fatalf("err: %v", err)
	}
	m2.schedule()
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}()

	num, err := m2.Join([]string{c1.Name + "/" + t1.addr.String()})
	if num != 1 {
		t.Fatalf("bad: %d", num)
	}
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if len(m2.Members()) != 2 {
		t.Fatalf("bad: %v", m2.Members())
	}
	if m2.estNumNodes() != 2 {
		t.Fatalf("bad: %v", m2.Members())
	}

	t.Run("large delegate state", func(t *testing.T) {
		for _, algo := range []CompressionAlgorithm{"none", CompressionAlgorithmLZW, CompressionAlgorithmSnappy} {
			t.Run(string(algo), func(t *testing.T) {
				network := &MockNetwork{}
				seed, _, seedDelegate := newStreamTestMemberlist(t, network, "seed", algo)
				joiner, _, joinerDelegate := newStreamTestMemberlist(t, network, "joiner", algo)
				seedState := bytes.Repeat([]byte{'s'}, maxPushStateBytes+1)
				joinerState := bytes.Repeat([]byte{'j'}, maxPushStateBytes+1)
				seedDelegate.setState(seedState)
				joinerDelegate.setState(joinerState)

				n, err := joiner.Join([]string{seed.config.Name + "/" + seed.LocalNode().Address()})
				require.NoError(t, err)
				require.Equal(t, 1, n)
				seedReceived := seedDelegate.waitForMerge(t, joinerState, true)
				joinerReceived := joinerDelegate.waitForMerge(t, seedState, true)

				seedDelegate.setState(joinerState)
				joinerDelegate.setState(seedState)
				require.NoError(t, joiner.pushPullNode(seed.LocalNode().FullAddress(), false))
				seedDelegate.waitForMerge(t, seedState, false)
				joinerDelegate.waitForMerge(t, joinerState, false)
				require.True(t, bytes.Equal(seedReceived, joinerState), "previous delegate state was overwritten")
				require.True(t, bytes.Equal(joinerReceived, seedState), "previous delegate state was overwritten")
			})
		}
	})
}

func TestTransport_Send(t *testing.T) {
	net := &MockNetwork{}

	t1 := net.NewTransport("node1")
	d1 := &MockDelegate{}

	c1 := DefaultLANConfig()
	c1.Name = "node1"
	c1.Transport = t1
	c1.Delegate = d1
	m1, err := Create(c1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m1.setAlive(); err != nil {
		t.Fatalf("err: %v", err)
	}
	m1.schedule()
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}()

	c2 := DefaultLANConfig()
	c2.Name = "node2"
	c2.Transport = net.NewTransport("node2")
	m2, err := Create(c2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m2.setAlive(); err != nil {
		t.Fatalf("err: %v", err)
	}
	m2.schedule()
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}()

	num, err := m2.Join([]string{c1.Name + "/" + t1.addr.String()})
	if num != 1 {
		t.Fatalf("bad: %d", num)
	}
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := m2.SendTo(t1.addr, []byte("SendTo")); err != nil {
		t.Fatalf("err: %v", err)
	}

	var n1 *Node
	for _, n := range m2.Members() {
		if n.Name == c1.Name {
			n1 = n
			break
		}
	}
	if n1 == nil {
		t.Fatalf("bad")
	}

	if err := m2.SendToUDP(n1, []byte("SendToUDP")); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m2.SendToTCP(n1, []byte("SendToTCP")); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m2.SendBestEffort(n1, []byte("SendBestEffort")); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m2.SendReliable(n1, []byte("SendReliable")); err != nil {
		t.Fatalf("err: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	expected := []string{"SendTo", "SendToUDP", "SendToTCP", "SendBestEffort", "SendReliable"}

	msgs1 := d1.getMessages()

	received := make([]string, len(msgs1))
	for i, bs := range msgs1 {
		received[i] = string(bs)
	}
	// Some of these are UDP so often get re-ordered making the test flaky if we
	// assert send ordering. Sort both slices to be tolerant of re-ordering.
	require.ElementsMatch(t, expected, received)

	t.Run("reliable size limits", func(t *testing.T) {
		payload := bytes.Repeat([]byte{'m'}, maxUserMsgBytes+1)
		for _, algo := range []CompressionAlgorithm{"none", CompressionAlgorithmLZW, CompressionAlgorithmSnappy} {
			t.Run(string(algo), func(t *testing.T) {
				network := &MockNetwork{}
				receiver, _, delegate := newStreamTestMemberlist(t, network, "receiver", algo)
				sender, transport, _ := newStreamTestMemberlist(t, network, "sender", algo)
				for _, method := range []struct {
					name string
					send func(*Node, []byte) error
				}{
					{"SendReliable", sender.SendReliable},
					{"SendToTCP", sender.SendToTCP},
				} {
					t.Run(method.name, func(t *testing.T) {
						require.NoError(t, method.send(receiver.LocalNode(), payload[:maxUserMsgBytes]))
						select {
						case got := <-delegate.messages:
							require.True(t, bytes.Equal(payload[:maxUserMsgBytes], got), "user message changed")
						case <-time.After(10 * time.Second):
							t.Fatal("user message was not delivered")
						}

						dials := transport.dials.Load()
						require.ErrorContains(t, method.send(receiver.LocalNode(), payload), "user message length")
						require.Equal(t, dials, transport.dials.Load(), "oversized message dialed the peer")
					})
				}

				t.Run("required name takes precedence", func(t *testing.T) {
					node := *receiver.LocalNode()
					node.Name = ""
					dials := transport.dials.Load()
					require.ErrorIs(t, sender.SendReliable(&node, payload), errNodeNamesAreRequired)
					require.Equal(t, dials, transport.dials.Load())
				})
			})
		}
	})
}

type streamTestTransport struct {
	NodeAwareTransport
	dials atomic.Int32
}

func (t *streamTestTransport) DialAddressTimeout(addr Address, timeout time.Duration) (net.Conn, error) {
	t.dials.Add(1)
	return t.NodeAwareTransport.DialAddressTimeout(addr, timeout)
}

type streamTestMerge struct {
	state []byte
	join  bool
}

type streamTestDelegate struct {
	MockDelegate
	messages chan []byte
	merges   chan streamTestMerge
}

func (d *streamTestDelegate) NotifyMsg(msg []byte) {
	d.messages <- bytes.Clone(msg)
}

func (d *streamTestDelegate) MergeRemoteState(state []byte, join bool) {
	d.merges <- streamTestMerge{state: state, join: join}
}

func (d *streamTestDelegate) waitForMerge(t *testing.T, want []byte, join bool) []byte {
	t.Helper()
	select {
	case got := <-d.merges:
		require.Equal(t, join, got.join)
		require.True(t, bytes.Equal(want, got.state), "delegate state changed")
		return got.state
	case <-time.After(10 * time.Second):
		t.Fatal("delegate state was not merged")
		return nil
	}
}

func newStreamTestMemberlist(t *testing.T, network *MockNetwork, name string, algo CompressionAlgorithm) (*Memberlist, *streamTestTransport, *streamTestDelegate) {
	t.Helper()
	transport := &streamTestTransport{NodeAwareTransport: network.NewTransport(name)}
	delegate := &streamTestDelegate{messages: make(chan []byte, 1), merges: make(chan streamTestMerge, 1)}
	config := DefaultLANConfig()
	config.Name = name
	config.BindAddr = "127.0.0.1"
	config.Transport = transport
	config.Delegate = delegate
	config.RequireNodeNames = true
	config.ProbeInterval, config.PushPullInterval, config.GossipInterval = 0, 0, 0
	config.EnableCompression = algo != "none"
	if config.EnableCompression {
		config.CompressionAlgorithm = algo
	}
	config.Logger = log.New(io.Discard, "", 0)
	m, err := Create(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })
	return m, transport, delegate
}

type testCountingWriter struct {
	t        *testing.T
	numCalls *int32
}

func (tw testCountingWriter) Write(p []byte) (n int, err error) {
	atomic.AddInt32(tw.numCalls, 1)
	if !strings.Contains(string(p), "memberlist: Error accepting TCP connection") {
		tw.t.Error("did not receive expected log message")
	}
	tw.t.Log("countingWriter:", string(p))
	return len(p), nil
}

// TestTransport_TcpListenBackoff tests that AcceptTCP() errors in NetTransport#tcpListen()
// do not result in a tight loop and spam the log. We verify this here by counting the number
// of entries logged in a given time period.
func TestTransport_TcpListenBackoff(t *testing.T) {

	// testTime is the amount of time we will allow NetTransport#tcpListen() to run
	// This needs to be long enough that to verify that maxDelay is in force,
	// but not so long as to be obnoxious when running the test suite.
	const testTime = 4 * time.Second

	var numCalls int32
	countingWriter := testCountingWriter{t, &numCalls}
	countingLogger := log.New(countingWriter, "test", log.LstdFlags)
	transport := NetTransport{
		streamCh: make(chan net.Conn),
		logger:   countingLogger,
	}
	transport.wg.Add(1)

	// create a listener that will cause AcceptTCP calls to fail
	listener, _ := net.ListenTCP("tcp", nil)
	if err := listener.Close(); err != nil {
		t.Fatalf("not able to close the listener: %v", err)
	}
	go transport.tcpListen(listener)

	// sleep (+yield) for testTime seconds before asking the accept loop to shut down
	time.Sleep(testTime)
	transport.shutdown.Store(1)

	// Verify that the wg was completed on exit (but without blocking this test)
	// maxDelay == 1s, so we will give the routine 1.25s to loop around and shut down.
	c := make(chan struct{})
	go func() {
		defer close(c)
		transport.wg.Wait()
	}()
	select {
	case <-c:
	case <-time.After(1250 * time.Millisecond):
		t.Error("timed out waiting for transport waitgroup to be done after flagging shutdown")
	}

	// In testTime==4s, we expect to loop approximately 12 times (and log approximately 11 errors),
	// with the following delays (in ms):
	//   0+5+10+20+40+80+160+320+640+1000+1000+1000 == 4275 ms
	// Too few calls suggests that the minDelay is not in force; too many calls suggests that the
	// maxDelay is not in force or that the back-off isn't working at all.
	// We'll leave a little flex; the important thing here is the asymptotic behavior.
	// If the minDelay or maxDelay in NetTransport#tcpListen() are modified, this test may fail
	// and need to be adjusted.
	require.True(t, numCalls > 8)
	require.True(t, numCalls < 14)

	// no connections should have been accepted and sent to the channel
	require.Equal(t, len(transport.streamCh), 0)
}

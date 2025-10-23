// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUtil_PortFunctions(t *testing.T) {
	tests := []struct {
		addr       string
		hasPort    bool
		ensurePort string
	}{
		{"1.2.3.4", false, "1.2.3.4:8301"},
		{"1.2.3.4:1234", true, "1.2.3.4:1234"},
		{"2600:1f14:e22:1501:f9a:2e0c:a167:67e8", false, "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:8301"},
		{"[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]", false, "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:8301"},
		{"[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:1234", true, "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:1234"},
		{"localhost", false, "localhost:8301"},
		{"localhost:1234", true, "localhost:1234"},
		{"hashicorp.com", false, "hashicorp.com:8301"},
		{"hashicorp.com:1234", true, "hashicorp.com:1234"},
	}
	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			if got, want := hasPort(tt.addr), tt.hasPort; got != want {
				t.Fatalf("got %v want %v", got, want)
			}
			if got, want := ensurePort(tt.addr, 8301), tt.ensurePort; got != want {
				t.Fatalf("got %v want %v", got, want)
			}
		})
	}
}

func TestEncodeDecode(t *testing.T) {
	msg := &ping{SeqNo: 100}
	buf, err := encode(pingMsg, msg, false)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	var out ping
	if err := decode(buf.Bytes()[1:], &out); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	if msg.SeqNo != out.SeqNo {
		t.Fatalf("bad sequence no")
	}
}

func TestRandomOffset(t *testing.T) {
	vals := make(map[int]struct{})
	for i := 0; i < 100; i++ {
		offset := randomOffset(2 << 30)
		if _, ok := vals[offset]; ok {
			t.Fatalf("got collision")
		}
		vals[offset] = struct{}{}
	}
}

func TestRandomOffset_Zero(t *testing.T) {
	offset := randomOffset(0)
	if offset != 0 {
		t.Fatalf("bad offset")
	}
}

func TestSuspicionTimeout(t *testing.T) {
	timeouts := map[int]time.Duration{
		5:    1000 * time.Millisecond,
		10:   1000 * time.Millisecond,
		50:   1698 * time.Millisecond,
		100:  2000 * time.Millisecond,
		500:  2698 * time.Millisecond,
		1000: 3000 * time.Millisecond,
	}
	for n, expected := range timeouts {
		timeout := suspicionTimeout(3, n, time.Second) / 3
		if timeout != expected {
			t.Fatalf("bad: %v, %v", expected, timeout)
		}
	}
}

func TestRetransmitLimit(t *testing.T) {
	lim := retransmitLimit(3, 0)
	if lim != 0 {
		t.Fatalf("bad val %v", lim)
	}
	lim = retransmitLimit(3, 1)
	if lim != 3 {
		t.Fatalf("bad val %v", lim)
	}
	lim = retransmitLimit(3, 99)
	if lim != 6 {
		t.Fatalf("bad val %v", lim)
	}
}

func TestShuffleNodes(t *testing.T) {
	orig := []*nodeState{
		&nodeState{
			State: StateDead,
		},
		&nodeState{
			State: StateAlive,
		},
		&nodeState{
			State: StateAlive,
		},
		&nodeState{
			State: StateDead,
		},
		&nodeState{
			State: StateAlive,
		},
		&nodeState{
			State: StateAlive,
		},
		&nodeState{
			State: StateDead,
		},
		&nodeState{
			State: StateAlive,
		},
	}
	nodes := make([]*nodeState, len(orig))
	copy(nodes[:], orig[:])

	if !reflect.DeepEqual(nodes, orig) {
		t.Fatalf("should match")
	}

	shuffleNodes(nodes)

	if reflect.DeepEqual(nodes, orig) {
		t.Fatalf("should not match")
	}
}

func TestPushPullScale(t *testing.T) {
	sec := time.Second
	for i := 0; i <= 32; i++ {
		if s := pushPullScale(sec, i); s != sec {
			t.Fatalf("Bad time scale: %v", s)
		}
	}
	for i := 33; i <= 64; i++ {
		if s := pushPullScale(sec, i); s != 2*sec {
			t.Fatalf("Bad time scale: %v", s)
		}
	}
	for i := 65; i <= 128; i++ {
		if s := pushPullScale(sec, i); s != 3*sec {
			t.Fatalf("Bad time scale: %v", s)
		}
	}
}

func TestMoveDeadNodes(t *testing.T) {
	nodes := []*nodeState{
		&nodeState{
			State:       StateDead,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		&nodeState{
			State:       StateAlive,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		// This dead node should not be moved, as its state changed
		// less than the specified GossipToTheDead time ago
		&nodeState{
			State:       StateDead,
			StateChange: time.Now().Add(-10 * time.Second),
		},
		// This left node should not be moved, as its state changed
		// less than the specified GossipToTheDead time ago
		&nodeState{
			State:       StateLeft,
			StateChange: time.Now().Add(-10 * time.Second),
		},
		&nodeState{
			State:       StateLeft,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		&nodeState{
			State:       StateAlive,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		&nodeState{
			State:       StateDead,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		&nodeState{
			State:       StateAlive,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		&nodeState{
			State:       StateLeft,
			StateChange: time.Now().Add(-20 * time.Second),
		},
	}

	idx := moveDeadNodes(nodes, (15 * time.Second))
	if idx != 5 {
		t.Fatalf("bad index")
	}
	for i := 0; i < idx; i++ {
		switch i {
		case 2:
			// Recently dead node remains at index 2,
			// since nodes are swapped out to move to end.
			if nodes[i].State != StateDead {
				t.Fatalf("Bad state %d", i)
			}
		case 3:
			//Recently left node should remain at 3
			if nodes[i].State != StateLeft {
				t.Fatalf("Bad State %d", i)
			}
		default:
			if nodes[i].State != StateAlive {
				t.Fatalf("Bad state %d", i)
			}
		}
	}
	for i := idx; i < len(nodes); i++ {
		if !nodes[i].DeadOrLeft() {
			t.Fatalf("Bad state %d", i)
		}
	}
}

func TestKRandomNodes(t *testing.T) {
	nodes := []*nodeState{}
	for i := 0; i < 90; i++ {
		// Half the nodes are in a bad state
		state := StateAlive
		switch i % 3 {
		case 0:
			state = StateAlive
		case 1:
			state = StateSuspect
		case 2:
			state = StateDead
		}
		nodes = append(nodes, &nodeState{
			Node: Node{
				Name: fmt.Sprintf("test%d", i),
			},
			State: state,
		})
	}

	filterFunc := func(n *nodeState) bool {
		if n.Name == "test0" || n.State != StateAlive {
			return true
		}
		return false
	}

	s1 := kRandomNodes(3, nodes, nil, filterFunc)
	s2 := kRandomNodes(3, nodes, nil, filterFunc)
	s3 := kRandomNodes(3, nodes, nil, filterFunc)

	if reflect.DeepEqual(s1, s2) {
		t.Fatalf("unexpected equal")
	}
	if reflect.DeepEqual(s1, s3) {
		t.Fatalf("unexpected equal")
	}
	if reflect.DeepEqual(s2, s3) {
		t.Fatalf("unexpected equal")
	}

	for _, s := range [][]Node{s1, s2, s3} {
		if len(s) != 3 {
			t.Fatalf("bad len")
		}
		for _, n := range s {
			if n.Name == "test0" {
				t.Fatalf("Bad name")
			}
			if n.State != StateAlive {
				t.Fatalf("Bad state")
			}
		}
	}
}

func TestKRandomNodesWithDelegate(t *testing.T) {
	var nodes []*nodeState
	for i := 0; i < 20; i++ {
		state := StateAlive
		switch i % 3 {
		case 0:
			state = StateAlive
		case 1:
			state = StateSuspect
		case 2:
			state = StateDead
		}
		nodes = append(nodes, &nodeState{
			Node:  Node{Name: fmt.Sprintf("%d", i)},
			State: state,
		})
	}

	t.Run("with preferred nodes", func(t *testing.T) {
		// Create a delegate that selects nodes 3, 6, 9, 12
		// and prefers nodes 6 and 12
		delegate := &testNodeSelectionDelegate{
			selectFunc: func(n Node) (selected, preferred bool) {
				switch n.Name {
				case "3", "6", "9", "12":
					selected = true
					preferred = n.Name == "6" || n.Name == "12"
				}
				return
			},
		}

		filterFunc := func(n *nodeState) bool {
			return n.State != StateAlive
		}

		// Request 3 nodes
		result := kRandomNodes(3, nodes, delegate, filterFunc)

		// Should get up to 3 nodes
		require.LessOrEqual(t, len(result), 3)
		require.Greater(t, len(result), 0)

		// At least one should be a preferred node
		hasPreferred := false
		for _, node := range result {
			if node.Name == "6" || node.Name == "12" {
				hasPreferred = true
				break
			}
		}
		assert.True(t, hasPreferred)

		// All nodes should be in the selected set
		for _, node := range result {
			assert.Contains(t, []string{"3", "6", "9", "12"}, node.Name)
			assert.Equal(t, StateAlive, node.State)
		}
	})

	t.Run("with no preferred nodes", func(t *testing.T) {
		// Create a delegate that selects nodes 3, 6, 9 but marks none as preferred
		delegate := &testNodeSelectionDelegate{
			selectFunc: func(n Node) (selected, preferred bool) {
				switch n.Name {
				case "3", "6", "9":
					selected = true
					preferred = false
				}
				return
			},
		}

		filterFunc := func(n *nodeState) bool {
			return n.State != StateAlive
		}

		// Request 2 nodes
		result := kRandomNodes(2, nodes, delegate, filterFunc)

		// Should get up to 2 nodes
		require.LessOrEqual(t, len(result), 2)
		require.Greater(t, len(result), 0)

		// All should be in the selected set
		for _, node := range result {
			assert.Contains(t, []string{"3", "6", "9"}, node.Name)
			assert.Equal(t, StateAlive, node.State)
		}
	})
}

func TestMakeCompoundMessage(t *testing.T) {
	msg := &ping{SeqNo: 100}
	buf, err := encode(pingMsg, msg, false)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	msgs := [][]byte{buf.Bytes(), buf.Bytes(), buf.Bytes()}
	compound := makeCompoundMessage(msgs)

	if compound.Len() != 3*buf.Len()+3*compoundOverhead+compoundHeaderOverhead {
		t.Fatalf("bad len")
	}
}

func TestDecodeCompoundMessage(t *testing.T) {
	msg := &ping{SeqNo: 100}
	buf, err := encode(pingMsg, msg, false)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	msgs := [][]byte{buf.Bytes(), buf.Bytes(), buf.Bytes()}
	compound := makeCompoundMessage(msgs)

	trunc, parts, err := decodeCompoundMessage(compound.Bytes()[1:])
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	if trunc != 0 {
		t.Fatalf("should not truncate")
	}
	if len(parts) != 3 {
		t.Fatalf("bad parts")
	}
	for _, p := range parts {
		if len(p) != buf.Len() {
			t.Fatalf("bad part len")
		}
	}
}

func TestDecodeCompoundMessage_NumberOfPartsOverflow(t *testing.T) {
	buf := []byte{0x80}
	_, _, err := decodeCompoundMessage(buf)
	require.Error(t, err)
	require.Equal(t, err.Error(), "truncated len slice")
}

func TestDecodeCompoundMessage_Trunc(t *testing.T) {
	msg := &ping{SeqNo: 100}
	buf, err := encode(pingMsg, msg, false)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	msgs := [][]byte{buf.Bytes(), buf.Bytes(), buf.Bytes()}
	compound := makeCompoundMessage(msgs)

	trunc, parts, err := decodeCompoundMessage(compound.Bytes()[1:38])
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	if trunc != 1 {
		t.Fatalf("truncate: %d", trunc)
	}
	if len(parts) != 2 {
		t.Fatalf("bad parts")
	}
	for _, p := range parts {
		if len(p) != buf.Len() {
			t.Fatalf("bad part len")
		}
	}
}

func TestCompressDecompressPayload(t *testing.T) {
	buf, err := compressPayload([]byte("testing"), false)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	decomp, err := decompressPayload(buf.Bytes()[1:])
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	if !reflect.DeepEqual(decomp, []byte("testing")) {
		t.Fatalf("bad payload: %v", decomp)
	}
}

type testNodeSelectionDelegate struct {
	selectFunc func(Node) (selected, preferred bool)
}

func (d *testNodeSelectionDelegate) SelectNode(n Node) (selected, preferred bool) {
	return d.selectFunc(n)
}

func TestMakeCompoundMessages(t *testing.T) {
	const (
		smallMsgSeqNo         = uint32(1)
		smallMsgPayloadLength = 1
		bigMsgSeqNo           = uint32(2)
		bigMsgPayloadLength   = 70000
	)

	// Generate some fixtures.
	smallMessages := make([][]byte, 300)
	for i := 0; i < len(smallMessages); i++ {
		msg := &ackResp{SeqNo: smallMsgSeqNo, Payload: []byte{byte(i)}}
		encoded, err := encode(ackRespMsg, msg, false)
		require.NoError(t, err)

		smallMessages[i] = encoded.Bytes()
	}

	bigMessages := make([][]byte, 3)
	for i := 0; i < len(bigMessages); i++ {
		payload := []byte{bigMsgPayloadLength - 1: byte(i)}
		require.Len(t, payload, bigMsgPayloadLength)

		msg := &ackResp{SeqNo: bigMsgSeqNo, Payload: payload}
		encoded, err := encode(ackRespMsg, msg, false)
		require.NoError(t, err)

		bigMessages[i] = encoded.Bytes()
	}

	tests := map[string]struct {
		input    [][]byte
		expected [][]byte
	}{
		"no input": {
			input:    [][]byte{},
			expected: [][]byte{},
		},
		"one small message": {
			input:    smallMessages[0:1],
			expected: [][]byte{makeCompoundMessage(smallMessages[0:1]).Bytes()},
		},
		"few small messages": {
			input:    smallMessages[0:3],
			expected: [][]byte{makeCompoundMessage(smallMessages[0:3]).Bytes()},
		},
		"many small messages (more than 255)": {
			input: smallMessages[0:300],
			expected: [][]byte{
				makeCompoundMessage(smallMessages[0:255]).Bytes(),
				makeCompoundMessage(smallMessages[255:300]).Bytes(),
			},
		},
		"one big message": {
			input:    bigMessages[0:1],
			expected: bigMessages[0:1],
		},
		"few big messages": {
			input:    bigMessages[0:3],
			expected: bigMessages[0:3],
		},
		"mix of many small and big messages": {
			input: func() [][]byte {
				var out [][]byte

				out = append(out, bigMessages[0])
				out = append(out, smallMessages[0:20]...)
				out = append(out, bigMessages[1])
				out = append(out, smallMessages[20:260]...)
				out = append(out, bigMessages[2])
				out = append(out, smallMessages[260:300]...)

				return out
			}(),
			expected: [][]byte{
				bigMessages[0],
				bigMessages[1],
				bigMessages[2],
				makeCompoundMessage(smallMessages[0:255]).Bytes(),
				makeCompoundMessage(smallMessages[255:300]).Bytes(),
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual := makeCompoundMessages(testData.input)

			// Get the actual []byte of each message.
			actualBytes := make([][]byte, 0, len(actual))
			for _, data := range actual {
				actualBytes = append(actualBytes, data.Bytes())
			}

			assert.Equal(t, testData.expected, actualBytes)

			// Ensure we can successfully decode every message.
			for i := 0; i < len(actual); i++ {
				msg := actualBytes[i]
				typ := messageType(msg[0])

				switch typ {
				case ackRespMsg:
					var got ackResp
					require.NoError(t, decode(msg[1:], &got))

					//nolint:staticcheck // reason: linter suggests a switch but we prefer to keep the code synced with upstream
					if got.SeqNo == smallMsgSeqNo {
						assert.Len(t, got.Payload, smallMsgPayloadLength)
					} else if got.SeqNo == bigMsgSeqNo {
						assert.Len(t, got.Payload, bigMsgPayloadLength)
					} else {
						require.Fail(t, "unexpected seq no")
					}
				case compoundMsg:
					trunc, parts, err := decodeCompoundMessage(msg[1:])
					require.NoError(t, err)
					require.Equal(t, 0, trunc)

					for _, part := range parts {
						require.Equal(t, ackRespMsg, messageType(part[0]))

						var got ackResp
						require.NoError(t, decode(part[1:], &got))
						assert.Equal(t, smallMsgSeqNo, got.SeqNo)
						assert.Len(t, got.Payload, smallMsgPayloadLength)
					}
				default:
					require.Fail(t, "unexpected message")
				}
			}
		})
	}
}

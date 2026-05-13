// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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
	if err := decode(buf[1:], &out); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	if msg.SeqNo != out.SeqNo {
		t.Fatalf("bad sequence no")
	}
}

// TestEncodeRoundTrip verifies repeated encode/decode calls each produce
// independently correct bytes.
func TestEncodeRoundTrip(t *testing.T) {
	const inputs = 32
	for i := range inputs {
		buf, err := encode(pingMsg, &ping{SeqNo: uint32(i), Node: "n"}, false)
		require.NoError(t, err)
		require.Greater(t, len(buf), 0)

		var p ping
		require.NoError(t, decode(buf[1:], &p))
		require.Equal(t, uint32(i), p.SeqNo)
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
	orig := []*NodeState{
		&NodeState{
			State: StateDead,
		},
		&NodeState{
			State: StateAlive,
		},
		&NodeState{
			State: StateAlive,
		},
		&NodeState{
			State: StateDead,
		},
		&NodeState{
			State: StateAlive,
		},
		&NodeState{
			State: StateAlive,
		},
		&NodeState{
			State: StateDead,
		},
		&NodeState{
			State: StateAlive,
		},
	}
	nodes := make([]*NodeState, len(orig))
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
	nodes := []*NodeState{
		&NodeState{
			State:       StateDead,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		&NodeState{
			State:       StateAlive,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		// This dead node should not be moved, as its state changed
		// less than the specified GossipToTheDead time ago
		&NodeState{
			State:       StateDead,
			StateChange: time.Now().Add(-10 * time.Second),
		},
		// This left node should not be moved, as its state changed
		// less than the specified GossipToTheDead time ago
		&NodeState{
			State:       StateLeft,
			StateChange: time.Now().Add(-10 * time.Second),
		},
		&NodeState{
			State:       StateLeft,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		&NodeState{
			State:       StateAlive,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		&NodeState{
			State:       StateDead,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		&NodeState{
			State:       StateAlive,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		&NodeState{
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
	nodes := []*NodeState{}
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
		nodes = append(nodes, &NodeState{
			Node: Node{
				Name: fmt.Sprintf("test%d", i),
			},
			State: state,
		})
	}

	filterFunc := func(n *NodeState) bool {
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
	var nodes []*NodeState
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
		nodes = append(nodes, &NodeState{
			Node:  Node{Name: fmt.Sprintf("%d", i)},
			State: state,
		})
	}

	t.Run("with preferred nodes", func(t *testing.T) {
		// Create a delegate that selects nodes 3, 6, 9, 12
		// and prefers node 6
		delegate := &testNodeSelectionDelegate{
			selectFunc: func(nodes []*NodeState) (selected []*NodeState, preferred *NodeState) {
				for _, n := range nodes {
					switch n.Name {
					case "3", "6", "9", "12":
						selected = append(selected, n)
						if n.Name == "6" {
							preferred = n
						}
					}
				}
				return
			},
		}

		excludeFunc := func(n *NodeState) bool {
			return n.State != StateAlive
		}

		// Request 3 nodes
		result := kRandomNodes(3, nodes, delegate, excludeFunc)

		// Should get up to 3 nodes
		require.LessOrEqual(t, len(result), 3)
		require.Greater(t, len(result), 0)

		// The preferred node "6" should be in the result
		hasPreferred := false
		for _, node := range result {
			if node.Name == "6" {
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
			selectFunc: func(nodes []*NodeState) (selected []*NodeState, preferred *NodeState) {
				for _, n := range nodes {
					switch n.Name {
					case "3", "6", "9":
						selected = append(selected, n)
					}
				}
				return // preferred is nil
			},
		}

		filterFunc := func(n *NodeState) bool {
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

	t.Run("all nodes selected with one preferred", func(t *testing.T) {
		// Create a delegate that selects all nodes and picks the first alive one as preferred
		delegate := &testNodeSelectionDelegate{
			selectFunc: func(nodes []*NodeState) (_ []*NodeState, preferred *NodeState) {
				for _, n := range nodes {
					if preferred == nil && n.State == StateAlive {
						preferred = n
					}
				}
				return nodes, preferred
			},
		}

		excludeFunc := func(n *NodeState) bool {
			return n.State != StateAlive
		}

		result := kRandomNodes(len(nodes), nodes, delegate, excludeFunc)

		// Verify all returned nodes are alive
		for _, node := range result {
			assert.Equal(t, StateAlive, node.State)
		}

		// Verify we got all unique nodes (no duplicates)
		seen := make(map[string]bool)
		for _, node := range result {
			assert.False(t, seen[node.Name], "duplicate node: %s", node.Name)
			seen[node.Name] = true
		}
	})
}

func TestMakeCompoundMessage(t *testing.T) {
	msg := &ping{SeqNo: 100}
	buf, err := encode(pingMsg, msg, false)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	msgs := [][]byte{buf, buf, buf}
	compound := makeCompoundMessage(msgs)

	if len(compound) != 3*len(buf)+3*compoundOverhead+compoundHeaderOverhead {
		t.Fatalf("bad len")
	}
}

func TestDecodeCompoundMessage(t *testing.T) {
	msg := &ping{SeqNo: 100}
	buf, err := encode(pingMsg, msg, false)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	msgs := [][]byte{buf, buf, buf}
	compound := makeCompoundMessage(msgs)

	trunc, parts, err := decodeCompoundMessage(compound[1:])
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
		if len(p) != len(buf) {
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

	msgs := [][]byte{buf, buf, buf}
	compound := makeCompoundMessage(msgs)

	trunc, parts, err := decodeCompoundMessage(compound[1:38])
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
		if len(p) != len(buf) {
			t.Fatalf("bad part len")
		}
	}
}

type testNodeSelectionDelegate struct {
	selectFunc func([]*NodeState) (selected []*NodeState, preferred *NodeState)
}

func (d *testNodeSelectionDelegate) SelectNodes(nodes []*NodeState) (selected []*NodeState, preferred *NodeState) {
	return d.selectFunc(nodes)
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
		smallMessages[i] = encoded
	}

	bigMessages := make([][]byte, 3)
	for i := 0; i < len(bigMessages); i++ {
		payload := []byte{bigMsgPayloadLength - 1: byte(i)}
		require.Len(t, payload, bigMsgPayloadLength)

		msg := &ackResp{SeqNo: bigMsgSeqNo, Payload: payload}
		encoded, err := encode(ackRespMsg, msg, false)
		require.NoError(t, err)
		bigMessages[i] = encoded
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
			expected: [][]byte{makeCompoundMessage(smallMessages[0:1])},
		},
		"few small messages": {
			input:    smallMessages[0:3],
			expected: [][]byte{makeCompoundMessage(smallMessages[0:3])},
		},
		"many small messages (more than 255)": {
			input: smallMessages[0:300],
			expected: [][]byte{
				makeCompoundMessage(smallMessages[0:255]),
				makeCompoundMessage(smallMessages[255:300]),
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
				makeCompoundMessage(smallMessages[0:255]),
				makeCompoundMessage(smallMessages[255:300]),
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual := makeCompoundMessages(testData.input)

			assert.Equal(t, testData.expected, actual)

			// Ensure we can successfully decode every message.
			for i := 0; i < len(actual); i++ {
				msg := actual[i]
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

func BenchmarkKRandomNodes(b *testing.B) {
	// Create 10K alive nodes
	nodes := make([]*NodeState, 10000)
	for i := 0; i < 10000; i++ {
		nodes[i] = &NodeState{
			Node:  Node{Name: fmt.Sprintf("node%d", i)},
			State: StateAlive,
		}
	}

	excludeFunc := func(n *NodeState) bool {
		return n.State != StateAlive
	}

	b.Run("without delegate", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			kRandomNodes(3, nodes, nil, excludeFunc)
		}
	})

	b.Run("with delegate", func(b *testing.B) {
		delegate := &testNodeSelectionDelegate{
			selectFunc: func(nodes []*NodeState) (selected []*NodeState, preferred *NodeState) {
				if len(nodes) > 0 {
					preferred = nodes[0]
				}
				return nodes, preferred
			},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			kRandomNodes(3, nodes, delegate, excludeFunc)
		}
	})
}

// TestPushPullBuffer_Reused asserts the push-pull pool actually pools —
// i.e., steady-state Get/Release cycles don't allocate. A regression that
// drops the pool path entirely (e.g., always returning new(bytes.Buffer))
// would cause one alloc per iteration and fail this test.
//
// sync.Pool may drop entries on GC, so we measure averaged allocations
// across many iterations and tolerate a small upper bound.
func TestPushPullBuffer_Reused(t *testing.T) {
	allocs := testing.AllocsPerRun(1000, func() {
		b := getPushPullBuffer()
		b.WriteString("hello")
		releasePushPullBuffer(b)
	})
	require.Less(t, allocs, 0.5, "expected push-pull pool to amortize allocations to ~0/op")
}

// TestReleasePushPullBuffer_BoundedCap is the push-pull-pool counterpart
// of TestReleaseLZWBuffer_BoundedCap.
func TestReleasePushPullBuffer_BoundedCap(t *testing.T) {
	big := getPushPullBuffer()
	big.Write(make([]byte, maxPooledPushPullBufCap+1))
	require.Greater(t, big.Cap(), maxPooledPushPullBufCap)

	releasePushPullBuffer(big)
	for i := range 10 {
		b := getPushPullBuffer()
		require.LessOrEqual(t, b.Cap(), maxPooledPushPullBufCap,
			"oversized buffer leaked through push-pull pool on iteration %d", i)
		releasePushPullBuffer(b)
	}
}

// BenchmarkEncode isolates the encode() path (msgpack only, no
// compression).
func BenchmarkEncode(b *testing.B) {
	sizes := []int{64, 256, 1500, 16 * 1024}
	assertBenchSizes(b, sizes)
	for _, c := range benchCorpora() {
		for _, size := range sizes {
			b.Run(fmt.Sprintf("%s/%d", c.name, size), func(b *testing.B) {
				payload := &compressedPayload{Algo: lzwCompressionType, Buf: c.payload[:size]}
				b.ResetTimer()
				b.ReportAllocs()
				for b.Loop() {
					_, err := encode(compressMsg, payload, false)
					require.NoError(b, err)
				}
			})
		}
	}
}

// BenchmarkMakeCompoundMessage measures the compound-message hot path,
// which sits behind every gossip-piggyback send.
func BenchmarkMakeCompoundMessage(b *testing.B) {
	sizes := []int{64, 256, 1500}
	counts := []int{1, 8, 64}
	for _, sz := range sizes {
		for _, n := range counts {
			msgs := make([][]byte, n)
			for i := range msgs {
				msgs[i] = randBytes(sz)
			}
			b.Run(fmt.Sprintf("%d-msgs-of-%d", n, sz), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_ = makeCompoundMessage(msgs)
				}
			})
		}
	}
}

// BenchmarkPushPullBuffer isolates pushPullBufPool's contribution on
// decryptRemoteState's io.CopyN staging buffer. NoPool uses a
// function-local bytes.Buffer per call. Push-pull state can range
// from a few KiB (single-node gossip) up to maxPushStateBytes
// (20 MiB); the pool earns its keep on the larger sizes where
// io.CopyN's growth-doubling does the most work.
func BenchmarkPushPullBuffer(b *testing.B) {
	keyring, err := NewKeyring(nil, TestKeys[0])
	require.NoError(b, err)
	conf := DefaultLANConfig()
	conf.Keyring = keyring
	conf.GossipVerifyOutgoing = true
	m := &Memberlist{config: conf}
	m.initCompressionMetricLabels()

	for _, sz := range []int{64 * 1024, 1 << 20, 8 << 20} {
		sendBuf := randBytes(sz)
		envelope, err := m.encryptLocalState(sendBuf, "")
		require.NoError(b, err)
		// decryptRemoteState writes the encryptMsg byte into the
		// staging buffer itself, so the input it reads from begins
		// immediately after that byte.
		cipherInput := envelope[1:]

		b.Run(fmt.Sprintf("%d/pool", sz), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := m.decryptRemoteState(bytes.NewReader(cipherInput), "")
				require.NoError(b, err)
			}
		})
		b.Run(fmt.Sprintf("%d/no pool", sz), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := decryptRemoteStateNoBufPool(m, bytes.NewReader(cipherInput), "")
				require.NoError(b, err)
			}
		})
	}
}

// decryptRemoteStateNoBufPool mirrors decryptRemoteState but stages
// cipher text in a function-local bytes.Buffer instead of via
// pushPullBufPool. Benchmark-only — used by BenchmarkPushPullBuffer
// to measure the pool's marginal contribution.
func decryptRemoteStateNoBufPool(m *Memberlist, bufConn io.Reader, streamLabel string) ([]byte, error) {
	var cipherText bytes.Buffer
	cipherText.WriteByte(byte(encryptMsg))
	if _, err := io.CopyN(&cipherText, bufConn, 4); err != nil {
		return nil, err
	}
	moreBytes := binary.BigEndian.Uint32(cipherText.Bytes()[1:5])
	if moreBytes > maxPushStateBytes {
		return nil, fmt.Errorf("remote node state is larger than limit (%d)", moreBytes)
	}
	if _, err := io.CopyN(&cipherText, bufConn, int64(moreBytes)); err != nil {
		return nil, err
	}
	dataBytes := appendBytes(cipherText.Bytes()[:5], []byte(streamLabel))
	cipherBytes := cipherText.Bytes()[5:]
	keys := m.config.Keyring.GetKeys()
	return decryptPayload(keys, cipherBytes, dataBytes)
}

// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"

	metrics "github.com/hashicorp/go-metrics/compat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompressDecompress(t *testing.T) {
	t.Run("RoundTrip", func(t *testing.T) {
		algos := []compressionType{lzwCompressionType, snappyCompressionType}
		sizes := []int{0, 1, 100, 100 * 1024, 1024 * 1024}

		for _, algo := range algos {
			for _, size := range sizes {
				t.Run(fmt.Sprintf("%s/%d", compressionTypeLabel(algo), size), func(t *testing.T) {
					input := randBytes(size)
					buf, err := compressPayload(algo, input, false)
					require.NoError(t, err)

					gotAlgo, decoded, err := decompressPayload(buf[1:])
					require.NoError(t, err)
					require.Equal(t, algo, gotAlgo)
					require.Equal(t, input, decoded)
				})
			}
		}
	})

	// Lock down the rollout invariant: receivers MUST
	// decode every algorithm they understand, regardless of which algorithm
	// they would emit themselves.
	t.Run("MixedCodecReceiver", func(t *testing.T) {
		for _, senderAlgo := range []compressionType{lzwCompressionType, snappyCompressionType} {
			t.Run(compressionTypeLabel(senderAlgo), func(t *testing.T) {
				input := []byte("the quick brown fox jumps over the lazy dog")
				buf, err := compressPayload(senderAlgo, input, false)
				require.NoError(t, err)

				// The receiver dispatches off the on-wire algo tag, never off
				// any local config — exercise both code paths via decompressPayload.
				gotAlgo, decoded, err := decompressPayload(buf[1:])
				require.NoError(t, err)
				require.Equal(t, senderAlgo, gotAlgo)
				require.Equal(t, input, decoded)
			})
		}
	})

	t.Run("ConcurrentRace", func(t *testing.T) {
		const iterations = 200
		algos := []compressionType{lzwCompressionType, snappyCompressionType}

		var wg sync.WaitGroup
		for w := range 8 {
			wg.Add(1)
			go func(seed int64) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(seed))
				for range iterations {
					algo := algos[rng.Intn(len(algos))]
					input := randBytesWith(rng, rng.Intn(2048))
					buf, err := compressPayload(algo, input, false)
					if !assert.NoError(t, err) {
						return
					}
					gotAlgo, decoded, err := decompressPayload(buf[1:])
					if !assert.NoError(t, err) {
						return
					}
					if !assert.Equal(t, algo, gotAlgo) {
						return
					}
					if !assert.True(t, bytes.Equal(decoded, input), "payload mismatch") {
						return
					}
				}
			}(int64(w))
		}
		wg.Wait()
	})

	// Encode a long input, then a short input. The short
	// decompressed result must equal the short input exactly, with no
	// leftover bytes from the long one. Catches Reset/Put-side hygiene bugs
	// in the LZW writer/reader pools and the snappy buffer pools.
	t.Run("PoolResetHygiene", func(t *testing.T) {
		for _, algo := range []compressionType{lzwCompressionType, snappyCompressionType} {
			t.Run(compressionTypeLabel(algo), func(t *testing.T) {
				long := bytes.Repeat([]byte("A"), 16*1024)
				short := []byte("BB")

				bufLong, err := compressPayload(algo, long, false)
				require.NoError(t, err)
				_, decLong, err := decompressPayload(bufLong[1:])
				require.NoError(t, err)
				require.Equal(t, long, decLong)

				bufShort, err := compressPayload(algo, short, false)
				require.NoError(t, err)
				_, decShort, err := decompressPayload(bufShort[1:])
				require.NoError(t, err)
				require.Equal(t, short, decShort)
			})
		}
	})

	// Lock down the contract: compressPayload returns a slice that is
	// independent of any internal pool, so subsequent compressPayload
	// calls (which churn the same pools) cannot mutate an earlier
	// return. A regression that returned pool memory would let the
	// churn loop overwrite buf's underlying array, breaking the
	// snapshot equality.
	t.Run("DoesNotRetainScratch", func(t *testing.T) {
		for _, algo := range []compressionType{lzwCompressionType, snappyCompressionType} {
			t.Run(compressionTypeLabel(algo), func(t *testing.T) {
				input := bytes.Repeat([]byte("xy"), 512)
				buf, err := compressPayload(algo, input, false)
				require.NoError(t, err)
				snapshot := append([]byte(nil), buf...)

				// Force pool churn: do several more encodes of different bytes.
				// If compressPayload retained any reference into pooled scratch,
				// the snapshot would diverge from buf.
				for range 5 {
					_, err := compressPayload(algo, []byte(strings.Repeat("z", 1024)), false)
					require.NoError(t, err)
				}

				require.Equal(t, snapshot, buf)
			})
		}
	})
}

// TestReleaseLZWBuffer_BoundedCap verifies the LZW scratch pool drops
// oversized buffers rather than retaining them forever.
func TestReleaseLZWBuffer_BoundedCap(t *testing.T) {
	big := getLZWBuffer()
	big.Write(make([]byte, maxPooledLZWBufferCap+1))
	require.Greater(t, big.Cap(), maxPooledLZWBufferCap)

	releaseLZWBuffer(big)
	// We can't directly assert the pool's contents (sync.Pool's interface
	// permits the runtime to drop entries on its own), but we can assert
	// that getLZWBuffer() never returns a buffer with cap > limit unless one
	// was explicitly retained — release of an oversized buffer must not
	// re-surface here.
	for i := range 10 {
		b := getLZWBuffer()
		require.LessOrEqual(t, b.Cap(), maxPooledLZWBufferCap,
			"oversized buffer leaked through pool on iteration %d", i)
		releaseLZWBuffer(b)
	}
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

func TestMemberlist_initCompressionMetricLabels(t *testing.T) {
	base := []metrics.Label{{Name: "cluster", Value: "test"}}
	m := &Memberlist{
		metricLabels:    base,
		compressionAlgo: snappyCompressionType,
	}
	m.initCompressionMetricLabels()

	// Compress side.
	require.Equal(t, []metrics.Label{
		{Name: "cluster", Value: "test"},
		{Name: "algo", Value: "snappy"},
	}, m.compressMetricLabels)
	require.Equal(t, len(m.compressMetricLabels), cap(m.compressMetricLabels),
		"compressMetricLabels must cap-trim")
	require.Equal(t, []metrics.Label{
		{Name: "cluster", Value: "test"},
		{Name: "algo", Value: "snappy"},
		{Name: "reason", Value: "size_worse_than_original"},
	}, m.compressSkippedSizeWorseLabels)
	require.Equal(t, len(m.compressSkippedSizeWorseLabels), cap(m.compressSkippedSizeWorseLabels),
		"compressSkippedSizeWorseLabels must cap-trim")

	// Decompress side — exercise via the production decompressLabels()
	// dispatch so we cover both the field contents and the function.
	require.Equal(t, []metrics.Label{
		{Name: "cluster", Value: "test"},
		{Name: "algo", Value: "lzw"},
	}, m.decompressLabels(lzwCompressionType))
	require.Equal(t, len(m.decompressLabels(lzwCompressionType)), cap(m.decompressLabels(lzwCompressionType)),
		"decompressMetricLabels[lzwCompressionType] must cap-trim")

	require.Equal(t, []metrics.Label{
		{Name: "cluster", Value: "test"},
		{Name: "algo", Value: "snappy"},
	}, m.decompressLabels(snappyCompressionType))
	require.Equal(t, len(m.decompressLabels(snappyCompressionType)), cap(m.decompressLabels(snappyCompressionType)),
		"decompressMetricLabels[snappyCompressionType] must cap-trim")
	require.Equal(t, []metrics.Label{
		{Name: "cluster", Value: "test"},
		{Name: "algo", Value: "unknown"},
	}, m.decompressLabels(unknownCompressionType))

	// Mutation safety: poisoning base after init must not affect any
	// precomputed slice.
	base[0] = metrics.Label{Name: "cluster", Value: "other"}
	require.Equal(t, "test", m.compressMetricLabels[0].Value)
	require.Equal(t, "test", m.compressSkippedSizeWorseLabels[0].Value)
	require.Equal(t, "test", m.decompressLabels(lzwCompressionType)[0].Value)
	require.Equal(t, "test", m.decompressLabels(snappyCompressionType)[0].Value)
}

// TestDecompressErrors covers all decompress-side error paths: per-algorithm
// bomb-defense caps, wrapper-frame decode failure, and the unknown-algo
// dispatch in decompressBuffer.
func TestDecompressErrors(t *testing.T) {
	t.Run("ExceedsCap", func(t *testing.T) {
		// Compress a plaintext that decompresses just past the cap and
		// assert the decoder refuses to return more than the limit. LZW
		// on a stream of identical bytes achieves ~1000:1 compression,
		// so (cap+1) bytes of 'A' compresses down to a few KiB. The
		// test allocates ~21 MiB of plaintext; gated by testing.Short.
		t.Run("lzw", func(t *testing.T) {
			if testing.Short() {
				t.Skip("allocates ~21 MiB of plaintext; skipping under -short")
			}
			plain := bytes.Repeat([]byte{'A'}, maxDecompressBytes+1)
			buf, err := lzwCompress(plain)
			require.NoError(t, err)
			compressed := append([]byte(nil), buf.Bytes()...)
			releaseLZWBuffer(buf)

			_, err = lzwDecompress(compressed)
			require.EqualError(t, err, fmt.Sprintf("memberlist: LZW-decompressed payload exceeds %d bytes", maxDecompressBytes))
		})

		// Snappy carries a varint-encoded decoded length at the start
		// of the stream; snappy.Decode would happily allocate whatever
		// the peer claimed (up to 4 GiB on 64-bit). The cap check in
		// snappyDecompress must reject before allocation. We forge just
		// the varint header here — no actual snappy body is needed,
		// since the cap check fires on snappy.DecodedLen alone.
		t.Run("snappy", func(t *testing.T) {
			var hdr [binary.MaxVarintLen64]byte
			n := binary.PutUvarint(hdr[:], maxDecompressBytes+1)

			_, err := snappyDecompress(hdr[:n])
			require.EqualError(t, err, fmt.Sprintf("memberlist: snappy-decompressed payload would exceed %d bytes (claimed %d)", maxDecompressBytes, maxDecompressBytes+1))
		})
	})

	// Lock down the contract that makes the decompress error metric
	// labelable: when the outer compressedPayload itself can't be
	// msgpack-decoded, decompressPayload returns unknownCompressionType (255) so
	// the caller's compressionTypeLabel maps to "unknown" rather than the lzwCompressionType
	// zero value.
	t.Run("WrapperDecodeError", func(t *testing.T) {
		algo, _, err := decompressPayload([]byte{0xff, 0xff, 0xff, 0xff})
		require.ErrorContains(t, err, "msgpack decode error")
		require.Equal(t, unknownCompressionType, algo)
		require.Equal(t, "unknown", compressionTypeLabel(algo))
	})

	t.Run("UnknownAlgorithm", func(t *testing.T) {
		c := &compressedPayload{Algo: 99, Buf: nil}
		_, err := decompressBuffer(c)
		require.EqualError(t, err, "cannot decompress unknown algorithm 99")
	})
}

// TestCompressPayload_UnknownAlgo locks down the send-side dispatch's
// default arm: compressPayload returns a clear error rather than panicking
// or returning a partially-formed buffer when handed an unrecognized algo.
// Symmetric to TestDecompressErrors/UnknownAlgorithm on the receive side.
func TestCompressPayload_UnknownAlgo(t *testing.T) {
	buf, err := compressPayload(unknownCompressionType, []byte("data"), false)
	require.EqualError(t, err, "memberlist: cannot compress with unknown algorithm 255")
	require.Nil(t, buf)
}

// TestDecompressBuffer_MalformedBody covers garbage and truncated inputs
// for both algorithms. ExceedsCap covers the claimed-length bomb path;
// this covers the body-corruption path. We don't pin exact error messages
// (the compress/lzw and snappy libraries can change them) but we do pin
// the wrapper-origin substring so a regression that loses the wrap or
// the per-algorithm dispatch is caught.
func TestDecompressBuffer_MalformedBody(t *testing.T) {
	for _, tc := range []struct {
		algo        compressionType
		errContains string
	}{
		{lzwCompressionType, "lzwDecompress"},
		{snappyCompressionType, "snappy"},
	} {
		t.Run(compressionTypeLabel(tc.algo), func(t *testing.T) {
			t.Run("garbage", func(t *testing.T) {
				garbage := bytes.Repeat([]byte{0xff}, 32)
				_, err := decompressBuffer(&compressedPayload{Algo: tc.algo, Buf: garbage})
				require.ErrorContains(t, err, tc.errContains)
			})

			t.Run("truncated", func(t *testing.T) {
				input := bytes.Repeat([]byte("the quick brown fox "), 100)
				wrapped, err := compressPayload(tc.algo, input, false)
				require.NoError(t, err)
				var c compressedPayload
				require.NoError(t, decode(wrapped[1:], &c))

				_, err = decompressBuffer(&compressedPayload{Algo: tc.algo, Buf: c.Buf[:len(c.Buf)/2]})
				require.ErrorContains(t, err, tc.errContains)
			})
		})
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

// TestMemberlistCompression exercises the full Memberlist gossip path with
// compression enabled: both nodes snappy, and a mixed-rollout where one
// node emits snappy and the other LZW. The latter pins the in-rollout
// invariant that receivers MUST decode every algorithm regardless of
// their own emit setting.
func TestMemberlistCompression(t *testing.T) {
	t.Run("SnappyOnly", func(t *testing.T) {
		c1 := testConfig(t)
		c1.EnableCompression = true
		c1.CompressionAlgorithm = CompressionAlgorithmSnappy
		m1, err := Create(c1)
		require.NoError(t, err)
		t.Cleanup(func() { _ = m1.Shutdown() })

		c2 := testConfig(t)
		c2.BindPort = m1.config.BindPort
		c2.EnableCompression = true
		c2.CompressionAlgorithm = CompressionAlgorithmSnappy
		m2, err := Create(c2)
		require.NoError(t, err)
		t.Cleanup(func() { _ = m2.Shutdown() })

		num, err := m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
		require.NoError(t, err)
		require.Equal(t, 1, num)
		require.Equal(t, 2, len(m2.Members()))
	})

	t.Run("MixedRollout", func(t *testing.T) {
		c1 := testConfig(t)
		c1.EnableCompression = true
		c1.CompressionAlgorithm = CompressionAlgorithmSnappy
		m1, err := Create(c1)
		require.NoError(t, err)
		t.Cleanup(func() { _ = m1.Shutdown() })

		c2 := testConfig(t)
		c2.BindPort = m1.config.BindPort
		c2.EnableCompression = true
		c2.CompressionAlgorithm = CompressionAlgorithmLZW
		m2, err := Create(c2)
		require.NoError(t, err)
		t.Cleanup(func() { _ = m2.Shutdown() })

		num, err := m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
		require.NoError(t, err)
		require.Equal(t, 1, num)
		require.Equal(t, 2, len(m2.Members()))
		require.Equal(t, 2, len(m1.Members()))
	})
}

// randBytes returns a deterministic byte sequence of length n. The PRNG is
// seeded by n itself, so the same length always produces the same bytes.
// This makes benchmarks reproducible across runs and gives stable fuzz
// seeds; it is NOT a source of cryptographic randomness.
func randBytes(n int) []byte {
	return randBytesWith(rand.New(rand.NewSource(int64(n))), n)
}

func randBytesWith(rng *rand.Rand, n int) []byte {
	if n == 0 {
		return []byte{}
	}
	b := make([]byte, n)
	rng.Read(b)
	return b
}

// corpusSize is the length of every benchmark-corpus payload. All bench
// `sizes` must be <= corpusSize; benchCorpora's callers assert this.
const corpusSize = 16 * 1024

// corpusEntry pairs a corpus name with a payload of exactly corpusSize bytes.
type corpusEntry struct {
	name    string
	payload []byte
}

// benchCorpora returns two benchmark inputs of exactly corpusSize bytes:
//
//   - "compressible": a repeating ASCII string. LZW compresses this ~20:1.
//     Useful for measuring best-case compression speed.
//   - "random": deterministic high-entropy bytes (incompressible). Real
//     memberlist payloads are mostly already-snappy-compressed dskit KV
//     values whose entropy is near-uniform, so this corpus is closer to
//     production than "compressible". On this corpus the compress paths
//     also implicitly exercise the size-guard rejection branch in
//     rawSendMsgPacket (compressed >= original → fall back to plaintext).
func benchCorpora() []corpusEntry {
	compressible := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog"), 512)[:corpusSize]
	return []corpusEntry{
		{"compressible", compressible},
		{"random", randBytes(corpusSize)},
	}
}

// assertBenchSizes fails the bench loudly if any requested size exceeds
// corpusSize, rather than silently slicing out of bounds at the call site.
func assertBenchSizes(b *testing.B, sizes []int) {
	b.Helper()
	for _, s := range sizes {
		require.LessOrEqual(b, s, corpusSize,
			"bench size %d exceeds corpusSize %d; bump corpusSize and update benchCorpora", s, corpusSize)
	}
}

// BenchmarkCompressPayload measures the compress hot path for both
// algorithms across realistic payload sizes (small UDP, typical UDP, MTU,
// mid-sized push-pull) on two corpora (compressible vs incompressible).
// The returned encode buffer is released back to the pool every iteration
// so the bench reflects steady-state pool-warm cost (which is the
// production case), not first-call allocation cost. Use with -benchmem to
// surface per-call alloc count.
func BenchmarkCompressPayload(b *testing.B) {
	sizes := []int{64, 256, 1500, 16 * 1024}
	assertBenchSizes(b, sizes)
	for _, c := range benchCorpora() {
		for _, algo := range []compressionType{lzwCompressionType, snappyCompressionType} {
			for _, size := range sizes {
				b.Run(fmt.Sprintf("%s/%s/%d", c.name, compressionTypeLabel(algo), size), func(b *testing.B) {
					src := c.payload[:size]
					b.ResetTimer()
					b.ReportAllocs()
					for b.Loop() {
						_, err := compressPayload(algo, src, false)
						require.NoError(b, err)
					}
				})
			}
		}
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

// BenchmarkDecompressBuffer measures decompressBuffer in isolation: the
// outer compressMsg framing is decoded once before the timed loop so we
// don't double-count msgpack work that is the same on every call.
func BenchmarkDecompressBuffer(b *testing.B) {
	sizes := []int{64, 256, 1500, 16 * 1024}
	assertBenchSizes(b, sizes)
	for _, c := range benchCorpora() {
		for _, algo := range []compressionType{lzwCompressionType, snappyCompressionType} {
			for _, size := range sizes {
				b.Run(fmt.Sprintf("%s/%s/%d", c.name, compressionTypeLabel(algo), size), func(b *testing.B) {
					src := c.payload[:size]
					wrapped, err := compressPayload(algo, src, false)
					require.NoError(b, err)
					var compressed compressedPayload
					require.NoError(b, decode(wrapped[1:], &compressed))
					b.ResetTimer()
					b.ReportAllocs()
					for b.Loop() {
						out, err := decompressBuffer(&compressed)
						require.NoError(b, err)
						_ = out
					}
				})
			}
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

// BenchmarkEncryptLocalState measures the TCP push-pull encryption
// path. Sized to span the small (single-node gossip ack) through medium
// and large (push-pull-state) cases.
func BenchmarkEncryptLocalState(b *testing.B) {
	keyring, err := NewKeyring(nil, TestKeys[0])
	require.NoError(b, err)

	conf := DefaultLANConfig()
	conf.Keyring = keyring
	conf.GossipVerifyOutgoing = true

	// Build a minimal Memberlist with the bits encryptLocalState needs;
	// avoiding newMemberlist here keeps the bench setup independent of
	// network transport availability. initCompressionMetricLabels is
	// called so the bench remains valid if encryptLocalState ever gains
	// metric instrumentation that reads the precomputed label slices.
	m := &Memberlist{config: conf}
	m.initCompressionMetricLabels()

	sizes := []int{1024, 64 * 1024, 1 << 20} // 1 KiB, 64 KiB, 1 MiB
	for _, sz := range sizes {
		sendBuf := randBytes(sz)
		b.Run(fmt.Sprintf("%d", sz), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := m.encryptLocalState(sendBuf, "")
				require.NoError(b, err)
			}
		})
	}
}

// FuzzCompressDecompressRoundTrip exercises every supported algorithm with
// arbitrary inputs and asserts the decompressed payload byte-equals the input.
// Catches regressions in dispatch, pool reset hygiene, and snappy/LZW glue.
func FuzzCompressDecompressRoundTrip(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("testing"))
	f.Add(bytes.Repeat([]byte("ab"), 1024))
	f.Fuzz(func(t *testing.T, src []byte) {
		for _, algo := range []compressionType{lzwCompressionType, snappyCompressionType} {
			buf, err := compressPayload(algo, src, false)
			require.NoError(t, err, fmt.Sprintf("compress %s: %v", compressionTypeLabel(algo), err))
			gotAlgo, decoded, err := decompressPayload(buf[1:])
			require.NoError(t, err, fmt.Sprintf("decompress %s: %v", compressionTypeLabel(algo), err))
			require.Equal(t, algo, gotAlgo)
			require.True(t, bytes.Equal(decoded, src), fmt.Sprintf("payload mismatch (algo %s): got %q want %q",
				compressionTypeLabel(algo), decoded, src))
		}
	})
}

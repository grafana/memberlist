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

	metrics "github.com/hashicorp/go-metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompressDecompress(t *testing.T) {
	t.Run("roundtrip", func(t *testing.T) {
		types := []compressionType{lzwCompressionType, snappyCompressionType}
		sizes := []int{0, 1, 100, 100 * 1024, 1024 * 1024}
		for _, typ := range types {
			for _, size := range sizes {
				t.Run(fmt.Sprintf("%s/%d", compressionTypeLabel(typ), size), func(t *testing.T) {
					input := randBytes(size)
					buf, err := compressPayload(typ, input, false)
					require.NoError(t, err)

					gotType, decoded, err := decompressPayload(buf[1:])
					require.NoError(t, err)
					require.Equal(t, typ, gotType)
					require.Equal(t, input, decoded)
				})
			}
		}
	})

	// Lock down the rollout invariant: receivers MUST
	// decode every algorithm they understand, regardless of which algorithm
	// they would emit themselves.
	t.Run("mixed codec receiver", func(t *testing.T) {
		for _, senderType := range []compressionType{lzwCompressionType, snappyCompressionType} {
			t.Run(compressionTypeLabel(senderType), func(t *testing.T) {
				input := []byte("the quick brown fox jumps over the lazy dog")
				buf, err := compressPayload(senderType, input, false)
				require.NoError(t, err)

				// The receiver dispatches off the on-wire algo tag, never off
				// any local config — exercise both code paths via decompressPayload.
				gotType, decoded, err := decompressPayload(buf[1:])
				require.NoError(t, err)
				require.Equal(t, senderType, gotType)
				require.Equal(t, input, decoded)
			})
		}
	})

	t.Run("concurrent race", func(t *testing.T) {
		const iterations = 200
		types := []compressionType{lzwCompressionType, snappyCompressionType}

		var wg sync.WaitGroup
		for w := range 8 {
			wg.Add(1)
			go func(seed int64) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(seed))
				for range iterations {
					typ := types[rng.Intn(len(types))]
					input := randBytesWith(rng, rng.Intn(2048))
					buf, err := compressPayload(typ, input, false)
					if !assert.NoError(t, err) {
						return
					}
					gotType, decoded, err := decompressPayload(buf[1:])
					if !assert.NoError(t, err) {
						return
					}
					if !assert.Equal(t, typ, gotType) {
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
	t.Run("pool reset hygiene", func(t *testing.T) {
		for _, typ := range []compressionType{lzwCompressionType, snappyCompressionType} {
			t.Run(compressionTypeLabel(typ), func(t *testing.T) {
				long := bytes.Repeat([]byte("A"), 16*1024)
				short := []byte("BB")

				bufLong, err := compressPayload(typ, long, false)
				require.NoError(t, err)
				_, decLong, err := decompressPayload(bufLong[1:])
				require.NoError(t, err)
				require.Equal(t, long, decLong)

				bufShort, err := compressPayload(typ, short, false)
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
	t.Run("does not retain scratch", func(t *testing.T) {
		for _, typ := range []compressionType{lzwCompressionType, snappyCompressionType} {
			t.Run(compressionTypeLabel(typ), func(t *testing.T) {
				input := bytes.Repeat([]byte("xy"), 512)
				buf, err := compressPayload(typ, input, false)
				require.NoError(t, err)
				snapshot := bytes.Clone(buf)

				// Force pool churn: do several more encodes of different bytes.
				// If compressPayload retained any reference into pooled scratch,
				// the snapshot would diverge from buf.
				for range 5 {
					_, err := compressPayload(typ, []byte(strings.Repeat("z", 1024)), false)
					require.NoError(t, err)
				}

				require.Equal(t, snapshot, buf)
			})
		}
	})
}

func TestMemberlist_initCompressionMetricLabels(t *testing.T) {
	base := []metrics.Label{{Name: "cluster", Value: "test"}}
	m := &Memberlist{
		metricLabels:    base,
		compressionType: snappyCompressionType,
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
	t.Run("exceeds cap", func(t *testing.T) {
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
	// msgpack-decoded, decompressPayload returns unknownCompressionType so
	// the caller's compressionTypeLabel maps to "unknown" rather than the lzwCompressionType
	// zero value.
	t.Run("wrapper decode error", func(t *testing.T) {
		gotType, _, err := decompressPayload([]byte{0xff, 0xff, 0xff, 0xff})
		require.ErrorContains(t, err, "msgpack decode error")
		require.Equal(t, unknownCompressionType, gotType)
		require.Equal(t, "unknown", compressionTypeLabel(gotType))
	})

	t.Run("unknown algorithm", func(t *testing.T) {
		c := compressedPayload{Algo: 99, Buf: nil}
		_, err := decompressBuffer(&c)
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
		typ         compressionType
		errContains string
	}{
		{lzwCompressionType, "lzw"},
		{snappyCompressionType, "snappy"},
	} {
		t.Run(compressionTypeLabel(tc.typ), func(t *testing.T) {
			t.Run("garbage", func(t *testing.T) {
				garbage := bytes.Repeat([]byte{0xff}, 32)
				_, err := decompressBuffer(&compressedPayload{Algo: tc.typ, Buf: garbage})
				require.ErrorContains(t, err, tc.errContains)
			})

			t.Run("truncated", func(t *testing.T) {
				input := bytes.Repeat([]byte("the quick brown fox "), 100)
				wrapped, err := compressPayload(tc.typ, input, false)
				require.NoError(t, err)
				var c compressedPayload
				require.NoError(t, decode(wrapped[1:], &c))

				_, err = decompressBuffer(&compressedPayload{Algo: tc.typ, Buf: c.Buf[:len(c.Buf)/2]})
				require.ErrorContains(t, err, tc.errContains)
			})
		})
	}
}

// TestMemberlistCompression exercises the full Memberlist gossip path with
// compression enabled: both nodes snappy, and a mixed-rollout where one
// node emits snappy and the other LZW. The latter pins the in-rollout
// invariant that receivers MUST decode every algorithm regardless of
// their own emit setting.
func TestMemberlistCompression(t *testing.T) {
	t.Run("snappy only", func(t *testing.T) {
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

	t.Run("mixed rollout", func(t *testing.T) {
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
		for _, typ := range []compressionType{lzwCompressionType, snappyCompressionType} {
			for _, size := range sizes {
				b.Run(fmt.Sprintf("%s/%s/%d", c.name, compressionTypeLabel(typ), size), func(b *testing.B) {
					src := c.payload[:size]
					b.ResetTimer()
					b.ReportAllocs()
					for b.Loop() {
						_, err := compressPayload(typ, src, false)
						require.NoError(b, err)
					}
				})
			}
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
		for _, typ := range []compressionType{lzwCompressionType, snappyCompressionType} {
			for _, size := range sizes {
				b.Run(fmt.Sprintf("%s/%s/%d", c.name, compressionTypeLabel(typ), size), func(b *testing.B) {
					src := c.payload[:size]
					wrapped, err := compressPayload(typ, src, false)
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

// FuzzCompressDecompressRoundTrip exercises every supported algorithm with
// arbitrary inputs and asserts the decompressed payload byte-equals the input.
// Catches regressions in dispatch, pool reset hygiene, and snappy/LZW glue.
func FuzzCompressDecompressRoundTrip(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("testing"))
	f.Add(bytes.Repeat([]byte("ab"), 1024))
	f.Fuzz(func(t *testing.T, src []byte) {
		for _, typ := range []compressionType{lzwCompressionType, snappyCompressionType} {
			buf, err := compressPayload(typ, src, false)
			require.NoError(t, err, fmt.Sprintf("compress %s: %v", compressionTypeLabel(typ), err))
			gotType, decoded, err := decompressPayload(buf[1:])
			require.NoError(t, err, fmt.Sprintf("decompress %s: %v", compressionTypeLabel(typ), err))
			require.Equal(t, typ, gotType)
			require.True(t, bytes.Equal(decoded, src), fmt.Sprintf("payload mismatch (type %s): got %q want %q",
				compressionTypeLabel(typ), decoded, src))
		}
	})
}

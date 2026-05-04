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

	"github.com/stretchr/testify/require"
)

func TestResolveCompressionAlgorithm(t *testing.T) {
	tests := []struct {
		in   CompressionAlgorithm
		want compressionType
		err  bool
	}{
		{"", lzwAlgo, false},
		{CompressionAlgorithmLZW, lzwAlgo, false},
		{CompressionAlgorithmSnappy, snappyAlgo, false},
		{"zstd", 0, true},
		{"LZW", 0, true},
	}
	for _, tc := range tests {
		t.Run(string(tc.in), func(t *testing.T) {
			got, err := resolveCompressionAlgorithm(tc.in)
			if tc.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestCompressDecompress_RoundTrip(t *testing.T) {
	algos := []compressionType{lzwAlgo, snappyAlgo}
	sizes := []int{0, 1, 100, 100 * 1024, 1024 * 1024}

	for _, algo := range algos {
		for _, size := range sizes {
			t.Run(fmt.Sprintf("%s/%d", algoLabel(algo), size), func(t *testing.T) {
				input := randBytes(size)
				buf, err := compressPayload(algo, input, false)
				require.NoError(t, err)

				gotAlgo, decoded, err := decompressPayload(buf.Bytes()[1:])
				require.NoError(t, err)
				require.Equal(t, algo, gotAlgo)
				require.Equal(t, input, decoded)
			})
		}
	}
}

// TestCompressDecompress_MixedCodecReceiver locks down the rollout invariant:
// receivers MUST decode every algorithm they understand, regardless of which
// algorithm they would emit themselves.
func TestCompressDecompress_MixedCodecReceiver(t *testing.T) {
	for _, senderAlgo := range []compressionType{lzwAlgo, snappyAlgo} {
		t.Run(algoLabel(senderAlgo), func(t *testing.T) {
			input := []byte("the quick brown fox jumps over the lazy dog")
			buf, err := compressPayload(senderAlgo, input, false)
			require.NoError(t, err)

			// The receiver dispatches off the on-wire algo tag, never off
			// any local config — exercise both code paths via decompressPayload.
			gotAlgo, decoded, err := decompressPayload(buf.Bytes()[1:])
			require.NoError(t, err)
			require.Equal(t, senderAlgo, gotAlgo)
			require.Equal(t, input, decoded)
		})
	}
}

func TestCompressDecompress_ConcurrentRace(t *testing.T) {
	const iterations = 200
	algos := []compressionType{lzwAlgo, snappyAlgo}

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < iterations; i++ {
				algo := algos[rng.Intn(len(algos))]
				input := randBytesWith(rng, rng.Intn(2048))
				buf, err := compressPayload(algo, input, false)
				if err != nil {
					t.Errorf("compress: %v", err)
					return
				}
				gotAlgo, decoded, err := decompressPayload(buf.Bytes()[1:])
				if err != nil {
					t.Errorf("decompress: %v", err)
					return
				}
				if gotAlgo != algo {
					t.Errorf("algo mismatch: got %d want %d", gotAlgo, algo)
					return
				}
				if !bytes.Equal(decoded, input) {
					t.Errorf("payload mismatch")
					return
				}
			}
		}(int64(w))
	}
	wg.Wait()
}

// TestCompressDecompress_PoolResetHygiene encodes a long input, then a short
// input. The short decompressed result must equal the short input exactly,
// with no leftover bytes from the long one. Catches Reset/Put-side hygiene
// bugs in the LZW writer/reader pools and the snappy buffer pools.
func TestCompressDecompress_PoolResetHygiene(t *testing.T) {
	for _, algo := range []compressionType{lzwAlgo, snappyAlgo} {
		t.Run(algoLabel(algo), func(t *testing.T) {
			long := bytes.Repeat([]byte("A"), 16*1024)
			short := []byte("BB")

			bufLong, err := compressPayload(algo, long, false)
			require.NoError(t, err)
			_, decLong, err := decompressPayload(bufLong.Bytes()[1:])
			require.NoError(t, err)
			require.Equal(t, long, decLong)

			bufShort, err := compressPayload(algo, short, false)
			require.NoError(t, err)
			_, decShort, err := decompressPayload(bufShort.Bytes()[1:])
			require.NoError(t, err)
			require.Equal(t, short, decShort)
		})
	}
}

// TestReleaseBuffer_BoundedCap verifies the pool drops oversized buffers
// rather than retaining them forever.
func TestReleaseBuffer_BoundedCap(t *testing.T) {
	big := getBuffer()
	big.Write(make([]byte, maxPooledCompressBufCap+1))
	require.Greater(t, big.Cap(), maxPooledCompressBufCap)

	releaseBuffer(big)
	// We can't directly assert the pool's contents (sync.Pool's interface
	// permits the runtime to drop entries on its own), but we can assert
	// that getBuffer() never returns a buffer with cap > limit unless one
	// was explicitly retained — release of an oversized buffer must not
	// re-surface here.
	for i := 0; i < 10; i++ {
		b := getBuffer()
		require.LessOrEqual(t, b.Cap(), maxPooledCompressBufCap,
			"oversized buffer leaked through pool on iteration %d", i)
		releaseBuffer(b)
	}
}

// TestLZWDecompress_ExceedsCap is a regression test for the
// maxLZWDecompressedBytes bomb-defense. It compresses a plaintext that
// decompresses just past the cap and asserts lzwDecompress refuses to
// return more than the limit.
//
// LZW on a stream of identical bytes achieves ~1000:1 compression, so
// (cap+1) bytes of 'A' compresses down to a few KiB — quick to build and
// quick to feed back through the decoder.
//
// The test allocates ~64 MiB of plaintext; gated by testing.Short to keep
// the fast-test loop snappy.
func TestLZWDecompress_ExceedsCap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates ~64 MiB of plaintext; skipping under -short")
	}
	plain := bytes.Repeat([]byte{'A'}, maxLZWDecompressedBytes+1)
	buf, err := lzwCompress(plain)
	require.NoError(t, err)
	compressed := append([]byte(nil), buf.Bytes()...)
	releaseBuffer(buf)

	_, err = lzwDecompress(compressed)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds")
}

// TestSnappyDecompress_ExceedsCap is the snappy counterpart of
// TestLZWDecompress_ExceedsCap. snappy carries a varint-encoded decoded
// length at the start of the stream; snappy.Decode would happily allocate
// whatever the peer claimed (up to 4 GiB on 64-bit). The cap check in
// snappyDecompress must reject before allocation.
//
// We forge just the varint header here — no actual snappy body is needed,
// since the cap check fires on snappy.DecodedLen alone. That keeps the test
// fast and isolates the defense from snappy's own decode behavior.
func TestSnappyDecompress_ExceedsCap(t *testing.T) {
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], maxSnappyDecompressedBytes+1)

	_, err := snappyDecompress(hdr[:n])
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceed")
}

// TestDecompressPayload_WrapperDecodeErrorAlgo locks down the contract that
// makes the compress.error metric labelable: when the outer compress{}
// wrapper itself can't be msgpack-decoded, decompressPayload returns
// unknownAlgo (255) so the caller's algoLabel maps to "unknown" rather
// than the lzwAlgo zero value.
func TestDecompressPayload_WrapperDecodeErrorAlgo(t *testing.T) {
	algo, _, err := decompressPayload([]byte{0xff, 0xff, 0xff, 0xff})
	require.Error(t, err)
	require.Equal(t, unknownAlgo, algo)
	require.Equal(t, "unknown", algoLabel(algo))
}

func TestDecompressBuffer_UnknownAlgorithm(t *testing.T) {
	c := &compress{Algo: 99, Buf: nil}
	_, err := decompressBuffer(c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot decompress unknown algorithm 99")
}

// TestCompressPayload_DoesNotRetainScratch verifies the contract that
// makes pooling correct: msgpack synchronously copies the inner Buf into
// the outer encoded buffer, so once compressPayload returns the inner
// pooled scratch can be reused without affecting the returned bytes.
func TestCompressPayload_DoesNotRetainScratch(t *testing.T) {
	for _, algo := range []compressionType{lzwAlgo, snappyAlgo} {
		t.Run(algoLabel(algo), func(t *testing.T) {
			input := bytes.Repeat([]byte("xy"), 512)
			buf, err := compressPayload(algo, input, false)
			require.NoError(t, err)
			snapshot := append([]byte(nil), buf.Bytes()...)

			// Force pool churn: do another encode of different bytes,
			// release, and re-encode several times. If the first call
			// retained any reference into pooled scratch, the snapshot
			// would diverge from the live buffer.
			for i := 0; i < 5; i++ {
				_, err := compressPayload(algo, []byte(strings.Repeat("z", 1024)), false)
				require.NoError(t, err)
			}

			require.Equal(t, snapshot, buf.Bytes())
		})
	}
}

// TestEncodeRoundTrip verifies repeated encode/decode calls each produce
// independently correct bytes. encode() doesn't pool its output buffer
// (see comment in util.go), so this guards basic correctness across calls.
func TestEncodeRoundTrip(t *testing.T) {
	const inputs = 32
	for i := 0; i < inputs; i++ {
		buf, err := encode(pingMsg, &ping{SeqNo: uint32(i), Node: "n"}, false)
		require.NoError(t, err)
		require.Greater(t, buf.Len(), 0)

		var p ping
		require.NoError(t, decode(buf.Bytes()[1:], &p))
		require.Equal(t, uint32(i), p.SeqNo)
	}
}

// TestMemberlist_SnappyCompression exercises the full Memberlist gossip path
// with both nodes configured for snappy.
func TestMemberlist_SnappyCompression(t *testing.T) {
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
}

// TestMemberlist_MixedCompressionRollout asserts a snappy sender and an
// LZW sender can join and exchange state — the in-rollout invariant.
func TestMemberlist_MixedCompressionRollout(t *testing.T) {
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
		if s > corpusSize {
			b.Fatalf("bench size %d exceeds corpusSize %d; bump corpusSize and update benchCorpora", s, corpusSize)
		}
	}
}

// BenchmarkCompressPayload measures the compress hot path for both
// algorithms across realistic payload sizes (small UDP, typical UDP, MTU,
// mid-sized push-pull) on two corpora (compressible vs incompressible).
// Use with -benchmem to surface per-call alloc count.
func BenchmarkCompressPayload(b *testing.B) {
	sizes := []int{64, 256, 1500, 16 * 1024}
	assertBenchSizes(b, sizes)
	for _, c := range benchCorpora() {
		for _, algo := range []compressionType{lzwAlgo, snappyAlgo} {
			for _, size := range sizes {
				b.Run(fmt.Sprintf("%s/%s/%d", c.name, algoLabel(algo), size), func(b *testing.B) {
					src := c.payload[:size]
					b.ResetTimer()
					b.ReportAllocs()
					for b.Loop() {
						buf, err := compressPayload(algo, src, false)
						if err != nil {
							b.Fatal(err)
						}
						_ = buf
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
		for _, algo := range []compressionType{lzwAlgo, snappyAlgo} {
			for _, size := range sizes {
				b.Run(fmt.Sprintf("%s/%s/%d", c.name, algoLabel(algo), size), func(b *testing.B) {
					src := c.payload[:size]
					wrapped, err := compressPayload(algo, src, false)
					if err != nil {
						b.Fatal(err)
					}
					var compressed compress
					if err := decode(wrapped.Bytes()[1:], &compressed); err != nil {
						b.Fatal(err)
					}
					b.ResetTimer()
					b.ReportAllocs()
					for b.Loop() {
						out, err := decompressBuffer(&compressed)
						if err != nil {
							b.Fatal(err)
						}
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
		for _, algo := range []compressionType{lzwAlgo, snappyAlgo} {
			buf, err := compressPayload(algo, src, false)
			if err != nil {
				t.Fatalf("compress %s: %v", algoLabel(algo), err)
			}
			gotAlgo, decoded, err := decompressPayload(buf.Bytes()[1:])
			if err != nil {
				t.Fatalf("decompress %s: %v", algoLabel(algo), err)
			}
			if gotAlgo != algo {
				t.Fatalf("algo: got %d want %d", gotAlgo, algo)
			}
			if !bytes.Equal(decoded, src) {
				t.Fatalf("payload mismatch (algo %s): got %q want %q",
					algoLabel(algo), decoded, src)
			}
		}
	})
}

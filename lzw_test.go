// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"bytes"
	"compress/lzw"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

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

// BenchmarkLZWBufferPool isolates lzwBufferPool's contribution while
// keeping lzwWriterPool active. NoPool swaps the destination buffer
// for a fresh bytes.NewBuffer(nil) per call. A regression that turns
// the pool into a no-op should show up as the Pool variant's B/op
// and allocs/op matching NoPool's.
func BenchmarkLZWBufferPool(b *testing.B) {
	for _, sz := range []int{256, 1500, 16 * 1024} {
		src := randBytes(sz)
		b.Run(fmt.Sprintf("%d/pool", sz), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				buf, err := lzwCompress(src)
				require.NoError(b, err)
				releaseLZWBuffer(buf)
			}
		})
		b.Run(fmt.Sprintf("%d/no pool", sz), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				buf := bytes.NewBuffer(nil)
				w := lzwWriterPool.Get().(*lzw.Writer)
				w.Reset(buf, lzw.LSB, lzwLitWidth)
				_, err := w.Write(src)
				require.NoError(b, err)
				require.NoError(b, w.Close())
				w.Reset(io.Discard, lzw.LSB, lzwLitWidth)
				lzwWriterPool.Put(w)
				_ = buf
			}
		})
	}
}

// BenchmarkLZWWriterPool isolates lzwWriterPool's contribution while
// keeping lzwBufferPool active. NoPool swaps the *lzw.Writer for a
// fresh lzw.NewWriter per call. lzw.NewWriter allocates a ~70 KiB
// internal dictionary, so the pool's marginal contribution is large.
func BenchmarkLZWWriterPool(b *testing.B) {
	for _, sz := range []int{256, 1500, 16 * 1024} {
		src := randBytes(sz)
		b.Run(fmt.Sprintf("%d/pool", sz), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				buf, err := lzwCompress(src)
				require.NoError(b, err)
				releaseLZWBuffer(buf)
			}
		})
		b.Run(fmt.Sprintf("%d/no pool", sz), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				buf := getLZWBuffer()
				w := lzw.NewWriter(buf, lzw.LSB, lzwLitWidth).(*lzw.Writer)
				_, err := w.Write(src)
				require.NoError(b, err)
				require.NoError(b, w.Close())
				releaseLZWBuffer(buf)
			}
		})
	}
}

// BenchmarkLZWReaderPool isolates lzwReaderPool's contribution.
// NoPool swaps the *lzw.Reader for a fresh lzw.NewReader per call;
// the compress step happens outside the timed loop so the only delta
// is reader-pool reuse vs. fresh decoder state.
func BenchmarkLZWReaderPool(b *testing.B) {
	for _, sz := range []int{256, 1500, 16 * 1024} {
		src := randBytes(sz)
		compBuf, err := lzwCompress(src)
		require.NoError(b, err)
		compressed := bytes.Clone(compBuf.Bytes())
		releaseLZWBuffer(compBuf)

		b.Run(fmt.Sprintf("%d/pool", sz), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := lzwDecompress(compressed)
				require.NoError(b, err)
			}
		})
		b.Run(fmt.Sprintf("%d/no pool", sz), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				r := lzw.NewReader(bytes.NewReader(compressed), lzw.LSB, lzwLitWidth).(*lzw.Reader)
				lr := io.LimitedReader{R: r, N: maxDecompressBytes + 1}
				var buf bytes.Buffer
				_, err := io.Copy(&buf, &lr)
				require.NoError(b, err)
				_ = r.Close()
				_ = bytes.Clone(buf.Bytes())
			}
		})
	}
}

// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"bytes"
	"compress/lzw"
	"fmt"
	"io"
	"sync"

	"github.com/golang/snappy"
)

// CompressionAlgorithm selects the algorithm used to compress outgoing
// memberlist messages when Config.EnableCompression is true. Receivers
// always decode every algorithm they understand, independent of this
// setting; the field only controls what a sender emits.
type CompressionAlgorithm string

const (
	// CompressionAlgorithmLZW selects lzw compression. This is the historical
	// default and the only algorithm understood by older builds.
	CompressionAlgorithmLZW CompressionAlgorithm = "lzw"

	// CompressionAlgorithmSnappy selects snappy compression. This uses
	// substantially less CPU and allocates less than LZW for similar
	// bandwidth.
	CompressionAlgorithmSnappy CompressionAlgorithm = "snappy"
)

// resolveCompressionAlgorithm maps algo to the wire-level compressionType byte. Empty string is treated
// as LZW for backward compatibility with bare Config{} construction.
func resolveCompressionAlgorithm(algo CompressionAlgorithm) (compressionType, error) {
	switch algo {
	case "", CompressionAlgorithmLZW:
		return lzwAlgo, nil
	case CompressionAlgorithmSnappy:
		return snappyAlgo, nil
	default:
		return 0, fmt.Errorf("memberlist: unknown CompressionAlgorithm %q", algo)
	}
}

// algoLabel converts algo to a stable string for use as a metric label.
func algoLabel(algo compressionType) string {
	switch algo {
	case lzwAlgo:
		return "lzw"
	case snappyAlgo:
		return "snappy"
	default:
		return "unknown"
	}
}

// compressionType is used to specify the compression algorithm on the wire.
// Values are part of the protocol and must not be reordered or removed.
type compressionType uint8

const (
	lzwAlgo compressionType = iota
	snappyAlgo
)

// compress is used to wrap an underlying payload using a specified
// compression algorithm. It is the on-wire structure carried inside a
// compressMsg frame.
type compress struct {
	Algo compressionType
	Buf  []byte
}

const (
	// lzwLitWidth is the literal width passed to compress/lzw. The value
	// is part of the wire format and must not be changed.
	lzwLitWidth = 8

	// maxPooledCompressBufCap bounds the capacity of buffers retained by
	// the compression-internal pools (LZW scratch *bytes.Buffer and snappy
	// destination []byte). UDP packets are bounded by UDPBufferSize
	// (default 1400 bytes); TCP push-pull state is larger but rare.
	// 256 KiB pools typical cases without retaining outsized payloads.
	maxPooledCompressBufCap = 256 * 1024
)

// bytesBufferPool recycles *bytes.Buffer values used as LZW-scratch space
// inside lzwCompress. The buffer is acquired and released within a single
// compressPayload call; it never escapes to the network or is held across
// goroutines, so it is safe to pool here even though the encode() output
// buffer is not (see encode function).
//
// Tuning is for LZW scratch sizes only — do NOT reuse this pool for other
// callers without revisiting maxPooledCompressBufCap.
var bytesBufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

func getBuffer() *bytes.Buffer {
	buf := bytesBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func releaseBuffer(b *bytes.Buffer) {
	if b.Cap() > maxPooledCompressBufCap {
		return
	}
	b.Reset()
	bytesBufferPool.Put(b)
}

// bytesReaderPool recycles *bytes.Reader values used as the source reader
// for the LZW decoder. Callers MUST Reset(nil) before
// Put to avoid pinning the previous src slice in the pool.
var bytesReaderPool = sync.Pool{
	New: func() any {
		return new(bytes.Reader)
	},
}

// emptyBytesReader is a shared, never-mutated *bytes.Reader handed to
// lzw.NewReader inside the lzwReaderPool New function as a placeholder.
// Callers MUST Reset the returned lzw.Reader before any Read; this
// invariant is what makes the shared placeholder safe across goroutines.
var emptyBytesReader = bytes.NewReader(nil)

// lzwWriterPool recycles compress/lzw encoder state machines.
// (*lzw.Writer).Reset zeros the entire internal struct (*w = Writer{}) and
// re-inits, so the errClosed state set by our post-use Close is cleared on
// the next Reset before any other call. We rely on this Reset-zeros-all-state
// behavior; it has been stable since Go 1.5 but is not formally documented.
var lzwWriterPool = sync.Pool{
	New: func() any {
		return lzw.NewWriter(io.Discard, lzw.LSB, lzwLitWidth).(*lzw.Writer)
	},
}

// lzwReaderPool recycles compress/lzw decoder state machines.
// Same Reset-zeros-all-state assumption as lzwWriterPool.
var lzwReaderPool = sync.Pool{
	New: func() any {
		// Type-assert *lzw.Reader so we can call Reset on subsequent Gets.
		return lzw.NewReader(emptyBytesReader, lzw.LSB, lzwLitWidth).(*lzw.Reader)
	},
}

// snappyEncodeBufPool recycles destination slices for snappy.Encode.
// Initial capacity is the default UDPBufferSize plus a small headroom for
// the snappy frame's varint length prefix and minor expansion of
// incompressible payloads. The total (~1500 bytes) matches the standard
// Ethernet MTU, so a typical gossip-sized encode will not grow the
// underlying array on first use.
var snappyEncodeBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, defaultUDPBufferSize+100)
		return &b
	},
}

func putSnappyEncodeBuf(p *[]byte) {
	if cap(*p) > maxPooledCompressBufCap {
		return
	}
	*p = (*p)[:0]
	snappyEncodeBufPool.Put(p)
}

// compressPayload takes an opaque input buffer, compresses it using the
// requested algo, and wraps the result in a compress{} message that
// is encoded as a compressMsg frame.
//
// The returned *bytes.Buffer is owned by the caller. It comes from encode()
// (which doesn't pool) and is released by the GC.
func compressPayload(algo compressionType, inp []byte, msgpackUseNewTimeFormat bool) (*bytes.Buffer, error) {
	var encoded []byte
	switch algo {
	case lzwAlgo:
		buf, err := lzwCompress(inp)
		if err != nil {
			return nil, err
		}
		defer releaseBuffer(buf)
		encoded = buf.Bytes()
	case snappyAlgo:
		bufPtr, err := snappyCompress(inp)
		if err != nil {
			return nil, err
		}
		defer putSnappyEncodeBuf(bufPtr)
		encoded = *bufPtr
	default:
		return nil, fmt.Errorf("memberlist: cannot compress with unknown algorithm %d", algo)
	}

	return encode(compressMsg, &compress{Algo: algo, Buf: encoded}, msgpackUseNewTimeFormat)
}

// unknownAlgo is the sentinel returned by decompressPayload when the outer
// compress{} wrapper itself fails to decode — i.e., the failure happened
// before any algorithm tag was read from the wire. Callers emit it via
// algoLabel so the resulting error metric carries algo="unknown" rather
// than misattributing a frame-level failure to LZW (the zero value).
//
// A uint8 max value (255) avoids any future collision with newly-assigned
// real algorithm IDs that grow upward from snappyAlgo=1.
const unknownAlgo compressionType = 255

// decompressPayload unpacks an encoded compress{} message and returns the
// algorithm used along with its uncompressed payload. The returned slice is
// freshly allocated and may be retained by the caller.
//
// On wrapper-decode failure (the compress{} frame itself is malformed) the
// returned algo is unknownAlgo so the caller's error metric is not falsely
// labeled as the lzwAlgo zero value.
func decompressPayload(msg []byte) (compressionType, []byte, error) {
	var c compress
	if err := decode(msg, &c); err != nil {
		return unknownAlgo, nil, err
	}
	payload, err := decompressBuffer(&c)
	return c.Algo, payload, err
}

// decompressBuffer decompresses the buffer of a single compress message,
// dispatching on the algorithm tag. The returned slice is freshly allocated
// and may be retained by the caller.
func decompressBuffer(c *compress) ([]byte, error) {
	switch c.Algo {
	case lzwAlgo:
		return lzwDecompress(c.Buf)
	case snappyAlgo:
		return snappyDecompress(c.Buf)
	default:
		return nil, fmt.Errorf("cannot decompress unknown algorithm %d", c.Algo)
	}
}

// lzwCompress compresses src using lzw and returns the pooled
// scratch buffer holding the encoded bytes. The caller MUST releaseBuffer
// the returned buffer once the bytes are no longer needed.
func lzwCompress(src []byte) (*bytes.Buffer, error) {
	buf := getBuffer()
	w := lzwWriterPool.Get().(*lzw.Writer)
	// The writer is reusable after the next Reset, so we return it to the
	// pool unconditionally.
	defer lzwWriterPool.Put(w)
	w.Reset(buf, lzw.LSB, lzwLitWidth)

	if _, err := w.Write(src); err != nil {
		_ = w.Close()
		releaseBuffer(buf)
		return nil, err
	}
	if err := w.Close(); err != nil {
		releaseBuffer(buf)
		return nil, err
	}

	return buf, nil
}

// maxLZWDecompressedBytes bounds the decompressed size of a single LZW
// payload. memberlist's TCP push-pull is capped at maxPushStateBytes
// (20 MiB compressed) and UDP packets at UDPBufferSize (1400 bytes).
// Anything larger indicates a malformed peer or a decompression
// bomb (LZW can expand small inputs by orders of magnitude on highly
// redundant data — e.g., 20 MiB compressed → 2 GiB+ decompressed in the
// worst case for a crafted payload).
const maxLZWDecompressedBytes = 64 * 1024 * 1024

// lzwDecompress returns a freshly allocated []byte holding the decompressed
// payload. The bytes.Buffer used to drain the LZW reader is per-call (its
// underlying array becomes the returned slice), so no copy is needed and
// the caller may retain the result indefinitely. The lzw.Reader and the
// bytes.Reader wrapping src are pooled.
func lzwDecompress(src []byte) ([]byte, error) {
	r := lzwReaderPool.Get().(*lzw.Reader)
	defer lzwReaderPool.Put(r)
	br := bytesReaderPool.Get().(*bytes.Reader)
	// Reset(nil) runs before Put, so the pooled reader doesn't pin src across
	// the next Get. Two top-level defers stay open-coded;
	// a defer of an anonymous closure would heap-allocate the closure literal.
	defer bytesReaderPool.Put(br)
	defer br.Reset(nil)
	br.Reset(src)
	r.Reset(br, lzw.LSB, lzwLitWidth)

	// io.LimitedReader as a value, not via io.LimitReader. The wrapper
	// function returns &LimitedReader{...} unconditionally; using a value
	// type at least makes the intent explicit. NOTE: in practice escape
	// analysis still moves &lr to the heap because io.Copy takes an
	// io.Reader interface and the compiler can't prove the interface
	// doesn't escape across that boundary. Net cost: +1 alloc/op (24 B)
	// per LZW decompress vs. an unbounded io.Copy — the price of bomb
	// defense. See compress_test.go BenchmarkDecompressBuffer for numbers.
	lr := io.LimitedReader{R: r, N: maxLZWDecompressedBytes + 1}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, &lr); err != nil {
		_ = r.Close()
		return nil, fmt.Errorf("lzwDecompress: read from src: %w", err)
	}
	_ = r.Close()
	if buf.Len() > maxLZWDecompressedBytes {
		return nil, fmt.Errorf("memberlist: LZW-decompressed payload exceeds %d bytes", maxLZWDecompressedBytes)
	}
	return buf.Bytes(), nil
}

// snappyCompress compresses src using snappy and returns a
// pointer to the pooled destination slice. The caller MUST putSnappyEncodeBuf
// the returned pointer once the bytes are no longer needed.
func snappyCompress(src []byte) (*[]byte, error) {
	bufPtr := snappyEncodeBufPool.Get().(*[]byte)
	*bufPtr = snappy.Encode((*bufPtr)[:0], src)
	return bufPtr, nil
}

// maxSnappyDecompressedBytes bounds the decompressed size of a single snappy
// payload. snappy.DecodedLen returns the *claimed* decoded length from the
// varint header at the start of src — up to 2^32-1 on 64-bit systems — and
// snappy.Decode allocates that many bytes before any data is actually
// decoded. A malicious peer could send a tiny frame claiming a multi-GiB
// decoded length to trigger out-of-memory. Tracks the LZW cap so both
// decoders share the same defense-in-depth ceiling.
const maxSnappyDecompressedBytes = maxLZWDecompressedBytes

// snappyDecompress returns a freshly allocated []byte holding the
// decompressed payload. snappy.Decode writes into the right-sized dst we
// allocate here; pooling the dst would force a copy-out (the caller retains
// the slice indefinitely) and add net overhead, so we don't.
//
// The claimed decoded length is checked against maxSnappyDecompressedBytes
// before allocation so a malformed peer cannot trigger an oversized make().
func snappyDecompress(src []byte) ([]byte, error) {
	n, err := snappy.DecodedLen(src)
	if err != nil {
		return nil, fmt.Errorf("snappy.DecodedLen: %w", err)
	}
	if n > maxSnappyDecompressedBytes {
		return nil, fmt.Errorf("memberlist: snappy-decompressed payload would exceed %d bytes (claimed %d)", maxSnappyDecompressedBytes, n)
	}
	return snappy.Decode(make([]byte, n), src)
}

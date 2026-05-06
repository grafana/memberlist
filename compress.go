// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"bytes"
	"fmt"

	metrics "github.com/hashicorp/go-metrics/compat"
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

// compressionType is used to specify the compression algorithm on the wire.
// Values are part of the protocol and must not be reordered or removed.
type compressionType uint8

const (
	lzwAlgo    compressionType = iota // 0
	snappyAlgo                        // 1
	// unknownAlgo is the sentinel representing an unknown compression algorithm.
	//
	// A uint8 max value (255) avoids any future collision with newly-
	// assigned real algorithm IDs that grow upward from snappyAlgo=1.
	unknownAlgo compressionType = 255
)

// compressedPayload wraps an underlying payload along with the algorithm
// used to compress it. It is the on-wire struct carried inside a compressMsg
// frame.
type compressedPayload struct {
	Algo compressionType
	Buf  []byte
}

// compressPayload takes an opaque input buffer, compresses it using the
// requested algo, and wraps the result in a compressedPayload that is
// encoded as a compressMsg frame.
//
// On success the returned *bytes.Buffer is drawn from encodeBufPool; the
// caller MUST releaseEncodeBuffer it once the bytes have been consumed.
// On error a nil buffer is returned and the caller does not need to
// release.
func compressPayload(algo compressionType, inp []byte, msgpackUseNewTimeFormat bool) (*bytes.Buffer, error) {
	var encoded []byte
	switch algo {
	case lzwAlgo:
		buf, err := lzwCompress(inp)
		if err != nil {
			return nil, err
		}
		defer releaseLZWScratch(buf)
		encoded = buf.Bytes()
	case snappyAlgo:
		bufPtr := snappyCompress(inp)
		defer putSnappyEncodeBuf(bufPtr)
		encoded = *bufPtr
	default:
		return nil, fmt.Errorf("memberlist: cannot compress with unknown algorithm %d", algo)
	}

	return encode(compressMsg, &compressedPayload{Algo: algo, Buf: encoded}, msgpackUseNewTimeFormat)
}

// decompressPayload unpacks an encoded compressedPayload and returns the
// algorithm used along with its uncompressed payload. The returned slice is
// freshly allocated and may be retained by the caller.
//
// On wrapper-decode failure (the compressedPayload itself is malformed) the
// returned algo is unknownAlgo.
func decompressPayload(msg []byte) (compressionType, []byte, error) {
	var c compressedPayload
	if err := decode(msg, &c); err != nil {
		return unknownAlgo, nil, err
	}
	payload, err := decompressBuffer(&c)
	return c.Algo, payload, err
}

// decompressBuffer decompresses the buffer of a single compressedPayload,
// dispatching on the algorithm tag. The returned slice is freshly allocated
// and may be retained by the caller.
func decompressBuffer(c *compressedPayload) ([]byte, error) {
	switch c.Algo {
	case lzwAlgo:
		return lzwDecompress(c.Buf)
	case snappyAlgo:
		return snappyDecompress(c.Buf)
	default:
		return nil, fmt.Errorf("cannot decompress unknown algorithm %d", c.Algo)
	}
}

// maxDecompressBytes bounds the decompressed size of any compressed
// memberlist payload, irrespective of algorithm. Memberlist's TCP push-pull
// is capped at maxPushStateBytes (20 MiB compressed input) and UDP packets
// at UDPBufferSize (default 1400 B), so legitimate decoded output sits well
// below this. The 1 MiB headroom absorbs any compressed-frame metadata
// expansion. Anything larger indicates a malformed peer or a decompression
// bomb — LZW can expand small inputs by orders of magnitude on highly
// redundant data, and snappy.Decode allocates the *claimed* decoded length
// before reading data, so a tiny frame claiming a multi-GiB body could
// trigger out-of-memory without this cap.
const maxDecompressBytes = maxPushStateBytes + 1<<20

// Hoisted metric-name slices, to avoid heap allocation on hot path.
var (
	metricCompressAttempts   = []string{"memberlist", "compress", "attempts_total"}
	metricCompressSkipped    = []string{"memberlist", "compress", "skipped_total"}
	metricCompressErrors     = []string{"memberlist", "compress", "errors_total"}
	metricDecompressAttempts = []string{"memberlist", "decompress", "attempts_total"}
	metricDecompressErrors   = []string{"memberlist", "decompress", "errors_total"}
)

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

// withLabel returns base + a metrics label `{<name>: <value>}`. The
// returned slice has its capacity set to its length so subsequent appends
// allocate a new array rather than mutating the precomputed slice. Used at
// Memberlist construction time to precompute hot-path label slices.
func withLabel(base []metrics.Label, name, value string) []metrics.Label {
	out := make([]metrics.Label, len(base), len(base)+1)
	copy(out, base)
	return append(out, metrics.Label{Name: name, Value: value})
}

// initMetricLabels populates the per-Memberlist precomputed metric label
// slices used on the compress/decompress hot paths. Called once at
// construction; the resulting slices are read concurrently from the send
// and receive paths and never mutated thereafter.
func (m *Memberlist) initMetricLabels() {
	m.compressMetricLabels = withLabel(m.metricLabels, "algo", algoLabel(m.compressionAlgo))
	m.compressSkippedSizeWorseLabels = withLabel(m.compressMetricLabels, "reason", "size_worse_than_original")
	m.decompressLZWLabels = withLabel(m.metricLabels, "algo", algoLabel(lzwAlgo))
	m.decompressSnappyLabels = withLabel(m.metricLabels, "algo", algoLabel(snappyAlgo))
}

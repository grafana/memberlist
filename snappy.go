// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"fmt"
	"sync"

	"github.com/golang/snappy"
)

// maxPooledSnappyEncodeCap bounds the capacity of []byte values
// retained by snappyEncodeBufPool (snappy destination). Unlike LZW
// scratch, snappy.Encode's output scales with input. The cap targets
// the high-rate UDP/gossip path and small-to-mid TCP push-pull
// bodies; larger push-pull payloads (closer to maxPushStateBytes =
// 20 MiB) exceed 4 MiB and are intentionally not pooled, so the
// idle-pool footprint stays bounded.
const maxPooledSnappyEncodeCap = 4 * 1024 * 1024

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
	if cap(*p) > maxPooledSnappyEncodeCap {
		return
	}
	*p = (*p)[:0]
	snappyEncodeBufPool.Put(p)
}

// snappyCompress compresses src using snappy and returns a pointer to the
// pooled destination slice. The caller MUST putSnappyEncodeBuf the returned
// pointer once the bytes are no longer needed.
func snappyCompress(src []byte) *[]byte {
	bufPtr := snappyEncodeBufPool.Get().(*[]byte)
	*bufPtr = snappy.Encode((*bufPtr)[:0], src)
	return bufPtr
}

// snappyDecompress returns a freshly allocated []byte holding the
// decompressed payload.
//
// The claimed decoded length is checked against maxDecompressBytes before
// allocation so a malformed peer cannot trigger an oversized make().
func snappyDecompress(src []byte) ([]byte, error) {
	n, err := snappy.DecodedLen(src)
	if err != nil {
		return nil, fmt.Errorf("snappy.DecodedLen: %w", err)
	}
	if n > maxDecompressBytes {
		return nil, fmt.Errorf("memberlist: snappy-decompressed payload would exceed %d bytes (claimed %d)", maxDecompressBytes, n)
	}
	// Pooling the buffer would force a copy-out (the caller retains
	// the slice indefinitely) and add net overhead.
	return snappy.Decode(make([]byte, n), src)
}

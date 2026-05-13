// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
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

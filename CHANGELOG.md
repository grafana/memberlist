## Unreleased

### Improvements

- Add `Config.CompressionAlgorithm` to optionally select snappy compression as
  an alternative to the LZW default. Receivers always decode every supported
  algorithm; senders emit only the configured one. Default behaviour is
  unchanged.

  Rollout note: every cluster member must be upgraded to a build that decodes
  snappy BEFORE any member is configured to emit it. A receiver that does not
  know the algorithm logs `cannot decompress unknown algorithm` and drops the
  packet — it does not panic. On the gossip path the message is retried; on
  the direct-probe path the drop can cause spurious dead-marking during a
  partial rollout.
- Reduce per-call allocations on the gossip and push-pull paths by reusing
  internal scratch buffers across calls. Every public-surface function
  (`encode`, `compressPayload`, `makeCompoundMessage(s)`, `encryptLocalState`)
  still returns a freshly-allocated `[]byte` independent of any pool — the
  pools are used internally only, to amortize the msgpack / LZW / snappy /
  encryption growth allocations that would otherwise occur on every call.
  The steady-state win is largest on the compression/encode hot path; the
  encryption-pool win is bounded by allocations inside `crypto/cipher`'s
  `gcm.Seal` that pooling does not reach. Covers:
  - LZW writers/readers and snappy destination buffers (compression).
  - The internal scratch buffer used by `encode()` and `compressPayload`.
  - The compound-message scratch buffer used by `makeCompoundMessage(s)`.
  - The UDP encryption scratch buffer in `rawSendMsgPacket`.
  - A separate large-buffer pool covering TCP push-pull state, push-pull
    encryption (`encryptLocalState` / `decryptRemoteState`), and user
    messages (`sendUserMsg`), sized for `maxPushStateBytes`.
- Add per-algorithm compression metrics:
  `memberlist_compress_attempts_total{algo}`,
  `memberlist_compress_skipped_total{algo,reason="size_worse_than_original"}`,
  `memberlist_compress_errors_total{algo}`,
  `memberlist_decompress_attempts_total{algo}`,
  `memberlist_decompress_errors_total{algo}`.

### Changes

### Fixed

### Security

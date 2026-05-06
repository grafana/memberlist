## Unreleased

### Improvements

- Add `Config.CompressionAlgorithm` to optionally select snappy compression as
  an alternative to the LZW default. Receivers always decode every supported
  algorithm; senders emit only the configured one. Default behaviour is
  unchanged.
- Pool `*bytes.Buffer` values used along the gossip and push-pull paths so
  the buffer struct and its growth path are reused across calls. The
  steady-state win is largest on the compression/encode hot path; the
  encryption-pool win is bounded by allocations inside `crypto/cipher`'s
  `gcm.Seal` that pooling does not reach. Covers:
  - LZW writers/readers and snappy destination buffers (compression).
  - The msgpack encode buffer used by `encode()` and propagated through
    `compressPayload`.
  - The compound-message buffers produced by `makeCompoundMessage(s)`.
  - The UDP encryption buffer in `rawSendMsgPacket`.
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

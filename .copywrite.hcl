# Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

schema_version = 1

project {
  license        = "MPL-2.0"
  copyright_year = 2013

  # Preserve the existing headers on files introduced by the Grafana fork.
  header_ignore = [
    "compress.go",
    "compress_test.go",
    "lzw.go",
    "lzw_test.go",
    "node_selection_delegate.go",
    "snappy.go",
  ]
}

module github.com/Voltaic314/ByteWave

// go version we are using
go 1.24.2

// main dependencies -- quack quack 🦆
require github.com/marcboeker/go-duckdb v1.7.0

require github.com/google/uuid v1.6.0

// Indirect dependencies required for duckdb
require (
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	golang.org/x/exp v0.0.0-20250408133849-7e4ce0ab07d0 // indirect
	golang.org/x/mod v0.24.0 // indirect
	golang.org/x/sync v0.13.0 // indirect
	golang.org/x/sys v0.32.0 // indirect
	golang.org/x/tools v0.32.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
)

require (
	github.com/apache/arrow/go/v14 v14.0.2 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	gonum.org/v1/gonum v0.16.0 // indirect
)

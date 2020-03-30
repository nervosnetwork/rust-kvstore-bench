# Rocksdb v.s Lmdb v.s Sled

https://github.com/rust-rocksdb/rust-rocksdb

https://github.com/AltSysrq/lmdb-zero

https://github.com/spacejam/sled

## Usage

bench 30000 times write and 5000 times random read with 4KB value:

```
cargo build --release
./example-bench.sh 4096 30000 5000
```
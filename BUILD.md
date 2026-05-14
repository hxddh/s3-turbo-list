# Building s3-turbo-list

## Dev Build

```bash
cargo build
cargo test
```

## Release Build (requires workaround)

The `aws-lc-sys` crate (dependency of `aws-smithy-runtime`) detects a
GCC memcmp bug in GCC < 10 on aarch64 and aborts the build with:

```
error: failed to run custom build command for `aws-lc-sys v...`
Caused by:
  process didn't exit successfully: ...
  --- stderr
  ...
  This environment (GCC 9.4.0, aarch64) uses a known buggy memcmp
  implementation. Aborting build.
```

Use one of these workarounds:

### Option 1: Use Clang
```bash
sudo apt install clang
export CC=clang
cargo build --release
```

### Option 2: Use GCC 10+
```bash
sudo apt install gcc-10
export CC=gcc-10
cargo build --release
```

### Option 3: Disable ASM (Rust/C fallback)
```bash
export AWS_LC_SYS_CFLAGS=-DAWS_LC_NO_ASM=1
cargo build --release
```

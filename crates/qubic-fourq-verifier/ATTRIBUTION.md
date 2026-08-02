# Attribution

The FourQ arithmetic in `src/four_q/` and the verification algorithm in
`src/lib.rs` were derived from SCAPI commit
`3403107b8acfe0534faa809108906c1d6f2d8cee`.

The canonical-scalar and low-order public-key rejection rules are kept in
parity with QThirtyFour Core revision
`f55b46126c99a1c3f3266164c744b3d0cd694d9c`, specifically `src/four_q.h` and
its regression vectors in `test/fourq.cpp`.

SCAPI is Copyright (c) 2024 SCAPI Contributors and distributed under the MIT
license reproduced in `LICENSE`. This crate intentionally exposes only the
Qubic SchnorrQ digest-verification path.

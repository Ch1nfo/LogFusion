# Packaging Metadata Compatibility Design

## Scope

Remove the two setuptools compatibility warnings reported while building the
distribution. The local `data/` ignore policy is explicitly unchanged.

## Decision

Use the modern PEP 621 SPDX license string and explicitly list `LICENSE` as a
license file. Remove the `Documentation` entry because its current value is a
repository-relative path rather than a valid project URL; README keeps the
local documentation link.

## Verification

Build a wheel and run the complete pytest suite in the `agent` conda
environment. Successful verification means both commands exit zero and the
build output contains neither metadata warning.

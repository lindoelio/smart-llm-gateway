# Release Governance

## Purpose

This document defines the public governance for publishing a Smart LLM Gateway
release. Releases are deliberate maintainer decisions; merging a change does
not by itself publish a release.

## Release criteria

Before publishing a release, a maintainer must confirm that:

- The intended source revision is identified and reviewable.
- The local formatting, test, and lint checks documented in the README pass.
- Release notes accurately describe user-visible changes, compatibility impact,
  known limitations, and any required operator action.
- Security fixes have been coordinated according to [SECURITY.md](../SECURITY.md).
- The Apache-2.0 license and required notices are present.

## Versioning

Releases use Semantic Versioning:

- A patch release fixes backward-compatible defects.
- A minor release adds backward-compatible functionality.
- A major release may introduce incompatible public API or configuration
  changes.

Pre-release identifiers may be used to solicit feedback before a stable
release. A stable release must identify the exact reviewed source revision it
contains.

## Release notes

Each release note should include:

- A concise summary of the release.
- Added, changed, fixed, and removed behavior as applicable.
- Upgrade and configuration guidance when required.
- Security-impacting changes, without disclosing uncoordinated vulnerability
  details.
- Known limitations and links to relevant public issues.

## Corrections and rollback

If a published release is found to be defective, maintainers should promptly
publish a corrective release or clearly document a safe downgrade path. Released
artifacts must remain traceable to their source revision and release notes.

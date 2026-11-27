# Cloud copy and cleanup: design decisions

The README covers commands and recovery. This note explains the constraints behind
cloud deduplication and cleanup.

## Preserve references to a completed owner

If B links to A, restore reads A's disk archive and the objects it names. The
original encryption settings must still decode those bytes. Copy therefore skips
a completed A even with `--no-resume`. An unfinished backup can still complete
its scopes in separate runs.

## Separate source identity from stored representation

Copy can skip transferring a part if the destination already has the same source
objects in a form that restore can read. To discover a new owner, copy uses the same
`copy.deduplication_age_limit` as the files scope. Zero disables discovery. A positive
window admits completed physical owners by their original start time. It never extends
an owner’s eligibility because a newer backup refers to it. A freeze or a mutation can change part
metadata while leaving those objects unchanged. A fingerprint identifies the original
object references, including their storage namespace: equal suffixes in different
buckets or prefixes are not equal sources. Each file contributes the SHA-256 of its name and ordered
object references to a sum modulo 2^256. Files can appear in any order. A repeated
filename (including an empty TAR member) or inline data makes the part ineligible
for fingerprint reuse.
Persisting fingerprints prevents destination
rewrites from replacing source identity. They are not content hashes or authentication.

Equal source data can require different ciphertext. Restore uses one configured key,
so reuse also requires complete matching encryption descriptors.

## Restore a part under its requested name

A backup can request a renamed part whose files still have the old name.
Restore progress uses the requested part identity to keep mutation-renamed parts'
states separate. File access and retention use the stored identity and owner to find
and protect the actual files.

## Keep a cloud owner until its last cloud reference is gone

Several parts can share an object. While a surviving backup links to any cloud part
of an owner, deletion keeps that owner's cloud payload and disk archives intact.
This can retain unused objects longer, but the child can still restore and cleanup
does not need to rewrite an archive while another backup depends on it.

Copy records planned destination keys before transfer. If A uploads X, stops, then
retries and links to B's object Y, A's manifest still identifies X and its original
destination. After the last cloud reference to A is gone, explicit cleanup uses
A's disk archives and recorded attempts to reclaim its objects. B still owns Y.
Cleanup uses the same compressed transfer records as the files scope. It keeps one
merged manifest per destination under `copy_context/cleanup/`. A small location
list records each destination prefix so deletion can validate every key before use.

Cleanup validates the batch before deletion and retains metadata needed to retry
after a storage failure. Recorded destinations must pass the configured endpoint
checks. Metadata writers must be trusted because those records identify keys
that cleanup can delete. Copy and cleanup on different hosts need matching
ZooKeeper locks or external serialization to prevent new references during deletion.

## Validate restore preparation before writes

A configured staging path is shared by the owners needed for one restore. Using it
as each owner's exact destination would merge equal relative object keys. Instead,
each owner/disk pair gets a stable subdirectory below that path. A digest of the pair
keeps backup-name separators and punctuation from creating nested or colliding paths.
Cleanup removes only allocated subdirectories, leaving the parent directory's other
objects alone. Separate restores of the same owner/disk still require serialization.

Decryption resolves locations once and checks all selected owners before any staging
cleanup or writes. Subdirectories prevent collisions within a shared root; checks
still reject allocations that overlap encrypted sources or nest under allocations
from another root. ClickHouse disk setup uses those checked locations rather than
implementing the staging rules or resolving them again. Disk identifiers also use
owner-name digests because punctuation replacement could merge different owners.

Treating the configured path as an exact destination would preserve the old temporary
layout but require rejecting multi-owner restores or comparing their object keys.
Using it as a parent directory avoids that restriction without changing backup data
or configuration syntax. Old plaintext directly under the parent is not part of a
new allocation and requires manual cleanup after the old restore has stopped.

In-place restore would leave live tables using backup-owned objects. Later cleanup
could delete those objects, so copied cloud data requires ordinary restore.

## Report evidence rather than assume success or failure

When deletion retains shared parts, the retention log identifies the dependent backups.

A failed command does not prove publication failed. The manifest write can succeed
before its response or a later journal write fails. Reports therefore distinguish
acknowledged publication from uncertainty. The full manifest is authoritative because
a later light-metadata write can fail.

Deletion acknowledgments do not prove keys existed, and failed requests can have
partial effects. Logs distinguish plans, acknowledged deletions, and completed operations
without claiming reclaimed bytes. Resume counts differ from reuse counts because
confirming an earlier transfer differs from avoiding one. Staging cleanup failures
use ERROR because decrypted data can remain after restore fails.

## Bound metadata reads during copy

Copy overlaps a bounded number of candidate reads and processes results in order.
This limits the number of full manifests in memory and keeps owner selection
independent of network timing.

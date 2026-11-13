# Cloud copy and cleanup: design decisions

The README covers commands and recovery. This note explains the constraints behind
cloud deduplication, replacement, and cleanup.

## Preserve references across owner replacement

Changing only the payload prefix does not change the owner's disk archive address
in backup storage. Recopy can overwrite that archive and redirect a child, even
when old objects survive. Changing encryption can also replace bytes that the child
still decodes with its old key. A revision marker would detect replacement but
could not recover overwritten bytes.

Generations therefore preserve owner metadata, archives, and payload together.
Existing part links name them without another reference abstraction. The public
backup switches only after both copy scopes complete, preserving the previous
version until then.

Allocating output on every retry would abandon confirmed work. Reusing it after a
policy change could overwrite another representation. Retries therefore share
compatible pending generations but never reopen sealed ones. Staged owner payload
stays unchanged to avoid encrypting it once per scope.

Legacy links cannot name generations, so their original owner metadata and paths
remain available. Generations do not consume user-backup retention slots, but
referenced parts remain protected. Older binaries cannot enforce this contract.

## Separate source identity from stored representation

Freeze bookkeeping and mutation names can change without changing cloud objects.
Comparing those bytes would repeat transfers. Fingerprints instead identify original
object references, including their storage namespace: equal suffixes in different
buckets or prefixes are not equal sources. Persisting fingerprints prevents destination
rewrites from replacing source identity. They are not content hashes or authentication.

Equal source data can require different ciphertext. Restore uses one configured key,
so reuse also requires complete matching encryption descriptors. New links select
current published owners so replacement can retire old representations. Recopy
excludes its previous generation because reusing it would defeat replacement.
Existing children retain their old owners.

Restore progress uses the requested part identity to keep mutation-renamed parts'
states separate. File access and retention use the stored identity and owner to find
and protect the actual files.

## Delete only data that surviving backups do not need

Prefix deletion could remove another cluster's objects. Even recorded keys can be
shared with retained parts, so cleanup subtracts protected keys from exact recorded keys.

Checking backups only when deleting them could discover a bad later archive after
earlier deletions. Cleanup validates the whole batch first. Storage failures can
still leave partial progress, so metadata remains available for retry. Interrupted
generations record planned destination keys because their disk archives might not exist yet.

Recorded destinations let purge span locations without configuring each one again.
This gives metadata writers deletion authority within credential permissions.
Endpoint checks prevent those credentials from reaching an unconfigured service.

Generations do not make reference scans and deletion atomic. A concurrent copy can
add references after cleanup checks them. Matching ZooKeeper locks or external
serialization remain prerequisites. The shared local lock is insufficient.

### Keep an empty archive after partial cleanup

Suppose an owner has parts on disks A and B, but a child references only A.
Partial cleanup deletes B's unreferenced objects. A later whole-backup deletion
still encounters B in the owner's disk list. Simply skipping B when its archive
is missing would also accept lost metadata: cleanup could report success and
remove the backup records while leaving objects behind. An empty part list does
not prove object deletion either, because part records can be removed with cloud
cleanup disabled.

We could instead remove B from the disk list after deleting its objects, then
remove its archives. That would make the disk list record cleanup completion,
but would require handling interruption between those writes. Removing archives
first could leave B listed without cleanup input; saving the disk list first
could leave archives that a retry no longer visits.

We keep the empty archive because partial cleanup already rewrites archives to
remove deleted references. Keeping that result, even when empty, lets the same
retry path work without coordinating archive removal with another completion
record in backup metadata. The tradeoff is a small archive kept until whole-backup
deletion. It records no remaining references in that archive; it does not prove
that the bucket is empty or that the whole deletion finished. Missing required
archives still stop cleanup, and an empty archive cannot satisfy a retained part's
metadata requirement.

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

Remaining objects can be retained data rather than leaks. Inspection and retention
logs identify their dependents. Missing metadata makes inspection incomplete because
it could hide references.

A failed command does not prove publication failed. The manifest write can succeed
before its response or a later journal write fails. Reports therefore distinguish
acknowledged publication from uncertainty. The full manifest is authoritative because
a later light-metadata write can fail.

Deletion acknowledgments do not prove keys existed, and failed requests can have
partial effects. Logs distinguish plans, acknowledged deletions, and archive rewrites
without claiming reclaimed bytes. Resume counts differ from reuse counts because
confirming an earlier transfer differs from avoiding one. Staging cleanup failures
use ERROR because decrypted data can remain after restore fails.

## Bound reads without changing decisions

Sequential remote reads prolong operations while storage locks are held. Loading all
candidates together would retain full metadata in proportion to the backup count.
A bounded window overlaps reads, then processes results in order so network timing
cannot change owner selection. Cleanup still validates the whole batch before mutation,
and readers finish before locks are released.

The window does not bound retained deletion plans. Cloud-owner validation can also
hold one full public manifest to establish which generation is current.

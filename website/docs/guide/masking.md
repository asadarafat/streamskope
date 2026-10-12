# Configure record masking

Choose which fields the host may disclose before reading records. The
[masking reference](data-handling.md#mask-records-before-they-reach-the-workbench)
describes controls, bounds and the protection applied to each workflow.

1. Disconnect Kafka and finish any active plugin capture and cleanup work.
2. Open **Preferences → Protection**. Choose the keys, exact header names, JSON
   Pointer paths or complete values to mask. The settings apply to every connection
   on this host, not just the current profile.
3. Review the policy with the intended recipient or data owner, then save it.
   Saving clears retained records and transform logs; it does not erase prior
   exports or clipboard copies.
4. Reconnect and read an approved sample. Confirm **Masking active** and inspect
   the protected fields before exporting or sharing data.

Selective JSON paths apply to the decoded projection. If safe selective masking
cannot be established, the value is fully masked. Original-byte envelopes are
withheld while masking is active, so a second representation cannot disclose the
unprotected value. This can make replay unavailable.

Masking does not encrypt an exported file or revoke copies already shared. Keep
files in an approved location and follow the [export disclosure rules](data-handling.md#understand-an-export).

# JSON Output Stability

Every hpc-compose command that supports `--format json` (or `--json`) emits a
**versioned, documented contract** rather than an incidental serialization of an
internal struct. Scripts and CI can depend on these outputs across releases
under the policy below.

## The `schema_version` field

Each top-level JSON object carries a `schema_version` integer:

```json
{
  "schema_version": 1,
  "job_id": "1234567",
  "...": "..."
}
```

`schema_version` describes the **output contract**, and is independent of any
`schema_version` nested deeper in the payload (for example the one under a
`record` object, which versions the on-disk job-metadata format). Read the
top-level `schema_version` to branch on contract changes; read a nested one only
if you consume that nested object directly.

### Versioning policy

* **Additive changes do not bump `schema_version`.** New fields may be added to
  any output at any time. Consumers must ignore unknown fields.
* **A removed or renamed field bumps `schema_version`** for that command.
* A field that is present is stable: its name and meaning do not change without a
  version bump.

Because additions are always allowed, pin your parsing to the fields you read and
tolerate extras.

## Published schemas

The JSON Schema for a command's output is served by the CLI and checked into the
repository under `schema/outputs/<command>.schema.json`:

```text
hpc-compose schema --output status
hpc-compose schema --output jobs-list
```

Passing an unknown name lists the commands that have a published schema. The
schemas are generated from the same Rust types that produce the output, and a
test fails the build if a checked-in schema drifts from what the code emits — so
a published schema always matches the real output byte-for-byte in shape.

## A few outputs are intentionally unwrapped

A handful of commands emit a bare JSON array, a record that already carries its
own version, or a mirror of the compose spec surface, so a top-level
`schema_version` cannot be added without a breaking reshape. These keep their
exact current output and are versioned only through their published schema
(and, where present, the record's own field):

* `cache list` and `rendezvous list` — bare arrays.
* `rendezvous resolve` — a record whose own `schema_version` is its disk format.
* `config` and `inspect` — mirror the compose spec surface (see below), so
  their top-level object has no `schema_version` field either.

## Notes for the effective-config outputs

`config` and `inspect` print the resolved compose configuration (their
published schema names are `spec-config` and `spec-inspect`, as in
`hpc-compose schema --output spec-config`). Their structure follows the compose
spec surface documented in the [Spec Reference](spec-reference.md); secret
values are redacted before printing.

## Related Docs

* [CLI Reference](cli-reference.md)
* [Wire Up CI](ci-integration.md)
* [Spec Reference](spec-reference.md)

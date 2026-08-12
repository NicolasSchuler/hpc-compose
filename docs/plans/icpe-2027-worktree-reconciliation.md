# ICPE 2027 Worktree Reconciliation

- **Reconciliation date:** 2026-08-09
- **Active target:** repository root on branch `main`
- **Shared repository baseline:** commit
  `c53dac20a867470aa4184e7d35f5d76b56679801`, tag `v0.2.3`
- **Review snapshot:** independent review-focused worktree result set, identified
  below by artifact hashes
- **Literature snapshot:** independent literature-focused worktree result set,
  identified below by artifact hashes
- **Live planning authority after reconciliation:**
  [ICPE 2027 open improvement ledger](icpe-2027-open-improvement-ledger.md)

## Purpose and authority

This record explains how two independent task-worktree result sets were merged
with the active workspace. It preserves provenance, identifies exact duplicates,
records conflicts and decisions, and prevents the older ledgers from becoming
competing sources of truth.

The source reports remain evidence and dated recommendations. The active open
improvement ledger alone owns current item status, priority, gate, dependencies,
and acceptance criteria. The product backlog owns public product candidates.
Neither a literature recommendation nor an older ledger entry establishes that a
feature exists or has been approved for implementation.

## Source snapshots and file disposition

| Source artifact | Source SHA-256 | Reconciliation action | Reason |
| --- | --- | --- | --- |
| Review-snapshot literature prompt | `59a0177b40f681f0566cd95697d83ed672988b6b788e2a3e8feef0a9ef4c4a02` | Retain active copy | Byte-identical |
| Review-snapshot meta-draft | `f1b7dad6cdcf39b6a333b33903705c04e6fc6d6178966d89cf885073b899a904` | Retain active copy | Byte-identical manuscript intake |
| Review-snapshot review prompt | `847b42b1814a0414c103859ac317df9a37dcaa222af7184247d784c1d7b61cc6` | Retain active copy | Byte-identical |
| Review-snapshot candidate inventory | `e1e40f5035057eaa72c8dde8e564464d7b48e18da13bee750c3867547fdca246` | Retain active copy | Byte-identical 25-candidate/RC-ID inventory |
| Review-snapshot consolidated review | `1365bfcb2c5e087375a41651d57c440c370806631cb952e039b2b0793b3785d5` | Retain active copy as dated snapshot | Byte-identical; later corrections live in the meta-review and ledger |
| Review-snapshot ledger | `81fc49b81e408750c9a0f58449d8d85aba151ca9737fffc054952c91b6bf5982` | Do not copy | Older 31-item baseline; all IDs are retained and corrected in the active successor |
| Literature-snapshot literature prompt | `59a0177b40f681f0566cd95697d83ed672988b6b788e2a3e8feef0a9ef4c4a02` | Retain active copy | Byte-identical |
| Literature-snapshot meta-draft | `f1b7dad6cdcf39b6a333b33903705c04e6fc6d6178966d89cf885073b899a904` | Retain active copy | Byte-identical |
| Literature-snapshot review prompt | `847b42b1814a0414c103859ac317df9a37dcaa222af7184247d784c1d7b61cc6` | Retain active copy | Byte-identical |
| Literature-snapshot first literature report | `84b63e96028c5d11ef7dae8efb69a43170f208d6aa8e9e0055c179e33724f0a5` | Import technical body; add historical-status banner | Unique 628-line nearest-neighbor analysis |
| Literature-snapshot first-pass BibTeX | `9027323d5b7dd0430c782dc14bfd8d613420b07d2aa45efeedfd6e11d013e6e4` | Import unchanged | Unique 30-entry bibliography; no key or DOI collision with pass two |
| Literature-snapshot ledger | `91ede2a60807e95a01db7e8c3135b7557bf9dbc831a1d943964e5e690d5f3fb0` | Mine and crosswalk; do not copy | Older 19-item alternative taxonomy; useful acceptance boundaries but weaker on later corrections |
| Literature-snapshot product backlog | tracked-file modification | Selectively reconcile | Promote only genuine public candidates, preserving go/no-go gates |

The active files retained or added by this merge are:

- [meta-level draft](icpe-2027-meta-draft.md);
- [consolidated review](icpe-2027-review-results.md) and
  [meta-review](icpe-2027-review-meta-review.md);
- [25-candidate review inventory](icpe-2027-review-reference-candidates.md);
- [independent first literature report](icpe-2027-related-work-report.md) and
  [30-entry first-pass bibliography](icpe-2027-reference-candidates.bib);
- [recent/canonical second pass](icpe-2027-related-work-second-pass.md) and
  [31-entry pass-two bibliography](icpe-2027-reference-candidates-pass2.bib);
- [live open improvement ledger](icpe-2027-open-improvement-ledger.md); and
- [product backlog](2026-07-feature-brainstorm.md).

## Alternative-ledger crosswalk

Every literature-snapshot ledger item is retained by meaning. The older ID is not
reused because the active ledger already has stable workstream IDs and more
precise ownership.

| Literature-snapshot item | Active owner(s) | Disposition and retained value |
| --- | --- | --- |
| ICPE27-01 | ICPE-M01 | Merge: keep the narrowed thesis, but retain the active default of two contributions with evidence supporting unless later promoted |
| ICPE27-02 | ICPE-M03, ICPE-I01–I03, ICPE-E01 | Merge: semantic contract, resource decisions, lifecycle state model, and independent conformance oracle |
| ICPE27-03 | ICPE-I06/I08/I09/I11, ICPE-M04 | Split: artifact taxonomy, exact submitted-byte integrity, optional receipt, optional complete source mapping, and manuscript wording are separate acceptance boundaries |
| ICPE27-04 | ICPE-I03, ICPE-M07, ICPE-E01, ICPE-L03 | Merge: make post-acceptance autonomy a measured invariant and name every allowed allocation-local helper |
| ICPE27-05 | ICPE-M05, ICPE-I04, ICPE-E05/E09 | Merge: evidence identity, availability/degradation, run/job namespace, fault behavior, and optional attribution accuracy |
| ICPE27-06 | ICPE-E01–E07 | Merge: pre-registration obligations are distributed to the study or artifact item that owns each estimand |
| ICPE27-07 | ICPE-I08/I09 | Correct and split: exact scheduler-consumed-byte preservation is mandatory before relevant experiments; a broader sealed receipt remains conditional |
| ICPE27-08 | ICPE-I05/L05 and RW-11 | Conditional: standards-shaped export is a product candidate only after privacy, omission, validation, and contribution go/no-go decisions |
| ICPE27-09 | ICPE-E01 | Merge: lifecycle-transition corpus and independent partial-order oracle |
| ICPE27-10 | ICPE-I01/I02, ICPE-E01/E04 | Merge: real-Slurm placement/resource conformance and crossed site/backend evidence |
| ICPE27-11 | ICPE-E02/E11 | Merge: phase-separated cost study plus optional supervisor scaling envelope |
| ICPE27-12 | ICPE-L01/L02/L08, ICPE-E02 | Merge: comparator-role registry with native Slurm primary and narrow/conditional component comparators |
| ICPE27-13 | ICPE-E03 | Merge: Eng et al. replication package becomes the primary reproducible corpus frame |
| ICPE27-14 | ICPE-E05/E06/E10 | Merge: recovery, assurance-effect, tamper, and end-to-end identity fault protocols |
| ICPE27-15 | ICPE-E04 | Merge: backend-by-site study or explicit narrowing |
| ICPE27-16 | ICPE-E09 | Retain deferred: service-level attribution requires cross-site ground truth |
| ICPE27-17 | ICPE-M08, ICPE-L01/L08/L09 | Merge: seven-section paper and six-part related-work architecture supersede the older outline |
| ICPE27-18 | ICPE-R01/R03, ICPE-M09 | Split: review auditability and statement-level claim/evidence provenance |
| ICPE27-19 | ICPE-E07/L07 | Merge: tiered artifact, claim map, literature freshness, and bibliography quality freeze |

## Conflict decisions

### Contribution hierarchy

The first literature report proposed four contribution-shaped areas. The later
persona synthesis and meta-review found that this would diffuse the paper.
ICPE-M01 therefore remains authoritative: two contributions by default—the
finite typed semantic contract and static Slurm lowering/generated-supervisor
design. Evidence is supporting infrastructure unless ICPE-L05/L08 and executed
ICPE-E05 results justify deliberate promotion.

### Preview, mapping, and submitted-byte integrity

The later technical audit overrules the older compressed preview statement:
ordinary non-remote/non-local Slurm `up --dry-run` has a qualified same-context
byte relation to the corresponding submission path. Portable preview,
remote/local rendering, selected source attribution, scheduler consumption,
retention, and export remain separate artifacts.

The active plan also retains two defects absent from the older ledgers:

1. evidence initialization rereads a mutable path after `sbatch` returns, so
   the manifest may not attest the bytes consumed by the scheduler; and
2. bundle export later rereads that mutable path without verifying it against
   the manifest digest.

ICPE-I08/E10 own these mandatory integrity boundaries. ICPE-I09's sealed receipt
and ICPE-I11's complete source-to-submitted mapping remain separate, optional
go/no-go decisions.

### Product candidates

The literature snapshot treated a sealed receipt and RO-Crate export as if their
implementation had already been selected. The active product backlog records
them more narrowly:

- OP-11 is a candidate whose prerequisite is closing ICPE-I08 and approving
  ICPE-I09's canonicalization, schema, privacy, legacy, and verification design.
- RW-11 is a candidate whose prerequisite is closing ICPE-I05/L05's publish-safe
  export and standards crosswalk.

Neither row is a release commitment or evidence of current behavior.

### Baselines and comparator roles

Expert native Slurm remains the mandatory executable control. Other systems are
admitted only for the dimensions they implement equivalently:

- Executorlib is a potential narrow native-`srun`/resource/concurrency
  component comparator.
- IOPS is a potential validation/script/status comparator; its sequential Bash
  semantics must not be recast as native-step service lifecycle.
- DockSing requires a recorded semantic-equivalence decision.
- Singularity Compose and Mini Compose are primarily Compose-side comparators.
- InterLink and HPK require their full resident control planes and are not
  lightweight drop-in baselines.
- HPE Capsules and SCUP-HPC remain descriptive unless reproducible access and
  equivalent execution units are established.

An “evidence-off” ablation is not assumed equivalent. It must preserve every
non-evidence byte, behavior, lifecycle, tracking boundary, and measured phase or
be labeled a non-equivalent diagnostic control.

### Literature authority

The three literature inventories are complementary:

1. the consolidated review's 25-candidate inventory owns RC IDs;
2. the independent first report adds close operational systems and 30 BibTeX
   keys; and
3. the second pass adds recent closest neighbors, canonical/high-impact lineage,
   strong-venue work, and 31 more BibTeX keys.

The first report is preserved as a historical recommendation. Its technical
source analysis remains usable, but its contribution count and product
recommendations do not override the meta-review or live ledger.

## Concrete reconciled additions

- Imported the unique first literature report and first-pass bibliography.
- Linked both literature passes and both BibTeX files from the live ledger.
- Added ICPE-I11 for an optional complete source→plan→verified submitted-span
  mapping decision.
- Added ICPE-M09 for mandatory statement-level claim→implementation→evidence
  provenance.
- Strengthened controller-autonomy, evidence-object, comparator-admission,
  external-corpus, literature-freshness, and artifact-integrity acceptance
  criteria.
- Added product candidates RW-11 and OP-11 with explicit go/no-go prerequisites.
- Kept the review-snapshot artifacts once, retained its ledger as a superseded
  baseline by hash, and avoided importing any stale wording.

## Verification results

Verification was run on 2026-08-09 after integration.

- Source preservation:
  - the first-pass bibliography remains byte-identical to the literature snapshot
    at SHA-256
    `9027323d5b7dd0430c782dc14bfd8d613420b07d2aa45efeedfd6e11d013e6e4`;
  - the imported report differs from the literature snapshot only by the
    historical-status banner
    and the clarification “before this pass”; its integrated SHA-256 is
    `06f52dce55339232a32529e5273de39f7fe4b12d25d8dda5c367c48dc65d824d`;
  - the integrated live ledger SHA-256 is
    `4b9c23af259e29477d396da9337bd5d1310e266b5fa2980f657572680061715f`.
- Ledger consistency: 44 overview IDs equal 44 detailed headings; each item has
  matching status, priority, and gate plus type, tags, owner, sources, evidence
  class, acceptance criteria, and dependency/guardrail fields. All 44 explicit
  item references resolve.
- Alternative-ledger coverage: ICPE27-01 through ICPE27-19 occur exactly once in
  the crosswalk.
- Bibliography integrity:
  - Biber 2.21 datamodel validation passed for both files;
  - 61 entries have unique keys, 53 distinct DOI values have no collision, and
    all 25 BibTeX keys cited directly by the live ledger resolve in the union.
- Focused document validation:
  - `markdownlint-cli2` reported zero issues across its 78-file configured
    surface;
  - `typos` passed the planning documents, bibliographies, backlog, and spelling
    configuration;
  - offline link validation checked 423 links/205 unique targets with zero
    errors.
- Repository `just docs-check`:
  - passed site/agent generators and checks, 14 script/skill tests, mdBook, Rust
    documentation with warnings denied, generated-manpage checks, spelling,
    Markdown, and 4,208-link/1,645-unique-target validation with zero errors;
  - stopped only at Pa11y because the local Puppeteer cache is missing the
    `Google Chrome for Testing Framework` binary for Chrome
    `148.0.7778.97`. This is an environment/runtime installation failure after
    the document checks, not a manuscript, ledger, bibliography, or link defect.

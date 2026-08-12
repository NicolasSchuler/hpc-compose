# Consolidated ICPE 2027 Manuscript Review

Review date: 2026-08-09

Manuscript: docs/plans/icpe-2027-meta-draft.md

Repository pin: CURRENT_WORKTREE at code commit c53dac20a867470aa4184e7d35f5d76b56679801, exact tag v0.2.3

Manuscript intake SHA-256: f1b7dad6cdcf39b6a333b33903705c04e6fc6d6178966d89cf885073b899a904

Bibliography input: NONE

Review method: seven personas in the required two waves, with the lead independently reading the complete manuscript and checking every consolidated P1 against current code, tests, documentation, official Slurm documentation, or primary literature.

Early-draft rule applied: missing experiments, measurements, results, plots, and final artifact packaging are neutral FUTURE_EVIDENCE. They were not treated as defects and did not reduce scores merely because they are pending.

## 1. Executive verdict

### Story-readiness decision: READY_WITH_TARGETED_REVISIONS

The Research Track story survives adversarial review. A finite, allocation-scoped compiler is a defensible and performance-relevant design point, the current implementation has real staged representations and lowering machinery, and the draft does not fabricate results. Full prose expansion should nevertheless wait for eight P1 resolutions. The most important are not missing experiments: they are current-contract issues. Partial-node task inheritance and universal step overlap leave resource semantics unresolved; the generated batch script is an allocation-resident supervisor despite the “no control plane” wording; source attribution is selected and preview-based rather than exact for the submitted script; and the evidence story conflates immutable records, mutable or un-hashed inputs, best-effort observations, and non-redacted exports. The novelty claim also needs a closest-neighbor semantic invariant rather than novelty by feature combination.

### Proposed final thesis

> For finite, allocation-scoped multi-service workloads, hpc-compose compiles a constrained Compose-style specification into one inspectable Slurm job whose generated batch supervisor coordinates native job steps, without deploying a separate cluster daemon or nested scheduler.

This wording keeps the strongest boundary, names the runtime controller honestly, and does not overclaim exact provenance or end-to-end reproducibility.

### Recommended contribution order

1. **A frozen allocation-scoped application language and semantic contract.** Define the supported typed subset, allocation-versus-step resource rules, placement, readiness observer, service state transitions, failure behavior, sharing, rejection, and escape-hatch boundary.
2. **Inspectable lowering to native Slurm artifacts.** Explain the staged representations, deterministic domain, native job-step lowering, generated batch supervisor, and selected source-to-preview attribution.
3. **A bounded run-identity and evidence protocol, conditionally retained.** Keep this as a contribution only if RQ5 yields scheduler-specific insight about identity, faults, degradation, or interpretability beyond established provenance standards. Otherwise present it as supporting assurance and artifact infrastructure.

The safe novelty claim is the explicit, testable semantic mapping and its rejection boundary—not Compose syntax, YAML, container launch, script generation, multi-service orchestration, allocation-internal scheduling, or provenance individually or in a loose bundle.

## 2. Review coverage and evidence quality

### Manuscript coverage

- The lead read all 416 manuscript lines through EOF before synthesis.
- Each of the seven personas independently read the complete manuscript through EOF.
- The review used the live untracked manuscript bytes at CURRENT_WORKTREE; the code pin was commit c53dac20a867470aa4184e7d35f5d76b56679801.
- The manuscript was not edited.

### Exact Markdown union manifest

A hidden-file-safe intake inventory found **79 repository Markdown files** before the two review outputs were created. The primary assignments below are non-overlapping and their union is exactly that intake corpus. Build output under target, Git internals, and the two subsequently generated review files are not repository input documentation and are excluded.

#### Wave 1 — performance methods, 16 files

- docs/src/example-source.md
- docs/plans/icpe-2027-meta-draft.md
- docs/plans/icpe-2027-review-prompt.md
- docs/implementation-plan.md
- docs/src/runbook.md
- docs/src/notebook.md
- docs/src/production-readiness.md
- docs/src/workspaces.md
- docs/src/troubleshooting.md
- docs/src/cluster-profiles.md
- skills/hpc-compose/references/command-safety.md
- docs/src/support-matrix.md
- skills/hpc-compose/references/cluster-setup.md
- docs/src/roadmap.md
- GOVERNANCE.md
- docs/brand/README.md

#### Wave 1 — Slurm/HPC runtime, 17 files

- CHANGELOG.md
- dev-cluster/README.md
- docs/plans/icpe-2027-literature-prompt.md
- docs/src/docker-compose-migration.md
- docs/dual-mode-source-sync-design.md
- docs/src/failure-recovery.md
- docs/src/agent-command-safety.md
- docs/src/ai-agent-setup.md
- skills/hpc-compose/SKILL.md
- examples/llm-curl/README.md
- docs/src/command-families.md
- docs/src/json-output-stability.md
- docs/site-guides/template.md
- skills/hpc-compose/references/operations-recovery.md
- examples/llama-uv-worker/README.md
- docs/src/brand-assets.md
- docs/src/backlog.md

#### Wave 1 — compiler/language design, 12 files

- docs/src/spec-reference.md
- docs/src/examples.md
- docs/src/slurm-container-basics.md
- README.md
- docs/src/development-workflow.md
- docs/src/cache-management.md
- docs/src/runtime-backends.md
- docs/src/glossary.md
- skills/hpc-compose/references/authoring-migration.md
- docs/src/README.md
- docs/src/artifacts-and-resume.md
- examples/vllm-uv-worker/README.md

#### Wave 1 — reproducibility/provenance, 15 files

- docs/src/cli-reference.md
- docs/src/run-evidence.md
- docs/src/sweeps.md
- docs/src/haicore-guide.md
- docs/src/canary-runs.md
- CONTRIBUTING.md
- docs/src/ci-integration.md
- docs/plans/2026-07-feature-brainstorm.md
- docs/src/architecture.md
- examples/README.md
- docs/src/secrets.md
- docs/src/SUMMARY.md
- docs/src/slurm-capability-scope.md
- SECURITY.md
- examples/models/README.md

#### Wave 1 — practitioner/software-engineering reader, 15 files

- docs/spec-language-features-design.md
- docs/src/runtime-observability.md
- docs/src/quickstart.md
- docs/src/files-and-directories.md
- docs/src/installation.md
- docs/src/local-slurm-dev-cluster.md
- docs/src/execution-model.md
- CODE_OF_CONDUCT.md
- docs/src/evolve.md
- docs/src/task-guide.md
- docs/src/why-hpc-compose.md
- docs/src/cross-job-rendezvous.md
- docs/src/faq.md
- docs/src/exit-codes.md
- AGENTS.md

#### Wave 2 — reliability/privacy meta-review, four hidden files

- .github/LAUNCH_CHECKLIST.md
- .github/PULL_REQUEST_TEMPLATE.md
- .github/RELEASE_TEMPLATE.md
- .github/copilot-instructions.md

Wave 2’s novelty reviewer additionally re-read the current architecture, execution, evidence, secrets, runtime, rationale, artifact/resume, and Slurm-container pages, but those deliberate second-pass overlaps are not counted twice. **Coverage gaps: none.**

### Evidence classification

| Class | Treatment in this review | Representative evidence |
| --- | --- | --- |
| VERIFIED_CURRENT | Present behavior confirmed in current code, current user documentation, current tests, or release metadata | docs/src/architecture.md:34–58; docs/src/execution-model.md; src/planner.rs; src/render.rs; current v0.2.3 tests |
| HISTORICAL | Design, plan, roadmap, backlog, brainstorm, and release-positioning material not promoted without current confirmation | docs/spec-language-features-design.md:1–5; docs/dual-mode-source-sync-design.md; docs/src/roadmap.md; docs/src/backlog.md; docs/plans/2026-07-feature-brainstorm.md |
| PROPOSED_DESIGN | Manuscript commitments that are not yet paper content or implemented contracts | semantic table, state machine, final figure, final structure, artifact tiers, comparison matrix |
| FUTURE_EVIDENCE | Properly framed measurements and results still to be collected | RQ1–RQ7 outcomes, real-site/backend validation, overhead, fault studies |
| UNRESOLVED | Current evidence is contradictory, incomplete, or requires an author/implementation decision | partial-placement task geometry, aggregate sharing, contribution-three status, exact comparator set |

### Venue guidance

Checked on 2026-08-09:

- The official ICPE 2027 broad Call for Contributions now exists: <https://icpe2027.spec.org/call-for-contributions/>
- It explicitly includes measurement and empirical evaluation, design and development, runtime management, resource scheduling, platform/compiler topics, benchmarking, performance, efficiency, and reliability. The paper’s proposed mechanism and RQs fit that scope.
- The detailed 2027 Research Track page still says “Details TBA”: <https://icpe2027.spec.org/tracks-and-submissions/research-paper-track/>
- The detailed 2027 Artifact Evaluation Track page also still says “Details TBA”: <https://icpe2027.spec.org/tracks-and-submissions/artifact-evaluation-track/>
- Material change from the manuscript and from ICPE 2026: manuscript line 5 is stale because a 2027 broad call now exists. The 2026 regular-paper/EERCS distinction, ten-page limit, detailed review criteria, and artifact categories remain only provisional until the 2027 track pages are populated.

The regular Research Track remains the recommended target. EERCS is a fallback only if the eventual evidence is primarily deployment experience or a case study. A later companion research artifact remains distinct from the standalone tool-artifact category.

### Verification quality and limits

- The compiler reviewer ran the current library suite: 1,194 passed, zero failed, one ignored.
- Focused reviewers also ran current tests for semantic validation, plan/render annotation and explanation, a fake-tool end-to-end submission path, evidence initialization, evidence bundles, record identity, and scheduler-ID reuse. The reproducibility reviewer reported 15 evidence tests, nine bundle tests, and two record identity/reuse tests passing.
- No consolidated claim is made that the full integration suite, production Slurm, a real container backend, GPU/fabric behavior, or production multi-node behavior passed. Those remain FUTURE_EVIDENCE or explicit verification gaps.
- Official Slurm documentation was checked for step semantics. It states that step-level --exact limits a step to requested resources and --overlap allows steps to share CPUs, memory, and GRES: <https://slurm.schedmd.com/srun.html>

## 3. Consolidated scorecard

Scale: 0 BLOCK; 1 MAJOR_REVISION; 2 PASS_WITH_REVISIONS; 3 STRONG. The median and range summarize seven persona judgments; the lead decision does not average away technical correctness.

| # | Dimension | Persona median and range | Lead | Lead rationale |
| ---: | --- | --- | ---: | --- |
| 1 | Track and genre fit | 3, range 3–3 | **3** | A systems/compiler mechanism tied to resource correctness, launch cost, reliability, and measurement fits the current 2027 broad scope. |
| 2 | One-sentence thesis | 2, range 1–3 | **1** | The core is memorable, but “no resident control plane” and “traceable path” materially exceed the precise current boundaries. |
| 3 | Problem significance and specificity | 2, range 2–2 | **2** | The mismatch and costs are concrete; prevalence and one stakeholder-centered causal account remain weak. |
| 4 | Motivating scenario | 1, range 1–1 | **1** | All seven reviewers found that no single workload carries the full argument. |
| 5 | Red thread | 2, range 2–2 | **2** | Allocation-scoped compilation is a strong spine, but assurance and evidence still risk becoming adjacent mini-papers. |
| 6 | Argumentative progression | 2, range 2–3 | **2** | The intended order is coherent, while meta-planning and unproven prior-art gaps interrupt it. |
| 7 | Overall section structure | 2, range 1–3 | **1** | Fourteen sections plus a paper-local artifact guide are not page-budget coherent. |
| 8 | Contribution coherence and alignment | 2, range 2–3 | **2** | The order is stable, but contribution three’s research status and the supervisor boundary remain unsettled. |
| 9 | Novelty boundary and calibration | 2, range 1–3 | **1** | The cautious tone is good, but novelty still rests on a feature combination rather than a verified unmatched invariant. |
| 10 | Technical and semantic fidelity | 1, range 1–2 | **1** | Partial-placement task inheritance, universal overlap, and understated supervision are core current-contract issues. |
| 11 | Architecture and design rationale | 2, range 1–2 | **1** | The stages exist, but the figure omits the batch supervisor, observer/storage loci, and resource-sharing contract. |
| 12 | Claim-status discipline | 3, range 1–3 | **2** | Overall discipline is unusually strong, but current claims about venue status, immutable inputs, exact preview, and control-plane absence need correction. |
| 13 | Claim-to-evidence traceability | 2, range 1–2 | **1** | Selected preview spans and several evidence trust levels are presented too uniformly. |
| 14 | Performance-engineering relevance | 3, range 2–3 | **3** | Resource geometry, contention, readiness latency, overhead, failure timing, and attribution are causal and measurable. |
| 15 | Evaluation readiness | 2, range 1–3 | **2** | RQs are falsifiable, but oracle independence, equivalence, estimands, fault applicability, and confounding need freezing. |
| 16 | Related-work positioning | 2, range 1–2 | **1** | The seed list is strong, but the manuscript lacks a charitable closest-neighbor argument and named direct baselines. |
| 17 | Scope, non-goals, limitations, and threats | 3, range 2–3 | **2** | The bounded scope is a strength; shared-resource and generated-supervisor limits must join it. |
| 18 | Reproducibility precision and artifact evaluability | 2, range 1–3 | **1** | Record immutability, input identity, view reconstruction, rerunning, payload completeness, and publish safety are conflated. |
| 19 | Terminology consistency | 2, range 2–3 | **2** | Most terms are disciplined, but control plane, job/application, evidence, and run/job namespace need correction. |
| 20 | Software-engineering audience accessibility | 2, range 1–2 | **2** | The prose is approachable, but a scenario-first Slurm and locus primer is still required. |
| 21 | Skim-proofness | 2, range 2–3 | **2** | Title and contributions align, while the first-page thesis and figure omit decisive qualifications. |
| 22 | Natural-language flow and presentation | 2, range 2–2 | **2** | The abstract and introduction are clear; the remainder is intentionally list- and instruction-heavy. |
| 23 | Redundancy and compression readiness | 2, range 1–2 | **1** | Thesis, caveats, risks, guardrails, and gates repeat across too many sections. |
| 24 | Reliability, security, and privacy boundaries | 2, range 1–2 | **1** | Strong repository safeguards are not yet represented as manuscript boundaries for secrets, exports, storage, and identity reuse. |

No core dimension is 0. The formal no-core-blocker gate therefore passes. The full-prose gate remains conditional on the P1 queue below.

### Neutral FUTURE_EVIDENCE summary

The following did not reduce scores because the draft correctly presents them as planned work: semantic-conformance results; launch and steady-state overhead; public-corpus coverage; site/backend portability; failure and evidence-recovery outcomes; assurance discrimination; telemetry attribution; user/productivity effects; and final artifact packaging. They become defects only if later prose implies outcomes before measurement or if the frozen designs cannot support their claims.

## 4. Consensus strengths

Only strengths independently supported by at least two personas or rechecked by the lead are retained.

1. **Strong venue and mechanism fit.** All seven reviewers scored track fit 3. The proposed questions concern native resource semantics, launch/coordination cost, reliability, failure timing, and performance-run evidence rather than software availability alone.
2. **A real compiler substrate exists.** Current code implements parsing, extension/interpolation handling, validation, an internal plan, runtime derivation, preflight/preparation, rendering, submission, and evidence tracking. The label “compiler” is defensible for a frozen typed subset if invariants and conformance become central.
3. **The bounded scope is unusually clear.** One resolved application instance or expanded trial targets one allocation; dynamic cross-allocation scheduling, bin packing, heterogeneous jobs, and cluster administration are excluded.
4. **The early-draft claim discipline is strong.** The manuscript explicitly says it contains no results, labels RQs as commitments, and warns against claims of formal preservation, universal portability, complete reproducibility, or measured debugging improvement.
5. **The evaluation skeleton is performance-relevant and falsifiable.** Native scripts, negative cases, fault injection, practical margins, absolute measures, and non-recoverable cases are all anticipated; the remaining work is to make controls and estimands independent and precise.
6. **The evidence implementation has meaningful reliability mechanisms.** Immutable published documents, persistent locks, atomic replacement, valid-prefix handling, deterministic RunView folding, scheduler-ID-reuse checks, and explicit degraded states are current mechanisms, even though the manuscript currently compresses their trust boundaries.

## 5. Severity-ranked findings

### P0 blockers

None.

### P1-01 — Freeze the paper-core language and resolve its resource semantics

- **Manuscript anchor:** lines 65, 105, 123, 161, 173–189, and 221.
- **Current evidence:** docs/src/spec-reference.md:1135–1154; src/planner.rs:724–806; src/planner/tests.rs:1068–1145; src/render/command.rs:57–68; official Slurm srun documentation.
- **Observation:** A service placed on two nodes can inherit allocation-wide ntasks=24 and ntasks_per_node=4 from a six-node allocation; the current test explicitly expects that pair. Rendering emits both values with --nodes=2. Every service step also receives --exact and --overlap, while planner capacity checks are per service rather than an aggregate proof across concurrently running steps. Raw shell and Slurm escape hatches also sit outside typed validation.
- **Interpretation and impact:** The primary compiler claim does not yet have one unambiguous, feasible resource relation. Native-baseline overhead is uninterpretable if sharing/isolation differs, and “unsupported semantics are rejected” is too broad for raw escape hatches and site-level validity.
- **Concrete revision:** Freeze a paper-core table with syntax, normalization, preconditions, IR form, Slurm effect, task-geometry rule after placement, CPU/memory/GRES sharing, rejection layer, and excluded escape hatches. Decide whether contradictory inherited geometry is rejected, recomputed, or deliberately delegated to Slurm, and align code, tests, and current docs before making a preservation claim.
- **Regression/read-back check:** Every core row must have a generated-command fixture, a negative/rejection case, and at least one real-Slurm conformance case where appropriate. Partial-node and overlapping cases must agree across table, planner, renderer, and observed task/resource distribution.
- **Persona agreement:** Four personas independently raised the resource-contract problem: compiler, runtime, novelty, and meta. No reviewer supplied evidence that the current partial geometry is semantically safe.

### P1-02 — Name the generated batch supervisor and readiness observer honestly

- **Manuscript anchor:** lines 19, 43, 69, 105, 117–119, 191–205, and 265.
- **Current evidence:** docs/src/execution-model.md:17–30 and 66–78; src/render.rs:590–608, 1674–1736, 1900–1973, 2108–2225, 2278–2308, and 2342–2385; src/planner.rs:925–940.
- **Observation:** The allocation’s generated shell records a supervisor PID, launches services, performs TCP/HTTP/log/sleep readiness, applies restart and failure policy, writes state, monitors exits, and cleans up. Those checks execute from the batch-script host; implicit localhost is permitted only for primary-node placement.
- **Interpretation and impact:** “Without a resident orchestration control plane” can be read as “no runtime controller,” which is false in the ordinary systems sense. Omitting the observer and storage/network loci hides performance overhead, reachability assumptions, shared-log requirements, and supervisor failure modes.
- **Concrete revision:** Use “without deploying a separate cluster-resident daemon or nested scheduler.” Put the allocation-resident generated batch supervisor in the first architecture figure and state machine. Add a locus table for preparation, submission, supervision, readiness types, service execution, storage, and evidence collection.
- **Regression/read-back check:** A reader must identify who launches, observes readiness, restarts, terminates, and cleans each service, and correctly predict when localhost, an explicit routable host, or a shared log is required.
- **Persona agreement and dissent:** Runtime, novelty, and meta reviewers raised this independently. Methods and practitioner reviews were initially more permissive; current code resolves the disagreement in favor of the qualified wording.

### P1-03 — Replace novelty by combination with a closest-neighbor semantic invariant

- **Manuscript anchor:** lines 77–83, 127, 294–365, and 400.
- **Primary evidence:** Singularity Compose, DockSing, StreamFlow, Sarus Suite, benchkit, dagster-slurm, Maestro, Flux, HyperQueue, QCG-PilotJob, RADICAL-Pilot, SmartSim, SAIA, Workflow Run RO-Crate, CWLProv, and ReproZip; exact verified records appear in section 10 and the companion candidate file.
- **Observation:** Compose-style multi-service orchestration, Compose-to-Slurm translation, declarative HPC experiments, generated scripts, allocation-internal task management, Slurm-native services, and workflow provenance all have direct prior art.
- **Interpretation and impact:** The phrase “combination and explicit contract” still reads as a feature bundle unless it names an invariant that the most charitable neighbor does not supply. This is the strongest route for a “YAML to shell” rejection.
- **Concrete revision:** Build a closest-neighbor matrix with compilation unit, allocation ownership, concurrency/readiness, resource semantics, controller locus, rejection behavior, inspectable artifact, source attribution, and evidence scope. State the narrow difference only after that matrix. Avoid first, only, and unique.
- **Regression/read-back check:** Every thesis phrase must have a closest-neighbor row and a substantive difference; absence of a convenience feature is insufficient.
- **Persona agreement:** The methods and novelty reviewers independently required a direct-neighbor argument; all reviewers scored related work as unfinished. No dissent supported a broad feature-combination claim.

### P1-04 — Select one recurring causal workload

- **Manuscript anchor:** lines 103–141, 191–194, 239–265, 395–416.
- **Observation:** The draft lists server/client, simulation/service, database/worker, training/checkpoint, and SUT/driver cases but explicitly leaves the choice open.
- **Interpretation and impact:** Without one scenario, reviewers cannot see why separate jobs or a direct script are inadequate, how readiness and placement interact, or why evidence continuity matters to the same performance study. The paper reads as a product capability map.
- **Concrete revision:** Prefer a performance system-under-test plus driver/load generator, or an equally ICPE-centered finite workload. Carry the same named services, allocation, step resources, readiness, placement, failure, inspection, and evidence from first page through RQs.
- **Regression/read-back check:** A two-minute reader can narrate one source-to-plan-to-allocation-to-evidence path without introducing a second workload or changing assumptions.
- **Persona agreement:** All seven personas scored this dimension 1 and independently identified the missing scenario. There was no dissent.

### P1-05 — Correct “exact submitted-script” and complete-traceability wording

- **Manuscript anchor:** lines 57, 69, 117, 124, 163, 201, 262, 286, and especially 291.
- **Current evidence:** docs/src/execution-model.md:98–130; src/render.rs:285–302 and 590–608; src/commands/spec.rs:640–675.
- **Observation:** Provenance spans deliberately cover selected directives and feature regions, not glue lines. Annotations are preview-only. Explain maps the portable preview; a submitted batch file differs because submission bakes absolute runtime paths into it.
- **Interpretation and impact:** “Exact submission-script preview” and unqualified field-to-script preservation are false for the current mechanism and create an easy artifact-review contradiction.
- **Concrete revision:** Call the current feature “selected source-to-preview attribution.” Distinguish portable preview, annotated preview, submitted artifact, mapped regions, and unmapped glue. Treat exact submitted-artifact attribution as PROPOSED_DESIGN if the authors still want it.
- **Regression/read-back check:** Audit every use of exact, submitted script, trace, map, and attribute; each must identify preview versus submission and selected versus complete coverage.
- **Persona agreement and dissent:** Compiler, novelty, and meta reviewers raised this directly; the practitioner raised adjacent sharing/inspection caveats. Some Wave 1 wording treated exactness as verified, but current implementation comments and documentation resolve the disagreement against that interpretation.

### P1-06 — Split the evidence story by trust level, reconstruction target, and namespace

- **Manuscript anchor:** lines 19, 44, 105, 117, 125, 166, 209–213, 225, 288, and 381–393.
- **Current evidence:** docs/src/run-evidence.md:33–68, 73–92, 110–165, and 167–234.
- **Observation:** Immutable RunManifest and InputsLock documents may record mutable image references, missing identity, or explicitly not-hashed input. Only RunView is deterministically reconstructed from the manifest and valid event prefix. Other state and collectors are mutable or best effort. Run ID is durable, but current storage remains job-ID-keyed and one metadata root cannot contain two distinct runs with the same scheduler ID.
- **Interpretation and impact:** “Immutable inputs,” “rebuild,” and one generic “link” can imply byte immutability, complete provenance, environment reconstruction, result reproduction, or cross-cluster aggregation that the implementation deliberately does not provide.
- **Concrete revision:** Add an evidence table with item, writer/locus, binding or digest, mutability, degradation/omission behavior, exact reconstruction role, export role, and namespace. Replace “immutable inputs” with “immutable records of available input identities and digests.” Name RunView whenever it is the rebuilt object.
- **Regression/read-back check:** A reader can distinguish hashed bytes, content-addressed identities, mutable references, frozen metadata, mutable observations, bundle-time snapshots, unavailable evidence, and the job-ID compatibility constraint.
- **Persona agreement and dissent:** Reproducibility, novelty, and meta reviewers raised this independently; the practitioner also required a decision on contribution-three scope. Some methods language treated continuity as broadly adequate; the narrower protocol evidence prevails.

### P1-07 — Make privacy, redaction, and export safety first-class

- **Manuscript anchor:** lines 209–213, 250, 255, and 290.
- **Current evidence:** docs/src/secrets.md:64–68; docs/src/run-evidence.md:236–253; docs/src/runtime-observability.md:165–205.
- **Observation:** Diagnostic redaction does not sanitize the rendered or submitted script or persisted job state; resolved secrets may be present. Bundles can copy effective configuration, scripts, paths, scheduler identifiers, notes, tags, source references, and selected payloads. They are neither automatically redacted nor necessarily complete.
- **Interpretation and impact:** “Inspectable,” “bundle,” or “artifact-ready” can be misread as “safe to share,” producing a credible credential, privacy, operational-metadata, or unpublished-result disclosure path.
- **Concrete revision:** Add a privacy/export subsection and a surface table for diagnostics, preview, submitted script, tracked state, logs/metrics, evidence, payloads, and bundle. State permissions, redaction, completeness, and required human review.
- **Regression/read-back check:** Every instruction to export, share, publish, or attach a bundle states its redaction and completeness boundary. Include secret-canary tests in the later artifact plan.
- **Persona agreement:** Reproducibility, practitioner, novelty, and meta reviewers raised this independently. No dissent supplied evidence that bundles are publish-safe.

### P1-08 — Freeze evaluation controls that isolate each claim

- **Manuscript anchor:** lines 219–237.
- **Current evidence:** docs/src/command-families.md:13–16 and 42–55; docs/src/agent-command-safety.md:90 and 132; current planner/render/evidence behavior.
- **Observation:** RQ1’s declarative oracle is not yet independent of the implementation. RQ2 does not isolate plan/render, cold/warm preparation, queue, step launch/readiness, supervisor, and steady-state costs or fully define semantic equivalence. RQ4 risks confounding site with backend. RQ6 calls assurance risk-ordered although filesystem probes and smokes can themselves submit jobs or consume quota, and fault applicability differs by stage.
- **Interpretation and impact:** Later data could be precise yet fail to establish semantic correctness, practical non-inferiority, portability, or earlier failure discrimination.
- **Concrete revision:** Define an implementation-independent semantic relation for RQ1; predeclare RQ2 estimands, paired design, equivalence checks, uncertainty, and practical margin; cross backend by site or narrow RQ4; create a fault × stage × effect × quota × proof matrix for RQ6.
- **Regression/read-back check:** Before collection, every RQ names its estimand or invariant, independent control, workload/site cells, analysis, practical threshold, and falsifier.
- **Persona agreement:** Methods, runtime, compiler, and meta reviewers independently raised these design gaps. Missing measurements themselves remain neutral.

### P2-01 — Update venue status and neutralize unsupported prevalence

- **Manuscript anchor:** lines 5, 103, and 260.
- **Observation:** The 2027 broad call exists; only detailed track pages remain TBA. “Increasingly” and “often” are not yet supported by a corpus or citations.
- **Fix:** Use the 2027 broad scope, label only detailed 2026 rules provisional, and replace prevalence language with “some” until supported.
- **Check:** Every venue claim has a current official URL or an explicit provisional qualifier; every frequency word has evidence.
- **Persona agreement:** Both Wave 2 reviewers independently corrected the venue status; the lead verified it. Earlier methods and reproducibility searches missed the new broad call.

### P2-02 — Teach allocation, step, host, readiness, and shared storage before using them

- **Manuscript anchor:** lines 103–119 and 379–393.
- **Current evidence:** docs/src/quickstart.md:19–35; docs/src/execution-model.md:5–20 and 42–78.
- **Observation:** Non-Slurm readers meet dense scheduler vocabulary before receiving a compact execution and locus model.
- **Fix:** Put a two-sentence execution primer and one mismatch/locus table immediately after the running scenario.
- **Check:** A software engineer unfamiliar with Slurm can explain allocation versus step and where prepare, submit, supervise, check readiness, execute, and collect occur without consulting the manual.
- **Persona agreement:** Practitioner and runtime reviewers raised this directly; several others requested the same semantic tables.

### P2-03 — Compress the planned paper and move artifact instructions out

- **Manuscript anchor:** lines 15–21, 61–83, 101–127, 239–292, and 395–406.
- **Observation:** Thesis, risks, guardrails, acceptance logic, and readiness gates repeat across fourteen proposed sections plus an artifact guide.
- **Fix:** Use the eight-section structure in section 8 below, consolidate caveats into authoritative tables, and move the reviewer artifact guide and CLI detail to companion material.
- **Check:** Every section has one unique argumentative job; removing meta-review scaffolding does not remove a technical premise.
- **Persona agreement:** Practitioner, reproducibility, novelty, and meta reviewers independently flagged compression or page-budget risk.

### P3-01 — Tighten the terminology contract

- **Manuscript anchor:** lines 381–393.
- **Observation:** “One spec becomes one allocation” can obscure expansion; control plane and evidence are overloaded; repeat, rerun, rebuild, reproduce, and replicate are not operationally separated.
- **Fix:** Use “one resolved application instance or expanded trial targets one allocation”; define generated supervisor, run/job/attempt namespace, RunView reconstruction, rerun, reproduction, and statistical replicate.
- **Check:** Search the final manuscript for one spec, control plane, immutable input, rebuild, reproduce, replicate, and evidence; each occurrence maps to the terminology table.
- **Persona agreement:** Compiler, reproducibility, and meta reviewers raised independent parts of this issue.

## 6. Disagreements and author decisions

| Issue | Competing interpretations | Evidence on both sides | Recommended author decision | Consequence |
| --- | --- | --- | --- | --- |
| Is “compiler” justified? | Compiler reviewer found real typed stages; novelty reviewer warned it can look ornamental or like YAML-to-shell. | Current parse/normalize/plan/runtime/render stages and strict validation support the label; missing frozen semantics and the resource edge weaken it. | Keep “compiler” only for the frozen typed paper subset, with explicit invariants, rejection, and RQ1 conformance. | The paper becomes a semantic systems paper, not an implementation tour. |
| Is there “no control plane”? | Methods/practitioner tolerated the shorthand; runtime and both Wave 2 reviewers rejected it as misleading. | There is no separately deployed long-running service, but the generated batch script supervises the live allocation. | Say “no separately deployed cluster daemon or nested scheduler” and name the generated batch supervisor. | Removes a reviewer-visible contradiction and creates an honest Flux/pilot comparison. |
| Is evidence contribution three? | Reproducibility review supported it conditionally; practitioner and novelty reviews would demote it if broad. | Current fault/identity mechanisms are substantial; RO-Crate, CWLProv, and ReproZip make general provenance novelty unsafe. | Retain only if RQ5 yields generalizable scheduler-specific insight; otherwise make it supporting assurance. | Protects the compiler red thread and reduces empirical burden. |
| Is script attribution exact? | Some early Wave 1 language treated exactness as verified; compiler/practitioner caveats and meta review disagreed. | Current docs and code explicitly say best-effort preview mapping and submitted-path differences. | Claim selected source-to-preview attribution. Treat exact submitted-script mapping as future design if desired. | Aligns contribution two with current implementation. |
| Regular Research Track or EERCS? | All reviewers found regular research defensible; several retained EERCS as fallback. | Semantic conformance and overhead can support original systems research; deployment-only evidence would fit EERCS better. | Target regular Research Track; switch only if final evidence becomes primarily experience/case study. | Keeps evaluation comparative and mechanism-centered. |
| How to interpret the 2027 venue basis? | Two Wave 1 searches said no call; both Wave 2 reviewers and the lead found the broad call. | The broad call is live; detailed Research and Artifact pages still say TBA. | Use 2027 scope now and 2026 only for explicitly provisional detailed rules. | Corrects manuscript line 5 without pretending track details are final. |
| How serious is technical fidelity? | Practitioner/reproducibility scores were more generous; compiler/runtime and both Wave 2 reviewers scored it 1. | Most architecture is current, but task geometry, overlap, supervisor locus, exactness, and evidence wording are central. | Preserve the lower technical score until the semantic contract and prose are aligned. | Prevents consensus scoring from averaging away a correctness risk. |

## 7. Top-ten revision queue

### Writing and design-contract changes

| Order | Revision | Owner type | Affected section | Expected outcome | Effort | Prerequisite |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | Decide partial-placement task inheritance and aggregate CPU/memory/GRES sharing; align code, tests, and docs | Runtime/compiler implementers plus Slurm expert | Scope, language, design | One feasible, testable resource contract | L | None |
| 2 | Freeze the paper-core typed subset, representations, invariants, rejection layers, and escape-hatch exclusions | Compiler/language-design lead | System model and semantic contract | Finite universe for thesis and RQ1 | L | 1 |
| 3 | Revise thesis, abstract, first figure, and state model to include the generated batch supervisor and all execution/storage loci | Runtime architect plus lead writer | First page, architecture, execution | Honest controller and readiness boundary | M | 1–2 |
| 4 | Select and instantiate one SUT-plus-driver or equally ICPE-centered workload | Lead author plus domain practitioner | Introduction through evaluation | One causal red thread | M | 1–3 |
| 5 | Replace exact/complete traceability claims with a preview/submission and mapped/unmapped taxonomy | Compiler/docs owner | Contribution two, inspectability, limitations | Current claim matches implementation | S | 2 |
| 6 | Add the evidence trust/namespace table and privacy/export surface table; decide contribution-three status | Provenance and security reviewers | Evidence, limitations, artifact | Bounded identity claim and safe sharing guidance | M | 4 |
| 7 | Write the closest-neighbor matrix and narrow the novelty sentence; name direct baselines | Related-work lead | Introduction, related work, evaluation | Defensible non-feature-count novelty | M | 2–4 |
| 8 | Compress to the proposed eight-section structure; update venue status, prevalence language, and terminology | Lead scientific writer | Whole manuscript | Page-budget-coherent full-draft scaffold | M | 3–7 |

### Future experiments and artifact work

| Order | FUTURE_EVIDENCE task | Owner type | Affected section | Expected outcome | Effort | Prerequisite |
| ---: | --- | --- | --- | --- | --- | --- |
| 9 | Freeze the evaluation protocol: independent semantic oracle, baseline equivalence, estimands, margins, workload/site cells, fault applicability, and artifact tiers | Performance-methods lead plus runtime evaluator | Evaluation and artifact plan | Pre-registered falsifiable study design | L | 1–8 |
| 10 | Pin a release/commit, execute RQ1–RQ6 and optional RQ7 as evidence permits, archive scripts/data, and run secret-canary/export checks | Evaluation and artifact team | Results and companion artifact | Reportable evidence with honest site and privacy limits | L | 9 plus site/backend access |

Items 9 and 10 are neutral at this stage. They become readiness conditions for experimental execution, not retroactive defects in the pre-experimental manuscript.

## 8. Suggested structural revision

Use roughly eight core sections rather than expanding the current fourteen-section plan directly.

1. **Introduction and recurring workload.** Establish one stakeholder, one expensive failure pattern, the finite SUT-plus-driver application, thesis, and ordered contributions.
2. **Slurm mismatch and closest alternatives.** Teach allocation versus step, host/storage loci, and why scripts, direct translators, workflows, and pilots solve related but different problems.
3. **Paper-core language and semantic contract.** Define the typed subset, normalization, resources, placement, readiness, state transitions, sharing, rejection, and escape hatches in one authoritative table.
4. **Compiler, generated supervisor, and inspectability.** Walk source to representations to submitted artifact; show the allocation-resident supervisor and selected preview attribution.
5. **Bounded run identity, evidence, and privacy.** Present trust levels, reconstruction target, namespace, degradation, redaction, and export limits; keep this section short if evidence is supporting rather than a contribution.
6. **Evaluation methodology.** Map each retained contribution to independent controls, workloads/sites, invariants or estimands, analysis, threats, and falsifiers.
7. **Related work and novelty boundary.** Compare direct translators first, then allocation-internal runtimes, workflows/services, runtime substrates, and provenance standards.
8. **Limitations and conclusion.** State unsupported topology, controller and sharing boundaries, site/backend limits, evidence/privacy limits, and the narrow supported claim.

Move command inventories, setup recipes, the reviewer artifact guide, full provenance schemas, and extended feature matrices to the companion artifact or appendix. Merge progressive assurance into sections 4 and 6 rather than giving it a parallel paper spine.

## 9. Candidate rewrites

### Thesis

> For finite, allocation-scoped multi-service workloads, hpc-compose compiles a constrained Compose-style specification into one inspectable Slurm job whose generated batch supervisor coordinates native job steps, without deploying a separate cluster daemon or nested scheduler.

### Novelty boundary

> Our contribution is not declarative syntax, container launch, workflow orchestration, or provenance alone; it is an explicit and testable mapping from a bounded concurrent-service model to one Slurm allocation and its steps, including resource, readiness, failure, rejection, and inspectability rules.

### Resource-contract limitation

> The semantic guarantee covers the frozen typed subset and its published allocation/step invariants; user-authored commands, hooks, setup fragments, raw submission arguments, raw step arguments, and site-specific scheduler policy remain explicit escape hatches or external validity conditions.

### Inspectability boundary

> Annotated rendering maps selected source fields to a resolved preview rather than every submitted-script line; the submitted artifact can differ where absolute runtime paths are materialized.

### Evidence boundary

> New tracked runs preserve immutable records of available input identities and digests, bind selected submit-time metadata to a run ID and site-local job ID, and can reconstruct the derived RunView; mutable, un-hashed, missing, and best-effort evidence remains explicitly qualified.

### Privacy boundary

> Diagnostic redaction does not make submitted scripts, tracked state, logs, payloads, or exported bundles safe to publish; these surfaces can contain resolved secrets or sensitive metadata and require explicit review before sharing.

## 10. Related-work gaps and verified additions

The manuscript’s seed map is useful but not yet a related-work argument. The closest-neighbor lane must be written before broad novelty language. The full 25-source verified inventory, including runtime background and category-level workflow references, is in docs/plans/icpe-2027-review-reference-candidates.md.

### High-value verified additions and comparison roles

| Source and primary URL | Overlap | Difference | Novelty threat | Recommended placement |
| --- | --- | --- | --- | --- |
| Vanessa Sochat. 2019. “Singularity Compose: Orchestration for Singularity Instances.” JOSS 4(40), 1578. <https://doi.org/10.21105/joss.01578> | Multi-service Singularity configuration and lifecycle | Local instance/network orchestration, not one Slurm allocation and explicit step resources | Compose-style multi-container orchestration is not new | First direct-neighbor paragraph |
| DockSing 0.2.36. 2025. <https://pypi.org/project/docksing/> | Limited Compose-style input to Singularity/Slurm, SSH staging, command preview | Documented around one container/job; no established concurrent-service/evidence contract | Very high against “first Compose-to-Slurm” | Direct executable comparator where compatible |
| Iacopo Colonnelli et al. 2021. “StreamFlow: Cross-Breeding Cloud with HPC.” IEEE TETC 9(4):1723–1737. <https://doi.org/10.1109/TETC.2020.3019202> | Declarative multi-container execution and communicating tasks on HPC/cloud | CWL controller and multi-site data movement rather than one static Slurm artifact | High against dismissing workflows as non-concurrent | Direct-adjacent matrix |
| Alberto Madonna et al. 2026. “Sarus Suite: Cloud-native Containers for HPC.” arXiv:2604.17064. <https://arxiv.org/abs/2604.17064> | Declarative multi-container descriptions and scheduler-native HPC | Runtime/integration suite; different demonstrated multi-container path | High against broad declarative + multi-container + scheduler-native novelty | Direct-adjacent runtime; mark as preprint |
| Antonio Paolillo, Mats Van Molle, and Ken Hasselmann. 2026. “benchkit.” ICPE ’26, 170–183. <https://doi.org/10.1145/3777884.3796997> | Declarative composable performance studies, shell replacement, reproducibility, overhead | Benchmark campaigns rather than readiness-coupled services in one allocation | High to generic declarative performance experimentation | Same-venue framework and quality bar |
| Hernan Picatto et al. 2026. “dagster-slurm.” JOSS 11(119), 9795. <https://doi.org/10.21105/joss.09795> | Slurm integration and reproducible research workflow | Dagster job/workflow orchestration rather than demonstrated bounded service lowering | Moderate to broad reproducible-HPC language | Slurm workflow systems |
| Lawrence Livermore National Laboratory. “Maestro Workflow Conductor.” <https://maestrowf.readthedocs.io/en/latest/Maestro/index.html> | YAML studies, parameter expansion, script generation, Slurm adapters, monitoring | Multi-step study/DAG abstraction | YAML, generation, sweeps, and monitoring are not new | Workflow-engine comparison; documentation citation |
| Dong H. Ahn et al. 2020. “Flux: Overcoming Scheduling Challenges for Exascale Workflows.” FGCS 110:202–213. <https://doi.org/10.1016/j.future.2020.04.006> | Allocation-internal co-scheduling, resource management, coordination | Hierarchical nested scheduler and dynamic runtime | High to generic allocation-boundary orchestration | Primary no-nested-scheduler contrast |
| Jakub Beránek et al. 2024. “HyperQueue.” SoftwareX 27, 101814. <https://doi.org/10.1016/j.softx.2024.101814> | Work aggregation, allocation-internal resource management, task graphs | Dynamic runtime rather than finite service compilation | High to allocation-internal task management | Pilot/runtime subsection |
| Bartosz Bosak et al. 2021. “QCG-PilotJob.” ICCS 2021, 495–501. <https://doi.org/10.1007/978-3-030-77977-1_39> | Work management inside acquired HPC resources | Dynamic pilot-job resource manager | Moderate to allocation coordination | Pilot systems |
| Andre Merzky et al. 2022. “Design and Performance Characterization of RADICAL-Pilot.” IEEE TPDS 33(4):818–829. <https://doi.org/10.1109/TPDS.2021.3105994> | Pilot abstraction, late binding, heterogeneous tasks, utilization | Persistent dynamic runtime at broader scale | Canonical missing pilot comparison | Alongside Flux and QCG |
| Sam Partee et al. 2022. “Using Machine Learning at Scale in Numerical Simulations with SmartSim.” JCS 62, 101707. <https://doi.org/10.1016/j.jocs.2022.101707> | Simulation plus data/ML service co-execution | Specialized framework rather than general compiler | High to workload novelty | Motivating scenario and domain systems |
| Ali Doosthosseini et al. 2026. “SAIA.” Journal of Supercomputing 82, 403. <https://doi.org/10.1007/s11227-026-08508-3> | Slurm-native service lifecycle, discovery, and security | Persistent externally accessible services, proxy, autoscaling, renewed pools | High to broad Slurm-native services wording | Service-oriented adjacent work/non-goals |
| Simone Leo et al. 2024. “Recording Provenance of Workflow Runs with RO-Crate.” PLOS ONE 19(9), e0309210. <https://doi.org/10.1371/journal.pone.0309210> | Prospective/retrospective run provenance and interoperable records | Cross-system standard rather than local scheduler identity/fault mechanics | Very high to general spec-to-run-evidence novelty | Closest provenance comparison |
| Farah Zaib Khan et al. 2019. “Sharing Interoperable Workflow Provenance … CWLProv.” GigaScience 8(11), giz095. <https://doi.org/10.1093/gigascience/giz095> | Workflow provenance, input/output identities, execution records, packaging | General workflow standard rather than local Slurm durability/degradation | High to provenance terminology/scope | Provenance standards |
| Andy B. Yoo, Morris A. Jette, and Mark Grondona. 2003. “SLURM.” JSSPP, LNCS 2862:44–60. <https://doi.org/10.1007/10968987_3> | Allocation and job-step substrate | Scheduler foundation | No novelty threat; necessary semantic basis | Background and system model |

### Resulting safe and unsafe novelty language

Unsafe:

- first declarative multi-container orchestration for HPC;
- first Compose or YAML to Slurm translator;
- first orchestration, co-scheduling, or task management inside one allocation;
- first reproducible HPC workflow or specification-to-run provenance system;
- novelty based on visible scripts, parameter studies, monitoring, or container portability.

Potentially defensible after P1-01 and the comparison matrix:

- an explicit typed mapping from a finite readiness-coupled service model to one Slurm allocation and native steps;
- a published allocation/step resource, readiness, failure, sharing, rejection, and controller-locus contract;
- selected inspectable lowering without a separately deployed daemon or nested scheduler;
- scheduler-specific evidence fault behavior only if RQ5 establishes a generalizable insight beyond prior provenance standards.

### Unverified or incompletely verified leads

- Balsam’s current documentation verifies a dynamic HPC workflow role, but its cited workshop paper was not opened from primary proceedings in this review. Do not add that paper yet.
- A peer-reviewed Maestro paper was not established; use authoritative documentation until one is verified.
- DockSing’s software behavior and release are verified; PyPI marks the personal author field unverified, so the candidate file omits it.
- PSI/J is a plausible portable job-interface reference, but complete proceedings metadata and the author list require another primary-source check before addition.

No unverified lead was added to the verified candidate set.

## 11. Future-evidence ledger

All rows are neutral for present draft readiness.

| RQ or artifact task | Claim under test | Baseline or control | Metric or invariant | Workload and site | Analysis method | Falsifier |
| --- | --- | --- | --- | --- | --- | --- |
| RQ1 semantic conformance | The frozen typed subset lowers according to the independent semantics and rejects unsupported intent at the stated layer | Independent semantic contract, expert native scripts, positive/negative corpus | Allocation/step fields, task/resource/placement mapping, readiness/failure trace, rejection class/location | Recurring workload plus combinatorial core-language corpus; controlled and real Slurm where applicable | Predicate-by-predicate differential and metamorphic conformance | Any accepted core case violates the contract, or unsupported typed intent survives silently |
| RQ1 inspectability/determinism sub-study | Equal enumerated effective inputs and context yield stable previews and the declared core mapping coverage | Reordered equivalent input, repeated rendering, controlled context perturbations | Normalized-plan equality, byte identity under declared domain, mapped-core-field ratio, explicit unmapped classification | Static corpus across supported backends | Metamorphic tests and coverage accounting | Unexplained drift or an unmapped semantically material core field |
| RQ2 cost of abstraction | The abstraction is practically non-inferior to semantically equivalent native Slurm | Expert-authored native script with checked resource, readiness, and sharing equivalence | Plan/render time, cold/warm prepare, queue separately, step launch/readiness, supervisor CPU/memory, cleanup, steady-state runtime, script size | Short/long, single-/multi-node instances at declared sites | Paired estimates with uncertainty and predeclared practical margins | Upper uncertainty bound exceeds the relevant margin or baseline equivalence fails |
| RQ3 expressiveness/boundary | The language covers a useful bounded class and rejects out-of-scope intent clearly | Public Compose pattern corpus, independently authored HPC cases, direct tools | Coverage, adaptations, unsupported taxonomy, diagnostic actionability | Public coded corpus plus recurring workload | Reproducible coding protocol with disagreement handling | Representative target patterns need undocumented semantics or exclusions are misleading |
| RQ4 portability | Common intent survives backend and site changes within stated bounds | Crossed backend × site cells where feasible; otherwise a narrowed claim | Spec/context deltas, launch success, semantic deviations, preparation/launch timing, failure | Pyxis/Enroot, Apptainer/Singularity, and host on at least two materially distinct sites if available | Cell-wise effects, failures, prerequisites, and interaction analysis | Application intent needs unmodeled changes or meaning diverges beyond stated limits |
| RQ5 evidence/recovery | Run identity and bounded evidence remain interpretable under documented faults | Fault injection, legacy records, scheduler-ID reuse, missing payloads | Identity binding, hash consistency, lock behavior, valid-prefix preservation, exact RunView reconstruction, degradation accuracy | Local metadata plus relevant coherent shared filesystems | Fault matrix separating recoverable and non-recoverable outcomes | A committed event is lost, stale identity is rebound, or unavailable/corrupt evidence is presented as complete |
| RQ6 progressive assurance | Each stage discriminates its applicable fault class before a larger commitment than the control | Direct submission workflow and injected authoring/environment/scheduler/runtime/storage faults | Stage applicability, false positives/negatives, mutation, quota/time consumed, diagnosis action | Recurring workload on controlled Slurm, then declared sites | Fault × stage × effect × quota × proof matrix | A stage claims proof it cannot supply or adds quota cost without useful discrimination |
| Optional RQ7 service attribution | Attribution is accurate when evidence supports it and explicitly unknown otherwise | Instrumented ground truth | Error, unknown rate, collector coverage, overhead | Backend/site matrix with CPU/GPU evidence where accessible | Error intervals stratified by evidence status | Partial evidence is reported as complete or error exceeds the declared tolerance |
| Artifact exercisability | Each retained claim can be checked at an appropriate tier | Frozen release/commit, checksums, native baselines | Completion, expected outputs, integrity, omissions, time/cost | Offline/static; fake-tool or controlled Slurm; external site/backend tiers | Claim-to-command/data map | A reviewer cannot execute a claimed tier or relate a result to pinned inputs/code/output |
| Privacy/export | Diagnostic and bundle boundaries prevent accidental publication claims | Secret canaries and an explicit export allowlist/review procedure | Canary leakage, file permissions, included/omitted surfaces, warning coverage | Offline bundle fixtures plus final artifact package | Deterministic inspection of every export surface | A secret canary is exported without an explicit unsafe surface declaration or review gate |

Do not promote RQ7, user-productivity claims, saved-allocation-time claims, or evidence as an independent contribution unless their dedicated evidence is collected. Negative or null findings remain publishable evidence if the design and boundaries are reported honestly.

## 12. Final readiness gate

### Gate result

- **Core-dimension gate:** pass. No core dimension is 0.
- **Present-claim verification gate:** conditional. Current mechanisms were checked, but the manuscript must correct or explicitly mark the resource contract, generated supervisor, preview mapping, evidence wording, and venue status.
- **No-fabricated-result gate:** pass. The manuscript explicitly contains no results and does not report measurements as completed.
- **P0/P1 actionability gate:** pass as a review output. There are no P0s, and every P1 has a concrete resolution, owner path, and regression/read-back check.
- **Full-prose expansion gate:** not yet pass; READY_WITH_TARGETED_REVISIONS.

### Exact remaining conditions before prose expansion

1. Resolve partial-placement task geometry and concurrent resource-sharing semantics, then align current code, tests, and documentation.
2. Freeze the paper-core typed subset, stage representations, invariants, rejection layers, and escape-hatch exclusions.
3. Replace the thesis and architecture figure with the generated-supervisor, readiness-observer, and storage-locus model.
4. Select one recurring workload and use it through semantics, failure, inspectability, evidence, and evaluation.
5. Correct exact-preview and complete-traceability wording to the current selected source-to-preview boundary.
6. Add evidence trust/namespace and privacy/export tables; decide whether evidence remains contribution three.
7. Complete the verified closest-neighbor matrix and state the narrow unmatched semantic invariant.
8. Compress the section plan, update the venue paragraph, neutralize unsupported prevalence, and adopt the terminology contract.

### Exact remaining conditions before experimental execution

1. Pin the evaluation release or commit and archive the exact source, core-language table, native reference scripts, comparator versions, workloads, and analysis code.
2. Freeze an implementation-independent RQ1 oracle and resolve every core semantic inconsistency before treating implementation output as evidence.
3. Predeclare RQ2 semantic equivalence, phase-specific estimands, repetitions, uncertainty method, and practical non-inferiority margins.
4. Name executable direct baselines, including DockSing where compatible, and state why semantic-only comparisons are used for incompatible neighbors.
5. Obtain the declared backend/site cells or narrow RQ4 before collection; distinguish fake-tool, controlled real-Slurm, and production-site evidence.
6. Freeze the RQ5 fault matrix, RQ6 fault-stage applicability matrix, and optional RQ7 accuracy thresholds.
7. Define artifact tiers, expected outputs, immutable pins, payload omissions, privacy review, and secret-canary tests.
8. Keep all unmeasured outcomes in future tense until data and analysis exist.

### Final lead judgment

The smallest coherent paper is viable: a bounded concurrent-service language compiled into one inspectable Slurm allocation, with an explicit generated supervisor and no separately deployed daemon or nested scheduler. The draft should not be reframed away from that core. It should be narrowed and corrected before expansion, and evidence continuity should remain conditional on a genuinely generalizable RQ5 result.

Post-write verification: the manuscript retained SHA-256 f1b7dad6cdcf39b6a333b33903705c04e6fc6d6178966d89cf885073b899a904; no tracked file changed; the review created only this report and docs/plans/icpe-2027-review-reference-candidates.md; and both outputs passed the repository Markdown linter and typo checker.

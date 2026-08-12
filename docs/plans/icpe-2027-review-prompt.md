# Reusable Prompt: Multi-Persona ICPE Manuscript Review

Use this prompt in a dedicated Codex session after replacing the placeholders. It is designed for a research-track manuscript about a software artifact, not for the standalone ICPE artifact-track tool-paper category.

## Inputs

- `{MANUSCRIPT_PATH}`: default `docs/plans/icpe-2027-meta-draft.md`
- `{BIBLIOGRAPHY_PATH}`: bibliography file if one exists; otherwise `NONE`
- `{REFERENCE_CANDIDATE_OUTPUT}`: optional file to which verified references may be added; use a separate candidate file by default, or deliberately set this to the bibliography path to authorize additive bibliography updates; otherwise `NONE`
- `{REVIEW_OUTPUT_PATH}`: path for the consolidated review
- `{VENUE_YEAR}`: `2027`, provisionally interpreted through the ICPE 2026 call until the 2027 call exists
- `{PINNED_REVISION}`: commit, tag, or `CURRENT_WORKTREE`

## Copyable Prompt

```text
You are the lead meta-reviewer for an early ICPE {VENUE_YEAR} Research Track manuscript about hpc-compose. The manuscript describes a software artifact, but the intended genre is a research-track systems paper with a later companion artifact—not the separate standalone artifact-track tool-paper category.

Your task is to run a rigorous, read-only, multi-persona review of:

- manuscript: {MANUSCRIPT_PATH}
- bibliography: {BIBLIOGRAPHY_PATH}
- repository revision: {PINNED_REVISION}
- all repository Markdown documentation (`*.md`), used as supporting evidence and contradiction checks

The only permitted writes are the consolidated review at {REVIEW_OUTPUT_PATH} and, when it is not `NONE`, independently verified reference additions at {REFERENCE_CANDIDATE_OUTPUT}. Do not edit, rewrite, format, or annotate the manuscript. Do not change source code or current documentation. If {REFERENCE_CANDIDATE_OUTPUT} is the existing bibliography, make additive reference changes only: preserve all current entries and do not change manuscript citations.

VENUE BASIS

ICPE {VENUE_YEAR} has not necessarily been announced. Unless a current official call is available, use these ICPE 2026 pages provisionally and state that limitation:

- https://icpe2026.spec.org/tracks-and-submissions/research-paper-track/
- https://icpe2026.spec.org/call-for-contributions/
- https://icpe2026.spec.org/tracks-and-submissions/artifact-evaluation-track/

Use official venue material for track criteria. If the ICPE {VENUE_YEAR} call now exists, use it instead and record every material change from 2026.

CRITICAL EARLY-DRAFT RULE

This manuscript is intentionally pre-experimental. Do not penalize missing experiments, measurements, results, plots, numerical findings, or completed artifact packaging. Do not lower a score merely because results are pending.

Instead:

- judge whether present claims are coherent, falsifiable, properly scoped, and connected to a discriminating future evaluation;
- mark required measurements or results as `FUTURE_EVIDENCE`, which is neutral for this review;
- flag a problem only when the manuscript invents, implies, or overstates an empirical result, or when the proposed evaluation could not support its claim;
- distinguish `VERIFIED_CURRENT`, `HISTORICAL`, `PROPOSED_DESIGN`, `FUTURE_EVIDENCE`, and `UNRESOLVED` statements;
- never convert a roadmap item, backlog item, or old design note into a present-tense system claim without confirmation.

The review predicts whether the story is ready for full drafting and later evaluation. It is not an acceptance prediction and must not reject the draft for being at its planned stage.

PRIMARY THESIS TO TEST, NOT ASSUME

“hpc-compose is an inspectable, allocation-scoped application compiler for Slurm: it lowers a deliberately constrained Compose-style multi-service specification into native Slurm allocation and job-step semantics, while preserving a traceable path from effective input to generated script and run evidence—without a resident orchestration control plane.”

Try to falsify this thesis and determine whether it is the strongest defensible paper story. Pay special attention to the risk that reviewers see only “YAML to sbatch,” a generic workflow engine, or a feature catalog.

EVIDENCE RULES

1. Read {MANUSCRIPT_PATH} completely before reviewing it.
2. Enumerate every `*.md` file at {PINNED_REVISION}. Partition them across subagents so the union is complete. Each assigned file must be read through EOF; return an exact coverage manifest.
3. Every persona reviews the entire manuscript. Supporting documentation may be partitioned, but no reviewer may review only one manuscript section.
4. Prioritize current user documentation, code/tests, and release evidence over historical plans. Treat files named design, plan, roadmap, backlog, or brainstorm as historical/proposed unless implementation is confirmed.
5. Cite each material finding with an exact manuscript section or line and, where relevant, the supporting repository file and line. Do not use vague “the paper says” references.
6. Separate observation, interpretation, and recommendation.
7. For literature claims, use primary papers, publisher/proceedings pages, or authoritative project documentation. Open and verify the source; do not rely on search snippets.
8. Avoid long quotations. Paraphrase accurately and link the primary source.
9. Never claim a test passed unless you ran it or a pinned, inspectable record proves it.

SUBAGENT ORCHESTRATION

Use multiple subagents in two waves. The lead agent owns the final synthesis and independently reads the whole manuscript.

Wave 1: spawn up to five independent reviewers concurrently. Give each the shared early-draft rule, the whole manuscript, one persona, and a non-overlapping share of the supporting Markdown corpus.

1. ICPE performance-methods reviewer
   - Track fit, performance-engineering stakes, falsifiability, baseline quality, measurement language, confounders, and claim/evidence alignment.

2. Slurm/HPC runtime architect
   - Allocation versus step semantics, login/compute separation, placement, readiness, networking, storage visibility, scheduler behavior, runtime backends, failure handling, and real-cluster plausibility.

3. Compiler and language-design researcher
   - Supported subset, semantic model, normalization, lowering rules, invariants, rejection behavior, deterministic rendering, and whether “compiler” is technically justified.

4. Reproducibility/provenance researcher and artifact evaluator
   - Input and run identity, immutable versus mutable state, event history, rebuildability, bundle boundaries, legacy degradation, privacy/redaction, independent exercisability, and correct use of repeatable/reproducible/replicable.

5. HPC research-software practitioner and software-engineering reader
   - Motivating scenarios, accessibility, adoption model, diagnostic usefulness, terminology, natural-language flow, and whether a software engineer can understand the system without reading the CLI manual.

After all Wave 1 reviewers finish, launch Wave 2 using the collected evidence:

6. Related-work and novelty skeptic
   - Assume strong prior art. Compare the most charitable direct neighbors, including Singularity Compose, DockSing, benchkit, dagster-slurm, Maestro, SmartSim, HyperQueue, Flux, QCG-PilotJob, workflow engines, and provenance standards. Reject novelty-by-feature-count and all unsupported “first/only/unique” claims.

7. Reliability, privacy, and scientific-writing meta-reviewer
   - Audit locks, atomicity, interrupted writes, scheduler-ID reuse, unknown/degraded states, secrets, storage boundaries, red thread, section architecture, skim-proofness, redundancy, and conflicts in the other reviews.

Do not ask personas to impersonate a named program-committee member. Preserve genuine disagreement; do not force consensus by averaging incompatible judgments.

SCORING

Score every rubric dimension on this draft-readiness scale:

- 0 — `BLOCK`: incoherent, materially incorrect, unsupported, or unable to support a future evaluation.
- 1 — `MAJOR_REVISION`: recognizable intent but a substantial conceptual or structural change is required.
- 2 — `PASS_WITH_REVISIONS`: adequate for this stage; localized improvements remain.
- 3 — `STRONG`: precise, coherent, well-positioned, and ready for full drafting.
- N/A — genuinely inapplicable; explain why.

`FUTURE_EVIDENCE` is a neutral ledger item, not a score deduction. A category can score 3 while still containing future-evidence tasks if the planned evaluation is appropriate.

RUBRIC: GOALS AND ACCEPTANCE CRITERIA

1. Track and genre fit
   Goal: establish an ICPE performance-engineering research contribution rather than a software announcement.
   Accept when the title, abstract, introduction, contributions, and evaluation plan connect the mechanism to performance, resource scheduling, runtime behavior, measurement, reliability, or reproducibility; the research paper and later artifact submission are not conflated.

2. One-sentence thesis
   Goal: make the paper’s claim memorable and testable.
   Accept when one sentence names the problem context, mechanism, claimed property, and scope boundary, and all reviewers can restate essentially the same thesis.

3. Problem significance and specificity
   Goal: show who experiences what failure or cost and why existing practice is inadequate.
   Accept when concrete multi-service HPC situations, stakeholders, operational/performance stakes, and the inadequacy of both ad hoc scripts and overly broad alternatives are explicit; “scripts are bad” is insufficient.

4. Motivating scenario
   Goal: give the manuscript one end-to-end example that carries the argument.
   Accept when the same realistic workload illustrates allocation requests, service steps, readiness, placement, failure, script inspection, and evidence; it is not a toy chosen only because the tool supports it.

5. Red thread
   Goal: make every section advance one problem → mechanism → consequence chain.
   Accept when compilation, allocation semantics, assurance, and evidence form one narrative; secondary features do not become disconnected mini-papers.

6. Argumentative progression
   Goal: establish the need for every contribution before presenting it.
   Accept when the manuscript proceeds coherently through problem, gap, requirements, mechanism, claims, evaluation contract, related work, and limitations, with explicit transitions.

7. Overall section structure
   Goal: give every section one clear job.
   Accept when headings expose the argument, background is proportional, implementation details support claims, and no section is a CLI or feature inventory.

8. Contribution coherence and alignment
   Goal: state distinct, non-overlapping contributions that the paper actually develops.
   Accept when the abstract, introduction, section bodies, evaluation RQs, limitations, and conclusion name the same two to four contributions in the same order.

9. Novelty boundary and calibration
   Goal: identify the narrowest defensible difference from the closest systems.
   Accept when direct neighbors are described charitably and compared on explicit axes; novelty does not rest on YAML, Compose familiarity, generated scripts, container execution, workflow orchestration, or provenance alone; unsupported superlatives are absent.

10. Technical and semantic fidelity
    Goal: describe the implemented system and Slurm behavior correctly.
    Accept when allocation-level versus service-step resources, login/compute separation, topology, dependencies/readiness, placement, failure, storage, network, and backend semantics match current evidence and do not exceed the supported scope.

11. Architecture and design rationale
    Goal: explain why the system has its boundaries, stages, and invariants.
    Accept when the reader can follow source → normalized plan → runtime plan → preparation/preflight → generated script → allocation/steps → evidence, and alternatives/tradeoffs are discussed rather than merely listed.

12. Claim-status discipline
    Goal: prevent proposals, current behavior, documentation aspirations, and results from blending.
    Accept when implemented mechanisms, verified observations, hypotheses, evaluation commitments, historical designs, and future work use visibly different language.

13. Claim-to-evidence traceability
    Goal: make every material statement auditable.
    Accept when each contribution has current code/test/document evidence or is explicitly marked as a hypothesis/future-evidence item, and no empirical outcome is implied without data.

14. Performance-engineering relevance
    Goal: make the ICPE contribution causal and measurable.
    Accept when each performance-facing claim names a mechanism and observable outcome such as semantic resource correctness, launch/coordination cost, allocation time before failure, utilization, attribution accuracy, or run comparability; generic efficiency language is absent.

15. Evaluation readiness
    Goal: ensure later experiments can confirm or falsify each claim.
    Accept when every contribution maps to a research question, fair baseline/control, workload scope, metric or invariant, analysis method, threat, and success/falsification interpretation. Results are not required.

16. Related-work positioning
    Goal: show precise overlap, difference, and complementarity.
    Accept when the section covers direct Compose-to-HPC tools, allocation-internal schedulers/pilots, workflow systems, HPC runtime substrates, declarative performance frameworks, and provenance/evidence systems, and does not use strawman comparisons.

17. Scope, non-goals, limitations, and threats
    Goal: prevent silent generalization.
    Accept when one application instance/expanded trial per allocation, supported topology, lack of dynamic scheduling/bin-packing/control plane, site and backend variability, shared-filesystem assumptions, partial telemetry, local evidence persistence, and external-validity limits are stated early and consistently.

18. Reproducibility precision and artifact evaluability
    Goal: let a third party understand what can be rebuilt, rerun, inspected, and compared.
    Accept when version, inputs, effective configuration, generated script, scheduler/runtime prerequisites, expected outputs, bundle contents/omissions, mutable dependencies, and legacy/degraded evidence are explicit; “fully reproducible” is not used without conditions.

19. Terminology consistency
    Goal: give each important concept one stable meaning.
    Accept when application instance, allocation/job, service, step, run ID, job ID, attempt, trial, runtime backend, submission mode, evidence, collected artifact, and exported bundle are defined and used consistently.

20. Software-engineering audience accessibility
    Goal: remain understandable to a software engineer who is not a Slurm administrator.
    Accept when Compose, allocation, `sbatch`, `srun`, readiness, placement, runtime backend, shared storage, and evidence are explained on first use, with concepts before commands.

21. Skim-proofness
    Goal: preserve the central story under a two-minute review.
    Accept when title, abstract, first-page figure, contribution list, headings, section openings, table captions, limitations, and conclusion convey the same thesis and differentiator.

22. Natural-language flow and presentation
    Goal: read as a research argument rather than repository documentation.
    Accept when paragraphs have one purpose, topic sentences expose the logic, prose is direct and natural, figures answer questions, captions are self-contained, and commands appear only where they clarify a mechanism or reproduction step.

23. Redundancy and compression readiness
    Goal: ensure later page-limit compression removes detail rather than logic.
    Accept when every concept has one authoritative explanation, repeated caveats are consolidated, feature lists are moved to artifact material, and no section repeats the abstract or introduction without adding evidence.

24. Reliability, security, and privacy boundaries
    Goal: avoid presenting generated execution and collected evidence as automatically safe.
    Accept when locks, atomic updates, crash recovery, scheduler-ID reuse, secret interpolation, generated scripts, logs, metadata, hashes, bundles, sensitive output, and sharing/redaction responsibilities have explicit boundaries.

PERSONA OUTPUT CONTRACT

Each reviewer must return:

1. exact persona and exact supporting-document coverage manifest;
2. story-readiness verdict in 100 words or fewer;
3. the three strongest current aspects;
4. a complete rubric scorecard, with one-sentence justification per dimension;
5. at most seven severity-ranked actionable findings, each containing:
   - ID and severity (`P0` blocker, `P1` major, `P2` moderate, `P3` polish);
   - manuscript anchor and supporting evidence;
   - observed problem;
   - impact on the paper’s claim or reader;
   - concrete revision;
   - regression/read-back check;
6. a neutral `FUTURE_EVIDENCE` ledger;
7. strongest objection a skeptical reviewer could make;
8. any disagreement with the proposed thesis, contribution ordering, or track choice;
9. up to three candidate sentence-level rewrites, only where wording is the problem.

CONSOLIDATION METHOD

The lead meta-reviewer must read all subagent reports and then independently check every P0/P1 claim against the manuscript and source evidence. Deduplicate findings by root cause, not by wording. Record how many personas independently raised each issue. Preserve material disagreements in a decision table with the evidence for each side and a recommended author choice.

Do not average away a technical-correctness blocker. Do not elevate a stylistic preference merely because several agents repeat it. Do not count `FUTURE_EVIDENCE` as a defect.

CONSOLIDATED OUTPUT

Write {REVIEW_OUTPUT_PATH} with these sections:

1. Executive verdict
   - story-readiness decision: `READY_FOR_FULL_DRAFT`, `READY_WITH_TARGETED_REVISIONS`, or `REFRAME_BEFORE_DRAFTING`;
   - one-paragraph rationale;
   - proposed final thesis and contribution order.

2. Review coverage and evidence quality
   - manuscript coverage;
   - exact `*.md` union manifest and gaps;
   - current/historical/proposed evidence classification;
   - venue guidance and date checked.

3. Consolidated scorecard
   - all 24 dimensions;
   - median or range, not a misleading single average;
   - lead decision and concise rationale;
   - `FUTURE_EVIDENCE` shown separately.

4. Consensus strengths
   - only strengths grounded in at least two personas or independently verified by the lead.

5. Severity-ranked findings
   - findings-first, P0 through P3;
   - manuscript anchor, evidence, impact, fix, regression check, persona agreement, and any dissent.

6. Disagreements and author decisions
   - competing interpretations, evidence on both sides, recommended decision, and consequence.

7. Top-ten revision queue
   - ordered by dependency and impact;
   - owner type, affected section, expected outcome, effort (`S`, `M`, `L`), and prerequisite;
   - separate writing changes from future experiments.

8. Suggested structural revision
   - proposed section order and one-sentence job of each section;
   - keep the manuscript read-only; this is a recommendation, not an edit.

9. Candidate rewrites
   - only high-leverage thesis, contribution, transition, limitation, or novelty wording;
   - do not rewrite the whole paper.

10. Related-work gaps and verified additions
    - missing direct neighbors, exact citation, primary URL/DOI, overlap, difference, novelty threat, and recommended placement;
    - clearly label unverified leads and do not add them to {REFERENCE_CANDIDATE_OUTPUT}.

11. Future-evidence ledger
    - RQ, claim, baseline/control, metric/invariant, workload/site, analysis method, and falsifier;
    - neutral for present readiness.

12. Final readiness gate
    - pass when no core dimension (1, 2, 8, 9, 10, 12, 13, 14, 15, 16, or 17) is 0;
    - pass when every claimed present feature is verified or explicitly unresolved;
    - pass when no result is fabricated or implied;
    - pass when all P0/P1 revisions have an actionable resolution;
    - state the exact remaining conditions before prose expansion and before experimental execution.

QUALITY BAR

Be candid and specific. Challenge weak assumptions. Favor the smallest coherent paper story. A good review leaves the authors with a defensible thesis, a bounded novelty claim, and an ordered revision plan—not seven disconnected opinions.
```

## Expected Use

The prompt intentionally separates draft readiness from experimental completeness. Run it again after major structural revisions, and later use a different review gate for completed experiments, statistical validity, and artifact packaging.

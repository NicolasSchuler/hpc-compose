# Reusable Prompt: Multi-Agent Literature and Reference Research

Use this prompt in a dedicated Codex session to build a verified, recent related-work set and stress-test the manuscript’s novelty claim.

## Inputs

- `{META_DRAFT_PATH}`: default `docs/plans/icpe-2027-meta-draft.md`
- `{EXISTING_BIBLIOGRAPHY_PATH}`: existing `.bib` or reference list; otherwise `NONE`
- `{RELATED_WORK_REPORT_PATH}`: consolidated Markdown report
- `{BIBTEX_OUTPUT_PATH}`: new candidate `.bib` file; otherwise `NONE`
- `{SEARCH_DATE}`: actual date on which the search is run
- `{RECENT_START_YEAR}`: default `2022`
- `{PINNED_REVISION}`: commit, tag, or `CURRENT_WORKTREE`

## Copyable Prompt

```text
You are the lead literature researcher for an intended ICPE 2027 Research Track submission about hpc-compose. Run a multi-agent, adversarial nearest-neighbor search. The goal is not to collect many loosely related citations; it is to discover the strongest prior art, narrow the novelty claim, and provide a verified related-work set suitable for the paper.

Inputs:

- meta-draft/system description: {META_DRAFT_PATH}
- existing bibliography: {EXISTING_BIBLIOGRAPHY_PATH}
- repository revision: {PINNED_REVISION}
- search date: {SEARCH_DATE}
- recent horizon: {RECENT_START_YEAR} through {SEARCH_DATE}, plus canonical older work

Outputs:

- consolidated report: {RELATED_WORK_REPORT_PATH}
- candidate BibTeX: {BIBTEX_OUTPUT_PATH}

Do not edit the manuscript, source code, or existing bibliography. If {BIBTEX_OUTPUT_PATH} is not `NONE`, create or update only that candidate file. Include only verified entries. Keep uncertain leads in the Markdown report, not in BibTeX.

SYSTEM CLAIM TO CHALLENGE

The current proposed thesis is:

“hpc-compose is an inspectable, allocation-scoped application compiler for Slurm: it lowers a deliberately constrained Compose-style multi-service specification into native Slurm allocation and job-step semantics, while preserving a traceable path from effective input to generated script and run evidence—without a resident orchestration control plane.”

Do not assume this is novel. Try to falsify each part:

1. constrained Compose-style application model;
2. multi-service topology, dependencies, readiness, and failure semantics;
3. one Slurm allocation containing native `srun` steps;
4. explicit allocation-level versus service-level resources and placement;
5. static/standalone compilation rather than a resident or nested scheduler;
6. inspectable generated script and field-to-script explanation;
7. runtime-backend and site-policy handling;
8. continuity from effective specification and script identity to run evidence;
9. progressive checks before quota-consuming execution.

The literature review must distinguish novelty of individual ingredients from novelty of their combination. Do not use “first,” “only,” “unique,” or “unprecedented” unless the search design could genuinely support the statement; safer comparative wording is preferred.

SOURCE AND VERIFICATION RULES

1. Use internet research. Search broadly, then verify narrowly.
2. Prefer primary scholarly sources: publisher/proceedings pages, DOI records, full papers, author manuscripts, and official project documentation for software with no paper.
3. For technical claims, read at least the abstract, system/design description, and evaluation/limitations of the actual paper. Do not infer overlap from a title or search snippet.
4. Resolve every DOI and verify exact title, authors, venue, year, volume/issue/pages or article number, and publication type.
5. When an archival paper and preprint describe the same work, cite the archival paper and retain the preprint only as a full-text link if useful.
6. Label every source as peer-reviewed paper, preprint, thesis, standard, official software/documentation, or unverified lead.
7. Use blogs, vendor pages, GitHub READMEs, or PyPI only when they are the authoritative source for software without a paper. Do not treat them as peer-reviewed evidence.
8. Do not use Wikipedia, SEO summaries, generated summaries, or search-result snippets as evidence.
9. Keep quotations short and exceptional; paraphrase with precise page/section references where possible.
10. Record access dates for mutable official documentation.
11. Never invent BibTeX fields. If metadata cannot be verified, omit the BibTeX entry and explain the gap.
12. Deduplicate workshop, preprint, journal, and software-record versions explicitly.
13. Search citation graphs in both directions: references used by close papers and later work that cites or extends them.
14. Prefer work from {RECENT_START_YEAR}–{SEARCH_DATE} for the state of the art, but include older canonical systems needed to make fair comparisons.
15. Stop after conceptual saturation: an additional search lane is saturated when two consecutive query/citation-chaining passes produce no new high- or medium-proximity system class. Record how saturation was assessed.

SUBAGENT ORCHESTRATION

The lead agent must read {META_DRAFT_PATH} fully and extract a one-page system/claim contract before delegation. Spawn up to five independent specialists concurrently in Wave 1. Give each the system contract, the verification rules, and one lane. Require each to search rather than rely on memory.

Wave 1 lanes:

1. Direct Compose-to-HPC and multi-container orchestration
   Search for Docker Compose, Podman Compose, Singularity/Apptainer Compose, Compose-to-Slurm translators, multi-container jobs, service readiness, container groups, and generated batch scripts on HPC.
   Seed names to verify, not assume: Singularity Compose, DockSing, Sarus Suite, Podman/Slurm container integration, Docker Compose pattern studies.

2. Slurm-native services, allocation-internal schedulers, pilot jobs, and co-scheduling
   Search for systems that run several components inside one allocation, nested schedulers, pilots, resource overlays, Slurm-native services, multi-program jobs, and dynamic task execution.
   Seed names: Flux, HyperQueue, QCG-PilotJob, RADICAL-Pilot, SmartSim, SAIA, executorlib, Merlin.

3. Scientific workflow, experiment DSL, and declarative performance frameworks
   Search for workflow and campaign systems that generate scheduler scripts, manage dependencies/parameters, execute containers, or support reproducible performance studies.
   Seed names: benchkit, Maestro Workflow Conductor, dagster-slurm, JUBE, ReFrame, Nextflow, Snakemake, Parsl, Pegasus, FireWorks, CWL, ExaWorks SDK, PSI/J.

4. HPC containers, runtime/scheduler integration, and site portability
   Search for runtime substrates and scheduler integrations that constrain or enable the proposed design: Pyxis/Enroot, Apptainer/Singularity, Sarus, Charliecloud, Shifter, Slurm container plugins, OCI support, multi-node container execution, shared-filesystem and network assumptions.
   This lane must distinguish enabling runtime work from application-level orchestration novelty.

5. Run provenance, reproducibility, experiment evidence, and service-level observability
   Search for Workflow Run RO-Crate, CWLProv, W3C PROV applications, ReproZip, reproducible HPC experiments, immutable run manifests, event-sourced experiment records, Slurm job provenance, performance experiment packaging, and service/process metric attribution.
   This lane must prevent claims that hpc-compose introduces a new general provenance model.

Each Wave 1 specialist must return:

- exact queries and databases/search engines used;
- citation-chaining sources and date searched;
- 8–12 strongest candidates, ordered by proximity;
- rejected false positives and why they are not relevant;
- saturation status;
- for each candidate, the full per-reference record defined below;
- the three strongest novelty threats from that lane;
- the safest defensible comparison sentence.

Wave 2:

After Wave 1 completes, spawn two adversarial cross-cutting agents, sequentially or concurrently as capacity permits:

6. Nearest-neighbor verifier
   Independently open and verify every proposed high-proximity source. Correct metadata, remove duplicates, downgrade unsupported overlap claims, and identify missing direct competitors through backward/forward citation chaining.

7. Novelty prosecutor and related-work architect
   Assume the paper’s novelty claim is wrong. Construct the strongest combination of prior systems that could reproduce the claimed contribution, identify which claim fragments are already established, and propose the narrowest defensible novelty wording and related-work organization.

The lead agent owns final metadata verification, synthesis, and BibTeX quality. Subagent consensus is not evidence.

PER-REFERENCE RECORD

For every retained source, report:

1. stable key;
2. exact title;
3. full author list;
4. year and publication date if known;
5. venue, volume/issue, pages or article number;
6. DOI and canonical primary URL;
7. publication type and peer-review status;
8. code/project/artifact URL when primary and relevant;
9. which hpc-compose claim fragments it overlaps;
10. concrete semantic or architectural differences;
11. evidence for the comparison, with paper page/section;
12. proximity: `DIRECT`, `HIGH`, `MEDIUM`, `CONTEXT`, or `EXCLUDE`;
13. novelty threat: `CRITICAL`, `HIGH`, `MODERATE`, `LOW`, or `NONE`;
14. use in the paper: motivation, direct comparison, conceptual contrast, enabling substrate, evaluation baseline, provenance limitation, or future work;
15. fair experimental-comparison status: executable baseline, descriptive comparison only, or not comparable;
16. verification status and any unresolved metadata;
17. recommended related-work subsection and a one-sentence positioning statement.

INCLUSION TEST

Retain a source only if it helps answer at least one of these questions:

- Has a Compose-like multi-service model already been mapped to HPC or Slurm?
- Has a system already executed readiness-coupled services within one Slurm allocation?
- Has a compiler already generated a standalone, inspectable native batch artifact with source mapping?
- Has a system already separated allocation-level and service-step resources in a comparable DSL?
- Has this result already been achieved through a pilot, nested scheduler, daemon, workflow engine, or service platform?
- Has a declarative performance framework already claimed the same repeatability/composability benefits?
- Are the run-evidence or provenance guarantees standard, stronger elsewhere, or interoperable through an existing profile?
- Does the source define a fair evaluation baseline, workload corpus, measurement method, or threat?
- Does the source establish a necessary technical constraint of Slurm or an HPC container runtime?

Exclude generic cloud orchestration, generic container introductions, unrelated workflow applications, and provenance surveys that do not sharpen a claim or evaluation choice.

COMPARISON AXES

Build one matrix covering all `DIRECT` and `HIGH` sources with these columns:

1. user-facing abstraction;
2. primary execution unit: container, service, task, job, DAG, pilot allocation, or application;
3. one allocation or multiple scheduled jobs;
4. static topology or dynamic scheduling;
5. concurrent service lifecycle and readiness;
6. dependency and failure semantics;
7. allocation-level versus service-level resources;
8. placement and multi-node semantics;
9. resident controller, nested scheduler, or standalone generated artifact;
10. generated Slurm script visibility and source mapping;
11. behavior for unsupported semantics;
12. container/runtime and site portability;
13. run identity and provenance;
14. metrics/artifact linkage and degraded states;
15. measured overhead/scalability;
16. artifact/license/maintenance status.

Do not use a checkmark when two systems mean different things by a feature. Use short factual descriptions and cite the source.

MINIMUM SEARCH NEIGHBORHOODS

The final review must cover, at minimum:

- direct Compose/container orchestration: Singularity Compose and DockSing;
- same-venue declarative performance work: ICPE 2026 benchkit;
- recent Slurm/workflow integration: dagster-slurm;
- YAML/campaign orchestration: Maestro;
- specialized multi-component HPC: SmartSim and Merlin;
- Slurm-native service platforms: SAIA;
- allocation-internal scheduling/pilots: Flux, HyperQueue, QCG-PilotJob, and at least one RADICAL-Pilot source;
- portable job interfaces: PSI/J;
- canonical workflow engines: at least Nextflow, Snakemake, Parsl, and Pegasus;
- current HPC runtime substrates: Pyxis/Enroot, Apptainer/Singularity, Sarus, and at least one of Charliecloud or Shifter;
- provenance/reproducibility: Workflow Run RO-Crate, CWLProv, and ReproZip;
- empirical Compose-pattern work that can inform an expressiveness corpus;
- at least three additional high-proximity sources discovered rather than supplied as seeds.

Do not force every named seed into the final bibliography. Verify and exclude when it does not materially help.

CONSOLIDATED REPORT

Write {RELATED_WORK_REPORT_PATH} with:

1. Executive conclusion
   - strongest defensible research gap;
   - which broad novelty claims the literature rules out;
   - recommended thesis and contribution wording;
   - confidence and search limitations.

2. Search protocol
   - search date, sources/databases, exact query families, citation chaining, recent-year policy, inclusion/exclusion rules, deduplication, and saturation evidence.

3. Nearest-neighbor ranking
   - top 10–15 sources ordered by semantic proximity, not publication prestige;
   - concise overlap, difference, and threat for each.

4. Full reference inventory
   - all verified retained sources using the per-reference record;
   - separate peer-reviewed, preprint, and authoritative software/documentation sources.

5. Novelty-threat matrix
   - claim fragment × strongest prior system;
   - verdict: `ALREADY_ESTABLISHED`, `PARTIALLY_OVERLAPS`, `COMBINATION_CLAIM_ONLY`, `POSSIBLE_GAP`, or `UNSUPPORTED_GAP`;
   - evidence and safe wording.

6. Direct comparison matrix
   - all required comparison axes and citations.

7. Related-work section architecture
   - recommended subsection order;
   - which citations appear in each;
   - paragraph-level argumentative job;
   - fair complementary relationships, not just differences.

8. Evaluation implications
   - which tools are fair executable baselines and for which RQs;
   - which are conceptual comparisons only;
   - public workload/pattern corpora and metrics worth reusing;
   - evaluation methods or threats learned from prior work.

9. Citation-ready positioning statements
   - concise paraphrases with primary citations;
   - no unsupported superlatives;
   - label inference explicitly.

10. Bibliography QA
    - duplicate/version decisions;
    - missing metadata;
    - preprints that later became archival papers;
    - software without papers;
    - retractions/corrections if any;
    - exact count of verified entries written to {BIBTEX_OUTPUT_PATH}.

11. Open leads and stopping rationale
    - unresolved but plausible sources;
    - searches attempted;
    - why the search is saturated enough for drafting and what should be rerun before submission.

BIBTEX RULES

If {BIBTEX_OUTPUT_PATH} is not `NONE`:

- write only records whose metadata was verified against a primary source;
- prefer DOI-backed archival entries;
- use stable, readable keys and protect capitalization such as `{Slurm}`, `{HPC}`, `{Compose}`, `{Docker}`, and tool names;
- include DOI and canonical URL when appropriate;
- use `@misc` only for authoritative software/documentation without a scholarly publication and include organization, year/date, URL, and access date where the bibliography style permits;
- do not insert abstracts, keywords, local file paths, or speculative metadata;
- run a duplicate-key and parse check;
- report exactly what was omitted and why.

QUALITY BAR

Aim for 20–30 verified, high-value references after deduplication, not an arbitrary long list. At least half of the state-of-the-art references should be from {RECENT_START_YEAR} onward when the literature permits, while canonical older work remains included where necessary. The final product must make the paper harder to reject for missing prior art and safer from an exaggerated novelty claim.
```

## Initial Novelty Warning

Initial verified evidence already makes several broad claims unsafe; the full search must confirm or revise these warnings:

- Singularity Compose makes “first declarative multi-container orchestration for HPC containers” unsafe.
- DockSing makes “first Compose-to-Slurm translator” unsafe.
- Flux, QCG-PilotJob, and HyperQueue make “first orchestration or task execution inside one allocation” unsafe.
- Maestro, dagster-slurm, and benchkit make broad novelty claims based on YAML, generated scripts, declarative experiments, or reproducible workflow execution unsafe.
- Workflow Run RO-Crate, CWLProv, and ReproZip make it unsafe to present the current run-evidence design as a new general provenance or reproducibility model.

The likely defensible seam is the static, inspectable combination of a constrained multi-service model, explicit Slurm allocation/step lowering and lifecycle semantics, and source-to-script auditability without a nested scheduler or persistent control plane.

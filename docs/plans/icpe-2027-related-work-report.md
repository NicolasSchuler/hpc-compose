# ICPE 2027 related-work and novelty-stress report

> **Historical first-pass note.** This report is preserved unchanged in its
> technical body as the independent first literature pass. Its contribution
> count and implementation recommendations are proposals from that pass, not
> current decisions. The
> [second literature pass](icpe-2027-related-work-second-pass.md),
> [review meta-review](icpe-2027-review-meta-review.md), and
> [open improvement ledger](icpe-2027-open-improvement-ledger.md) contain the
> later corrections, disposition decisions, and live status.

- Search date: **2026-08-09**
- Recent-work horizon: **2022–2026**, plus necessary canonical work
- System draft: `docs/plans/icpe-2027-meta-draft.md`
- Repository revision: `c53dac20a867470aa4184e7d35f5d76b56679801` (`CURRENT_WORKTREE`)
- Existing bibliography before this pass: **none**
- Candidate bibliography: `docs/plans/icpe-2027-reference-candidates.bib`

This report records a five-lane primary-source search followed by independent nearest-neighbor verification and adversarial novelty prosecution. It distinguishes peer-reviewed evidence from preprints and mutable official software documentation. “No system found” below is a bounded search result, not proof of priority.

## 1. Executive conclusion

### Strongest defensible research gap

The broad novelty claim does **not** survive. Prior systems already establish Compose-like multi-container orchestration, health-gated startup, Pod-to-Slurm translation, one-allocation task execution, per-component resource descriptions, native `srun` launches, generated scheduler scripts, site-aware container execution, and workflow/run provenance. Four particularly sharp unseeded threats are:

- [InterLink Slurm Plugin](https://github.com/interlink-hq/interlink-slurm-plugin), which lowers a multi-container Kubernetes Pod with init containers, probes, lifecycle hooks, runtime selection, generated scripts, cleanup, and job-ID/state tracking into one Slurm job, but requires resident Kubernetes/InterLink services and does not lower each service to a native `srun` step.
- [TSUBAME4 Mini Compose](https://www.t4.cii.isct.ac.jp/docs/all/experimental/mini-compose/), which executes an actual Compose file with `depends_on` and `healthcheck` under Docker, Singularity, or Apptainer inside an HPC job, but uses a node-local Flask controller and a user-written job script.
- [HPE Slurm Carrier for Capsules](https://support.hpe.com/hpesc/public/docDisplay?docId=a00115103en_us&docLocale=en_US&page=Slurm_Carrier_for_Capsules.html), which already supports one shared Slurm allocation with one manifest payload per job step, resource validation, generated `sbatch` plus `srun`, and IDs/logs/history, but is proprietary, not Compose-shaped, and does not document source mapping or readiness-coupled services.
- [IOPS 3.5.8](https://iops-benchmark.com/), which already combines YAML experiments, a single-allocation mode, incompatibility rejection, exact retained scripts, status, parameters, and system metadata, but runs tests sequentially in generated Bash rather than emitting native per-test steps, and retains a monitoring runner. Its documented `srun` example is user-supplied test code.

The gap that remains is therefore a **combination claim**:

> A deliberately bounded Compose-derived service application is statically lowered into one ordinary, inspectable Slurm allocation whose services are native job steps, with explicit allocation/service resource and placement semantics, bounded readiness/failure/shared-lifetime behavior, field-to-script explanation, and a compiler-owned identity chain from effective input through the exact submitted script to available job/step evidence—without a resident or nested scheduler.

The search found substantial antecedents for every ingredient. It did **not** find the entire conjunction in one verified system. This is a `POSSIBLE_GAP`, not support for “first,” “only,” “unique,” or “unprecedented.”

### Broad claims ruled out

The paper should not claim novelty for any of the following in isolation:

1. Compose-like multi-service orchestration: Singularity Compose, Mini Compose, DockSing, and container/Pod bridges already cover material subsets.
2. readiness, dependencies, or failure handling: Mini Compose, InterLink, workflow engines, and service platforms already implement variants.
3. several units inside one Slurm allocation or native `srun` launches: HPE Capsules, QCG-PilotJob, Flux, HyperQueue, RADICAL-Pilot, Executorlib, and Slurm itself establish these mechanisms. IOPS separately establishes single-allocation script generation and sequential trial execution.
4. allocation-level and per-unit resources: pilots, task executors, workflow grouping, and HPE payloads already separate or aggregate resource levels.
5. declarative YAML, generated scripts, or reproducible campaigns: Maestro, dagster-slurm, benchkit, IOPS, Snakemake, and Pegasus make those claims unsafe.
6. site-aware or portable HPC containers: Pyxis/Enroot, Apptainer, Charliecloud, Sarus Suite, and native Slurm container interfaces are mature substrate work.
7. a general provenance or run-package model: Workflow Run RO-Crate, CWLProv, ReproZip, and SCUP-HPC are broader or more standardized.
8. preflight, dry-run, or validation generically: workflow, regression-test, and experiment frameworks already provide staged checks.

### Recommended thesis and contribution wording

Recommended thesis:

> hpc-compose investigates a bounded design point between Compose-style host orchestration and controller-based HPC workflow systems: deterministic compilation of a strict finite service application into one inspectable Slurm allocation of native job steps, with explicit lifecycle/resource/placement semantics and traceable generated artifacts.

Recommended contribution claims:

1. **Semantic contract and lowering.** Define the accepted Compose-derived subset, reject unsupported behavior, and specify deterministic mapping to allocation-, service-, placement-, lifecycle-, and failure-level Slurm semantics.
2. **Inspectable standalone artifact.** Produce a normalized plan, annotated batch script, and field-to-script explanation that remain usable without a resident orchestration controller.
3. **Bounded evidence continuity.** Link effective-spec identity, exact submitted-script identity, application/service names, and observed Slurm job/step records; report missing, delayed, stale, partial, or unsupported evidence explicitly. Do not call this complete provenance.
4. **Empirical design-point evaluation.** Measure correctness, expressiveness, explanation quality, launch/teardown overhead, failure behavior, and operational burden against fair partial baselines, rather than only comparing feature lists.

### System/claim contract used for the search

The meta-draft describes a strict finite Compose-style application containing named services, commands, environment, volumes, dependencies/readiness, and Slurm extensions. Each ordinary run or trial maps to one Slurm allocation; services map to native `srun` steps; allocation resources are distinct from service resources and placement. The lifecycle includes preparation, ordered startup, bounded readiness, shared lifetime, failure propagation, and cleanup. The implementation is staged through parse, validation, normalization, derivation, context, preflight, preparation, rendering, submission, and tracking, and exposes a normalized plan plus generated/annotated script. Runtime/site policy supports Pyxis/Enroot, Apptainer, and host execution. Non-goals include a dynamic scheduler, bin-packer, arbitrary heterogeneous jobs, Kubernetes networking, cluster administration, a broad DAG engine, universal portability, and complete provenance.

### Confidence and limitations

- **High confidence** that the broad ingredient-level novelty claims are false: multiple primary sources independently establish them.
- **Moderate confidence** in the surviving conjunction: five independent lanes reached two-pass conceptual saturation, and the cross-cutting search found no exact controller-free, source-mapped Compose-to-native-step compiler. Negative search cannot establish priority.
- Official documentation is necessary for InterLink, Mini Compose, HPE Capsules, IOPS, DockSing, Maestro, Pyxis/Enroot, and Apptainer. Those claims are current as of the access date but are mutable and are not peer-reviewed evidence.
- Mini Compose lacks public source/version/license; HPE documentation lacks a reliable publication date and public artifact; DockSing exposes only a package and limited evaluation; Sarus Suite is a preprint. These cannot support stronger claims than their primary materials show.
- The search did not execute every artifact. “Executable baseline” means an artifact and a semantically fair comparison path exist; it does not claim compatibility with the target test cluster.

## 2. Search protocol

### Orchestration

Wave 1 used five independent specialist lanes: (1) direct Compose/HPC and multi-container orchestration; (2) Slurm-native services, pilots, and allocation-internal scheduling; (3) workflow/experiment DSLs; (4) HPC runtimes and scheduler integration; and (5) provenance/evidence. Wave 2 independently re-opened the proposed nearest neighbors and prosecuted the narrowed claim. The lead researcher rechecked DOI metadata through DOI/Crossref records and checked current package/release metadata through publisher, project, PyPI, arXiv, and repository pages.

### Sources and databases

Discovery used web search, publisher indexes, JOSS, ACM Digital Library, IEEE Xplore, SpringerLink, ScienceDirect, F1000Research, PLOS, GigaScience, arXiv, Crossref/DOI records, OSTI/LLNL, institutional repositories, PyPI, official documentation, and official repositories. Technical claims were retained only after reading the abstract plus relevant design/architecture and evaluation/limitation sections, or the corresponding official documentation/source for software without a paper. Search snippets, Wikipedia, blogs, and secondary generated summaries were not evidence.

### Exact query families

The table lists the exact discovery and saturation query strings that defined each family; additional title/DOI queries only resolved metadata.

| Lane | Exact queries used |
| --- | --- |
| Direct Compose/HPC | `"Docker Compose" Slurm multi-service HPC`; `"Compose" "sbatch" "srun" container orchestration HPC`; `"one Slurm allocation" multiple services readiness`; `"generated Slurm script" YAML Compose HPC`; `2022..2026 Kubernetes pod to Slurm script multi-container health probe sidecar`; `("single shared allocation" OR "shared Slurm allocation") (payloads OR services OR containers) orchestration`; `("one payload per-job step" OR "one payload per job step") Slurm`; `cites "Sarus Suite: Cloud-native Containers for HPC" orchestration Slurm`; `cites "Singularity Compose: Orchestration for Singularity Instances" HPC Slurm` |
| Pilots/services | `site:dl.acm.org HPC Slurm allocation task scheduler pilot jobs multiple applications services`; `site:ieeexplore.ieee.org Slurm pilot job nested scheduler HPC workflow allocation`; `"Slurm" "allocation" "services" HPC workflow system`; `Flux HPC framework paper resource management DOI full paper`; `HyperQueue HPC task scheduler paper DOI`; `QCG-PilotJob paper DOI`; `RADICAL-Pilot scalable execution heterogeneous dynamic workloads DOI`; `SAIA Seamless Slurm-Native Solution HPC-Based Services paper DOI`; `executorlib HPC Slurm paper JOSS DOI` |
| Workflow/experiments | `HPC workflow compiler "srun" "single allocation"`; `Slurm experiment framework generated job scripts YAML reproducible`; `scientific workflow service readiness Slurm allocation multiple processes framework`; `HPC workflow "job steps" per-task resources one allocation paper`; `"Executorlib" related work Parsl Dask Slurm srun existing allocation`; `"dagster-slurm" related work HPC workflow Slurm shared allocation`; `"benchkit" related work ReFrame JUBE campaign framework`; `2024 2025 review HPC workflow systems Slurm pilot job generated scripts` |
| Runtimes/site policy | `Slurm OCI containers job steps multi-container official`; `Pyxis Enroot Slurm multi-node container official`; `Apptainer Slurm multiple containers one allocation`; `Sarus Slurm container synchronization barrier paper`; `Sarus Suite Skybox Kubernetes multi-container Slurm`; `Charliecloud unprivileged containers HPC DOI`; `HPC container site hooks environment definition file Slurm` |
| Provenance/evidence | `Slurm job provenance source code batch script job ID`; `2022 2023 2024 2025 HPC experiment provenance Slurm`; `Workflow Run RO-Crate paper DOI`; `CWLProv workflow provenance paper DOI`; `ReproZip computational reproducibility DOI`; `HPC provenance metrics logs process attribution`; `SCUP-HPC provenance Slurm source files job definition`; `cites "Recording provenance of workflow runs with RO-Crate" HPC` |

### Citation chaining and recency

Backward chaining from Singularity Compose, Sarus Suite, HPK, benchkit, ExaWorks, Snakemake, Workflow Run RO-Crate, and SAIA exposed StreamFlow, HPE Capsules, OpenVenus, ReFrame/JUBE, Executorlib, IOPS, SCUP-HPC, and runtime predecessors. Forward/current chaining checked maintained successors, current execution modes, and newer 2022–2026 publications. Archival papers replaced matching preprints; a preprint was kept only when no archival version was found. Twenty of the thirty retained records have known dates in 2022–2026; older work is retained only for canonical abstractions or required substrates.

### Inclusion, exclusion, and deduplication

A source was retained if it materially constrained a claim fragment, defined a fair baseline/evaluation method, or established a runtime/Slurm constraint. Generic cloud orchestration, Slurm-in-Docker test clusters, raw job wrappers, cluster-administration operators, generic provenance surveys, and application-only workflow papers were excluded. Preprint/archival duplicates were collapsed; software records and current docs supplement rather than replace archival claims. Version decisions are detailed in Section 10.

### Saturation evidence

Each Wave 1 lane reset its saturation counter when it found a new `DIRECT`, `HIGH`, or new `MEDIUM` system class. The final new classes were HPE Capsules in the direct lane, Executorlib/IOPS in the workflow/allocation lanes, Sarus Suite/CSCS EDF in the runtime lane, and SCUP-HPC/HyProv in the provenance lane. Two subsequent passes per lane—one exact-mechanism query pass and one backward/forward citation-chain pass—returned only already-covered classes, lower-proximity wrappers, cluster administration, generic workflow engines, or runtime substrates. The independent verifier re-opened the nearest sources and did not identify a new system class that combined the surviving conjunction. This satisfies the prompt's two-consecutive-pass stopping rule for drafting.

## 3. Nearest-neighbor ranking

Ranking is by semantic proximity, not venue prestige. Threat applies to the proposed contribution as written before narrowing.

| Rank | Source | Overlap | Decisive difference | Proximity / threat |
| ---: | --- | --- | --- | --- |
| 1 | InterLink Slurm Plugin | Multi-container Pod; init/app lifecycle; probes; generated Slurm scripts; runtimes; cleanup; job ID/state | Resident Kubernetes/Virtual Kubelet/InterLink stack; shell-launched containers, not native service steps; no source map | `DIRECT` / `CRITICAL` |
| 2 | HPE Slurm Carrier for Capsules | Shared allocation; one payload per job step; manifest resources/dependencies; validation; generated `sbatch`+`srun`; durable launch evidence | Proprietary capsule model; resident product services possible; readiness/source mapping not documented | `DIRECT` / `CRITICAL` |
| 3 | TSUBAME4 Mini Compose | Real Compose parsing; `depends_on`; health checks; Docker/Singularity/Apptainer; concurrent services in an HPC job | Resident node-local Flask/PortShift services; user writes batch script; no Slurm resource/placement model | `DIRECT` / `CRITICAL` |
| 4 | High-Performance Kubernetes (HPK) | Kubernetes workloads/services to generated Slurm scripts/jobs with Apptainer, state synchronization, networking/storage adaptations | Resident user-level Kubernetes/custom kubelet; each workload is a separate Slurm job; nested parent container, not one application allocation/native steps | `DIRECT` / `HIGH` |
| 5 | Singularity Compose | Named Compose-like services, dependencies, volumes/network, lifecycle operations | Host-local persistent instances; no Slurm/resource/placement/script/evidence contract | `HIGH` / `HIGH` |
| 6 | DockSing 0.2.36 | Compose-inspired single-container config to inspectable `srun`/Singularity command; remote submission/log streaming | No multi-service graph, batch artifact, readiness, or evidence chain | `HIGH` / `HIGH` |
| 7 | Executorlib | Resource-annotated Python functions launched with native `srun` inside an existing allocation | Python/socket executor; task/future model; no service lifecycle or generated source-mapped script | `HIGH` / `CRITICAL` |
| 8 | IOPS 3.5.8 | YAML; single allocation; rejection/check/dry-run; exact scripts/status/parameters/system metadata | Tests run sequentially in generated Bash; no automatic native per-test step; monitoring runner; no service lifecycle/source map | `HIGH` / `HIGH` |
| 9 | QCG-PilotJob | Dependent resource-described tasks inside one allocation | Resident second-level manager dynamically schedules tasks | `HIGH` / `CRITICAL` |
| 10 | Flux | Hierarchical allocation overlays, placement, nested scheduling and co-scheduling | Nested resource manager/runtime, not static batch compilation | `HIGH` / `CRITICAL` |
| 11 | HyperQueue | Resource-aware task graph dynamically packed into one/few allocations | Resident server/workers; task scheduling rather than static service semantics | `HIGH` / `HIGH` |
| 12 | SmartSim | Coupled simulations plus database/ML service under Slurm | Specialized Python orchestrator and service; not a bounded standalone compiler | `HIGH` / `HIGH` |
| 13 | Sarus Suite | Declarative HPC environments, Slurm-native path, site policy, separate Kubernetes-manifest multi-container path | Slurm Skybox and multi-container `sarusctl` paths are separate; no Compose-to-native-step lowering | `HIGH` / `HIGH` |
| 14 | SAIA | Slurm-native health-checked service pools, scaling, proxy, failure handling, production evaluation | Resident scheduler/proxy; each instance is a separate Slurm job, not one finite allocation | `HIGH` / `HIGH` |
| 15 | dagster-slurm | Reproducible Slurm assets, logs/metrics/lineage, site-aware packaging, emerging shared allocations | Dagster control plane; stable mode is one job per asset; shared modes experimental/Ray-focused | `HIGH` / `HIGH` |

## 4. Full reference inventory

Each record covers the prompt's 17 fields in five compact groups: **1–8** metadata/type/artifact; **9–11** overlap, difference, and primary evidence; **12–15** proximity, threat, use, and experimental-comparison status; **16** verification gaps; and **17** recommended subsection/positioning. Mutable pages were accessed 2026-08-09.

### 4.1 Peer-reviewed sources

#### 1. `sochat2019singularitycompose` — Singularity Compose

- **1–8 Metadata/artifact:** Vanessa Sochat; *Singularity Compose: Orchestration for Singularity Instances*; published 2019-08-26; *Journal of Open Source Software* 4(40), article 1578; [DOI and primary page](https://doi.org/10.21105/joss.01578); peer-reviewed software paper; [project](https://github.com/singularityhub/singularity-compose) and [software archive](https://doi.org/10.5281/zenodo.11179823).
- **9–11 Overlap/difference/evidence:** Overlaps F1–F2 through named services, dependencies, volumes/networking, and lifecycle operations. It runs persistent local Singularity instances and has no Slurm allocation, resource/placement contract, readiness gate, generated batch artifact, source map, or job evidence. Evidence: JOSS paper pp. 1–3, especially the example specification and `up/down/restart` interface.
- **12–15 Classification/use/baseline:** `HIGH`; threat `HIGH` to Compose-shaped service novelty and `LOW` to the Slurm compiler. Use as canonical direct comparison. Executable partial baseline for local input/lifecycle behavior, not Slurm lowering.
- **16 Verification:** DOI, author, date, venue, code, and archive verified. The 2024 Zenodo record is a software release, not a replacement for the JOSS citation; current runtime compatibility must be retested.
- **17 Positioning:** “Compose-shaped orchestration.” Singularity Compose established Compose-like service orchestration; hpc-compose must be positioned around its bounded Slurm allocation/step contract and inspectable evidence, not the service abstraction itself.

#### 2. `paolillo2026benchkit` — benchkit

- **1–8 Metadata/artifact:** Antonio Paolillo, Mats Van Molle, Ken Hasselmann; *benchkit: A Declarative Framework for Composable Performance Evaluation of System Software*; published 2026-05-03; ICPE 2026, pp. 170–183; [DOI](https://doi.org/10.1145/3777884.3796997); peer-reviewed ACM conference paper; [code](https://github.com/open-s4c/benchkit).
- **9–11 Overlap/difference/evidence:** Overlaps F1, F6–F9 through declarative/composable campaigns, platforms, wrappers/hooks, per-run directories, CSV/JSON, reproducibility, and an overhead study. It is a sequential campaign runner rather than a one-allocation service compiler and has no allocation/service resource hierarchy or readiness/shared lifetime. Evidence: paper architecture pp. 3–5, overhead evaluation pp. 9–10, limitations/future work pp. 10–12.
- **12–15 Classification/use/baseline:** `HIGH` for declarative performance methodology, threat `MODERATE` overall. Use in same-venue related work and to borrow overhead methodology. Executable partial baseline for campaign/harness overhead, not service semantics.
- **16 Verification:** Exact DOI metadata, full paper, dates, and code verified. Current Slurm-specific behavior was not claimed by the paper and is not inferred.
- **17 Positioning:** “Declarative performance campaigns.” benchkit makes declarative/composable repeatability claims unsafe; hpc-compose's comparison must center on allocation/service lowering.

#### 3. `picatto2026dagsterslurm` — dagster-slurm

- **1–8 Metadata/artifact:** Hernan Picatto, Maximilian Heß, Georg Heiler, Martin Pfister; *Discovering the SUPER in computing — dagster-slurm for reproducible research on HPC*; published 2026-03-19; *JOSS* 11(119), article 9795; [DOI/JOSS page](https://doi.org/10.21105/joss.09795); peer-reviewed software paper; [code](https://github.com/ascii-supply-networks/dagster-slurm) and [docs](https://dagster-slurm.geoheil.com/).
- **9–11 Overlap/difference/evidence:** Overlaps F3–F4 and F6–F9 through Slurm assets, resource/launcher configuration, logs, state, metrics, and lineage; current docs describe run-scoped Ray and experimental shared/HET modes. Dagster remains the controller, stable mode submits one job per asset, and shared allocation is experimental/Ray-focused. Evidence: JOSS PDF pp. 1–4 and official “Execution modes” documentation.
- **12–15 Classification/use/baseline:** `HIGH`, threat `CRITICAL` to broad script/evidence/reproducible-Slurm claims. Use as leading recent workflow comparison. Executable for stable per-asset deployment/observability; shared mode is exploratory only.
- **16 Verification:** Publication metadata and code verified; current mutable docs were used to separate stable from experimental behavior. No readiness-coupled service/source-map evidence was found.
- **17 Positioning:** “Modern orchestration on Slurm.” Contrast an asset control plane with one complete controller-free service artifact.

#### 4. `partee2022smartsim` — SmartSim

- **1–8 Metadata/artifact:** Sam Partee, Matthew Ellis, Alessandro Rigazzi, Andrew E. Shao, Scott Bachman, Gustavo Marques, Benjamin Robbins; *Using Machine Learning at Scale in Numerical Simulations with SmartSim: An Application to Ocean Climate Modeling*; July 2022; *Journal of Computational Science* 62, 101707; [DOI](https://doi.org/10.1016/j.jocs.2022.101707); peer-reviewed journal paper; [project](https://github.com/CrayLabs/SmartSim).
- **9–11 Overlap/difference/evidence:** Overlaps F2–F4 and F7–F8 through orchestration of simulations and a shared in-memory database/ML service on HPC schedulers. It is a specialized Python experiment/orchestrator and database architecture, not a static Compose compiler or standalone native-step script. Evidence: paper architecture and SmartSim workflow sections, climate application, and scaling/evaluation sections.
- **12–15 Classification/use/baseline:** `HIGH`, threat `HIGH` to broad multi-component/service-on-Slurm wording. Use as specialized coupled-application comparison. Executable partial baseline only for a simulation-plus-service scenario with equivalent SmartSim semantics.
- **16 Verification:** DOI, authors, article number, and architecture/evaluation verified. Current scheduler behavior should be checked against the release used in any experiment.
- **17 Positioning:** “Specialized coupled applications.” SmartSim demonstrates production multi-component simulation/service orchestration; hpc-compose targets a general bounded service topology without a specialized runtime service.

#### 5. `doosthosseini2026saia` — SAIA

- **1–8 Metadata/artifact:** Ali Doosthosseini, Jonathan Decker, Hendrik Nolte, Julian Kunkel; *SAIA: A Seamless Slurm-native Solution for HPC-based Services*; published 2026-05-08; *The Journal of Supercomputing* 82(7), article 403; [DOI/full primary page](https://doi.org/10.1007/s11227-026-08508-3); peer-reviewed journal paper; [HPC component](https://github.com/gwdg/saia-hpc) and [hub](https://github.com/gwdg/saia-hub).
- **9–11 Overlap/difference/evidence:** Overlaps F2, F7–F9 through health checks, service startup, failure, accounting, scaling, and Slurm-native operation. A resident scheduler/proxy maintains demand-scaled pools; each service instance is a separate Slurm job. Evidence: architecture §5, methodology §6, evaluation §7, limitations §8; reported simple-request overhead 42 ms and 400/1000 RPS with one/16 SSH connections.
- **12–15 Classification/use/baseline:** `HIGH`, threat `HIGH` to any “Slurm services are new” claim. Use as conceptual contrast with persistent service platforms. Descriptive only for the finite one-allocation RQ; executable only for a separate service-platform RQ.
- **16 Verification:** Exact metadata, article number/date, full text, evaluation, limitations, and code URLs verified. It does not support one-allocation multi-service lowering.
- **17 Positioning:** “Slurm-native service platforms.” SAIA chooses persistent scheduling and dynamic job pools; hpc-compose chooses a finite application allocation and no resident service controller.

#### 6. `ahn2020flux` — Flux

- **1–8 Metadata/artifact:** Dong H. Ahn, Ned Bass, Albert Chu, Jim Garlick, Mark Grondona, Stephen Herbein, Helgi I. Ingólfsson, Joseph Koning, Tapasya Patki, Thomas R. W. Scogland, Becky Springmeyer, Michela Taufer; *Flux: Overcoming Scheduling Challenges for Exascale Workflows*; September 2020; *Future Generation Computer Systems* 110, 202–213; [DOI](https://doi.org/10.1016/j.future.2020.04.006); peer-reviewed journal paper; [project](https://flux-framework.org/).
- **9–11 Overlap/difference/evidence:** Overlaps F2–F5 and F7–F8 through hierarchical allocation overlays, nested scheduling, placement, KVS/event state, and CPU/GPU co-scheduling. The nested resource-manager runtime is central and accepts jobs dynamically. Evidence: architecture §IV and workflow/co-scheduling evaluation §V; the paper reports workflow throughput improvements up to 48× and low Flux-component overhead in the evaluated setting.
- **12–15 Classification/use/baseline:** `HIGH`, threat `CRITICAL` to resource hierarchy/placement/in-allocation novelty. Use as the canonical nested-scheduler contrast. Executable only for an RQ comparing static lowering with dynamic co-scheduling.
- **16 Verification:** Metadata and full author/project paper verified. The earlier 2018 WORKS paper is a predecessor, not a duplicate retained record.
- **17 Positioning:** “Nested resource managers.” Flux generalizes dynamic scheduling; hpc-compose deliberately trades that flexibility for a bounded ordinary Slurm artifact.

#### 7. `beranek2024hyperqueue` — HyperQueue

- **1–8 Metadata/artifact:** Jakub Beránek, Ada Böhm, Gianluca Palermo, Jan Martinovič, Branislav Jansík; *HyperQueue: Efficient and Ergonomic Task Graphs on HPC Clusters*; September 2024; *SoftwareX* 27, 101814; [DOI](https://doi.org/10.1016/j.softx.2024.101814); peer-reviewed journal/software paper; [project](https://github.com/It4innovations/hyperqueue).
- **9–11 Overlap/difference/evidence:** Overlaps F2–F5 and F7–F9 through resource-aware task graphs, dependencies/failure policy, workers inside one or a few allocations, automatic submission, and dry-run/explain facilities. A long-lived server/workers dynamically schedule tasks rather than compile readiness-coupled services. Evidence: paper system design, resource/task graph sections, evaluation, and limitations.
- **12–15 Classification/use/baseline:** `HIGH`, threat `HIGH` to allocation/task/resource claims. Use as a modern allocation-internal task scheduler. Executable for task-dispatch/utilization RQs, not as a service-semantic baseline.
- **16 Verification:** DOI, authors, article number, abstract/design/evaluation, and code verified. No resident-free service script/source map was found.
- **17 Positioning:** “Allocation-internal scheduling.” HyperQueue offers dynamic load balancing; hpc-compose's value must be static inspectability and service semantics.

#### 8. `bosak2021qcgpilotjob` — QCG-PilotJob

- **1–8 Metadata/artifact:** Bartosz Bosak, Tomasz Piontek, Paul Karlshoefer, Erwan Raffin, Jalal Lakhlili, Piotr Kopta; *Verification, Validation and Uncertainty Quantification of Large-Scale Applications with QCG-PilotJob*; 2021; *Computational Science — ICCS 2021*, pp. 495–501; [DOI](https://doi.org/10.1007/978-3-030-77977-1_39); peer-reviewed Springer conference chapter; [code](https://github.com/psnc-qcg/QCG-PilotJob).
- **9–11 Overlap/difference/evidence:** Overlaps F2–F5, F7, and F9 through dependent resource-described tasks inside a single batch allocation. A resident second-level manager schedules JSON/API-submitted tasks. Evidence: system §3; evaluation §5 reports 20,000 five-minute tasks on 100 nodes/5,000 cores with 99.2% occupied time; limitations/future work §6.
- **12–15 Classification/use/baseline:** `HIGH`, threat `CRITICAL` to broad one-allocation/dependency/resource claims. Use as a primary pilot comparison. Executable for dependent-command launch and utilization, descriptive for readiness/source mapping.
- **16 Verification:** Title, authors, pages, DOI, official proceedings PDF, and current code verified. No unresolved bibliographic field.
- **17 Positioning:** “Pilots.” QCG dynamically schedules work inside an allocation; hpc-compose precomputes one finite service topology into native Slurm.

#### 9. `titov2023radicalpilot` — RADICAL-Pilot

- **1–8 Metadata/artifact:** Mikhail Titov, Matteo Turilli, Andre Merzky, Thomas Naughton, Wael Elwasif, Shantenu Jha; *RADICAL-Pilot and PMIx/PRRTE: Executing Heterogeneous Workloads at Large Scale on Partitioned HPC Resources*; 2023; *Job Scheduling Strategies for Parallel Processing*, pp. 88–107; [DOI](https://doi.org/10.1007/978-3-031-22698-4_5); peer-reviewed revised workshop chapter; [project](https://github.com/radical-cybertools/radical.pilot).
- **9–11 Overlap/difference/evidence:** Overlaps F2–F5 and F7–F8 through pilot allocations, heterogeneous unit resources, dynamic scheduling/launch, profiling, and failure/state data. A pilot agent/runtime/database remains and tasks are dynamic, not finite services. Evidence: architecture/PMIx integration and large-scale evaluation sections; evaluation reaches 65.5k heterogeneous tasks on 2,048 nodes.
- **12–15 Classification/use/baseline:** `HIGH`, threat `HIGH` to in-allocation heterogeneous execution. Use as canonical pilot contrast. Executable only for a dynamic workload/launch-throughput RQ.
- **16 Verification:** DOI, authors, volume/pages, design and evaluation verified. This revised archival chapter replaces matching preprint/workshop variants.
- **17 Positioning:** “Pilots.” RADICAL-Pilot optimizes dynamic heterogeneous task execution; hpc-compose specializes a statically knowable service application.

#### 10. `janssen2025executorlib` — Executorlib

- **1–8 Metadata/artifact:** Jan Janssen, Michael Gilbert Taylor, Ping Yang, Joerg Neugebauer, Danny Perez; *Executorlib — Up-scaling Python Workflows for Hierarchical Heterogenous High-performance Computing*; published 2025-04-01; *JOSS* 10(108), article 7782; [DOI/JOSS page](https://doi.org/10.21105/joss.07782); peer-reviewed software paper; [code](https://github.com/pyiron/executorlib) and [docs](https://executorlib.readthedocs.io/).
- **9–11 Overlap/difference/evidence:** Directly overlaps F3–F4: `SlurmJobExecutor` launches Python calls as native `srun` job steps inside an existing allocation, with per-call CPU/GPU/memory/time resources, dependency resolution, and caching. It is a Python futures/socket coordinator without named services/readiness/shared lifetime or a standalone source-mapped application script. Evidence: JOSS pp. 1–3 and official “HPC Job Executor → SLURM” documentation.
- **12–15 Classification/use/baseline:** `HIGH`, threat `CRITICAL` to native-step and per-unit-resource novelty. Use as nearest task-executor comparison. Executable baseline for launch latency, resource isolation, concurrency, placement, and failure capture; not for service semantics.
- **16 Verification:** JOSS metadata, authors, current docs, and code verified. Published title intentionally preserves “heterogenous.”
- **17 Positioning:** “In-allocation executors.” Executorlib is the cleanest counterexample to treating `srun` per resource-described unit as new.

#### 11. `hateganmarandiuc2023psij` — PSI/J

- **1–8 Metadata/artifact:** Mihael Hategan-Marandiuc, Andre Merzky, Nicholson Collier, Ketan Maheshwari, Jonathan Ozik, Matteo Turilli, Andreas Wilke, Justin M. Wozniak, Kyle Chard, Ian Foster, Rafael Ferreira da Silva, Shantenu Jha, Daniel Laney; *PSI/J: A Portable Interface for Submitting, Monitoring, and Managing Jobs*; October 2023; IEEE e-Science 2023, pp. 1–10; [DOI](https://doi.org/10.1109/e-Science58273.2023.10254912); peer-reviewed conference paper; [project](https://github.com/ExaWorks/psij-python).
- **9–11 Overlap/difference/evidence:** Overlaps F6–F9 through a portable job model, scheduler executors, submission, monitoring, status, cancellation, and launcher abstraction. It submits/manages individual jobs rather than compiling a multi-service application or one allocation of service steps. Evidence: paper interface/design, executor/launcher implementation, and evaluation/use-case sections.
- **12–15 Classification/use/baseline:** `MEDIUM`, threat `MODERATE`; use as portable-job-interface context and a possible integration boundary. Executable for submission portability, not the central service RQ.
- **16 Verification:** DOI, full author list, venue/pages, and project verified. No claim is made that PSI/J preserves field-to-script mapping.
- **17 Positioning:** “Portable job APIs.” PSI/J abstracts scheduler job management; hpc-compose operates one semantic layer above it.

#### 12. `chazapis2025hpk` — High-Performance Kubernetes (HPK)

- **1–8 Metadata/artifact:** Antony Chazapis, Lefteris Vassilakis, Giannis Petsis, Manolis Marazakis, Angelos Bilas; *Evaluating HPK for Running Cloud-Native Workloads on Slurm Clusters*; November 2025; SC Workshops 2025, pp. 163–171; [DOI](https://doi.org/10.1145/3731599.3767352); peer-reviewed ACM workshop paper; [project](https://github.com/CARV-ICS-FORTH/HPK). The accessible 2024 design paper is [arXiv:2409.16919](https://arxiv.org/abs/2409.16919).
- **9–11 Overlap/difference/evidence:** Overlaps F2–F4 and F6–F9: a user-level Kubernetes control plane/custom kubelet translates Kubernetes container lifecycle and resources to generated Slurm scripts/jobs with Apptainer, synchronizes state, and adapts networking/storage. Each workload/deployment becomes a separate Slurm job, and Pod containers execute beneath a parent container; it is not one bounded application allocation or native service steps. Evidence: 2024 design paper architecture/implementation/limitations and 2025 archival evaluation.
- **12–15 Classification/use/baseline:** `DIRECT`, threat `HIGH` to multi-container-to-Slurm script/state claims. Use as the archival Kubernetes-bridge comparison. Conditionally executable, but setup/control-plane cost must be included; otherwise descriptive.
- **16 Verification:** 2025 DOI, authors, venue/pages, and 2024 full text verified. The 2023 foundation, 2024 expanded design, and 2025 evaluation are one lineage, not independent competitors.
- **17 Positioning:** “Kubernetes-to-batch bridges.” HPK already translates cloud-native workloads into tracked Slurm jobs through a resident Kubernetes layer; hpc-compose's remaining distinction is one static service allocation of native steps with source mapping and no controller.

#### 13. `molder2025snakemake` — Snakemake

- **1–8 Metadata/artifact:** Felix Mölder, Kim Philipp Jablonski, Brice Letcher, Michael B. Hall, Peter C. van Dyken, Christopher H. Tomkins-Tinch, Vanessa Sochat, Jan Forster, Filipe G. Vieira, Christian Meesters, Soohyun Lee, Sven O. Twardziok, Alexander Kanitz, Jake VanCampen, Venkat Malladi, Andreas Wilm, Manuel Holtgrewe, Sven Rahmann, Sven Nahnsen, Johannes Köster; *Sustainable Data Analysis with Snakemake*; version 3 published 2025-09-23; *F1000Research* 10:33; [DOI](https://doi.org/10.12688/f1000research.29032.3); openly peer-reviewed journal/software article; [project](https://snakemake.github.io/).
- **9–11 Overlap/difference/evidence:** Overlaps F2–F4 and F6–F9: connected rules can be grouped into one cluster job, grouped resources are aggregated by topological layer, and pipe outputs co-run producer/consumer. The controller remains and rules/file dependencies are not named-service readiness/shared lifetime. Evidence: article §2.5.3 “Graph partitioning,” §2.5.4 “Pipe outputs,” deployment/report/provenance sections, and current Slurm plugin docs.
- **12–15 Classification/use/baseline:** `HIGH`, threat `CRITICAL` to claims that workflow units cannot share an allocation or run concurrently. Use as the leading canonical workflow comparator. Executable partial baseline for grouped execution/producer-consumer behavior, not arbitrary services.
- **16 Verification:** Version-3 DOI and full author list verified; versions 1/2 were deduplicated. Current plugin behavior is mutable.
- **17 Positioning:** “Grouped DAG workflows.” Snakemake's grouped dataflow erodes a broad one-allocation boundary but remains semantically different from a service state machine.

#### 14. `babuji2019parsl` — Parsl

- **1–8 Metadata/artifact:** Yadu Babuji, Anna Woodard, Zhuozhao Li, Daniel S. Katz, Ben Clifford, Rohan Kumar, Lukasz Lacinski, Ryan Chard, Justin M. Wozniak, Ian Foster, Michael Wilde, Kyle Chard; *Parsl: Pervasive Parallel Programming in Python*; June 2019; HPDC 2019, pp. 25–36; [DOI](https://doi.org/10.1145/3307681.3325400); peer-reviewed ACM conference paper; [project](https://parsl-project.org/).
- **9–11 Overlap/difference/evidence:** Overlaps F2–F5 and F7–F9 through dynamic task graphs, provider allocation “blocks,” launchers including `srun`, task resources, monitoring, and failures. A DataFlowKernel/executor/provider control plane dynamically schedules tasks. Evidence: programming model §§2–3, architecture/provider/launcher §4, and latency/throughput/scaling evaluation.
- **12–15 Classification/use/baseline:** `HIGH`, threat `HIGH` overall and `CRITICAL` to allocation/task resource claims. Use as canonical pilot-style workflow comparison. Executable only for task launch/utilization RQs.
- **16 Verification:** DOI, authors, venue/pages, full paper, and project verified. Current behavior should be pinned if evaluated.
- **17 Positioning:** “Dynamic task runtimes.” Parsl separates allocations from tasks through a controller; hpc-compose precomputes a finite service application.

#### 15. `deelman2015pegasus` — Pegasus

- **1–8 Metadata/artifact:** Ewa Deelman, Karan Vahi, Gideon Juve, Mats Rynge, Scott Callaghan, Philip J. Maechling, Rajiv Mayani, Weiwei Chen, Rafael Ferreira da Silva, Miron Livny, Kent Wenger; *Pegasus, a Workflow Management System for Science Automation*; May 2015; *Future Generation Computer Systems* 46, 17–35; [DOI](https://doi.org/10.1016/j.future.2014.10.008); peer-reviewed journal paper; [project](https://pegasus.isi.edu/).
- **9–11 Overlap/difference/evidence:** Overlaps F1–F2 and F6–F9 through abstract workflow compilation, site catalogs, generated submit directories/job files, monitoring, retries, and provenance. It maps DAG tasks across distributed resources and retains a workflow engine; it does not express concurrent readiness-coupled services in one allocation. Evidence: paper architecture, planning/transformation, execution, data management, and provenance sections.
- **12–15 Classification/use/baseline:** `MEDIUM`, threat `MODERATE` to broad compiler/script/provenance claims. Use as canonical workflow compiler context. Descriptive for the service RQ.
- **16 Verification:** Exact authors, volume/pages, DOI, and primary paper verified. Current Pegasus features must be separated from this archival description.
- **17 Positioning:** “Workflow compilers.” Pegasus makes generic workflow compilation and generated artifacts established; hpc-compose's possible gap is its service/Slurm/source-map contract.

#### 16. `priedhorsky2017charliecloud` — Charliecloud

- **1–8 Metadata/artifact:** Reid Priedhorsky, Tim Randles; *Charliecloud: Unprivileged Containers for User-defined Software Stacks in HPC*; November 2017; SC 2017, pp. 1–10; [DOI](https://doi.org/10.1145/3126908.3126925); peer-reviewed ACM conference paper; [project](https://hpc.github.io/charliecloud/).
- **9–11 Overlap/difference/evidence:** Overlaps F7 and runtime constraints: unprivileged, daemonless user-defined environments suited to HPC. It is a container runtime/substrate, not an application/service orchestrator, resource compiler, or evidence model. Evidence: paper motivation/security/design, implementation, and performance evaluation sections.
- **12–15 Classification/use/baseline:** `CONTEXT`, threat `LOW` to the combined claim but decisive against daemonless-runtime novelty. Use as enabling substrate. Runtime control only, not an orchestration baseline.
- **16 Verification:** DOI, author list, pages, paper, project, and license status verified at the primary sources.
- **17 Positioning:** “Container substrates.” Charliecloud helps explain why no-daemon container execution is not novel at the application layer.

#### 17. `leo2024workflowrunrocrate` — Workflow Run RO-Crate

- **1–8 Metadata/artifact:** Simone Leo, Michael R. Crusoe, Laura Rodríguez-Navas, Raül Sirvent, Alexander Kanitz, Paul De Geest, Rudolf Wittner, Luca Pireddu, Daniel Garijo, José M. Fernández, Iacopo Colonnelli, Matej Gallo, Tazro Ohta, Hirotaka Suetake, Salvador Capella-Gutierrez, Renske de Wit, Bruno P. Kinoshita, Stian Soiland-Reyes; *Recording Provenance of Workflow Runs with RO-Crate*; 2024-09-10; *PLOS ONE* 19(9), e0309210; [DOI/full article](https://doi.org/10.1371/journal.pone.0309210); peer-reviewed journal article; [profiles/tooling](https://www.researchobject.org/workflow-run-crate/).
- **9–11 Overlap/difference/evidence:** Overlaps F8 through prospective plans, retrospective runs, parameters, actions/steps, agents, times, status, logs, inputs/outputs, and portable packaging. It is a representation/interoperability family, not an executor or Slurm compiler, and minimal metadata permits variable completeness. Evidence: profile definitions §2, seven implementations §3, use cases §4, mappings/limitations §5.
- **12–15 Classification/use/baseline:** `DIRECT` for provenance representation, threat `CRITICAL` to a new run-manifest/provenance model. Use as an interoperability target. Not a runtime baseline; executable validation/export target only.
- **16 Verification:** DOI, authors, article, model, implementations, and limitations verified. The living profile must be version-pinned in evaluation.
- **17 Positioning:** “Run packaging.” hpc-compose should produce Slurm-specific evidence that can map to Workflow Run RO-Crate, not claim a competing general model.

#### 18. `khan2019cwlprov` — CWLProv

- **1–8 Metadata/artifact:** Farah Zaib Khan, Stian Soiland-Reyes, Richard O. Sinnott, Andrew Lonie, Carole Goble, Michael R. Crusoe; *Sharing Interoperable Workflow Provenance: A Review of Best Practices and Their Practical Application in CWLProv*; November 2019; *GigaScience* 8(11); [DOI](https://doi.org/10.1093/gigascience/giz095); peer-reviewed journal article; [profile/project materials](https://www.commonwl.org/).
- **9–11 Overlap/difference/evidence:** Overlaps F8 through prospective/retrospective workflow provenance, inputs/outputs, logs, agents, software, and interoperable packaging. It is CWL/workflow provenance, not Slurm service-to-step identity or a compiler. Evidence: provenance requirements/model, implementation/application, and best-practice/limitations sections.
- **12–15 Classification/use/baseline:** `HIGH` for provenance, threat `HIGH` to generic traceability claims. Use as canonical provenance limitation/interoperability context. Not comparable as an orchestration baseline.
- **16 Verification:** DOI, authors, venue, full text, and publication type verified. Workflow Run RO-Crate is a later related profile family, not a duplicate.
- **17 Positioning:** “Workflow provenance.” Existing interoperable provenance constrains hpc-compose to a bounded scheduler/compiler linkage.

#### 19. `chirigati2016reprozip` — ReproZip

- **1–8 Metadata/artifact:** Fernando Chirigati, Rémi Rampin, Dennis Shasha, Juliana Freire; *ReproZip: Computational Reproducibility with Ease*; June 2016; SIGMOD 2016, pp. 2085–2088; [DOI](https://doi.org/10.1145/2882903.2899401); peer-reviewed ACM conference/demo paper; [project](https://www.reprozip.org/).
- **9–11 Overlap/difference/evidence:** Overlaps F8 through tracing and packaging an execution's software/data dependencies for reproduction. It does not model service topology, Slurm allocation/steps, generated-script source mapping, or scheduler evidence. Evidence: short paper's system workflow, packaging/unpacking, and demonstration sections.
- **12–15 Classification/use/baseline:** `CONTEXT`, threat `MODERATE` to broad reproducibility/package claims. Use as canonical reproducibility boundary. Not an orchestration baseline.
- **16 Verification:** DOI, authors, pages, project, and publication type verified. No claim of complete scientific reproducibility is inferred.
- **17 Positioning:** “Reproducibility packaging.” ReproZip shows environment/dependency packaging is established; hpc-compose's artifact record is narrower.

#### 20. `namiki2025scuphpc` — SCUP-HPC

- **1–8 Metadata/artifact:** Yuta Namiki, Takeo Hosomi, Hideyuki Tanushi, Akihiro Yamashita, Susumu Date; *SCUP-HPC: System for Constructing and Utilizing Provenance on High-Performance Computing Systems*; 2025; *IEEE Access* 13, 141090–141107; [DOI](https://doi.org/10.1109/ACCESS.2025.3597361); peer-reviewed journal article; operational deployment documented by the [University of Osaka](https://www.hpc.cmc.osaka-u.ac.jp/en/system/manual/octopus2-use/dps4h/).
- **9–11 Overlap/difference/evidence:** Directly overlaps F8: Slurm job definition/ID, execution process/environment, program/source, and file relationships are captured and queried. It needs a site-wide tracer, database, and infrastructure and does not preserve hpc-compose's source-field/plan/render transformation or service readiness. Evidence: architecture §§III–IV and evaluation §V/Tables 2–3; reported runtime increase is 1.00–2.84% on the evaluated small Slurm cluster.
- **12–15 Classification/use/baseline:** `DIRECT` for evidence, threat `CRITICAL` to “first Slurm provenance/evidence continuity.” Use as lead HPC provenance comparison. Descriptive unless privileged site deployment is available.
- **16 Verification:** Exact DOI metadata, paper design/evaluation, and operational page verified. No public code artifact was found; production retention/query policy remains unclear.
- **17 Positioning:** “HPC provenance.” SCUP-HPC is broader site instrumentation; hpc-compose can claim only compiler-owned byte/semantic linkage and explicit evidence missingness.

#### 21. `eng2024composepatterns` — empirical Compose patterns

- **1–8 Metadata/artifact:** Kalvin Eng, Abram Hindle, Eleni Stroulia; *Patterns of Multi-container Composition for Service Orchestration with Docker Compose*; published 2024-05-03; *Empirical Software Engineering* 29(3), article 65; [DOI](https://doi.org/10.1007/s10664-024-10462-8); peer-reviewed journal article; [replication package](https://doi.org/10.5281/zenodo.10648448).
- **9–11 Overlap/difference/evidence:** Overlaps F1–F2 as an empirical corpus of successful multi-container compositions and recurrent patterns. It does not execute on HPC or lower to Slurm. Evidence: study design/dataset, pattern-mining methodology, results, threats, and replication material.
- **12–15 Classification/use/baseline:** `CONTEXT`, threat `MODERATE` to an ungrounded expressiveness story. Use as the public workload/pattern corpus for RQ3. Not a system baseline.
- **16 Verification:** DOI, authors, article number/date, study, and replication DOI verified. The older Ibrahim et al. 2021 Compose study is screened as overlapping corpus context.
- **17 Positioning:** “Empirical application structures.” Use observed patterns to justify and test the accepted/rejected subset rather than inventing examples.

### 4.2 Preprint

#### 22. `madonna2026sarussuite` — Sarus Suite

- **1–8 Metadata/artifact:** Alberto Madonna, Matteo Chesi, Gwangmu Lee, Michele Brambilla, Fawzi Roberto Mohamed, Felipe A. Cruz; *Sarus Suite: Cloud-native Containers for HPC*; arXiv v1 submitted 2026-04-18; arXiv:2604.17064, cs.DC; [primary arXiv record](https://arxiv.org/abs/2604.17064); preprint, not verified as peer reviewed; [project](https://sarus-suite.github.io/) and [evaluation artifact](https://github.com/sarus-suite/cug26-artifacts).
- **9–11 Overlap/difference/evidence:** Overlaps F2–F4, F7, and F9 through a declarative environment definition, unmodified Podman, hooks/CDI/site policy, a Slurm SPANK/Skybox path, validation, and a separate Kubernetes-manifest multi-container path. The Slurm path uses one container environment per allocated node while the multi-container example uses `sarusctl`; the paper does not combine them into native service steps. Evidence: architecture/runtime-integration sections, Kubernetes-manifest demonstration, evaluation, and limitations; evaluated against Enroot+Pyxis on a Cray EX GH200 system.
- **12–15 Classification/use/baseline:** `HIGH`, threat `HIGH` to runtime/site-policy and broad multi-container-HPC wording. Use as current substrate/architectural contrast. Runtime baseline for container startup/performance only; not a service-orchestration baseline.
- **16 Verification:** arXiv metadata/category/authors/date and project artifact verified. No archival version was found as of the search date; rerun before submission.
- **17 Positioning:** “Container substrates and integration.” Sarus Suite demonstrates scheduler-native, upstream-aligned HPC containers and adjacent multi-container manifests; hpc-compose supplies the application-level service compiler above that layer.

### 4.3 Authoritative software and documentation

#### 23. `docksing2025` — DockSing 0.2.36

- **1–8 Metadata/artifact:** Package metadata names G. Angelotti as author and PyPI account `jhn-nt` as verified maintainer; *DockSing: CLI Utility for deployment of containerized jobs on SLURM HPCs*; version 0.2.36 released 2025-07-25; [PyPI primary page](https://pypi.org/project/docksing/); official package documentation/software, not peer reviewed; source distribution and wheel are on PyPI; no public repository or DOI was found.
- **9–11 Overlap/difference/evidence:** Overlaps F1, F3, F5–F7 through a Compose-inspired `slurm`/`container` YAML, local/remote execution, and `--cli` preview of the constructed `srun ... singularity` command. It supports one container and only `working_dir`, environment, volumes, commands, and entrypoint; no named topology/readiness/placement/batch artifact/evidence chain. Evidence: PyPI “Overview,” “Supported Compose Specification,” generated-command example, feature list, and limitations.
- **12–15 Classification/use/baseline:** `DIRECT`, threat `HIGH` to broad Compose-to-Slurm/inspectable-command claims. Use as direct comparison. Executable baseline for the single-container subset only.
- **16 Verification:** Version/date/package contents/behavior verified. PyPI marks the personal author field unverified, so the BibTeX entry intentionally omits an author rather than guessing; package testing is documented only on WSL.
- **17 Positioning:** “Compose-shaped HPC tools.” DockSing already provides inspectable single-container translation; hpc-compose must justify multi-service lifecycle and standalone artifact/evidence semantics.

#### 24. `interlink2026slurmplugin` — InterLink Slurm Plugin

- **1–8 Metadata/artifact:** InterLink project contributors; *InterLink Slurm Plugin*; release 0.6.1 published 2026-04-08, source inspected at commit `f8451d93e063b7addc606d227a525fe614d91958`; official open-source software/docs, not a verified peer-reviewed system paper; [repository](https://github.com/interlink-hq/interlink-slurm-plugin) and [project docs](https://interlink-project.dev/docs/); MIT license; no DOI or complete personal author list.
- **9–11 Overlap/difference/evidence:** Overlaps F2–F4 and F6–F9: a multi-container Pod with init/application containers, startup/readiness/liveness probes and lifecycle hooks becomes one generated/submitted Slurm job; scripts, runtime policy, cleanup, Pod UID, and Slurm job ID/state are retained. It requires Kubernetes, Virtual Kubelet, InterLink API/provider services; containers are background shell processes, not native `srun` service steps; CPU/memory are collapsed job-wide and no user-facing source map exists. Evidence: README architecture/annotations and pinned `pkg/slurm/Create.go` plus `pkg/slurm/prepare.go`, which create scripts, translate probes, launch containers, aggregate exits, clean up, and record IDs.
- **12–15 Classification/use/baseline:** `DIRECT`, threat `CRITICAL`. Use as the lead controller-based direct comparison. Conditionally executable only if the full Kubernetes/InterLink/Slurm stack can be deployed; otherwise descriptive.
- **16 Verification:** Current code and official docs verified. An archival system paper and complete author list were not found; release facts are mutable.
- **17 Positioning:** “Kubernetes-to-batch bridges.” InterLink already combines lifecycle/probes/scripts/state; hpc-compose's remaining distinction is native service steps, complete standalone artifact, strict rejection, and source/evidence explanation without the resident stack.

#### 25. `isct2026minicompose` — TSUBAME4 Mini Compose

- **1–8 Metadata/artifact:** Institute of Science Tokyo, Center for Information Infrastructure, TSUBAME Computing Services; *Mini Compose*; official page last modified 2026-08-06; [primary documentation](https://www.t4.cii.isct.ac.jp/docs/all/experimental/mini-compose/); experimental site software documentation, not peer reviewed; no public repository, version, license, DOI, or personal authors were found.
- **9–11 Overlap/difference/evidence:** Overlaps F1–F2 and F7: reads `docker-compose.yml`, resolves `depends_on`, imports/builds images, runs multiple Docker/Singularity/Apptainer services, executes `healthcheck.test`, reports health, injects environment, stops the group, and supports port offsets. It runs a Flask API/optional PortShift daemon; users write the TSUBAME job script; topology appears node-local; no allocation/service resource split, native-step compiler, source map, or identity chain. Evidence: official §§4.1 and 4.4–4.9; node-locality is an inference from documented `HOST_IP`, `/etc/hosts`, and per-host PortShift behavior.
- **12–15 Classification/use/baseline:** `DIRECT`, threat `CRITICAL` to Compose/readiness/runtime claims. Use as a mandatory direct comparison. Descriptive only because no portable public artifact was found.
- **16 Verification:** Current authoritative docs and modification date verified. Code, license, detailed failure semantics, performance, and multi-node behavior remain unresolved.
- **17 Positioning:** “Compose-shaped HPC tools.” Mini Compose already brings dependencies/health checks to HPC runtimes; hpc-compose must differentiate on static Slurm lowering and first-class artifacts/evidence.

#### 26. `hpe-urika-capsules-slurm-carrier` — HPE Slurm Carrier for Capsules

- **1–8 Metadata/artifact:** Hewlett Packard Enterprise; *Slurm Carrier for Capsules* in the *HPE Cray EX Urika Analytic Applications Guide* 1.4, document S-8006; [primary Slurm Carrier page](https://support.hpe.com/hpesc/public/docDisplay?docId=a00115103en_us&docLocale=en_US&page=Slurm_Carrier_for_Capsules.html) and [Urika Manager CLI](https://support.hpe.com/hpesc/public/docDisplay?docId=a00115103en_us&docLocale=en_US&page=Urika_Manager_CLI.html); proprietary official product documentation, not peer reviewed; no DOI/public code; the live guide exposes no reliable publication date.
- **9–11 Overlap/difference/evidence:** Strongly overlaps F2–F4 and F6/F8/F9: manifest payloads include commands/dependencies/resources; a first allocation payload can create one shared allocation and subsequent payloads run one per Slurm job step; resources map to nodes/tasks/CPUs/memory and inconsistencies are checked; batch mode generates `sbatch` and invokes payloads through `srun`; IDs/versions/status/history/logs/kill are retained. It is proprietary and not Compose-shaped; readiness/source mapping/content digests are undocumented; wider Urika services may remain resident. Evidence: the two official guide sections named above.
- **12–15 Classification/use/baseline:** `DIRECT`, threat `CRITICAL` to one-allocation/one-component-per-step/resource-validation/script/evidence novelty. Use as mandatory conceptual comparison. Descriptive only.
- **16 Verification:** Functional claims verified against primary HPE docs. Date, personal authors, license, public artifact, evaluation, precise container behavior, and readiness remain unresolved. It is omitted from BibTeX because the required year/date cannot be verified without invention.
- **17 Positioning:** “Allocation-scoped payload systems.” HPE already establishes the central Slurm mechanism; hpc-compose can claim only its bounded Compose/service semantics, source-explainable static lowering, and controller-free contract.

#### 27. `tadaam2026iops` — IOPS 3.5.8

- **1–8 Metadata/artifact:** TADAAM — Inria Bordeaux; *IOPS: A Generic Benchmark Orchestration Framework*; version 3.5.8 released 2026-06-20; [official docs](https://iops-benchmark.com/), [PyPI](https://pypi.org/project/iops-benchmark/), and [source](https://gitlab.inria.fr/lgouveia/iops); official beta software/docs, not peer reviewed for current features; BSD-3-Clause per project materials; no DOI for this release.
- **9–11 Overlap/difference/evidence:** Overlaps F3 partially and F4/F6/F8/F9: YAML experiment DSL, `slurm_options.allocation.mode: single`, generated `__iops_kickoff.sh`, exact rendered test/allocation scripts, parameters/status/system metadata, `check`/dry-run, and rejection of incompatible Bayesian/single-allocation mode. Official docs say tests run sequentially in generated Bash; the shown MPI `srun` is supplied by the user inside a test script. A runner monitors status; there is no concurrent service readiness/shared lifetime, automatic per-test native step, or stable field source map. Evidence: official “Single-Allocation Mode,” “Running MPI Programs,” “Writing Scripts,” “Metadata Files,” “Bayesian Optimization,” and “Quick Start” sections.
- **12–15 Classification/use/baseline:** `HIGH`, threat `HIGH` to one-allocation script generation, validation, and evidence novelty, but not to native-step lowering. Use as mandatory nearest neighbor. Executable baseline for script generation/validation, queue amortization, and status/evidence, not concurrent services or per-unit `srun` behavior.
- **16 Verification:** Version/date/organization/license/features/paths verified from official docs/PyPI. No archival paper supporting the current generic/single-allocation behavior was found.
- **17 Positioning:** “Experiment campaign systems.” IOPS is the closest unreviewed script/evidence neighbor; hpc-compose must show that service lifecycle/source mapping materially changes the problem.

#### 28. `dinatale2017maestro` — Maestro Workflow Conductor

- **1–8 Metadata/artifact:** Francesco Di Natale; *Maestro Workflow Conductor*; LLNL software record June 2017, current release 1.2.0 published 2026-03-27; [OSTI record](https://www.osti.gov/biblio/1372046), [LLNL page](https://computing.llnl.gov/projects/maestro-workflow-conductor), [docs](https://maestrowf.readthedocs.io/en/stable/), and [code](https://github.com/LLNL/maestrowf); official research software, not a verified peer-reviewed paper; MIT license.
- **9–11 Overlap/difference/evidence:** Overlaps F1–F2 and F4/F6–F9 through YAML studies, parameter expansion, DAG dependencies, generated per-step scripts, Slurm/LSF/Flux/local adapters, isolated workspaces, monitoring/status/cancellation. A Conductor daemon discovers ready steps, submits jobs, and polls status; units are normally separate jobs, not one service allocation. Evidence: LLNL overview and diagrams, official “Scheduling Studies,” `ExecutionGraph.generate_scripts`, conductor CLI, and Specification 1.0.
- **12–15 Classification/use/baseline:** `HIGH`, threat `HIGH` to YAML/script/site/evidence claims. Use as a direct campaign/workflow comparison. Executable partial baseline for script generation/parameterization/result layout, not readiness-coupled services.
- **16 Verification:** Software record, project, current release, docs, code, and license verified. No archival Maestro paper was found; access date is required for behavior.
- **17 Positioning:** “Workflow conductors.” Maestro already turns declarative studies into scheduler scripts; hpc-compose's possible gap is one complete controller-free service allocation with field mapping.

#### 29. `nvidia2026pyxis` / `nvidia2026enroot` — Pyxis/Enroot stack

- **1–8 Metadata/artifact:** NVIDIA; *Pyxis: Container Plugin for Slurm Workload Manager*, version 0.24.0 released 2026-05-12, [repository](https://github.com/NVIDIA/pyxis); and *Enroot*, version 4.2.1 released 2026-06-09, [repository](https://github.com/NVIDIA/enroot). Authoritative open-source software/docs, not peer-reviewed system papers; Apache-2.0 for Pyxis and project-stated licenses for Enroot.
- **9–11 Overlap/difference/evidence:** Overlaps F3, F4, and especially F7: Pyxis adds container arguments directly to `srun`, supports unprivileged tasks, shared filesystems, image caching, and multi-node MPI through PMI/PMIx; Enroot supplies daemonless unprivileged container sandboxes. They do not provide a service topology, readiness/failure state machine, allocation compiler, source map, or run-evidence model. Evidence: current repository READMEs, release records, usage and installation/limitations sections.
- **12–15 Classification/use/baseline:** `CONTEXT`, threat `HIGH` to generic Slurm container/site portability novelty and `LOW` to the combined application claim. Use as enabling substrate and runtime control. Executable backend baseline, not orchestration baseline.
- **16 Verification:** Versions/release dates/current docs verified. Pyxis requires cluster installation and Slurm-version compatibility; site setup must be reported. Two BibTeX records are used because they are separately versioned projects.
- **17 Positioning:** “Runtime substrates.” hpc-compose should claim policy-aware use of this stack, not invention of native container steps or daemonless execution.

#### 30. `apptainer2026` — Apptainer 1.5.3

- **1–8 Metadata/artifact:** Apptainer Project; *Apptainer User Guide*; release 1.5.3 published 2026-07-21; [official documentation](https://apptainer.org/docs/user/latest/) and [source/release](https://github.com/apptainer/apptainer/releases/tag/v1.5.3); authoritative open-source software/docs, not a paper for the current release; BSD-3-Clause.
- **9–11 Overlap/difference/evidence:** Overlaps F7 and runtime portions of F3 through unprivileged HPC container execution, image formats, binds/environment, and scheduler-friendly launch. It is a runtime, not a multi-service application compiler, resource/placement model, source mapper, or evidence system. Evidence: current user guide execution, bind/environment, security, and scheduler-oriented usage sections.
- **12–15 Classification/use/baseline:** `CONTEXT`, threat `LOW` overall but decisive against portable-HPC-container novelty. Use as enabling substrate. Executable backend control only.
- **16 Verification:** Version/date/docs/repository/license verified. The canonical 2017 Singularity paper is useful historical context but was not duplicated because Singularity Compose and the current Apptainer source cover the retained claims.
- **17 Positioning:** “Runtime substrates.” Apptainer supplies portable container execution; hpc-compose's contribution, if any, lies above it in application semantics and deterministic lowering.

## 5. Novelty-threat matrix

| Claim fragment | Strongest prior system/coalition | Verdict | Evidence and safe wording |
| --- | --- | --- | --- |
| F1. Constrained Compose-style application model | Singularity Compose; Mini Compose; DockSing; adjacent InterLink/HPK Pod models | `ALREADY_ESTABLISHED` | Named services, lifecycle, dependencies, and real Compose parsing already exist. Safe: “hpc-compose defines and rejects a specific Compose-derived subset whose semantics are chosen for Slurm lowering.” |
| F2. Topology, dependencies, readiness, failure, cleanup, shared lifetime | Mini Compose health/dependencies; InterLink probes/hooks/exit aggregation/cleanup; SAIA service health; workflow failure policies | `ALREADY_ESTABLISHED` broadly; `COMBINATION_CLAIM_ONLY` for the exact state machine | Do not claim dependencies, health checks, or cleanup. Safe: “the bounded service state machine is deterministically realized by native Slurm steps.” |
| F3. One allocation containing native `srun` service steps | HPE Capsules; Executorlib; QCG/Flux/RADICAL; ordinary Slurm scripts | `ALREADY_ESTABLISHED` | One allocation and native steps are prior mechanisms. Safe: “hpc-compose makes one allocation/one service-step mapping an invariant of its bounded source model.” IOPS is not evidence for automatic per-test `srun` after verification. |
| F4. Allocation-level versus per-service resources and placement | HPE payload/allocation checks; Executorlib per-call resources; Flux hierarchy; QCG/HyperQueue scheduling; workflow grouping | `ALREADY_ESTABLISHED`; exact strict mapping is `COMBINATION_CLAIM_ONLY` | Avoid “first hierarchical model.” Claim and evaluate the exact validation, placement, and rejection rules. |
| F5. Static standalone compilation without resident/nested scheduling | Close generators retain a Kubernetes, conductor, runner, pilot, socket coordinator, or do not encode service readiness; handwritten Slurm is the control | `POSSIBLE_GAP` | Safe: “the complete finite service state machine executes from an ordinary generated script after the compiler exits.” Formalize and test this; “daemonless” alone is established. |
| F6. Inspectable script and field-to-script explanation | InterLink, HPK, HPE, IOPS, Maestro, Pegasus, ReFrame, DockSing preview | Script visibility `ALREADY_ESTABLISHED`; stable source map `POSSIBLE_GAP` | Safe: “a versioned mapping links effective fields and normalized plan nodes to generated regions and exact submitted bytes.” Measure mapping coverage/correctness. |
| F7. Runtime/backend/site policy | Sarus Suite, Pyxis/Enroot, Apptainer, Slurm OCI, Maestro, workflow site profiles | `ALREADY_ESTABLISHED` | Keep as engineering scope/evaluation axis, not novelty. Safe: “policy-bounded lowering exposes unsupported site/runtime combinations.” |
| F8. Spec/plan/script identity to job/step/log/metric/artifact evidence | SCUP-HPC; Workflow Run RO-Crate; CWLProv; InterLink/HPE/IOPS/dagster artifacts; native Slurm | `COMBINATION_CLAIM_ONLY` | General provenance is established. A narrow residue is compiler-owned digest continuity and service-to-step attribution with explicit missingness. Demote unless requeue, mutation, accounting delay, and loss are evaluated. |
| F9. Progressive checks before quota-consuming execution | IOPS check/dry-run/rejection; ReFrame; HyperQueue explain; HPE/Sarus resource/runtime validation | `ALREADY_ESTABLISHED` | Treat as a product/evaluation structure. Any quota-saving or reliability benefit needs measured fault injection. |

Overall verdict: **`COMBINATION_CLAIM_ONLY`**. F5 and the source-map portion of F6 are the only mechanisms that remain `POSSIBLE_GAP`; they still require an explicit model and discriminating evaluation.

## 6. Direct comparison matrix

To keep unlike meanings visible, the required axes are split into two aligned tables. “Static” describes when topology is decided, not whether shell processes are short- or long-lived. “One job” is not treated as equivalent to “one allocation with one native service step per component.”

### 6.1 Semantics and execution axes (1–9)

| Source | User abstraction / primary unit | Allocation model | Static topology or dynamic scheduling | Lifecycle/readiness | Dependencies/failure | Allocation vs unit resources | Placement/multi-node | Resident/nested controller or standalone artifact |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [InterLink](https://github.com/interlink-hq/interlink-slurm-plugin) | Kubernetes Pod / container | One Pod → one Slurm job | Pod fixed; resident system reconciles | init plus startup/readiness/liveness probes and hooks | exit aggregation and cleanup | container requests collapsed to job-wide values | job-level Slurm placement; container shell processes | Kubernetes + Virtual Kubelet + InterLink/API sidecar; generated scripts |
| [HPE Capsules](https://support.hpe.com/hpesc/public/docDisplay?docId=a00115103en_us&docLocale=en_US&page=Slurm_Carrier_for_Capsules.html) | Capsule manifest / payload | independent allocations by default; optional one shared allocation | manifest fixed; carrier/manager executes | payload lifecycle; readiness not documented | manifest dependencies/status/kill; health semantics unclear | explicit allocation payload plus payload resources and consistency handling | one payload per Slurm job step; multi-node fields | generated `sbatch`/`srun`; wider proprietary manager services |
| [Mini Compose](https://www.t4.cii.isct.ac.jp/docs/all/experimental/mini-compose/) | real Compose / service | runs inside a user-authored HPC job | topology fixed; Flask controller runs it | Compose health checks and start/stop | `depends_on`; documented status check is limited | no Slurm allocation/service split | apparently host-local; port offsets/hosts mapping | resident Flask and optional PortShift; no generated Slurm artifact |
| [HPK](https://doi.org/10.1145/3731599.3767352) | Kubernetes workload/service / Pod | each workload/deployment becomes a Slurm job | Kubernetes desired state; resident custom kubelet | Kubernetes container lifecycle/state synchronization | Kubernetes/job failure and cleanup | Pod requests map to each job | parent/child Apptainer containers; networking/storage adaptations | user-level Kubernetes control plane/custom kubelet; generated scripts per workload |
| [Singularity Compose](https://doi.org/10.21105/joss.01578) | Compose-like file / service instance | host-local, not Slurm | static named services | `up/down/restart`; no health-gated readiness contract | start ordering through dependencies | no Slurm resource layers | one host/bridge network | CLI/controller manages persistent instances; no batch artifact |
| [DockSing](https://pypi.org/project/docksing/) | Compose-inspired YAML / one container | direct `srun`, not a generated application batch allocation | static one-container command | process/log streaming only | no service graph | one Slurm map plus one container; no service hierarchy | fields pass to direct `srun` | compiler/SSH client exits or streams; previewed command |
| [IOPS](https://iops-benchmark.com/) | YAML experiment / test trial | single-allocation mode available | trials sequential in generated Bash | trial completion, not service readiness | sequential failure/status; incompatible adaptive mode rejected | allocation sized by largest test; test descriptions separate | user test script may invoke MPI/`srun`; not auto-generated per test | generated kickoff plus external runner/status monitoring |
| [Sarus Suite](https://arxiv.org/abs/2604.17064) | EDF or Kubernetes manifest / container | Slurm Skybox path; separate multi-container path | declarative runtime, not service scheduler | staged container startup; no combined service-readiness contract | runtime validation/cleanup | job/runtime resources, not Compose service hierarchy | one environment/container per allocated node in Skybox | runtime/site layers; no application-level controller claim |
| [dagster-slurm](https://doi.org/10.21105/joss.09795) | Dagster asset graph / asset | stable one job per asset; Ray run-scoped and experimental shared/HET modes | dynamic asset execution | asset/job state, not service readiness | Dagster dependency/retry semantics | asset resources; experimental shared shapes | launcher/site configuration | resident Dagster control plane |
| [SmartSim](https://doi.org/10.1016/j.jocs.2022.101707) | Python experiment / simulation or database service | scheduler-managed components; not invariant one allocation | orchestrator-driven | database/service startup and monitoring | specialized experiment failure behavior | component-specific resources | scheduler/platform launchers; multi-node simulations | Python orchestrator and database service |
| [SAIA](https://doi.org/10.1007/s11227-026-08508-3) | service catalog/API / service instance | each instance is a separate Slurm job | dynamic demand-based job pools | health checks, renewal, scale-to-zero | scheduler replaces/retains pools; failures may require retry | per-service-job resources | Slurm jobs on suitable nodes; routed ports | resident scheduler/proxy/web plane |
| [Flux](https://doi.org/10.1016/j.future.2020.04.006) | job/resource graph / job | nested instance within allocation | dynamic hierarchical scheduling | job lifecycle, not service readiness | scheduler/KVS/event state | hierarchical resource sets | fine-grained CPU/GPU/node placement | nested Flux resource manager/scheduler |
| [HyperQueue](https://doi.org/10.1016/j.softx.2024.101814) | task graph / task | one/few allocations with workers | dynamic load balancing | task lifecycle | dependencies/failure policy | per-task arbitrary resources inside worker pool | worker/task placement across nodes | resident server/workers |
| [QCG-PilotJob](https://doi.org/10.1007/978-3-030-77977-1_39) | JSON/API task set / task | one manager per allocation | dynamic second-level scheduling | task lifecycle | task dependencies/failures | allocation plus task resources | scheduler/launcher maps tasks to resources | resident pilot manager |
| [RADICAL-Pilot](https://doi.org/10.1007/978-3-031-22698-4_5) | pilot/task API / compute unit | pilot allocation | dynamic agent scheduling | task lifecycle | state/failure handling | pilot resources plus unit requirements | PMIx/PRRTE heterogeneous placement | resident pilot agent/runtime/database |
| [Executorlib](https://doi.org/10.21105/joss.07782) | Python futures / function call | existing Slurm allocation | dynamic submission through executor | call/future lifecycle | dependencies/caching/failure | allocation plus per-call CPU/GPU/memory/time | native `srun` job steps | Python/socket coordinator, not standalone application script |
| [Snakemake](https://doi.org/10.12688/f1000research.29032.3) | file-rule DAG / rule or group | usually job per rule; group can share one job | dynamic controller with static/derived DAG | rule completion; pipes co-run producer/consumer | file dependencies/retries/failure | group-resource aggregation over rules | Slurm plugin/profiles; group placement | resident Snakemake controller |
| [Maestro](https://www.osti.gov/biblio/1372046) | YAML study / parameterized step | normally job per step/parameter | DAG fixed, Conductor schedules | job state, not service readiness | DAG dependencies/status/cancel | batch and step fields | adapter/launcher (`srun` when requested) | resident Conductor; generated scripts |
| [Workflow Run RO-Crate](https://doi.org/10.1371/journal.pone.0309210) | run crate / workflow run or process | representation only | records plan/run; no scheduling | represents action/status/times | records provenance, not execution policy | schema can describe resources; no Slurm contract | extensible representation | no controller; portable metadata package |
| [SCUP-HPC](https://doi.org/10.1109/ACCESS.2025.3597361) | provenance query / job, program, file | observes Slurm jobs | runtime tracing, not scheduling | process/job observation | not an orchestration policy | records environment/job context | cluster-wide instrumentation | resident privileged tracer/database |

### 6.2 Artifact, policy, evidence, and evaluation axes (10–16)

| Source | Slurm script visibility / source mapping | Unsupported semantics | Runtime/site policy | Run identity/provenance | Metrics/artifacts/degraded states | Measured overhead/scalability | Artifact/license/maintenance |
| --- | --- | --- | --- | --- | --- | --- | --- |
| InterLink | persists `job.slurm`/`job.sh`; no user-facing field source map | Kubernetes/plugin constraints; strict supported-subset boundary not documented | Singularity/Enroot plus annotations/config | Pod UID ↔ Slurm job ID and status | scripts/logs/status; no explicit degraded-evidence vocabulary | no peer-reviewed end-to-end comparison verified | open MIT repo; active 2026; no archival paper |
| HPE Capsules | generated `sbatch`/`srun`; no source map documented | resource consistency can abort/fix up; readiness coverage unknown | HPE/Urika carrier/site product | capsule/payload IDs, versions, launch/status/history | logs/history; degraded states not documented | no public evaluation verified | proprietary guide 1.4; date/public artifact unresolved |
| Mini Compose | user writes job script; no generated Slurm/source map | experimental documented subset; validation boundary unclear | Docker/Singularity/Apptainer modules | controller status only | health/status; dummy-status limitation documented | no performance evaluation found | site-only docs; no public code/license/version |
| HPK | generated scripts are implementation outputs; no source map | Kubernetes/Site limitations documented in papers | Apptainer, networking/storage adapters | Kubernetes state ↔ Slurm jobs/processes | status/output/exit codes; no content-identity/degraded contract | current SCW 2025 evaluation; setup/control-plane included conceptually | open project; 2025 archival paper; active lineage |
| Singularity Compose | configuration visible; no Slurm artifact/source map | basic/under-development semantics | Singularity local runtime | instance names/lifecycle | logs; no scheduler evidence | no Slurm overhead/scaling study | open project/JOSS; current release should be retested |
| DockSing | `--cli` previews command; no annotated batch/source map | only five Compose-like keys; WSL-only test caveat | Docker local → Singularity remote | remote command/log stream, no stable spec/job identity | logs only; no degraded states | no cluster evaluation | PyPI 0.2.36; source dist; author metadata limited |
| IOPS | retains kickoff/test/allocation scripts; no field source map | rejects Bayesian/adaptive with single allocation | Slurm/site scripts | run/status/parameter/system records | exact scripts/status; no service-step evidence vocabulary | campaign behavior documented; no archival current-feature evaluation | BSD project/PyPI 3.5.8; active 2026 |
| Sarus Suite | runtime commands/config inspectable; no application source map | validates runtime/manifest constraints | EDF, Podman, CDI/hooks, Slurm SPANK | runtime/job records, not compiler chain | evaluation artifacts; no application degraded-state model | production-scale benchmarks; matches Enroot+Pyxis and faster startup in tested cases | preprint plus public artifact; archival status pending |
| dagster-slurm | scripts/launchers and Dagster events; no stable field map shown | execution-mode maturity explicit in docs | Pixi/site launchers/Slurm | Dagster run/asset ↔ Slurm job/state | logs, CPU efficiency, memory, elapsed/node-hours | production validation; shared modes experimental | Apache-2.0 project, JOSS 2026, active |
| SmartSim | generated launch details not primary source-mapped artifact | specialized component/runtime constraints | scheduler/platform adapters | experiment/entity/job metadata | logs/database/service observations; no degraded contract | climate application/scaling evaluated | open maintained project, archival 2022 paper |
| SAIA | `sbatch` service scripts are implementation details, no source map | Slurm/network/security constraints explicit | SSH/Slurm; Docker Compose web plane | service/job/routing records | latency/throughput/startup/availability; failure limitations explicit | 42 ms simple-request overhead; 400/1000 RPS in tested proxy modes | GPL code and archival 2026 paper |
| Flux | jobs/resource/KVS inspectable through runtime, not standalone `sbatch` source map | plugin/site integration constraints | PMI/hwloc/site plugins | job/event/KVS identity | runtime state/metrics; no compiler degraded states | up to 48× workflow throughput in paper; component overhead reported | open mature project, archival journal paper |
| HyperQueue | task graph/commands visible; server runtime owns execution | task/resource validation and explain/dry run | PBS/Slurm workers, unprivileged | task/job/worker state | outputs/task state; runtime failure policies | SoftwareX evaluation of task-graph execution | open project, peer-reviewed 2024, maintained |
| QCG-PilotJob | submitted task descriptions, no standalone source map | local/physical-resource validation differs | Slurm/site launcher adaptation | manager/task state | task records/monitoring; no spec/script content chain | 20k tasks, 99.2% occupied time in paper | open project, archival paper |
| RADICAL-Pilot | profiles/runtime configs, no complete source-mapped batch artifact | pilot/platform constraints | multiple launch methods/platforms | pilot/unit state and profiles | extensive task/runtime profiles | 65.5k tasks/2,048 nodes in retained paper | open research software, archival chapter |
| Executorlib | Python/resource call visible; `srun` generated by executor, no source map | executor/resource constraints | Slurm and alternative backends; nested Flux suggested for throughput | futures/cache/workdir records | task/cache/failure evidence; no degraded vocabulary | JOSS feature validation; benchmark only relevant to launch RQ | open project/JOSS 2025, active |
| Snakemake | job scripts/DAG/dry-run visible; no field-to-shell source map | invalid workflow/resources rejected through engine/plugins | profiles/executor plugins/containers | run UUID, rule/job/report/provenance | traces/reports/outputs; no Slurm service degraded states | scalability reported; plugin behavior mutable | MIT project, versioned peer-reviewed article |
| Maestro | per-step scripts retained; no field source map | spec/schema and adapter checks | Slurm/LSF/Flux/local adapters | study/step/workspace/status | scripts/logs/results/status | no central one-allocation evaluation | MIT LLNL software, release 1.2.0; no peer-reviewed paper |
| Workflow Run RO-Crate | packages plan/run metadata; not a Slurm script generator | profile conformance; minimum metadata permits partiality | engine-neutral extensible profiles | stable entities/actions/agents/inputs/outputs | logs/artifacts may be packaged/referenced; completeness varies | seven implementations; 13/20 provenance subtypes represented fully/partly | open community profiles/tooling, peer-reviewed 2024 |
| SCUP-HPC | observes job definitions/scripts; no source-field transformation map | capture depends on site instrumentation | Linux/Slurm tracer and DB stack | job ↔ program/source/file/environment | detailed provenance; availability depends on tracer/store | 1.00–2.84% runtime increase on evaluated small cluster | peer-reviewed 2025; operational deployment; no public code verified |

## 7. Related-work section architecture

The paper should use five argumentative subsections, followed by one synthesis paragraph/matrix. Avoid a catalog ordered by publication date.

### 7.1 Compose-shaped application orchestration and Kubernetes-to-batch bridges

**Paragraph 1 — service semantics are prior art.** Open with [Singularity Compose](https://doi.org/10.21105/joss.01578), [DockSing](https://pypi.org/project/docksing/), and [Mini Compose](https://www.t4.cii.isct.ac.jp/docs/all/experimental/mini-compose/). Establish that named services, dependency ordering, health checks, lifecycle operations, and Compose-shaped configuration already exist. The paragraph's job is to abandon novelty of the input/lifecycle ingredients while identifying what these systems lack: a validated allocation/service contract, native step mapping, and source-linked batch artifact.

**Paragraph 2 — multi-container intent already reaches Slurm.** Compare [InterLink](https://github.com/interlink-hq/interlink-slurm-plugin) and [HPK](https://doi.org/10.1145/3731599.3767352), then [HPE Capsules](https://support.hpe.com/hpesc/public/docDisplay?docId=a00115103en_us&docLocale=en_US&page=Slurm_Carrier_for_Capsules.html). InterLink/HPK establish generated/tracked Slurm jobs, lifecycle, state, cleanup, and runtime policy through resident Kubernetes infrastructure. HPE establishes the shared-allocation/one-payload-per-step/resource-validation/script/evidence mechanism. The paragraph must end with the exact residual: strict Compose-derived native service steps plus a complete source-explainable artifact without runtime control-plane services.

### 7.2 Allocation-internal schedulers, pilots, and Slurm services

**Paragraph 1 — tasks inside allocations are established.** Group [QCG-PilotJob](https://doi.org/10.1007/978-3-030-77977-1_39), [Flux](https://doi.org/10.1016/j.future.2020.04.006), [Executorlib](https://doi.org/10.21105/joss.07782), [HyperQueue](https://doi.org/10.1016/j.softx.2024.101814), and [RADICAL-Pilot](https://doi.org/10.1007/978-3-031-22698-4_5). State directly that one-allocation execution, dependencies, native steps, per-unit resources, placement, and state are not new. The complementary distinction is dynamic adaptation/throughput versus a complete pre-execution artifact.

**Paragraph 2 — coupled applications and services are also established.** Use [SmartSim](https://doi.org/10.1016/j.jocs.2022.101707), [SAIA](https://doi.org/10.1007/s11227-026-08508-3), and the screened Merlin/OpenVenus work. Distinguish finite readiness-coupled applications from simulation/database controllers, ensemble queues, and persistent service pools built from separate jobs. This prevents the false boundary “workflow systems cannot run services.”

### 7.3 Workflow and experiment compilers

**Paragraph 1 — scripts, checks, and artifacts.** Lead with [IOPS](https://iops-benchmark.com/), [dagster-slurm](https://doi.org/10.21105/joss.09795), [Maestro](https://www.osti.gov/biblio/1372046), and [benchkit](https://doi.org/10.1145/3777884.3796997). Establish that YAML/declarative experiments, generated scheduler artifacts, site adapters, dry-run/validation, and run metadata already exist. Be precise that IOPS executes single-allocation tests sequentially and does not generate native per-test steps.

**Paragraph 2 — canonical workflow engines.** Compare [Snakemake](https://doi.org/10.12688/f1000research.29032.3), [Parsl](https://doi.org/10.1145/3307681.3325400), [Pegasus](https://doi.org/10.1016/j.future.2014.10.008), and screened [Nextflow](https://doi.org/10.1038/nbt.3820). Explain why file/task DAG completion differs from readiness and shared service lifetime, while acknowledging grouped/in-allocation execution, source-generated jobs, and persistent controllers that invalidate overbroad claims.

### 7.4 HPC container runtimes and site policy

Use [Sarus Suite](https://arxiv.org/abs/2604.17064), [Pyxis/Enroot](https://github.com/NVIDIA/pyxis), [Apptainer](https://apptainer.org/docs/user/latest/), and [Charliecloud](https://doi.org/10.1145/3126908.3126925), with native Slurm OCI and CSCS EDF as supporting documentation. Treat these as enabling substrates and explicit portability constraints. Native container steps, daemonless execution, multi-node launch, site hooks, and multiple containers in one job are established. The paper's layer is the service application contract above them.

### 7.5 Run evidence, provenance, and observability

Lead with [SCUP-HPC](https://doi.org/10.1109/ACCESS.2025.3597361) and [Workflow Run RO-Crate](https://doi.org/10.1371/journal.pone.0309210), then [CWLProv](https://doi.org/10.1093/gigascience/giz095), [ReproZip](https://doi.org/10.1145/2882903.2899401), and native Slurm accounting. State that plans/job definitions, scheduler identities, logs, metrics, files, and artifacts already have provenance systems and interoperable representations. Define hpc-compose as a bounded producer/bridge whose distinctive object is its deterministic transformation and semantic service-to-step map, not a new provenance model.

### Closing synthesis

The closing paragraph should explicitly acknowledge complementarity: pilots trade inspectability for dynamic utilization; Kubernetes bridges trade standalone operation for richer orchestration; runtime systems supply site integration; provenance profiles supply interoperable packaging. hpc-compose should be framed as a measured trade-off—less semantic breadth and dynamism in exchange for deterministic rejection, one complete pre-execution artifact, ordinary Slurm execution, and source-explainable service/job-step linkage.

## 8. Evaluation implications

### Fair executable baselines and controls

| Baseline/control | Fair research question | Required fairness constraint |
| --- | --- | --- |
| Expert handwritten `sbatch`/`srun` | semantic correctness, launch/readiness/teardown latency, placement, failure cleanup, script size/clarity | Match allocation, service steps, runtime options, readiness polling, failure policy, logs, and accounting. Use multiple independently reviewed scripts or explain authoring bias. |
| hpc-compose evidence-off/script-only ablation | isolate compilation/runtime overhead and evidence-collection cost | Keep rendered application bytes and runtime behavior identical except the ablated mechanism. |
| Executorlib `SlurmJobExecutor` | native `srun` launch overhead, per-unit resources, concurrency, placement, failure capture | Compare equivalent commands/resources only; do not score absent Compose/source-map semantics. |
| IOPS single-allocation mode | validation/rejection, generation time, queue amortization, retained-script/status completeness | Treat tests as sequential Bash; do not claim it supplies per-test native steps or concurrent services. |
| Singularity Compose | Compose input/lifecycle coverage on a single node | Compare only service/dependency/lifecycle axes; no Slurm placement claim. |
| DockSing | single-container configuration-to-command behavior and preview | Use the supported five-key subset and direct `srun`; do not penalize missing multi-service features it does not claim. |
| Snakemake group jobs/pipes | grouped allocation, resource aggregation, producer/consumer behavior | Declare workflow-specific adaptation and keep the controller cost visible. |
| InterLink or HPK | controller-based Pod-to-Slurm end-to-end behavior | Only if the full control plane can be legitimately deployed; include setup, resident footprint, and operational privileges, not just translation time. |
| dagster-slurm stable mode | Slurm deployment/logging/metrics for assets | Shared/HET modes must be labeled experimental; do not imply general service fusion. |
| Flux/QCG/HyperQueue/RADICAL | task launch throughput/utilization where dynamic scheduling should help | Use only for a predeclared static-versus-dynamic RQ and include control-plane resources. |
| Pyxis/Enroot and Apptainer | backend/site behavior and overhead | Treat as substrates used by both arms, not competing orchestrators. |

The minimum credible executable core is expert Slurm, the evidence-off ablation, Executorlib, IOPS, one Compose-side comparator, and the runtime backends. Add a controller/pilot system only for a research question it can answer fairly.

### Descriptive comparisons only

- HPE Capsules is proprietary and site-specific but mandatory as the closest shared-allocation/native-step precedent.
- Mini Compose has authoritative site documentation but no public portable artifact.
- SAIA is a persistent service platform with separate jobs and an opposite control-plane/allocation goal.
- SCUP-HPC requires privileged, site-wide tracing/database infrastructure and solves a broader provenance problem.
- Workflow Run RO-Crate, CWLProv, and ReproZip are interoperability/packaging references, not schedulers.
- Maestro, SmartSim, Pegasus, and screened Nextflow/Merlin are executable software but are descriptive for the central finite-service RQ unless a separately scoped workload makes them semantically comparable.

### Workloads and corpora

1. **Expressiveness corpus:** sample recurrent patterns and rare/unsupported features from the [Eng et al. replication package](https://doi.org/10.5281/zenodo.10648448). Report accepted-with-equivalent-semantics, rejected-with-actionable-reason, and incorrectly accepted/rejected cases.
2. **Three-component service application:** a long-lived server, a concurrent peer/observer, and a client gated on a real readiness predicate. Include startup-only, health-gated, and completion-gated edges.
3. **Multi-node placement application:** services with distinct CPU/GPU/task/node envelopes and explicit node-index placement; independently inspect Slurm step/node bindings.
4. **Failure corpus:** failure before readiness, after readiness, OOM, nonzero exit, timeout, signal, cancellation, node loss where available, partial startup, and cleanup failure.
5. **Backend/site matrix:** at least Pyxis/Enroot and Apptainer/host across two materially different site policies if feasible. Record unsupported combinations rather than silently narrowing the sample.

### Metrics worth reusing

- compile/validate/render latency and peak memory;
- script bytes/lines/commands and source-map coverage/incorrect mappings;
- submission-to-first-step, submission-to-ready, ready-to-client, and teardown-to-no-process latency; keep scheduler queue wait separate;
- number of allocations, steps, controller processes, resident CPU/memory, and scheduler RPCs;
- step/resource placement correctness and oversubscription/unused-allocation time;
- failure detection, propagation, cleanup completeness, and leaked processes/resources;
- accepted/rejected semantic coverage, diagnostic source accuracy, and false acceptance/rejection;
- effective-spec/plan/script digest continuity and exact submitted-byte recovery;
- job/service-step/log/metric/artifact linkage precision and recall against a ground-truth event log;
- explicit `available/partial/unavailable/stale/unsupported` evidence-state accuracy under accounting delay, purge, permissions, missing logs, requeue, and mutation;
- runtime overhead relative to expert Slurm, with confidence intervals and warmed/cold image-cache regimes.

### Evaluation threats learned from prior work

- Queue time is a site-load property; separate it from compiler, launch, and readiness time.
- Image caches, shared filesystems, runtime hooks, and Slurm accounting plugins can dominate or suppress overhead; disclose them and test cold/warm states.
- Dynamic schedulers should win on irregular workloads; a fair evaluation must include such a workload if claiming a static-design trade-off.
- A stored path is not evidence that a log/artifact exists or is complete. Record existence, collection time, size/digest, truncation, and permission failures.
- Job IDs need cluster, array/heterogeneous component, submission time, and attempt/requeue context; do not assume a globally unique immutable identifier.
- Step accounting is sampled/configuration-dependent, and energy can be job-wide unless the allocation is exclusive. Do not market task aggregates as exact service observability.
- Provenance supports diagnosis/rerun but does not prove scientific reproducibility, deterministic results, or semantic validity.

## 9. Citation-ready positioning statements

1. [Singularity Compose](https://doi.org/10.21105/joss.01578) established Compose-like orchestration for named Singularity instances, including dependency ordering and lifecycle commands, but did not target Slurm allocation or job-step semantics.
2. [InterLink's Slurm plugin](https://github.com/interlink-hq/interlink-slurm-plugin) translates a multi-container Kubernetes Pod with probes and lifecycle hooks into a generated, tracked Slurm job; its resident Kubernetes/InterLink stack and shell-launched containers distinguish it from a standalone native-step compiler.
3. [Mini Compose](https://www.t4.cii.isct.ac.jp/docs/all/experimental/mini-compose/) demonstrates that real Compose dependencies and health checks can operate with Docker, Singularity, and Apptainer inside an HPC job, while requiring a resident site controller and user-authored scheduler script.
4. HPE's [Slurm Carrier for Capsules](https://support.hpe.com/hpesc/public/docDisplay?docId=a00115103en_us&docLocale=en_US&page=Slurm_Carrier_for_Capsules.html) already supports manifest payloads sharing one allocation with one payload per Slurm step, resource consistency handling, generated `sbatch`/`srun`, and retained launch records.
5. [Executorlib](https://doi.org/10.21105/joss.07782) launches resource-described Python calls as native `srun` steps inside an existing allocation; hpc-compose therefore cannot claim novelty for native per-unit steps or two resource levels.
6. Pilot and nested-scheduler systems such as [QCG-PilotJob](https://doi.org/10.1007/978-3-030-77977-1_39) and [Flux](https://doi.org/10.1016/j.future.2020.04.006) already execute resource-aware dependent work inside allocations, but retain runtime schedulers rather than producing a complete service artifact before execution.
7. [IOPS](https://iops-benchmark.com/) generates and retains a single-allocation experiment kickoff plus validation/status metadata; its tests run sequentially in Bash, so it is a script/evidence neighbor rather than prior art for automatically generated native per-test steps.
8. [dagster-slurm](https://doi.org/10.21105/joss.09795), [Maestro](https://www.osti.gov/biblio/1372046), and [benchkit](https://doi.org/10.1145/3777884.3796997) make broad novelty claims based on declarative experiments, generated scheduler artifacts, or reproducible Slurm execution unsafe.
9. [Sarus Suite](https://arxiv.org/abs/2604.17064) demonstrates current scheduler-native runtime/site integration and a separate Kubernetes-manifest multi-container path, but the reviewed preprint does not combine that manifest path with native per-service Slurm-step lowering.
10. [SAIA](https://doi.org/10.1007/s11227-026-08508-3) operates health-checked, demand-scaled services as pools of Slurm jobs through a resident scheduler/proxy, showing that “services on Slurm” is established under a different application/control-plane model.
11. [SCUP-HPC](https://doi.org/10.1109/ACCESS.2025.3597361) links Slurm jobs to program/source/file provenance with low reported overhead in its evaluated environment; hpc-compose should claim only its narrower compiler-owned source/plan/script/service-step linkage.
12. [Workflow Run RO-Crate](https://doi.org/10.1371/journal.pone.0309210) and [CWLProv](https://doi.org/10.1093/gigascience/giz095) already provide interoperable run-provenance models, making hpc-compose a potential producer of Slurm-specific evidence rather than a new general provenance standard.
13. **Inference from the reviewed sources:** the defensible contribution is not any ingredient alone, but the evaluated coherence of a strict Compose-derived service contract, static one-allocation/native-step lowering, source-explainable standalone script, and bounded availability-aware evidence without a resident or nested scheduler.

## 10. Bibliography QA

### Entry count and validation

The full inventory contains **30 conceptual source records**: 21 peer-reviewed sources, one preprint, and eight authoritative software/documentation records. Pyxis and Enroot are one stack record in the inventory but two separately versioned BibTeX entries. HPE Capsules is retained in the report but omitted from BibTeX because its live guide exposes no reliable year/date. The candidate bibliography therefore contains **30 verified entries**.

`biber --tool --validate-datamodel` parsed all 30 entries without errors or warnings after conference chapters were represented as proceedings entries. Duplicate-key and duplicate-DOI checks found none. No abstracts, keywords, local paths, or speculative fields are present.

### Version and deduplication decisions

- **HPK:** cite the archival 2025 SC Workshops paper, DOI `10.1145/3731599.3767352`; use arXiv:2409.16919 as accessible design full text in the report. The 2023 foundation, 2024 expansion, and 2025 evaluation are one system lineage, not three independent competitors.
- **Singularity Compose:** cite the 2019 JOSS paper for scholarly claims; the 2024 Zenodo record is a software release only.
- **Snakemake:** cite version 3, DOI `10.12688/f1000research.29032.3`, rather than duplicating versions 1/2 or the older 2012 engine note.
- **Sarus Suite:** retain arXiv:2604.17064 as a clearly labeled preprint because no archival version was found by 2026-08-09. Do not merge its Slurm Skybox path with its separate Kubernetes-manifest path.
- **dagster-slurm:** JOSS metadata supports the paper; current docs support only explicitly marked current/experimental execution modes.
- **Maestro:** use the LLNL/OSTI software record and current official documentation. No peer-reviewed Maestro system paper was found.
- **Pyxis/Enroot:** retain separate versioned entries (0.24.0 and 4.2.1) because they are distinct projects, while discussing their normal stack relationship in one inventory record.
- **SCUP-HPC:** the 2025 IEEE Access paper supersedes the earlier precursor for the retained system claim.
- **SAIA:** retain the 2026 archival paper rather than the earlier Chat AI preprint.
- **Workflow Run RO-Crate and CWLProv:** retain both because they are distinct profile/model generations and the later paper directly evaluates their overlap.

### Metadata gaps and intentional BibTeX omissions

| Source/lead | Decision and reason |
| --- | --- |
| HPE Slurm Carrier for Capsules | Retained in report, no BibTeX: corporate author/guide/version/URL are verified, but no reliable publication year/date is exposed. |
| OpenVenus, DOI `10.1007/978-3-031-28124-2_13` | Open lead, no BibTeX: publisher metadata/abstract verified, but full design/evaluation/limitations were not read; detailed overlap claims would exceed the evidence. |
| DockSing author | BibTeX omits author: PyPI exposes “G. Angelotti” but labels it unverified; version/date/title/package URL are verified. |
| InterLink personal authors | Organization-only software entry: no archival system paper or complete contributor author list was verified. |
| Mini Compose | Organization/date/official URL retained; no public source version, license, DOI, or personal authors found. |
| Nextflow | Required neighborhood covered and canonical paper verified ([DOI](https://doi.org/10.1038/nbt.3820)); omitted from the 30-entry candidate file because its one-job-per-process/controller class is already represented more closely by Snakemake, Parsl, and Pegasus. |
| Merlin | Required specialized-HPC neighborhood covered ([DOI](https://doi.org/10.1016/j.future.2022.01.024)); omitted after verification because its ensemble/controller/resource story is redundant with retained SmartSim and Flux for this paper's claims. |
| Sarus 2019 base-runtime paper | Verified but omitted to avoid duplicating runtime history; current Sarus claims use Sarus Suite, while Charliecloud, Pyxis/Enroot, and Apptainer cover the required substrate comparisons. |
| ReFrame, JUBE, signac/Row, Swift/T, StreamFlow, Balsam | Verified/screened class representatives; omitted from the final 30 because retained IOPS/Maestro/benchkit/Executorlib/HPK sources are closer on the claimed axes. Reintroduce only if the paper makes validation, compiled dataflow, or hybrid deployment a central RQ. |
| W3C PROV/base RO-Crate/generic provenance surveys | Supporting standards/context, not retained system candidates; Workflow Run RO-Crate, CWLProv, ReproZip, and SCUP-HPC are the sharper claim limiters. |

No retraction or correction affecting a retained result was found. This is not a guarantee; recheck Crossmark/publisher notices at paper freeze.

## 11. Open leads and stopping rationale

### Open or lower-confidence leads

1. **InterLink publication lineage:** search again for an archival InterLink/Slurm system paper and a citable author list. Current source is stronger technical evidence than available publication metadata.
2. **HPE guide provenance:** obtain a versioned PDF/date and clarify whether shared-allocation payload dependencies include health/readiness and whether payloads are normally containers.
3. **Mini Compose artifact:** check for a public repository, module version, license, multi-node guidance, and a technical report.
4. **Sarus Suite archival version:** search CUG/SC/ISC proceedings and forward citations; replace the preprint if an archival paper appears.
5. **OpenVenus full text:** obtain and inspect the complete paper before using anything beyond abstract-level service-startup claims.
6. **CSCS Container Engine/EDF:** a close runtime/site-policy lead; retain official docs or its 2024 paper if site-policy compilation becomes a primary contribution.
7. **Provenance systems:** HyProv, PROV-IO+, MIDA, Complete Provenance, and ProvBench become necessary if the paper expands from compiler-owned evidence linkage to online observability, fine-grained I/O, hardware metadata, or general provenance.
8. **Recent utilities:** `scitex-agent-container`/`scitex-hpc` and `lqck` occupy already-covered reservation/service-reuse classes but merit a freeze-date check because Slurm service tooling is moving quickly.
9. **Artifact executability:** rerun DockSing, Singularity Compose, Executorlib, IOPS, dagster-slurm, HPK/InterLink where feasible, and all selected runtimes on the actual evaluation cluster before declaring them runnable baselines.

### Rejected false positives and why

- Docker/Podman Compose specifications define source semantics but do not lower applications to Slurm.
- Slurm-in-Docker/Compose repositories create test clusters rather than compile user applications for an existing HPC site.
- Kompose converts Compose to Kubernetes, not to one Slurm allocation/native steps.
- Generic `sbatch` generators and SSH wrappers describe one conventional job/command without service topology, readiness, or evidence continuity.
- Slurm/Kubernetes administration operators, Slinky, ephemeral Kubernetes clusters, and co-scheduling plugins change cluster/control-plane administration and violate the application-scoped non-goals.
- KNoC, WLM/Torque Operator, and Bridge Operator are predecessors or less close members of the retained HPK/InterLink bridge class.
- Generic workflow applications and provenance surveys were excluded unless they changed a claim, baseline, or evaluation choice.

### Why the search is saturated enough for drafting

The last direct new classes were HPE Capsules in the shared-allocation query pass and the archival HPK 2025 paper in Wave 2 citation chaining. After integrating those results and correcting IOPS:

1. a direct-conjunction pass combining 2022–2026, Slurm, generated scripts, Compose/multi-container, readiness, `srun`, and one allocation found only retained systems or already-represented reservation/service utilities;
2. an exact Compose/Slurm pass found base Slurm docs, Compose-based test clusters, generic generators, Sarus Suite, and known neighbors, with no new `HIGH`/`DIRECT` class; and
3. backward/forward chaining around InterLink, HPK, HPE Capsules, Sarus Suite, Singularity Compose, Executorlib, dagster-slurm, Workflow Run RO-Crate, and SCUP-HPC returned known bridge, pilot, workflow, runtime, and provenance lineages.

Thus at least two consecutive post-discovery passes produced no new high- or medium-proximity system class in every specialist lane, and the independent verifier's final three passes produced no new direct class. The set is saturated enough for related-work drafting, not for a priority claim.

### Before-submission rerun

Repeat DOI/version/release and forward-citation searches near the submission freeze, focusing on InterLink, HPK, Mini Compose, HPE Capsules, Sarus Suite, DockSing, IOPS, dagster-slurm, and 2026 Slurm-service tools. Re-run BibTeX validation and recheck retractions/corrections. If the manuscript's contribution wording changes, rerun a targeted adversarial search against the new wording rather than assuming this report still covers it.

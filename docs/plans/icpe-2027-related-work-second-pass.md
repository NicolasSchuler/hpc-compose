# ICPE 2027 related-work second pass

- **Search date:** 2026-08-09
- **Purpose:** extend, challenge, and prioritize the first related-work pass with
  recent papers, highly cited canonical papers, and papers from strong or directly
  relevant venues
- **First-pass report baseline:**
  [ICPE 2027 related-work and novelty-stress report](icpe-2027-related-work-report.md),
  SHA-256
  `84b63e96028c5d11ef7dae8efb69a43170f208d6aa8e9e0055c179e33724f0a5`
- **First-pass bibliography baseline:**
  [ICPE 2027 first-pass reference candidates](icpe-2027-reference-candidates.bib),
  SHA-256
  `9027323d5b7dd0430c782dc14bfd8d613420b07d2aa45efeedfd6e11d013e6e4`
- **Output bibliography:**
  [ICPE 2027 pass-two reference candidates](icpe-2027-reference-candidates-pass2.bib)
- **Method:** two independent searches, one optimized for 2022--2026 and strong
  HPC/systems venues, the other for citation impact and canonical lineage;
  primary papers or publisher records were used for technical claims

## 1. Executive update

The first pass found the right broad neighborhood and correctly rejected novelty
for Compose syntax, container launch, script generation, workflow orchestration,
allocation-internal task management, and provenance by themselves. The second
pass changes the emphasis in three ways.

1. **Readiness-coupled services inside acquired HPC resources are established.**
   The 2025 RADICAL-Pilot service-task paper supports explicit service lifetime,
   readiness/liveness, start order, resource placement, and concurrent services
   inside pilot resources. Wilkins supports declarative concurrent tasks inside a
   single submitted batch job. These are closer than generic DAG engines.
2. **Inspectable and integrity-checked Slurm planning is no longer a safe broad
   claim.** Drona generates and previews batch scripts from validated declarative
   schemas. BioCodex uses deterministic, hash-checked RunSpec artifacts linked to
   asynchronous Slurm execution. Neither supplies the proposed hpc-compose
   service semantics, but both narrow the inspectability/evidence language.
3. **The canonical lineage needs stronger representation.** The first bibliography
   should be supplemented by foundational Slurm, Singularity, pilot-job, Nextflow,
   AiiDA, RO-Crate, Sarus, and container-orchestration papers. Their citation
   impact makes their omission conspicuous even when they are not direct
   baselines.

No verified paper in this pass matches the complete proposed conjunction:

> a finite typed readiness-coupled service model, statically lowered to one Slurm
> allocation and native steps, with an explicit allocation-resident batch supervisor,
> published rejection/resource semantics, and bounded source/artifact evidence,
> without a separately deployed daemon or nested scheduler.

That is a search result, not proof of novelty. The paper should call it the
**best current novelty hypothesis** until the semantic contract and charitable
comparison matrix establish a precise unmatched invariant.

## 2. Search and evidence protocol

### 2.1 Search lanes

The pass covered:

- Slurm-native or Slurm-bridged multi-container and service execution;
- readiness, liveness, lifecycle, and coupled simulation/ML/data services;
- declarative plan validation, batch-script generation, and inspectability;
- static/no-resident orchestration versus pilots and nested schedulers;
- scheduler integration, workflow/resource-manager responsibility, and runtime
  substrates;
- run identity, provenance, research-object packaging, observability, and capture
  overhead;
- backward chains from Flux, RADICAL-Pilot, HPK, Workflow Run RO-Crate, and
  Executorlib; and forward chains to 2024--2026 work.

### 2.2 Inclusion and venue treatment

Papers were retained when they did at least one of the following:

- directly threatened a thesis phrase;
- established a canonical abstraction with substantial citation uptake;
- appeared in a strong archival venue such as IEEE TPDS/TSE, SC main track,
  HPDC, CCGrid, CLUSTER, FGCS, or ACM Computing Surveys;
- were recent and unusually close, even if published at a relevant workshop or
  practice venue; or
- supplied an evaluation method reusable for conformance, overhead, service
  lifecycle, or provenance studies.

Venue labels remain explicit. An SC or IPDPS workshop paper is not described as
an SC or IPDPS main-track paper. A short PEARC paper is useful current evidence
but receives less weight than a full archival systems study.

### 2.3 Citation-impact method

Citation counts are approximate `cited_by_count` snapshots from OpenAlex on
2026-08-09, not quality scores and not stable identifiers of truth. The
pilot-system survey uses an approximately 85-citation Semantic Scholar snapshot
because its OpenAlex records appear fragmented. Counts are used only to avoid
omitting canonical lineage; all technical statements were checked against
primary papers or publisher metadata.

## 3. Priority A: closest papers, claim limits, and flagship contrasts

These papers should be read and represented before the contribution and novelty
paragraphs are frozen.

| Paper | Verified overlap | Decisive difference | Threat and action |
| --- | --- | --- | --- |
| Merzky et al., “Scalable Runtime Architecture for Data-driven, Hybrid HPC and ML Workflow Applications,” IPDPSW 2025 ([DOI](https://doi.org/10.1109/IPDPSW66978.2025.00150)) | First-class service tasks, readiness/liveness, start relations, monitoring, placement, lifetime, termination, and concurrent services inside acquired pilot resources | Resident RADICAL-Pilot agents, scheduler, and control channels; dynamic runtime; no Compose input or generated native Slurm batch artifact/source map | **Critical.** Rules out novelty for readiness-coupled HPC services. Compare controller locus, static versus dynamic decisions, and artifact visibility. |
| Yildiz et al., “Wilkins: HPC in situ workflows made easy,” Frontiers in HPC 2024 ([DOI](https://doi.org/10.3389/fhpcp.2024.1472719)) | YAML task descriptions, producer/consumer coupling, concurrent tasks, and one submitted SPMD batch job | Shared-object/Henson/MPI communicator partition and dataflow coupling rather than native `srun` service steps and a readiness state machine | **Critical.** Rules out broad “declarative multi-component one-job” novelty. Add to the direct matrix. |
| Colonnelli et al., “StreamFlow,” IEEE TETC 2021 ([DOI](https://doi.org/10.1109/TETC.2020.3019202)) | Declarative multi-container execution environments, communicating tasks, deployment lifecycle, hybrid HPC/cloud | CWL/dataflow controller, connectors, and multi-site movement rather than a static one-allocation artifact | **High.** Prevents dismissing workflows as sequential or unable to host communicating tasks. Promote from screened lead to explicit neighbor. |
| Kryvenko et al., “Drona Workflow Engine,” SC Workshops 2025 ([DOI](https://doi.org/10.1145/3731599.3767431)) | Declarative schemas, validation, maps/templates, one or more generated batch scripts, editable preview, submission, history, and monitoring | Open OnDemand/server-side framework that can generate multiple job scripts, with template-authored mapping; editable preview is not an immutable submitted-byte relation | **Critical** to generic validation, generation, and inspectability claims. Compare field coverage, diagnostics, and preview/submission identity. |
| Ehrett et al., “BioCodex,” PEARC 2026 ([DOI](https://doi.org/10.1145/3785462.3815873)) | Deterministic hash-verified RunSpec, argument validation, tamper rejection, asynchronous Slurm service, status/log retrieval | Agentic genomics jobs rather than multi-service applications or one allocation; short practice paper | **Critical** to broad deterministic-plan and auditable-run-linkage language. Compare integrity and missing-evidence behavior, not service performance. |
| Ortiz-Martínez, “DeBasher,” BMC Bioinformatics 2025 ([DOI](https://doi.org/10.1186/s12859-025-06108-1)) | Stateful processes, static pre-resolution, up-front Slurm submission with virtually no resident scheduler resources, run status | Bash/flow-based workflow DSL and multiple jobs/arrays rather than one allocation and explicit service resources | **High.** Static/no-resident Slurm orchestration is not novel. Reuse head-node footprint and scaling methodology. |
| Maliaroudakis et al., “KNoC,” ISC Workshops 2022 ([DOI](https://doi.org/10.1007/978-3-031-23220-6_15)) | Argo YAML, container lifecycle, `sbatch` plus Singularity generation, job IDs, monitoring, Slurm annotations | Each Pod becomes a separate Slurm job and Kubernetes/Argo remains resident | **High** to generic cloud-native/Slurm-container claims. Use as a controller and scheduling-unit contrast. |
| Przybylski et al., “HPC-Whisk,” SC22 main track ([DOI](https://doi.org/10.1109/SC41404.2022.00045)) | Dynamic function/service capacity on HPC and explicit use of otherwise idle resources | FaaS infrastructure and resident service control rather than compilation of a finite application | **High** architectural contrast from a flagship venue. Do not treat it as a direct static-compiler baseline. |
| Wan et al., “OpenVenus,” SmartCom 2022 proceedings, published 2023 ([DOI](https://doi.org/10.1007/978-3-031-28124-2_13)) | Slurm service lifecycle, contention locking, Singularity overlay, startup/storage evaluation | Open service interface rather than a typed Compose-derived one-allocation compiler | **High** to broad Slurm-service lifecycle wording. Full text was inspected for the listed capabilities. |
| Mujkanovic et al., “Survey of adaptive containerization architectures for HPC,” SC Workshops 2023 ([DOI](https://doi.org/10.1145/3624062.3624588)) | Kubernetes agents inside workload-manager allocations, Pods on compute nodes, Slurm accounting, multi-container motivation | External Kubernetes control plane, dynamic Pods/Kubelets, and incomplete multi-node/multi-tenant semantics | **High** to “first multi-container workload inside a Slurm allocation.” Compare external reconciliation with a finite generated artifact that needs no external hpc-compose controller. |
| Han et al., “PROV-IO+,” IEEE TPDS 2024 ([DOI](https://doi.org/10.1109/TPDS.2024.3374555)) | Cross-platform HPC provenance, container and non-container execution, environment/data/checkpoint relations, large-scale overhead evaluation | Runtime I/O interception plus provenance engine/store/query services; data-centric lineage rather than compiler-owned mapping | **Critical only if evidence stays broad.** Promote for a provenance contribution; otherwise cite to delimit supporting evidence. |
| Zhou et al., “Container orchestration on HPC systems through Kubernetes,” Journal of Cloud Computing 2021 ([DOI](https://doi.org/10.1186/s13677-021-00231-z)) | YAML, dual-level scheduling, Singularity execution, status propagation, resident operator | Kubernetes--TORQUE bridge and embedded PBS jobs, not one Slurm allocation or an offline compiler-generated batch artifact | **High** to broad cloud-orchestration-to-HPC novelty. Place before recent HPK/InterLink work. |

New discoveries relative to the first report are RADICAL-Pilot service tasks,
Wilkins, Drona, BioCodex, DeBasher, HPC-Whisk, OpenVenus, and the adaptive
containerization study. StreamFlow, KNoC, PROV-IO+, and the Kubernetes/HPC bridge
are promotions from screened or open leads because their directness, venue, or
citation impact warrants explicit treatment.

The strongest direct comparison is not necessarily the best executable baseline.
RADICAL-Pilot service tasks, Wilkins, and StreamFlow are semantic/architectural
comparators unless the recurring workload can be represented fairly. Drona and
BioCodex are artifact/inspectability comparators. A native Slurm script remains
the primary RQ2 performance control.

## 4. Priority B: canonical and highly cited lineage

| Paper | Approximate citation snapshot | Why it is required |
| --- | ---: | --- |
| Di Tommaso et al., “Nextflow enables reproducible computational workflows,” Nature Biotechnology 2017 ([DOI](https://doi.org/10.1038/nbt.3820)) | 4,342 | Canonical portable, containerized, reproducible workflow system. Its omission makes the workflow comparison look selectively recent. |
| Kurtzer et al., “Singularity,” PLOS ONE 2017 ([DOI](https://doi.org/10.1371/journal.pone.0177459)) | 2,769 | Canonical HPC container/runtime and image-mobility foundation. Rules out portability, daemonless-container, and container-identity novelty. |
| Yoo, Jette, and Grondona, “SLURM,” JSSPP 2003 ([DOI](https://doi.org/10.1007/10968987_3)) | 1,548 | Required substrate citation for allocations, jobs, and steps; makes ownership of scheduler semantics explicit. |
| Huber et al., “AiiDA 1.0,” Scientific Data 2020 ([DOI](https://doi.org/10.1038/s41597-020-00638-4)) | 298 | Scheduler submission, error handling, UUID/hash identity, remote execution, provenance graphs, and a resident engine. Critical against broad workflow-plus-traceability claims. |
| Soiland-Reyes et al., “Packaging research artefacts with RO-Crate,” Data Science 2022 ([DOI](https://doi.org/10.3233/DS-210053)) | 180 | Establishes research-object packaging, identifiers, metadata, and relationships before Workflow Run RO-Crate specializes the model. |
| Wozniak et al., “Swift/T,” CCGrid 2013 ([DOI](https://doi.org/10.1109/CCGrid.2013.99)) | 145 | Canonical compiler/dataflow contrast for scalable application composition. Category-level comparison is enough. |
| Turilli, Santcroos, and Jha, “A Comprehensive Perspective on Pilot-Job Systems,” ACM Computing Surveys 2018 ([DOI](https://doi.org/10.1145/3177851)) | about 85 | Formalizes resource placeholders, multi-level scheduling, workload/task managers, dispatch, and execution in acquired resources. Cite before individual pilots. |
| Colonnelli et al., “StreamFlow” ([DOI](https://doi.org/10.1109/TETC.2020.3019202)) | 71 | Both close and well cited; deserves direct treatment rather than screening as redundant. |
| Zhou et al., Kubernetes/HPC orchestration ([DOI](https://doi.org/10.1186/s13677-021-00231-z)) | 68 | Archival predecessor to newer Kubernetes--HPC bridges and important control-plane comparison. |
| Zhou, Zhou, and Hoppe, “Containerization for HPC Systems: Survey and Prospects,” IEEE TSE 2023 ([DOI](https://doi.org/10.1109/TSE.2022.3229221)) | 52 | Strong survey/taxonomy for the cloud-orchestrator versus HPC workload-manager mismatch. Use to support field scope, not novelty. |
| Benedicic et al., “Sarus,” ISC HPC 2019 ([DOI](https://doi.org/10.1007/978-3-030-34356-9_5)) | 47 | Archival runtime foundation for site integration, OCI hooks, scaling, security, and parallel filesystems. Cite before the 2026 Sarus Suite preprint. |
| Peterson et al., “Merlin,” FGCS 2022 ([DOI](https://doi.org/10.1016/j.future.2022.01.024)) | 37 | Strong workload precedent for ML-ready HPC ensembles and a useful boundary against dynamic workflow systems. |
| Wilkinson et al., “Applying the FAIR Principles to computational workflows,” Scientific Data 2025 ([DOI](https://doi.org/10.1038/s41597-025-04451-9)) | 51 | Recent community guidance with unusually rapid uptake. Use to discipline FAIR/reusable/reproducible language, not as an execution baseline. |

Counts do not determine the direct-comparison order. The direct paragraph should
still begin with the closest systems, even when a background reference has far
more citations.

## 5. Priority C: strong venues, current synthesis, and evaluation methods

| Paper | Role in this paper |
| --- | --- |
| Merzky et al., “Design and Performance Characterization of RADICAL-Pilot,” IEEE TPDS 2022 ([DOI](https://doi.org/10.1109/TPDS.2021.3105994)) | Replace or supplement the first pass's workshop citation with the canonical archival performance paper. Keep the 2025 service-task paper for the distinct lifecycle claim. |
| Lehmann et al., “How Workflow Engines Should Talk to Resource Managers,” CCGrid 2023 ([DOI](https://doi.org/10.1109/CCGrid57682.2023.00025)) | Formal responsibility-boundary comparison and evidence that feedback-driven scheduling can improve makespan. Helps explain hpc-compose's deliberate compile-time choice. |
| Turilli et al., “ExaWorks SDK,” Frontiers in HPC 2024 ([DOI](https://doi.org/10.3389/fhpcp.2024.1394615)) | Current workflow ecosystem and interoperability context; do not use as a direct baseline. |
| Yildiz et al., “Extreme-scale workflows: A perspective from the JLESC international community,” FGCS 2024 ([DOI](https://doi.org/10.1016/j.future.2024.07.041)) | Recent synthesis for workload/runtime trends and open challenges. Use for background, not prevalence claims without the paper's specific evidence. |
| Souza et al., “Multi-Workflow Provenance and Data Observability,” IEEE e-Science 2023 ([DOI](https://doi.org/10.1109/e-Science58273.2023.10254822)) | Event/provenance/telemetry integration and observer-overhead methodology. Conditional on retaining an observability or evidence study. |
| Rosendo et al., “ProvLight,” IEEE CLUSTER 2023 ([DOI](https://doi.org/10.1109/CLUSTER52292.2023.00026)) | Capture-completeness versus CPU, memory, data, and energy methodology. Optional evidence-overhead comparator. |
| Chard et al., “funcX,” HPDC 2020 ([DOI](https://doi.org/10.1145/3369583.3392683)) | Highly cited function-serving/control-plane contrast. Useful only if the paper discusses service fabrics broadly. |
| Sochat et al., “The Flux Operator,” F1000Research 2024 ([DOI](https://doi.org/10.12688/f1000research.147989.1)) | Recent inverse architecture: Kubernetes creates scoped Flux clusters. Optional controller-cost and converged-computing context, not a Slurm compiler neighbor. |

## 6. Revised novelty-threat map

| Claim family | Prior work now requiring explicit treatment | Safe response |
| --- | --- | --- |
| Explicit readiness/liveness-managed HPC services | RADICAL-Pilot service tasks | Do not claim readiness/liveness or managed service lifetime as new. Compare static compilation, controller locus, scheduling unit, and artifact contract. |
| Concurrent one-job in-situ coupling | Wilkins and SmartSim | Do not claim declarative multi-component coupling or one batch job as new. Compare native service-step semantics and the published rejection/resource boundary. |
| Slurm service startup and lifecycle | OpenVenus and SAIA | Do not claim Slurm service lifecycle as new. Distinguish finite allocation scope, external accessibility, proxying, locking, and autoscaling. |
| Declarative multi-container/HPC execution | StreamFlow, KNoC, Kubernetes/HPC bridge, adaptive-container survey, HPK, Sarus Suite | Do not claim first declarative or first multi-container HPC orchestration. Focus on one allocation/native steps and finite semantics. |
| Static/no-resident orchestration | DeBasher, native scripts, compiler/dataflow systems | Say no **separately deployed** daemon or nested scheduler; name the generated batch supervisor. |
| Validation, script generation, and preview | Drona, Maestro, DockSing, BioCodex | Do not claim generation or inspection alone. State exact artifact, context, and mapping coverage. |
| One batch job for multiple components | Wilkins and allocation-internal systems | Do not claim one-job compilation alone. The candidate distinction must include native service steps and the published semantic/rejection boundary. |
| Scheduler-linked run evidence | BioCodex, AiiDA, RO-Crate/Workflow Run RO-Crate, CWLProv, PROV-IO+ | Keep general provenance and packaging out of the novelty claim. Test only scheduler-specific identity, fault, degradation, or linkage behavior. |
| Runtime portability and site integration | Singularity, Sarus, Pyxis/Enroot, Apptainer, container surveys | Attribute launch, isolation, scaling, and site-policy behavior to the owning runtime and Slurm. |

### Candidate novelty wording after pass two

> Prior systems provide declarative multi-container environments, workflow and
> pilot execution within acquired HPC resources, readiness-managed service tasks,
> generated Slurm scripts, and provenance-aware run records. We investigate a
> narrower design point: whether a finite typed service model can be lowered to
> one generated Slurm batch program whose orchestration runs inside the allocation,
> using native job steps with explicit resource,
> readiness, failure, rejection, and artifact-identity rules, without deploying a
> separate cluster daemon or nested scheduler.

This wording states a question and design point. It does not assert uniqueness or
successful evaluation.

## 7. Recommended related-work architecture

1. **Slurm and container substrate.** Slurm, Singularity, Sarus, Pyxis/Enroot,
   Apptainer, and the TSE container survey. State which semantics belong to each
   layer.
2. **Direct declarative and service neighbors.** Singularity Compose, DockSing,
   RADICAL-Pilot service tasks, Wilkins, StreamFlow, Drona, BioCodex, and
   DeBasher. This paragraph should carry the novelty comparison.
3. **Pilots, nested schedulers, and service fabrics.** Pilot survey,
   RADICAL-Pilot TPDS, Flux, HyperQueue, QCG-PilotJob, HPC-Whisk, and OpenVenus.
   Compare controller locus, dynamism, and deployment.
4. **Cloud/HPC bridges and persistent services.** KNoC, Kubernetes/HPC bridge,
   adaptive containerization, HPK/InterLink, and SAIA. Separate one job, one Pod,
   one allocation, and persistent service pools.
5. **Workflows and ensembles.** Nextflow, Snakemake, Parsl, Pegasus, Merlin,
   SmartSim, Swift/T, and ExaWorks. Avoid a false workflow-versus-service binary.
6. **Evidence and provenance.** Base RO-Crate, Workflow Run RO-Crate, CWLProv,
   ReproZip, AiiDA, PROV-IO+, and optionally MIDA/ProvLight. End with the narrow
   local scheduler-identity and degradation boundary.

The paper need not cite every representative in the main text. The comparison
matrix and appendix can preserve breadth while the prose cites the strongest
representative for each claim.

## 8. Evaluation implications

### RQ1: semantic conformance

- Extend the semantic matrix with execution unit, allocation ownership,
  readiness/liveness, placement, controller locus, lifecycle/failure, resource
  sharing, rejection, and artifact identity.
- Treat RADICAL-Pilot service tasks, Wilkins, and StreamFlow as semantic
  comparators, not presumed executable performance baselines.
- Add Drona/BioCodex dimensions for validation, preview mutability, hashing,
  tamper rejection, and plan-to-job/log linkage.

### RQ2: cost of abstraction

- Keep an expert native Slurm script as the primary equivalent control.
- Separate parse/plan/render, preparation, submission, queue, step launch,
  readiness, steady state, supervisor CPU/memory, cleanup, and evidence capture.
- Reuse DeBasher's head-node-footprint concern, RADICAL-Pilot's service bootstrap
  and lifecycle measures, and PROV-IO+/ProvLight capture-overhead dimensions.
- Do not rank systems with different scheduling units or controller services as
  if they executed the same semantics.

### RQ3: boundary and generalizability

- Include cases better suited to pilots, dynamic workflows, persistent services,
  and Kubernetes bridges; a clear rejection/handoff is a positive result.
- Use direct papers to predeclare what hpc-compose does not attempt: dynamic
  scheduling, late binding, autoscaling, persistent proxying, multi-site data
  movement, and general provenance capture.
- If no fair executable direct comparator exists, publish the mapping and no-go
  reason instead of engineering an artificial failure.

## 9. Bibliography and claim hygiene

- Use the companion pass-two BibTeX file as a candidate set, not an instruction to
  cite all entries.
- Do not duplicate first-pass records. The pass-two file contains restorations,
  new discoveries, and archival upgrades with distinct keys.
- Cite both RADICAL-Pilot papers only for distinct claims: 2022 for architecture
  and performance characterization, 2025 for first-class service lifecycle.
- Cite Sarus 2019 as the archival runtime foundation and Sarus Suite 2026 only for
  the newer integration suite.
- Retain the first-pass `chazapis2025hpk` record
  ([DOI](https://doi.org/10.1145/3731599.3767352)) as HPK's current archival
  evaluation; do not duplicate it in the pass-two file.
- OpenVenus full text was inspected for the lifecycle, contention-lock,
  Singularity-overlay, and startup/storage-evaluation statements used here.
- Keep BioCodex's four-page evidence proportionate; it is a claim-limiting current
  neighbor, not definitive proof of broad system behavior.
- Never convert missing documentation for a competitor into evidence that a
  capability is absent. Mark unknown cells `unknown`.
- Rerun forward and backward searches, metadata checks, and citation snapshots
  before submission; 2026--2027 work may change the closest-neighbor set.

## 10. Final literature judgment

The second pass strengthens the paper by narrowing it. It does not support a
feature-combination “first.” It supports investigating a precise static-compiler
design point that sits between native scripts, declarative workflow/service
systems, pilots, cloud/HPC bridges, and provenance frameworks.

The most important immediate additions are RADICAL-Pilot service tasks, Wilkins,
StreamFlow, Drona, BioCodex, DeBasher, KNoC, foundational Slurm, the pilot-job
survey, Singularity, Nextflow, AiiDA, base RO-Crate, and PROV-IO+ if evidence
remains a proposed contribution. These references should change the wording and
comparison axes before they expand the bibliography.

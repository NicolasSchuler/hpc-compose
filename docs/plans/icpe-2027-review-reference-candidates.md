# ICPE 2027 Review: Verified Reference Candidates

Checked: 2026-08-09

Manuscript reviewed: docs/plans/icpe-2027-meta-draft.md

Bibliography input: NONE

Status: candidate references only. The manuscript was not edited. Every retained item below was independently opened through a primary paper, publisher or proceedings page, or authoritative project documentation. The comparison notes are review guidance, not claims that should be copied into the paper without normal citation integration.

## Direct Compose, Slurm, and declarative neighbors

### RC-01 — Singularity Compose

- Citation: Vanessa Sochat. 2019. “Singularity Compose: Orchestration for Singularity Instances.” Journal of Open Source Software 4(40), 1578.
- Primary source: <https://doi.org/10.21105/joss.01578>
- Overlap: Multi-service configuration and lifecycle management for Singularity container instances.
- Difference: Local instance and network orchestration rather than compilation of one Slurm allocation with explicit native step-resource semantics.
- Novelty threat: Compose-style multi-container orchestration is prior art.
- Recommended placement: Open the direct-neighbor paragraph and use it to rule out novelty based on Compose familiarity or multi-container lifecycle alone.

### RC-02 — DockSing

- Citation: DockSing, version 0.2.36. 2025. Python software package.
- Authoritative source: <https://pypi.org/project/docksing/>
- Overlap: A limited Compose-style configuration is translated toward Singularity and Slurm execution; the package stages over SSH and exposes generated commands.
- Difference: Its documentation centers on one container/job execution path and does not establish the proposed finite concurrent-service semantic contract or run-evidence model.
- Novelty threat: Very high against any broad claim of being the first Compose/YAML-to-Slurm translator or first inspectable generated-command tool.
- Recommended placement: Treat as the closest executable implementation comparator where compatible. Omit personal author attribution because PyPI marks that field unverified.

### RC-03 — StreamFlow

- Citation: Iacopo Colonnelli, Barbara Cantalupo, Ivan Merelli, and Marco Aldinucci. 2021. “StreamFlow: Cross-Breeding Cloud with HPC.” IEEE Transactions on Emerging Topics in Computing 9(4):1723–1737.
- Primary source: <https://doi.org/10.1109/TETC.2020.3019202>
- Authoritative project documentation: <https://streamflow.di.unito.it/documentation/latest/>
- Overlap: Declarative multi-container execution, concurrent communicating tasks, and mixed HPC/cloud environments.
- Difference: A CWL workflow/controller and multi-site data-movement model rather than one static Slurm allocation artifact.
- Novelty threat: High against a blanket statement that workflow systems do not model concurrent communicating components.
- Recommended placement: Elevate from generic workflow background into the direct-adjacent comparison matrix.

### RC-04 — Sarus Suite

- Citation: Alberto Madonna, Matteo Chesi, Gwangmu Lee, Michele Brambilla, Fawzi Roberto Mohamed, and Felipe A. Cruz. 2026. “Sarus Suite: Cloud-native Containers for HPC.” arXiv:2604.17064.
- Primary source: <https://arxiv.org/abs/2604.17064>
- Overlap: Declarative multi-container descriptions, scheduler-native integration, container startup, and HPC performance.
- Difference: A runtime/integration suite whose demonstrated Kubernetes multi-container path is not the same as a one-allocation compiler contract.
- Novelty threat: High against a broad combination claim based on cloud-native input, multiple containers, and scheduler-native HPC.
- Recommended placement: Direct-adjacent container/runtime comparison; label it explicitly as a preprint.

### RC-05 — benchkit

- Citation: Antonio Paolillo, Mats Van Molle, and Ken Hasselmann. 2026. “benchkit: A Declarative Framework for Composable Performance Evaluation of System Software.” Proceedings of ICPE ’26, 170–183.
- Primary source: <https://doi.org/10.1145/3777884.3796997>
- Official ICPE preprint: <https://icpe2026.spec.org/preprint/benchkit_A_Declarative_Framework_for_Composable_Performance_Evaluation_Of_System_Software.pdf>
- Overlap: Declarative composition, replacement of ad hoc shell, performance-study lifecycle, reproducibility, and explicit overhead comparison.
- Difference: Benchmark-campaign and tool composition rather than readiness-coupled services sharing one Slurm allocation.
- Novelty threat: High to generic claims about declarative performance experimentation; it is also a useful same-venue quality bar.
- Recommended placement: Declarative performance frameworks and evaluation-method motivation.

### RC-06 — dagster-slurm

- Citation: Hernan Picatto, Maximilian Heß, Georg Heiler, and Martin Pfister. 2026. “Discovering the SUPER in computing—dagster-slurm for reproducible research on HPC.” Journal of Open Source Software 11(119), 9795.
- Primary source: <https://doi.org/10.21105/joss.09795>
- Overlap: Slurm integration and reproducible HPC research workflows.
- Difference: Dagster-based workflow and job orchestration, not presently shown to provide the same finite concurrent-service lowering contract.
- Novelty threat: Moderate against broad reproducible-HPC orchestration language.
- Recommended placement: Slurm workflow systems; keep mechanism comparisons conservative.

### RC-07 — Maestro Workflow Conductor

- Citation: Lawrence Livermore National Laboratory. “Maestro Workflow Conductor.”
- Authoritative source: <https://maestrowf.readthedocs.io/en/latest/Maestro/index.html>
- Overlap: YAML studies, parameter expansion, generated scheduler scripts, Slurm adapters, and execution monitoring.
- Difference: A multi-step study and DAG abstraction rather than readiness-coupled services within one allocation.
- Novelty threat: YAML, script generation, parameter studies, and monitoring are not novel.
- Recommended placement: Workflow-engine comparison. Cite as software documentation unless a peer-reviewed Maestro paper is separately verified.

## Allocation-internal schedulers and pilot systems

### RC-08 — Flux

- Citation: Dong H. Ahn et al. 2020. “Flux: Overcoming Scheduling Challenges for Exascale Workflows.” Future Generation Computer Systems 110:202–213.
- Primary source: <https://doi.org/10.1016/j.future.2020.04.006>
- Overlap: Allocation-internal execution, co-scheduling, resource management, and coordination.
- Difference: A hierarchical nested scheduler and dynamic runtime rather than static lowering to native Slurm steps.
- Novelty threat: High to generic claims about allocation-boundary orchestration or co-scheduling.
- Recommended placement: Primary architectural contrast for the choice not to deploy a nested scheduler.

### RC-09 — HyperQueue

- Citation: Jakub Beránek, Ada Böhm, Gianluca Palermo, Jan Martinovič, and Branislav Jansík. 2024. “HyperQueue: Efficient and ergonomic task graphs on HPC clusters.” SoftwareX 27, 101814.
- Primary source: <https://doi.org/10.1016/j.softx.2024.101814>
- Overlap: Aggregating work into allocations, internal resource management, and ergonomic task execution.
- Difference: A dynamic task-graph runtime rather than finite readiness-coupled services compiled into one inspectable batch artifact.
- Novelty threat: High to allocation-internal task-management claims.
- Recommended placement: Pilot and allocation-internal runtime subsection.

### RC-10 — QCG-PilotJob

- Citation: Bartosz Bosak, Tomasz Piontek, Paul Karlshoefer, Erwan Raffin, Jalal Lakhlili, and Piotr Kopta. 2021. “Verification, Validation and Uncertainty Quantification of Large-Scale Applications with QCG-PilotJob.” ICCS 2021, Lecture Notes in Computer Science, 495–501.
- Primary source: <https://doi.org/10.1007/978-3-030-77977-1_39>
- Overlap: Managing heterogeneous work inside acquired HPC resources.
- Difference: A pilot-job resource manager with dynamic task execution.
- Novelty threat: Moderate to generic allocation-level coordination novelty.
- Recommended placement: Pilot systems.

### RC-11 — RADICAL-Pilot

- Citation: Andre Merzky, Matteo Turilli, Mikhail Titov, Aymen Al-Saadi, and Shantenu Jha. 2022. “Design and Performance Characterization of RADICAL-Pilot on Leadership-Class Platforms.” IEEE Transactions on Parallel and Distributed Systems 33(4):818–829.
- Primary source: <https://doi.org/10.1109/TPDS.2021.3105994>
- Overlap: Pilot abstraction, late binding, heterogeneous tasks, and allocation utilization.
- Difference: A persistent dynamic pilot runtime operating at a much broader scale and scheduling scope.
- Novelty threat: Important canonical counterexample to broad pilot or allocation-internal execution novelty.
- Recommended placement: Alongside Flux and QCG-PilotJob.

## Workflow and service systems

### RC-12 — Merlin

- Citation: J. Luc Peterson et al. 2022. “Enabling Machine Learning-Ready HPC Ensembles with Merlin.” Future Generation Computer Systems 131:255–268.
- Primary source: <https://doi.org/10.1016/j.future.2022.01.024>
- Overlap: Producer-consumer HPC workflows, persistent workers, and ML-oriented orchestration.
- Difference: A dynamic, multi-job ensemble and control-plane architecture.
- Novelty threat: Moderate to broad ML/HPC coordination claims.
- Recommended placement: Workflow systems and motivating-workload context.

### RC-13 — SmartSim

- Citation: Sam Partee et al. 2022. “Using Machine Learning at Scale in Numerical Simulations with SmartSim: An Application to Ocean Climate Modeling.” Journal of Computational Science 62, 101707.
- Primary source: <https://doi.org/10.1016/j.jocs.2022.101707>
- Overlap: Co-execution of simulations with data and machine-learning services on HPC.
- Difference: A specialized simulation-plus-database framework rather than a general Compose-to-Slurm compiler.
- Novelty threat: High to workload novelty, but low to the narrow compiler-contract claim.
- Recommended placement: Motivating scenario and domain-specific related work.

### RC-14 — SAIA

- Citation: Ali Doosthosseini, Jonathan Decker, Hendrik Nolte, et al. 2026. “SAIA: A Seamless Slurm-Native Solution for HPC-Based Services.” Journal of Supercomputing 82, article 403.
- Primary source: <https://doi.org/10.1007/s11227-026-08508-3>
- Overlap: Slurm-native service execution, service discovery, lifecycle management, and security constraints.
- Difference: Persistent externally accessible services, proxying, autoscaling, and renewed job pools rather than one finite application allocation.
- Novelty threat: High to broad “Slurm-native services” language.
- Recommended placement: Service-oriented adjacent systems and explicit non-goals.

### RC-15 — Nextflow

- Citation: Paolo Di Tommaso et al. 2017. “Nextflow Enables Reproducible Computational Workflows.” Nature Biotechnology 35:316–319.
- Primary source: <https://doi.org/10.1038/nbt.3820>
- Overlap: Portable declarative workflow execution and containers on HPC.
- Difference: Dataflow processes and jobs rather than one concurrent-service allocation.
- Novelty threat: Category-level rather than a direct implementation baseline.
- Recommended placement: Representative workflow-engine citation.

### RC-16 — Snakemake

- Citation: Felix Mölder et al. 2025. “Sustainable Data Analysis with Snakemake,” version 3. F1000Research 10:33.
- Primary source: <https://doi.org/10.12688/f1000research.29032.3>
- Overlap: Reproducible declarative workflows and HPC backends.
- Difference: Rule and file-DAG execution.
- Novelty threat: Category-level rather than direct.
- Recommended placement: Workflow-engine background; identify the cited version explicitly.

### RC-17 — Parsl

- Citation: Yadu N. Babuji et al. 2019. “Parsl: Pervasive Parallel Programming in Python.” Proceedings of HPDC ’19, 25–36.
- Primary source: <https://doi.org/10.1145/3307681.3325400>
- Overlap: Portable task orchestration over HPC resources.
- Difference: A Python task and dataflow runtime.
- Novelty threat: Category-level rather than direct.
- Recommended placement: Workflow systems.

## Provenance and evidence

### RC-18 — RO-Crate

- Citation: Stian Soiland-Reyes et al. 2022. “Packaging Research Artefacts with RO-Crate.” Data Science 5(2).
- Primary source: <https://doi.org/10.3233/DS-210053>
- Overlap: Packaging structured research objects and contextual metadata.
- Difference: A general interoperable packaging standard rather than a local scheduler-specific evidence protocol.
- Novelty threat: General evidence packaging is prior art.
- Recommended placement: Provenance foundations.

### RC-19 — Workflow Run RO-Crate

- Citation: Simone Leo et al. 2024. “Recording Provenance of Workflow Runs with RO-Crate.” PLOS ONE 19(9), e0309210.
- Primary source: <https://doi.org/10.1371/journal.pone.0309210>
- Overlap: Prospective and retrospective workflow-run provenance, interoperable run records, and re-execution context across workflow systems.
- Difference: A cross-system standard rather than local Slurm submission, identity, and degraded-evidence mechanics.
- Novelty threat: Very high to any general “specification-to-run evidence” novelty.
- Recommended placement: Closest provenance comparison, not merely background.

### RC-20 — CWLProv

- Citation: Farah Zaib Khan et al. 2019. “Sharing Interoperable Workflow Provenance: A Review of Best Practices and Their Practical Application in CWLProv.” GigaScience 8(11), giz095.
- Primary source: <https://doi.org/10.1093/gigascience/giz095>
- Overlap: Portable workflow provenance, input/output identities, execution records, and packaging.
- Difference: A general workflow standard rather than scheduler-specific local durability and degraded-state behavior.
- Novelty threat: High to provenance terminology and scope.
- Recommended placement: Provenance standards.

### RC-21 — ReproZip

- Citation: Fernando Chirigati, Rémi Rampin, Dennis Shasha, and Juliana Freire. 2016. “ReproZip: Computational Reproducibility With Ease.” Proceedings of SIGMOD ’16.
- Primary source: <https://doi.org/10.1145/2882903.2899401>
- Authoritative documentation: <https://reprozip.readthedocs.io/en/latest/>
- Overlap: Capturing and packaging execution dependencies for reproducibility.
- Difference: System-call tracing and environment bundling rather than Slurm plan/run identity or distributed multi-node capture.
- Novelty threat: Moderate to broad rebuildability and packaging claims.
- Recommended placement: Evidence-system comparison and limitation contrast.

## Runtime substrate and canonical background

### RC-22 — Singularity

- Citation: Gregory M. Kurtzer, Vanessa Sochat, and Michael W. Bauer. 2017. “Singularity: Scientific Containers for Mobility of Compute.” PLOS ONE 12(5), e0177459.
- Primary source: <https://doi.org/10.1371/journal.pone.0177459>
- Overlap: HPC-compatible container execution.
- Difference: A runtime substrate, not application orchestration.
- Novelty threat: None to the narrow compiler contract; it prevents container-substrate novelty leakage.
- Recommended placement: Runtime background.

### RC-23 — Sarus

- Citation: Lucas Benedicic, Felipe A. Cruz, Alberto Madonna, and Kean Mariotti. 2019. “Sarus: Highly Scalable Docker Containers for HPC Systems.” ISC High Performance Workshops, 46–60.
- Primary source: <https://doi.org/10.1007/978-3-030-34356-9_5>
- Overlap: Scalable container execution integrated with HPC systems.
- Difference: A runtime substrate.
- Novelty threat: None to bounded application compilation, but container portability and performance are not novel.
- Recommended placement: Runtime-backend background.

### RC-24 — Pyxis

- Citation: NVIDIA. “Pyxis: Container Plugin for Slurm Workload Manager.”
- Authoritative source: <https://github.com/NVIDIA/pyxis>
- Overlap: Native Slurm step container launch, multi-node execution, and Enroot integration.
- Difference: A backend primitive rather than multi-service semantics or evidence.
- Novelty threat: Native container-step execution is substrate, not a paper contribution.
- Recommended placement: Implementation substrate.

### RC-25 — Slurm

- Citation: Andy B. Yoo, Morris A. Jette, and Mark Grondona. 2003. “SLURM: Simple Linux Utility for Resource Management.” Job Scheduling Strategies for Parallel Processing, Lecture Notes in Computer Science 2862:44–60.
- Primary source: <https://doi.org/10.1007/10968987_3>
- Author manuscript: <https://www.osti.gov/servlets/purl/15003520>
- Overlap: The allocation and job-step substrate into which hpc-compose lowers.
- Difference: Scheduler foundation rather than an application compiler.
- Novelty threat: None; it is necessary canonical background for the claimed semantics.
- Recommended placement: Background and system model.

## Verified software leads not yet ready for scholarly citation

- Balsam: <https://balsam.readthedocs.io/en/stable/index.html> verifies the project’s dynamic HPC workflow role, but its cited workshop paper was not independently opened from primary proceedings. Do not add that paper yet.
- Maestro: authoritative documentation is verified, but a peer-reviewed Maestro paper was not established in this pass.
- DockSing: software behavior and release metadata are verified from PyPI; personal author metadata is explicitly marked unverified there.
- PSI/J: potentially useful for portable job-submission positioning, but its complete proceedings metadata and author list require another primary-source check before addition.

These four leads are intentionally excluded from the verified scholarly-reference set beyond the software-documentation entries explicitly listed above.

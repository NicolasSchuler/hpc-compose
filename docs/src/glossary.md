# Glossary

Core `hpc-compose` terms, in one place. The short version of this list also appears on the Overview page; this page is the fuller reference.

One-line definitions; follow the link for the owning reference section.

<dl>

<dt id="allocation">allocation</dt>
<dd>The single Slurm job where all of an application's services run; one spec compiles to one allocation. See <a href="execution-model.md">Execution Model</a>.</dd>

<dt id="artifact-bundle">artifact bundle</dt>
<dd>A named group of output paths declared under <code>x-slurm.artifacts</code> and exported with <code>hpc-compose artifacts</code>. See <a href="spec-reference.md#x-slurmartifacts"><code>x-slurm.artifacts</code></a>.</dd>

<dt id="canary">canary</dt>
<dd>A short, minimized probe run from <code>hpc-compose germinate</code> that writes <code>latest-canary.json</code> and leaves <code>latest.json</code> untouched. See <a href="cli-reference.md#germinate-canary-runs"><code>germinate</code></a>.</dd>

<dt id="cache-directory">cache directory</dt>
<dd>Shared storage for imported and prepared images, visible from both the submission host and the compute nodes. See <a href="spec-reference.md#x-slurmcache_dir"><code>x-slurm.cache_dir</code></a>.</dd>

<dt id="checkpoint">checkpoint</dt>
<dd>Per-attempt run state recorded under <code>.hpc-compose/&lt;job-id&gt;/attempts/&lt;n&gt;/</code> for resume-aware runs, reconstructed with <code>hpc-compose checkpoints</code>; distinct from any model checkpoints your training code writes. See <a href="failure-recovery.md">Recover From Failures</a>.</dd>

<dt id="compose-file">compose file / spec</dt>
<dd>The YAML file describing services, runtime backend, and Slurm settings; "spec" and "compose file" are the same thing. See <a href="spec-reference.md">Spec Reference</a>.</dd>

<dt id="context">context</dt>
<dd>The resolved view of settings, profile, binaries, interpolation variables, and runtime paths for an invocation. See <a href="cli-reference.md#authoring-and-setup"><code>context</code></a>.</dd>

<dt id="failure-policy">failure policy</dt>
<dd>Per-service restart behavior under <code>services.&lt;name&gt;.x-slurm.failure_policy</code>. See <a href="spec-reference.md#servicesnamex-slurmfailure_policy"><code>failure_policy</code></a>.</dd>

<dt id="local-mode">local mode</dt>
<dd>Running a plan on the current Linux host through the local supervisor instead of submitting to Slurm; single-host and limited to Pyxis/Enroot or Apptainer. See <a href="cli-reference.md#up---local"><code>up --local</code></a>.</dd>

<dt id="login-node">login node / submission host</dt>
<dd>The host where you run <code>hpc-compose</code> and from which jobs are submitted; "login node" and "submission host" name the same machine. See <a href="runbook.md">Operate a Real Cluster Run</a>.</dd>

<dt id="preflight">preflight</dt>
<dd>Checks of local tools, paths, backend support, and optional cluster profiles before a run. See <a href="cli-reference.md#plan-and-run"><code>preflight</code></a>.</dd>

<dt id="prepare">prepare</dt>
<dd>The login-node phase that imports base images and builds prepared runtime artifacts, reused later by <code>up</code> and <code>run</code>. See <a href="spec-reference.md#x-runtimeprepare-and-x-enrootprepare"><code>x-runtime.prepare</code></a>.</dd>

<dt id="profile">profile</dt>
<dd>A named settings block in <code>.hpc-compose/settings.toml</code>, selected with <code>--profile &lt;name&gt;</code>. See <a href="cli-reference.md#common-flags">Common Flags</a>.</dd>

<dt id="readiness">readiness</dt>
<dd>A gate that holds a dependent service until a probe passes; types are <code>sleep</code>, <code>tcp</code>, <code>http</code>, and <code>log</code>. See <a href="spec-reference.md#readiness"><code>readiness</code></a>.</dd>

<dt id="rendezvous">rendezvous</dt>
<dd>Same-cluster service discovery through JSON records under the shared cache directory; not DNS, auth, or a service mesh. See <a href="spec-reference.md#x-slurmrendezvous"><code>x-slurm.rendezvous</code></a>.</dd>

<dt id="resume">resume</dt>
<dd>Resume-aware reruns backed by a shared <code>x-slurm.resume.path</code> and attempt-aware state. See <a href="spec-reference.md#x-slurmresume"><code>x-slurm.resume</code></a>.</dd>

<dt id="right-sizing">right-sizing</dt>
<dd>Comparing requested versus observed usage to suggest reductions (<code>inspect --rightsize</code>) plus the efficiency grade from <code>score</code>. See <a href="cli-reference.md#tracked-runtime">Tracked Runtime</a>.</dd>

<dt id="runtime-backend">runtime backend</dt>
<dd>The mechanism used to launch services: Pyxis/Enroot, Apptainer, Singularity, or host software, selected with <code>runtime.backend</code>. See <a href="spec-reference.md#runtime"><code>runtime</code></a>.</dd>

<dt id="service">service</dt>
<dd>One container or host process in the allocation, defined under <code>services.&lt;name&gt;</code> (<code>steps</code> is an accepted alias). See <a href="spec-reference.md#service-fields">Service fields</a>.</dd>

<dt id="smoke-test">smoke test</dt>
<dd>A finite end-to-end run (<code>hpc-compose test</code>) where every service must start, pass readiness, and complete successfully. See <a href="cli-reference.md#development-workflow"><code>test</code></a>.</dd>

<dt id="sweep">sweep</dt>
<dd>An embedded <code>sweep</code> block expanded by <code>hpc-compose sweep submit</code> into many independent tracked allocations, one per trial. See <a href="spec-reference.md#sweep"><code>sweep</code></a>.</dd>

<dt id="tracked-job">tracked job / tracked run</dt>
<dd>Metadata under <code>.hpc-compose/&lt;job-id&gt;/</code> that lets <code>status</code>, <code>ps</code>, <code>watch</code>, <code>logs</code>, <code>stats</code>, and <code>artifacts</code> reconnect to a run later; "tracked job" and "tracked run" are the same thing. See <a href="cli-reference.md#tracked-runtime">Tracked Runtime</a>.</dd>

<dt id="workspace">workspace</dt>
<dd>Site-managed storage with an expiry date, allocated through the <code>ws_*</code> tools and managed with <code>hpc-compose workspace status/allocate/extend/release</code>; a common home for the shared cache directory. See <a href="workspaces.md">Manage Cluster Workspaces</a>.</dd>

<dt id="x-runtime-prepare"><code>x-runtime.prepare</code></dt>
<dd>The spec block for image-preparation commands and mounts; <code>x-enroot.prepare</code> is an accepted Pyxis/Enroot alias. See <a href="spec-reference.md#x-runtimeprepare-and-x-enrootprepare"><code>x-runtime.prepare</code></a>.</dd>

<dt id="x-slurm"><code>x-slurm</code></dt>
<dd>The spec section for Slurm settings and <code>hpc-compose</code> runtime extensions, available at the top level and per service. See <a href="spec-reference.md#x-slurm"><code>x-slurm</code></a>.</dd>

</dl>

## Related Docs

- [CLI Reference](cli-reference.md)
- [Spec Reference](spec-reference.md)
- [Full Example Specs](example-source.md)
- [Roadmap and Non-Goals](roadmap.md)

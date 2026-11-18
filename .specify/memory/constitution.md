# Teller Constitution

## Core Principles

### I. Configuration-Driven Pipelines

- Teller is a platform: every ingestion pipeline MUST be defined by a declarative config file in
  the separate shared config repository. This repository MUST NOT contain pipeline-specific code,
  source-specific branches, or hard-coded table names. Example and test configs kept in this
  repository MUST describe synthetic sources only.
- Adding or changing a pipeline that uses existing platform features MUST NOT require a change to
  this repository.
- This repository MUST publish a versioned config schema and a validator that the config
  repository runs in CI. Configs MUST be fully validated before deployment; unknown keys, missing
  required fields, and invalid references MUST be rejected.
- Validation errors MUST be written for application teams: each error names the file, the field,
  what is wrong, and how to fix it.
- Config schema changes follow semantic versioning. A change that would invalidate existing
  configs is a MAJOR version and MUST ship with a migration guide or migration tool.

**Rationale**: Application teams self-serve their pipelines. The config file is the contract
between them and the platform, so it must be stable, strictly checked, and understandable
without reading platform code.

### II. Raw Vault Fidelity (Data Vault 2.0)

- The raw vault MUST hold source data as delivered. Only hard rules may be applied: data type
  alignment, hash key and hash diff calculation, and metadata columns. Business rules (derived
  values, cleansing, deduplication across sources, conforming) MUST NOT be applied in the raw
  vault; they belong in later layers.
- Raw vault entities MUST follow Data Vault 2.0 structures: hubs (business keys), links
  (relationships between business keys), and satellites (descriptive attributes and their
  history).
- Hash keys and hash diffs MUST be computed with SHA-256 by a single platform implementation.
  That implementation MUST document one standard for input normalization (trimming, casing),
  column delimiter, and null handling. Pipelines MUST NOT override the algorithm or the standard.
- Every raw vault row MUST carry a load timestamp and a record source that identifies the
  originating pipeline and landing-zone file.
- Raw vault tables MUST be insert-only. Existing rows MUST NOT be updated or deleted by
  ingestion; satellites record changes as new rows, detected by hash diff.
- Loads MUST be idempotent: reprocessing the same landing-zone file MUST NOT create duplicate
  hub, link, or satellite rows.
- Concurrent loads that write to the same table (for example, two pipelines feeding a shared
  hub) MUST NOT lose or corrupt commits. The plan MUST name the mechanism that guarantees this.

**Rationale**: The raw vault is the auditable system of record. If it changes source data or
computes keys inconsistently, nothing built on top of it can be trusted or reconciled.

### III. Simplicity First (YAGNI)

- Platform features and config options MUST be added only when a real pipeline needs them.
  Speculative options, plug-in systems, and support for hypothetical sources or targets MUST NOT
  be added.
- The config schema MUST offer one way to express each thing. Overlapping options that achieve
  the same result MUST NOT be added.
- Prefer built-in PySpark DataFrame operations and native Delta Lake features over custom code.
  Python UDFs MUST be justified in the plan when a built-in Spark function could do the job.
- Any deviation from these rules MUST be recorded in the plan's Complexity Tracking table with
  the simpler alternative that was rejected and why.

**Rationale**: Every config option is a permanent promise to every application team. A small,
unambiguous config surface is easier to validate, document, and support.

### IV. Security & Data Privacy

- Secrets (credentials, API keys, connection strings) MUST NOT appear in platform code, config
  files, job parameters, or logs. They MUST be read at runtime from AWS Secrets Manager or Glue
  connections.
- Each Glue job MUST run under an IAM role scoped to the specific S3 prefixes, catalog databases,
  and other resources it uses (least privilege). Platform access to the landing zone MUST be
  read-only. Wildcard resource grants MUST be justified in the plan.
- Data at rest (S3, catalog, temporary directories) and in transit MUST be encrypted; Glue
  security configurations MUST enable S3 and CloudWatch encryption, and job-bookmark encryption
  wherever bookmarks are used.
- Every pipeline config that maps personal or sensitive data into the raw vault MUST identify
  those fields and state how each is handled (masked, hashed, tokenized, or retained with
  justification).
- Columns not mapped in a pipeline config (including new columns from schema drift) MUST NOT
  enter the raw vault until they are mapped, which brings them under the rule above. Until then
  they exist only in landing-zone files and, during a load, in staging. Staging and quarantine
  MUST be treated as restricted because they can hold unclassified data.
- Raw personal data and sample records MUST NOT be written to logs, metrics, error messages, or
  test fixtures. Test data MUST be synthetic or irreversibly anonymized.

**Rationale**: The platform moves data in bulk for many teams, so one leak exposes whole
datasets. Mapping is the review point that keeps unclassified data out of the vault.

### V. Observability

- Every job MUST emit structured (JSON) logs that include pipeline name, config version, run ID,
  stage, and input/output locations. Deployed jobs log to CloudWatch; local runs log to the
  console in the same format.
- Every run MUST record, for each landing-zone file, row counts read, staged, rejected, and
  written to each hub, link, and satellite, so counts can be reconciled from file to vault.
- The platform MUST keep a queryable load audit record of which files were processed, when, by
  which pipeline and config version, and with what outcome.
- Failures MUST surface as a failed job run with an actionable error that names the pipeline,
  file, and stage. Jobs MUST NOT swallow exceptions or report success after partial failure.
- Records rejected by hard rules MUST be routed to a quarantine location with the rejection
  reason, not silently dropped.

**Rationale**: Application teams own the data, while the platform team owns the jobs. Both need
to see exactly what happened to each file without reading code.

## Scope & Boundaries

- **In scope**: loading files from the S3 landing zone into a staging layer, and from staging
  into the raw vault; the config schema and validator; platform deployment.
- **Out of scope**: moving data from source systems into the landing zone (owned by application
  teams); the business vault and information marts; personal-data erasure in the raw vault; the
  method for hashing personal data; how schema drift in landing-zone files is detected and
  handled. The last three are to be addressed by future amendments.
- **Landing zone as archive**: because staging is transient, the landing zone is the only
  long-term copy of delivered data. Application teams MUST NOT modify or delete delivered files
  within the agreed retention period of TODO(LANDING_ZONE_RETENTION): period not yet agreed with
  application teams. Backfills and vault rebuilds are possible only for files still in the
  landing zone.
- Features outside this scope MUST NOT be added without amending this section first.

## Platform & Technology Constraints

- Platform code MUST be written in PySpark (Python 3) and run as AWS Glue Spark jobs. Other
  runtimes require written justification in the plan.
- Staging and raw vault tables MUST be Delta Lake tables on S3.
- Storage and catalog access MUST go through one platform module, and the platform MUST use only
  the open Delta Lake protocol. Correctness (idempotency, load tracking) MUST NOT depend on
  Glue-only features such as job bookmarks. This keeps a later move to Databricks Unity Catalog
  limited to that module.
- Job logic MUST be separate from Glue entry-point code (argument parsing, `GlueContext` setup)
  so transformations can be tested against a local SparkSession without AWS.
- Every job MUST be runnable end to end on a developer machine without an AWS account, inside a
  containerized environment defined in this repository. That environment MUST match the pinned
  Glue version's Spark, Python, and Delta Lake versions, and SHOULD be built on the official AWS
  Glue Docker image for that version so local and deployed behavior stay the same.
- Local runs MUST use local stand-ins for the landing zone, staging, raw vault, and catalog. The
  storage and catalog module selects them by configuration; job logic MUST NOT contain
  local-only code paths.
- The local environment MUST start with one documented command, and CI MUST run the automated
  tests in that same environment.
- Local runs MUST NOT require or use production credentials or production data.
- Staging is transient. It holds only the batch being loaded and MUST be cleared once that
  batch's raw vault load succeeds. Failed loads are retried from the landing-zone files.
- Backfilling a newly mapped column and rebuilding raw vault tables MUST be done by reprocessing
  landing-zone files, using the same idempotent load path as normal runs.
- Glue version, worker type, and worker count MUST be pinned in infrastructure-as-code, not set
  by hand in the console.

## Development Workflow & Quality Gates

- Work follows the Spec Kit flow: specify → plan → tasks → implement. Each plan MUST include a
  Constitution Check against Principles I–V and the constraints above.
- Platform releases are versioned. Each pipeline config MUST declare the config schema version
  it targets.
- Transformation logic MUST have automated tests that run against a local SparkSession using
  synthetic data before merge.
- Hash key and hash diff calculation MUST have fixed-input/fixed-output tests, so any change to
  the hashing standard fails CI.
- The config validator MUST have tests covering valid configs, each class of invalid config,
  and the wording of its error messages.
- Code review MUST confirm: no pipeline-specific logic in platform code; no secrets or personal
  data in code, logs, or fixtures; IAM scope matches the resources used; audit and row-count
  logging are present; and any added complexity is justified.
- Code MUST pass the project's configured linter and formatter before merge.

## Governance

- This constitution overrides conflicting practices, templates, and guidance in the repository.
- Amendments MUST be made through `/speckit-constitution`, described in a pull request with
  the reason for the change, and approved by a project maintainer before merge.
- Versioning follows semantic versioning:
  - MAJOR: removing or redefining a principle in a backward-incompatible way.
  - MINOR: adding a principle or section, or materially expanding guidance.
  - PATCH: clarifications and wording fixes with no change in meaning.
- Every plan and code review MUST check compliance with this constitution. Violations MUST be
  fixed or explicitly justified in the plan's Complexity Tracking table.

**Version**: 1.0.0 | **Ratified**: 2026-11-17 | **Last Amended**: 2026-11-17

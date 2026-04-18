# File formats used by SpectrafUSE

This document is the single reference for every file format the pipeline
consumes or produces. When something looks unclear in the code, this page
is the source of truth for what bytes and columns must be present.

Contents:

- [1. QPX input parquets](#1-qpx-input-parquets)
- [2. Internal pipeline artifacts](#2-internal-pipeline-artifacts)
  - [2.1 MaRaCluster `.dat` + `.scan_info.dat` (binary)](#21-maracluster-dat--scan_infodat-binary)
  - [2.2 `.scan_titles.txt` (tab-separated text)](#22-scan_titlestxt-tab-separated-text)
  - [2.3 MaRaCluster cluster TSV](#23-maracluster-cluster-tsv)
- [3. Cluster DB outputs (parquet)](#3-cluster-db-outputs-parquet)
  - [3.1 `cluster_metadata.parquet`](#31-cluster_metadataparquet)
  - [3.2 `psm_cluster_membership.parquet`](#32-psm_cluster_membershipparquet)
- [4. MSP consensus spectral library](#4-msp-consensus-spectral-library)
- [5. Pre-existing cluster DB directory (for `--existing_cluster_db`)](#5-pre-existing-cluster-db-directory-for---existing_cluster_db)
- [6. USI convention](#6-usi-convention)
- [7. Format responsibility map](#7-format-responsibility-map)

---

## 1. QPX input parquets

A QPX project is a directory containing one `.psm.parquet` file (the PSM
spectra and scores) plus several metadata parquets. SpectrafUSE reads the
PSM file for spectra and the run/sample metadata to build the
`{run_file_name: [species, instrument]}` dict used during clustering.

Layout:

```
data/<project_accession>/
├── <project_accession>.psm.parquet          REQUIRED  — PSM spectra
├── <project_accession>.run.parquet          REQUIRED  — run metadata (instrument)
├── <project_accession>.sample.parquet       REQUIRED  — sample metadata (organism)
├── <project_accession>.ontology.parquet     optional  — CV term mappings
├── <project_accession>.dataset.parquet      optional  — project-level metadata
└── <project_accession>.sdrf.tsv             optional  — SDRF source (only used by `convert-to-qpx`)
```

Filenames must start with the project accession prefix (letters + digits,
e.g. `PXD014877`). `ParquetPathHandler.get_item_info()` extracts the
prefix and uses it as the `project_accession` in downstream outputs.

### 1.1 `<accession>.psm.parquet`

One row per PSM. Columns SpectrafUSE reads (verified against
`data/PXD014877/PXD014877.psm.parquet`):

| column | type | usage |
|---|---|---|
| `sequence` | string | peptide sequence (no mods) |
| `peptidoform` | string | peptide with inline modifications, e.g. `M[Oxidation]VVAEIEEGM[Oxidation]DEYNYSGPVVK` |
| `charge` | int16 | precursor charge |
| `observed_mz` | float | observed precursor m/z (preferred for `.dat`) |
| `calculated_mz` | float | theoretical precursor m/z (fallback when `observed_mz` is null) |
| `posterior_error_probability` | double | PEP score — used by the `best` consensus strategy |
| `additional_scores` | `list<struct<score_name, score_value, higher_better>>` | QPX score container; `global_qvalue` is extracted from here when there's no top-level `global_qvalue` column |
| `run_file_name` | string | RAW file stem; key for joining with `.run.parquet` |
| `scan` | `list<int32>` | scan number (QPX wraps in a list; adapter unwraps to the first element) |
| `mz_array` | `list<float>` | fragment m/z values |
| `intensity_array` | `list<float>` | fragment intensities, aligned 1:1 with `mz_array` |

Legacy MSNet-format parquets use `precursor_charge`, `exp_mass_to_charge`,
`reference_file_name`, and scalar `scan`. `ParquetSchemaAdapter` in
`common/constant.py` normalizes both schemas to the canonical names above
at read time.

### 1.2 `<accession>.run.parquet`

One row per run file. Read fields: `run_file_name`, `instrument`.

```
run_accession       string
run_file_name       string
file_name           string
samples             list<struct<sample_accession, label, biological_replicate, technical_replicate>>
fraction            string
instrument          string
enzymes             list<string>
dissociation_method string
modification_parameters list<struct<…>>
```

### 1.3 `<accession>.sample.parquet`

One row per sample. Read field: `organism`.

```
sample_accession  string
organism          string
organism_part     string
```

### 1.4 `<accession>.ontology.parquet` *(optional at runtime)*

Maps free-text values in `run.parquet` / `sample.parquet` to CV accessions.
SpectrafUSE does not consume this at clustering time; it is preserved for
downstream consumers of the cluster DB.

```
field_name / ontology_name / ontology_accession / ontology_source /
ontology_version / view / description / source_column_name / source_tool
```

### 1.5 `<accession>.dataset.parquet` *(optional at runtime)*

Project-level manifest — accession, title, software, file inventory and
checksums. Not read by the clustering steps.

### 1.6 `<accession>.sdrf.tsv`

Original SDRF source. Only consumed by `pyspectrafuse convert-to-qpx`
when generating the run/sample/ontology/dataset parquets for the first
time; never read during clustering.

---

## 2. Internal pipeline artifacts

These files live in the Nextflow work directory during a run. They are the
contract between `PARQUET_TO_DAT` / `EXTRACT_REPS_DAT`, MaRaCluster, and
the cluster-DB build step. Understanding them matters if you're debugging
or plugging in your own tooling.

### 2.1 MaRaCluster `.dat` + `.scan_info.dat` (binary)

Each spectrum is represented as a flat 100-byte `Spectrum` struct plus a
16-byte `ScanInfo` struct. No header, no framing — the number of spectra
is `filesize / 100`. Little-endian.

```c
struct Spectrum {   // 100 bytes total
    uint32 fileIdx;        //  offset  0, 4 B   — always 0 after CONCAT_DAT_FILES
    uint32 scannr;          //  offset  4, 4 B   — globally unique within the file
    uint32 charge;          //  offset  8, 4 B   — precursor charge
    float  precMz;          //  offset 12, 4 B   — precursor m/z
    float  retentionTime;   //  offset 16, 4 B   — 0.0 (not populated)
    int16  fragBins[40];    //  offset 20, 80 B  — top-40 peak bins, 0-padded
};

struct ScanInfo {    // 16 bytes
    uint32 fileIdx;       //  offset 0, 4 B
    uint32 scannr;        //  offset 4, 4 B
    float  precMz;        //  offset 8, 4 B
    float  precMzExp;     //  offset 12, 4 B   — same as precMz; kept for API parity
};
```

Peak binning replicates MaRaCluster's `BinSpectra::binBinaryTruncated`:

```
bin = floor(mz / 1.000508 + 0.32)
```

Sort peaks by intensity descending, skip any peak whose m/z is ≥ the
neutral precursor mass (`precMz*charge − protonMass*(charge − 1)`), take
the top 40 distinct bin indices, then sort ascending. New data is dropped
if it produces fewer than `MIN_SCORING_PEAKS = 15` bins; representative
spectra are kept even with a single bin (they are known-good consensus
spectra by construction).

Constants live at `pyspectrafuse-lib/pyspectrafuse/maracluster_dat.py:46-56`.

Sizing: ~100 bytes/spectrum → 1.3 GB for 12.8M PSMs, ~50 GB projected at
500M. Two sibling files per `.dat`: `{stem}.dat` (the spectra) and
`{stem}.scan_info.dat` (the ScanInfo structs).

### 2.2 `.scan_titles.txt` (tab-separated text)

Maps the `scannr` field of each `.dat` struct back to its original
identity. One line per spectrum; three tab-separated columns. No header.

```
<file_idx> <TAB> <scannr> <TAB> <title>
```

Two title flavors — both appear in the same file when reps and new data
are clustered together:

**New-data title** — written by `PARQUET_TO_DAT`:
```
id=mzspec::<run_file_name>:scan:<orig_scan>:<peptidoform>/<charge>
```

Example (from `test_output/PXD014877/dat_output/PXD014877.psm_charge2.scan_titles.txt`):
```
0	0	id=mzspec::20181127_QX1_JoMu_SA_Easy12-7_uPAC_500ng_MycoplasmeniRT:scan:186253:DISPLLANGEVLNYTINQMAELAK/2
0	1	id=mzspec::20181127_QX1_JoMu_SA_Easy12-7_uPAC_500ng_MycoplasmeniRT:scan:142531:GYQTIDLGPDTDQQPSSYAFYGK/2
0	2	id=mzspec::20181127_QX1_JoMu_SA_Easy12-7_uPAC_500ng_MycoplasmeniRT:scan:122629:M[Oxidation]VVAEIEEGM[Oxidation]DEYNYSGPVVK/2
```

**Representative title** — written by `EXTRACT_REPS_DAT` for each consensus
spectrum coming from `--existing_cluster_db`:
```
rep:<cluster_id>
```

Example:
```
0	0	rep:8f72f9be-c93a-5836-b3ca-01b7296e2e99
0	1	rep:a14c2b4e-1188-4c19-b1ec-8f3a4b1d9e77
```

The cluster-DB build step uses these markers to tell apart new PSMs
(which get added to `psm_cluster_membership.parquet`) from reps (which
are only used for cluster-ID resolution — reps are not PSMs).

### 2.3 MaRaCluster cluster TSV

MaRaCluster's own output, written with a `_p<cluster_threshold>.tsv`
suffix (default `_p30.tsv`). Three tab-separated columns, no header, one
row per spectrum:

```
<mgf_path> <TAB> <scannr> <TAB> <cluster_id>
```

- `mgf_path` — the dummy `.mgf` basename MaRaCluster sees (because its
  CLI hardcodes a `.mgf` file list). The corresponding `.dat` file shares
  the basename.
- `scannr` — matches the `scannr` in the `.dat` / `.scan_titles.txt`.
- `cluster_id` — MaRaCluster's intra-window cluster label.

After `SPLIT_MZ_WINDOWS` there is one TSV per window; `MERGE_MZ_WINDOWS`
deduplicates spectra in overlap zones (lowest window index wins, safe
because the 1 Da overlap is far wider than MaRaCluster's 20 ppm precursor
tolerance). `cluster_id` values are prefixed `w{i}_` per window before
dedup to guarantee uniqueness.

Example (pre-merge, window 0):
```
PXD014877.psm_charge2_w0.mgf	0	0
PXD014877.psm_charge2_w0.mgf	1	0
PXD014877.psm_charge2_w0.mgf	2	0
PXD014877.psm_charge2_w0.mgf	7	3
```

---

## 3. Cluster DB outputs (parquet)

Both schemas live in `pyspectrafuse-lib/pyspectrafuse/common/schemas.py`
and are written with zstd compression.

### 3.1 `cluster_metadata.parquet`

One row per cluster. Persistent across rounds — re-emitted (merged) every
time you pass `--existing_cluster_db`.

| column | type | description |
|---|---|---|
| `cluster_id` | string | stable UUID; inherited from a prior round when a rep matched, freshly minted otherwise |
| `species` | string | partition species |
| `instrument` | string | partition instrument (empty string when clustered with `--skip_instrument`) |
| `charge` | int8 | partition charge |
| `peptidoform` | string | peptidoform of the representative spectrum, e.g. `DISPLLANGEVLNYTINQMAELAK/2` |
| `peptide_sequence` | string | bare sequence with modifications stripped |
| `consensus_mz_array` | `list<float32>` | consensus fragment m/z |
| `consensus_intensity_array` | `list<float32>` | consensus fragment intensity, aligned with `consensus_mz_array` |
| `consensus_method` | string | `best`, `bin`, `most`, or `average` |
| `precursor_mz` | float64 | consensus precursor m/z |
| `member_count` | int32 | PSMs in the cluster across all rounds |
| `project_count` | int16 | distinct projects contributing PSMs |
| `best_pep` | float64 | minimum `posterior_error_probability` among members |
| `best_qvalue` | float64 | minimum `global_qvalue` among members |
| `purity` | float32 | fraction of members sharing the dominant peptidoform (1.0 = perfectly pure) |
| `is_reused_cluster` | bool | **provenance**: `True` when this `cluster_id` was inherited from `--existing_cluster_db` via a representative match |
| `source_datasets` | `list<string>` | **provenance**: distinct `project_accession` values that contribute PSMs, accumulated across rounds |

### 3.2 `psm_cluster_membership.parquet`

One row per PSM. Grows monotonically with each round (dedup by USI).

| column | type | description |
|---|---|---|
| `cluster_id` | string | FK to `cluster_metadata.cluster_id` |
| `usi` | string | unique spectrum identifier (see §6) |
| `project_accession` | string | source project |
| `reference_file_name` | string | RAW file stem (= `run_file_name` from QPX) |
| `scan` | int32 | scan number in the RAW file |
| `peptidoform` | string | peptide with mods |
| `charge` | int8 | precursor charge |
| `precursor_mz` | float64 | observed (or calculated) precursor m/z |
| `posterior_error_probability` | float64 | PSM PEP |
| `global_qvalue` | float64 | PSM q-value |
| `species` | string | partition species |
| `instrument` | string | partition instrument |

---

## 4. MSP consensus spectral library

`GENERATE_MSP_FORMAT` writes one gzipped MSP file per partition:

```
msp/<species>/<instrument>/<charge>/<project>_<uuid>.msp.gz
```

or, when `--skip_instrument` is set:

```
msp/<species>/<charge>/<project>_<uuid>.msp.gz
```

Each spectrum block follows the classic NIST MSP layout:

```
Name: <peptidoform>
MW: <precursor_mz>
Comment: clusterID=<uuid5-of-usi> Nreps=<N> PEP=<value>
Num peaks: <N>
<mz> <intensity>
<mz> <intensity>
…
```

Real excerpt (from a PXD004452 run):

```
Name: RM[Oxidation]GESDDSILR/3
MW: 432.2069091796875
Comment: clusterID=8f72f9be-c93a-5836-b3ca-01b7296e2e99 Nreps=10 PEP=4.40326e-06
Num peaks: 160
102.055419921875 617466.125
112.0870361328125 293152.5
115.05069732666016 68990.8515625
…
```

- `clusterID` is a UUID5 derived from the USI of the cluster's
  representative PSM (`MspUtil.usi_to_uuid`) — not the same as
  `cluster_metadata.cluster_id`. Use `cluster_id` for joins.
- `Nreps` is the number of member PSMs.
- `PEP` is the cluster's `best_pep`.
- Spectra are separated by two blank lines.
- Gzipped stream: multiple `.gz` writes concatenate into a valid gzip file
  (see `MspUtil.write2msp`).

---

## 5. Pre-existing cluster DB directory (for `--existing_cluster_db`)

When you pass `--existing_cluster_db <path>`, the pipeline expects the
output layout from a previous run, one `cluster_metadata.parquet` +
`psm_cluster_membership.parquet` pair per partition:

```
<existing_cluster_db>/
└── <species>/
    └── <instrument>/                (omit this level if the existing DB was built with --skip_instrument)
        └── <charge>/
            ├── cluster_metadata.parquet
            └── psm_cluster_membership.parquet
```

`workflows/spectrafuse.nf` discovers partitions by globbing
`${existing_cluster_db}/**/cluster_metadata.parquet` and infers
`(species, instrument, charge)` from the directory path. A mismatch
between the old layout's `--skip_instrument` mode and the current run
will misread species/instrument — keep the flag consistent across rounds.

Both parquets must match the schemas in §3. An older cluster DB written
before the provenance columns were added is forward-compatible: missing
`is_reused_cluster` / `source_datasets` columns will be populated on the
first merge.

---

## 6. USI convention

USIs link every PSM to its original spectrum and peptide call. The format
used in `psm_cluster_membership.usi` and in scan_titles:

```
mzspec:<project_accession>:<run_file_name>:scan:<scan>:<peptidoform>/<charge>
```

Example:
```
mzspec:PXD014877:20181127_QX1_JoMu_SA_Easy12-7_uPAC_500ng_MycoplasmeniRT:scan:186253:DISPLLANGEVLNYTINQMAELAK/2
```

Note that `scan_titles.txt` entries use the four-colon form
`id=mzspec::<run>:scan:<scan>:<peptidoform>/<charge>` (no project
accession embedded). The project accession is attached during the
`BUILD_CLUSTER_DB` step from the parquet directory name, then the full
USI is written into `psm_cluster_membership.parquet`. If you construct
USIs yourself, use the project-aware form above.

---

## 7. Format responsibility map

| Format | Produced by | Consumed by |
|---|---|---|
| `*.psm.parquet` | upstream quantms / `convert-to-qpx` | `PARQUET_TO_DAT`, `BUILD_CLUSTER_DB`, `GENERATE_MSP_FORMAT` |
| `*.run.parquet`, `*.sample.parquet` | upstream quantms / `convert-to-qpx` | `qpx_metadata.get_metadata_dict` |
| `.dat`, `.scan_info.dat` | `PARQUET_TO_DAT`, `EXTRACT_REPS_DAT`, `CONCAT_DAT_FILES` | `RUN_MARACLUSTER_DAT` (via `-D`) |
| `.scan_titles.txt` | `PARQUET_TO_DAT`, `EXTRACT_REPS_DAT`, `CONCAT_DAT_FILES` | `BUILD_CLUSTER_DB`, `MERGE_INTO_EXISTING_DB`, `GENERATE_MSP_FORMAT` |
| MaRaCluster `_p30.tsv` | `RUN_MARACLUSTER_DAT` | `MERGE_MZ_WINDOWS`, `BUILD_CLUSTER_DB`, `GENERATE_MSP_FORMAT` |
| `cluster_metadata.parquet` | `BUILD_CLUSTER_DB` / `MERGE_INTO_EXISTING_DB` | `EXTRACT_REPS_DAT` next round; downstream users |
| `psm_cluster_membership.parquet` | `BUILD_CLUSTER_DB` / `MERGE_INTO_EXISTING_DB` | `MERGE_INTO_EXISTING_DB` next round; downstream users |
| `*.msp.gz` | `GENERATE_MSP_FORMAT` | spectral library search tools (Comet/MSPepSearch/etc.) |

Every claim in this document is traceable to code in
`pyspectrafuse-lib/pyspectrafuse/`. If a field or filename here ever
disagrees with the source, the code is the truth — please open an issue
(or fix the doc).

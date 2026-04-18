# Workflow figure — design spec

Source for drawing the workflow figure that the README will reference.
The previous figures (`docs/algorithm.{excalidraw,png,svg}` and
`docs/images/spectrafuse_workflow.svg`) reflected the old two-mode
design (`--mode full` vs `--mode incremental`) and have been removed.
Use this spec to regenerate a figure that matches the unified pipeline.

## Layout

Five horizontal bands top-to-bottom:

1. **Inputs** — two source nodes at the top.
2. **Per-source conversion** — PARQUET_TO_DAT and (optionally) EXTRACT_REPS_DAT.
3. **Shared clustering spine** — CONCAT → SPLIT → MARACLUSTER → MERGE.
4. **Cluster DB build** — single box labelled *BUILD_CLUSTER_DB (merges into existing DB when provided)*.
5. **Outputs** — cluster_db parquets + MSP library, both partitioned.

The emphasis is that there is **one pipeline**; the existing-cluster-DB arm
is a dashed optional branch that joins the shared spine at CONCAT_DAT_FILES.

## Nodes

| id | label | sublabel | style |
|----|-------|----------|-------|
| IN_NEW | New QPX projects | `*.psm.parquet`, `*.run.parquet`, `*.sample.parquet` | solid box, primary color |
| IN_EXISTING | `--existing_cluster_db` (optional) | `cluster_metadata.parquet` per partition | dashed box, muted color |
| N1 | PARQUET_TO_DAT | charge-filtered `.dat` + `.scan_titles.txt` | solid |
| N2 | EXTRACT_REPS_DAT | one consensus spectrum per cluster → `rep:{id}` | dashed |
| S1 | CONCAT_DAT_FILES | per species / [instrument] / charge | solid, emphasize this is the join point |
| S2 | SPLIT_MZ_WINDOWS | 300 Da windows, 1 Da overlap (default) | solid |
| S3 | RUN_MARACLUSTER_DAT | parallel per window | solid, show fan-out |
| S4 | MERGE_MZ_WINDOWS | reconcile overlap zones | solid |
| B1 | BUILD_CLUSTER_DB | resolves `rep:` titles into existing cluster_ids when reps present; merges into existing DB | solid, label the dual behavior in the sublabel |
| O1 | cluster_db/ | `cluster_metadata.parquet` + `psm_cluster_membership.parquet` — per `species/[instrument]/charge` | solid, cylinder/stack icon |
| O2 | MSP library | `*.msp.gz` per partition | solid, document icon |

## Edges

```
IN_NEW         ───▶ N1
IN_EXISTING    ╌╌▶ N2     (dashed: only when param set)
N1             ───▶ S1
N2             ╌╌▶ S1     (dashed)
S1 ───▶ S2 ───▶ S3 ───▶ S4 ───▶ B1
B1 ───▶ O1
B1 ───▶ O2              (label: ". . . → GENERATE_MSP_FORMAT")
```

## Annotations

- Above the spine: "Always on — .dat only, no MGF" (small caption).
- Near S2/S3: "m/z windowing parallelizes MaRaCluster within each charge".
- Near B1: "Provenance columns: is_reused_cluster, source_datasets".
- Near O1/O2 partition path: "species/instrument/charge — or species/charge with --skip_instrument".

## Colors (suggestion, keep accessible)

- Primary solid boxes: slate/blue (#2c3e50 outline, #ecf0f1 fill).
- Dashed optional arm: muted gray (#7f8c8d outline, #f2f2f2 fill).
- Outputs: green accent (#27ae60 outline).
- Keep contrast AAA for body text.

## Minimal ASCII equivalent

```
  [New QPX projects]                     [--existing_cluster_db] (optional)
         │                                          ╎
         ▼                                          ▼
  PARQUET_TO_DAT                          EXTRACT_REPS_DAT
         │                                          ╎
         └──────────────────┬───────────────────────┘
                            ▼
                    CONCAT_DAT_FILES   (per species / [instrument] / charge)
                            │
                            ▼
                   SPLIT_MZ_WINDOWS    (300 Da windows, 1 Da overlap)
                            │
                            ▼
                 RUN_MARACLUSTER_DAT   (parallel per window)
                            │
                            ▼
                   MERGE_MZ_WINDOWS
                            │
                            ▼
                   BUILD_CLUSTER_DB    (merges into --existing_cluster_db when provided;
                            │          stamps is_reused_cluster + source_datasets)
            ┌───────────────┴──────────────┐
            ▼                              ▼
    cluster_db/<sp>/<inst>/<z>/    msp_files/<sp>/<inst>/<z>/*.msp.gz
       cluster_metadata.parquet
       psm_cluster_membership.parquet
```

When `--skip_instrument` is set, all `<sp>/<inst>/<z>` paths collapse to `<sp>/<z>`.

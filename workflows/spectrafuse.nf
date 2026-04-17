/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    SPECTRAFUSE WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Spectral clustering using MaRaCluster.

    Single unified pipeline — both full and incremental modes use the same
    dat + m/z windowing path:

    Full mode (default):
      PARQUET_TO_DAT → SPLIT_MZ_WINDOWS → RUN_MARACLUSTER_DAT → MERGE_MZ_WINDOWS → BUILD_CLUSTER_DB

    Incremental mode (--mode incremental --existing_cluster_db <path>):
      EXTRACT_REPS_DAT ─┐
      PARQUET_TO_DAT ────┼→ combine .dat → SPLIT_MZ_WINDOWS → RUN_MARACLUSTER_DAT
                         └→ MERGE_MZ_WINDOWS → RESOLVE_AND_MERGE_INCREMENTAL

    All modes partition by species/instrument/charge.
    Both modes parallelize by precursor m/z window within each charge.
----------------------------------------------------------------------------------------
*/

// ── Shared modules (used by both modes) ──
include { PARQUET_TO_DAT }       from '../modules/local/pyspectrafuse/parquet_to_dat/main'
include { SPLIT_MZ_WINDOWS }     from '../modules/local/pyspectrafuse/split_mz_windows/main'
include { RUN_MARACLUSTER_DAT }  from '../modules/local/maracluster/run_maracluster_dat/main'
include { MERGE_MZ_WINDOWS }     from '../modules/local/pyspectrafuse/merge_mz_windows/main'

// ── Full mode only ──
include { BUILD_CLUSTER_DB }     from '../modules/local/pyspectrafuse/build_cluster_db/main'
include { GENERATE_MSP_FORMAT }  from '../modules/local/pyspectrafuse/generate_msp_format/main'

// ── Incremental mode only ──
include { EXTRACT_REPS_DAT }              from '../modules/local/pyspectrafuse/extract_representatives_dat/main'
include { RESOLVE_AND_MERGE_INCREMENTAL } from '../modules/local/pyspectrafuse/resolve_and_merge_incremental/main'


workflow SPECTRAFUSE {
    take:
    ch_projects
    ch_parquet_dir

    main:
    ch_versions = channel.empty()

    // ════════════════════════════════════════════════════════════════════
    // Step 1: Convert new project data to .dat (both modes)
    // ════════════════════════════════════════════════════════════════════
    ch_projects
        .map { project_dir ->
            [ [ id: project_dir.baseName, file_idx: 0 ], project_dir ]
        }
        .set { ch_projects_for_dat }

    PARQUET_TO_DAT(ch_projects_for_dat)
    ch_versions = ch_versions.mix(PARQUET_TO_DAT.out.versions)

    // ════════════════════════════════════════════════════════════════════
    // Step 1b (incremental only): Extract representatives from existing DB
    // ════════════════════════════════════════════════════════════════════
    if (params.mode == 'incremental') {
        ch_existing_db = channel.fromPath("${params.existing_cluster_db}/**/cluster_metadata.parquet")
            .map { meta_file ->
                def membership = meta_file.parent.resolve('psm_cluster_membership.parquet')
                def charge = meta_file.parent.parent.name
                def instrument = meta_file.parent.parent.parent.name
                def species = meta_file.parent.parent.parent.parent.name
                def id = "${species}__${instrument}__${charge}"
                    .replaceAll(/[\\/]/, '_').replaceAll(/\s+/, '_')
                def meta = [ id: id, species: species, instrument: instrument, charge: charge ]
                return [meta, meta_file, membership]
            }

        EXTRACT_REPS_DAT(
            ch_existing_db.map { meta, meta_file, _mem -> [meta, meta_file] }
        )
        ch_versions = ch_versions.mix(EXTRACT_REPS_DAT.out.versions)
    }

    // ════════════════════════════════════════════════════════════════════
    // Step 2: Pair dat + scaninfo + titles by filename prefix, group by charge
    // ════════════════════════════════════════════════════════════════════
    // Helper: key .dat files by charge prefix
    PARQUET_TO_DAT.out.dat_files
        .transpose()
        .map { meta, dat_file ->
            if (dat_file.size() == 0) return null
            def m = (dat_file.name =~ /^(.+_charge\d+)\.dat$/)
            if (!m) return null
            return [m[0][1], dat_file]
        }
        .filter { it != null }
        .set { ch_dat_keyed }

    PARQUET_TO_DAT.out.scaninfo_files
        .transpose()
        .map { meta, si_file ->
            if (si_file.size() == 0) return null
            def m = (si_file.name =~ /^(.+_charge\d+)\.scan_info\.dat$/)
            if (!m) return null
            return [m[0][1], si_file]
        }
        .filter { it != null }
        .set { ch_scaninfo_keyed }

    PARQUET_TO_DAT.out.scan_titles
        .transpose()
        .map { meta, titles_file ->
            if (titles_file.size() == 0) return null
            def m = (titles_file.name =~ /^(.+_charge\d+)\.scan_titles\.txt$/)
            if (!m) return null
            return [m[0][1], titles_file]
        }
        .filter { it != null }
        .set { ch_titles_keyed }

    // Join new data by filename prefix
    ch_dat_keyed
        .join(ch_scaninfo_keyed)
        .join(ch_titles_keyed)
        .map { key, dat_file, scaninfo_file, titles_file ->
            def charge_match = (key =~ /_charge(\d+)$/)
            def charge = "charge${charge_match[0][1]}"
            def meta = [
                id: "partition__${charge}",
                species: params.default_species ?: 'Unknown',
                instrument: params.default_instrument ?: 'Unknown',
                charge: charge
            ]
            return [meta, dat_file, scaninfo_file, titles_file]
        }
        .set { ch_new_data_split_input }

    // Group scan_titles by charge for downstream BUILD_CLUSTER_DB / RESOLVE_AND_MERGE
    ch_titles_keyed
        .map { key, titles_file ->
            def charge_match = (key =~ /_charge(\d+)$/)
            def charge = "charge${charge_match[0][1]}"
            return [charge, titles_file]
        }
        .groupTuple()
        .set { ch_new_grouped_titles }

    // ════════════════════════════════════════════════════════════════════
    // Step 2b (incremental): Combine representative .dat with new data .dat
    //   per charge, then feed into the shared windowing pipeline
    // ════════════════════════════════════════════════════════════════════
    if (params.mode == 'incremental') {
        // Key representative outputs by charge
        EXTRACT_REPS_DAT.out.dat_files
            .transpose()
            .map { meta, dat_file ->
                if (dat_file.size() == 0) return null
                return [meta.charge, dat_file]
            }
            .filter { it != null }
            .set { ch_rep_dat_by_charge }

        EXTRACT_REPS_DAT.out.scaninfo_files
            .transpose()
            .map { meta, si_file ->
                if (si_file.size() == 0) return null
                return [meta.charge, si_file]
            }
            .filter { it != null }
            .set { ch_rep_scaninfo_by_charge }

        EXTRACT_REPS_DAT.out.scan_titles
            .transpose()
            .map { meta, titles_file ->
                if (titles_file.size() == 0) return null
                return [meta.charge, titles_file]
            }
            .filter { it != null }
            .set { ch_rep_titles_by_charge }

        // Key new data by charge too
        ch_new_data_split_input
            .map { meta, dat, scaninfo, titles ->
                [meta.charge, dat, scaninfo, titles]
            }
            .set { ch_new_by_charge }

        // Combine: for each charge, concatenate .dat files from reps + new data
        // using a Python script that handles scannr renumbering
        ch_new_by_charge
            .map { charge, dat, scaninfo, titles -> [charge, [dat], [scaninfo], [titles]] }
            .join(ch_rep_dat_by_charge.groupTuple())
            .join(ch_rep_scaninfo_by_charge.groupTuple())
            .join(ch_rep_titles_by_charge.groupTuple())
            .map { charge, new_dats, new_scaninfos, new_titles, rep_dats, rep_scaninfos, rep_titles ->
                def meta = [
                    id: "partition__${charge}",
                    species: params.default_species ?: 'Unknown',
                    instrument: params.default_instrument ?: 'Unknown',
                    charge: charge
                ]
                // Concatenate file lists: reps first, then new data
                def all_dats = (rep_dats + new_dats).flatten()
                def all_scaninfos = (rep_scaninfos + new_scaninfos).flatten()
                def all_titles = (rep_titles + new_titles).flatten()
                return [meta, all_dats, all_scaninfos, all_titles]
            }
            .set { ch_combined_split_input }

        // Also collect rep scan_titles by charge for RESOLVE_AND_MERGE
        ch_rep_titles_by_charge
            .groupTuple()
            .set { ch_rep_grouped_titles }

        // Merge new + rep scan_titles for resolve step
        ch_new_grouped_titles
            .join(ch_rep_grouped_titles)
            .map { charge, new_titles, rep_titles ->
                [charge, (rep_titles + new_titles).flatten()]
            }
            .set { ch_all_grouped_titles }

        ch_combined_split_input.set { ch_split_input }
    } else {
        ch_new_data_split_input.set { ch_split_input }
        ch_new_grouped_titles.set { ch_all_grouped_titles }
    }

    // ════════════════════════════════════════════════════════════════════
    // Step 3: Split into m/z windows (shared path)
    // ════════════════════════════════════════════════════════════════════
    // SPLIT_MZ_WINDOWS expects a single .dat per charge, but incremental
    // mode may have multiple .dat files. Concatenate them first.
    ch_split_input
        .branch {
            single: it[1] instanceof Path  // single file, pass directly
            multi: true                     // list of files, need concat
        }
        .set { ch_split_branched }

    // For single-file case, pass through as-is
    ch_split_branched.single
        .set { ch_single_for_split }

    // For multi-file case, concatenate .dat files per charge
    ch_split_branched.multi
        .map { meta, dats, scaninfos, titles ->
            [meta, dats instanceof List ? dats : [dats],
                   scaninfos instanceof List ? scaninfos : [scaninfos],
                   titles instanceof List ? titles : [titles]]
        }
        .set { ch_multi_for_concat }

    // Use a simple process-less approach: cat binary files together
    // and merge scan_titles, adjusting scannr
    CONCAT_DAT_FILES(ch_multi_for_concat)
    ch_versions = ch_versions.mix(CONCAT_DAT_FILES.out.versions)

    ch_single_for_split
        .mix(CONCAT_DAT_FILES.out.combined)
        .set { ch_ready_for_split }

    SPLIT_MZ_WINDOWS(ch_ready_for_split)
    ch_versions = ch_versions.mix(SPLIT_MZ_WINDOWS.out.versions)

    // ════════════════════════════════════════════════════════════════════
    // Step 4: Run MaRaCluster per window (shared path)
    // ════════════════════════════════════════════════════════════════════
    SPLIT_MZ_WINDOWS.out.window_dat_files
        .transpose()
        .map { meta, dat_file ->
            def w_match = (dat_file.name =~ /_w(\d+)\.(scan_info\.)?dat$/)
            if (!w_match) return null
            def window_idx = w_match[0][1]
            def key = "${meta.charge}__w${window_idx}"
            return [key, meta, dat_file]
        }
        .filter { it != null }
        .groupTuple(by: 0)
        .map { key, metas, dat_files ->
            def meta = metas[0] + [id: "partition__${key}"]
            return [meta, dat_files]
        }
        .set { ch_window_dat }

    SPLIT_MZ_WINDOWS.out.window_titles
        .transpose()
        .map { meta, titles_file ->
            def w_match = (titles_file.name =~ /_w(\d+)\.scan_titles\.txt$/)
            if (!w_match) return null
            def window_idx = w_match[0][1]
            def key = "${meta.charge}__w${window_idx}"
            return [key, titles_file]
        }
        .filter { it != null }
        .groupTuple()
        .set { ch_window_titles }

    ch_window_dat
        .map { meta, dat_files -> [meta.id.replace('partition__', ''), meta, dat_files] }
        .join(ch_window_titles)
        .map { _key, meta, dat_files, titles -> [meta, dat_files, titles] }
        .set { ch_mara_input }

    RUN_MARACLUSTER_DAT(ch_mara_input)
    ch_versions = ch_versions.mix(RUN_MARACLUSTER_DAT.out.versions)

    // ════════════════════════════════════════════════════════════════════
    // Step 5: Merge m/z windows back per charge (shared path)
    // ════════════════════════════════════════════════════════════════════
    RUN_MARACLUSTER_DAT.out.maracluster_results
        .map { meta, tsv ->
            def charge = meta.charge
            return [charge, tsv]
        }
        .groupTuple()
        .join(
            SPLIT_MZ_WINDOWS.out.window_manifest
                .map { meta, manifest -> [meta.charge, manifest] }
        )
        .map { charge, tsvs, manifest ->
            def meta = [
                id: "partition__${charge}",
                species: params.default_species ?: 'Unknown',
                instrument: params.default_instrument ?: 'Unknown',
                charge: charge
            ]
            return [meta, tsvs, manifest]
        }
        .set { ch_merge_windows_input }

    MERGE_MZ_WINDOWS(ch_merge_windows_input)
    ch_versions = ch_versions.mix(MERGE_MZ_WINDOWS.out.versions)

    // ════════════════════════════════════════════════════════════════════
    // Step 6: Build output (diverges by mode)
    // ════════════════════════════════════════════════════════════════════
    if (params.mode == 'incremental') {
        // ── Incremental: resolve cluster IDs and merge into existing DB ──
        MERGE_MZ_WINDOWS.out.merged_clusters
            .map { meta, tsv -> [meta.charge, meta, tsv] }
            .join(ch_all_grouped_titles)
            .map { _charge, meta, tsv, titles ->
                [meta, tsv, titles]
            }
            .set { ch_resolve_input }

        // Join with existing DB metadata + membership
        ch_resolve_input
            .map { meta, tsv, titles -> [meta.charge, meta, tsv, titles] }
            .join(
                ch_existing_db.map { meta, mf, mem ->
                    [meta.charge, mf, mem]
                }
            )
            .map { charge, meta, tsv, titles, mf, mem ->
                [meta, tsv, titles, mf, mem]
            }
            .set { ch_resolve_with_db }

        ch_project_dirs = ch_projects.collect()

        RESOLVE_AND_MERGE_INCREMENTAL(
            ch_resolve_with_db.combine(ch_project_dirs.map { dirs -> [dirs] })
        )
        ch_versions = ch_versions.mix(RESOLVE_AND_MERGE_INCREMENTAL.out.versions)

        ch_cluster_parquet = RESOLVE_AND_MERGE_INCREMENTAL.out.cluster_parquet_files
        ch_msp = channel.empty()
    } else {
        // ── Full: build cluster DB from scratch ──
        MERGE_MZ_WINDOWS.out.merged_clusters
            .map { meta, tsv -> [meta.charge, meta, tsv] }
            .join(ch_all_grouped_titles)
            .map { _charge, meta, tsv, titles ->
                [meta, tsv, titles]
            }
            .set { ch_db_input }

        ch_project_dirs = ch_projects.collect()

        BUILD_CLUSTER_DB(
            ch_db_input.combine(ch_project_dirs.map { dirs -> [dirs] })
        )
        ch_versions = ch_versions.mix(BUILD_CLUSTER_DB.out.versions)

        // Generate MSP consensus spectral library
        // ch_db_input already has [meta, tsv, titles] — reuse it for MSP
        GENERATE_MSP_FORMAT(ch_db_input, ch_parquet_dir)
        ch_versions = ch_versions.mix(GENERATE_MSP_FORMAT.out.versions)

        ch_cluster_parquet = BUILD_CLUSTER_DB.out.cluster_metadata
        ch_msp = GENERATE_MSP_FORMAT.out.msp_files
    }

    emit:
    maracluster_results = MERGE_MZ_WINDOWS.out.merged_clusters
    cluster_parquet     = ch_cluster_parquet
    msp_files           = ch_msp
    versions            = ch_versions
}


/*
 * CONCAT_DAT_FILES — Concatenate multiple .dat files per charge into one.
 *
 * Needed for incremental mode where representative .dat files and new-data
 * .dat files must be combined before windowing. Renumbers scannr to be
 * globally unique across all input files.
 */
process CONCAT_DAT_FILES {
    label 'process_low'
    tag { meta.id }

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.4' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.4' }"

    input:
    tuple val(meta), path(dat_files), path(scaninfo_files), path(titles_files)

    output:
    tuple val(meta), path("combined/*.dat"), path("combined/*.scan_info.dat"), path("combined/*.scan_titles.txt"), emit: combined
    path "versions.yml", emit: versions

    script:
    """
    mkdir -p combined

    python3 << 'PYEOF'
import struct
from pathlib import Path

SPECTRUM_SIZE = 100
SCANINFO_SIZE = 16
SPECTRUM_STRUCT = struct.Struct('<IIIff40h')
SCANINFO_STRUCT = struct.Struct('<IIff')

dat_files = sorted(Path('.').glob('*.dat'))
# Separate .dat from .scan_info.dat
data_dats = [f for f in dat_files if '.scan_info.' not in f.name]
scaninfo_dats = [f for f in dat_files if '.scan_info.' in f.name]
titles_files = sorted(Path('.').glob('*.scan_titles.txt'))

# Output stem from meta.id
stem = '${meta.id}'.replace('partition__', '')

out_dat = open(f'combined/{stem}.dat', 'wb')
out_si = open(f'combined/{stem}.scan_info.dat', 'wb')
out_titles = open(f'combined/{stem}.scan_titles.txt', 'w')

global_scannr = 0

for dat_path in data_dats:
    # Find matching scaninfo and titles by stem
    dat_stem = dat_path.stem  # e.g., "reps_cluster_metadata_charge2" or "PXD014877.psm_charge2"
    si_path = dat_path.parent / f"{dat_stem}.scan_info.dat"
    titles_path = dat_path.parent / f"{dat_stem}.scan_titles.txt"

    if not si_path.exists() or not titles_path.exists():
        print(f"WARNING: Missing scaninfo or titles for {dat_path.name}, skipping")
        continue

    # Read and renumber .dat structs
    dat_data = dat_path.read_bytes()
    si_data = si_path.read_bytes()
    n_spectra = len(dat_data) // SPECTRUM_SIZE

    for i in range(n_spectra):
        # Unpack spectrum, renumber scannr
        offset = i * SPECTRUM_SIZE
        fields = list(SPECTRUM_STRUCT.unpack_from(dat_data, offset))
        fields[0] = 0  # file_idx = 0 (single combined file)
        fields[1] = global_scannr
        out_dat.write(SPECTRUM_STRUCT.pack(*fields))

        # Unpack scaninfo, renumber
        si_offset = i * SCANINFO_SIZE
        if si_offset + SCANINFO_SIZE <= len(si_data):
            si_fields = list(SCANINFO_STRUCT.unpack_from(si_data, si_offset))
            si_fields[0] = 0
            si_fields[1] = global_scannr
            out_si.write(SCANINFO_STRUCT.pack(*si_fields))

        global_scannr += 1

    # Read and renumber titles
    with open(titles_path) as f:
        local_scannr = 0
        for line in f:
            parts = line.rstrip('\\n').split('\\t', 2)
            if len(parts) >= 3:
                new_scannr = global_scannr - n_spectra + local_scannr
                out_titles.write(f"0\\t{new_scannr}\\t{parts[2]}\\n")
                local_scannr += 1

print(f"Combined {len(data_dats)} .dat files -> {global_scannr} spectra")

out_dat.close()
out_si.close()
out_titles.close()
PYEOF

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.4")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

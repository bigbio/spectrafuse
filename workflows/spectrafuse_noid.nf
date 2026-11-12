/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    SPECTRAFUSE NO-ID MZML WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Raw mzML inputs are partitioned by species/instrument/charge, converted to
    .dat, clustered with the same MaRaCluster middle of the main workflow, and
    published as no-ID cluster DB + MSP outputs.
----------------------------------------------------------------------------------------
*/

include { MZML_TO_DAT } from '../modules/local/pyspectrafuse/mzml_to_dat/main'
include { SPLIT_MZ_WINDOWS } from '../modules/local/pyspectrafuse/split_mz_windows/main'
include { RUN_MARACLUSTER_DAT } from '../modules/local/maracluster/run_maracluster_dat/main'
include { MERGE_MZ_WINDOWS } from '../modules/local/pyspectrafuse/merge_mz_windows/main'
include { FILTER_NOID_CLUSTERS } from '../modules/local/pyspectrafuse/filter_noid_clusters/main'
include { BUILD_NOID_CLUSTER_DB; MERGE_INTO_EXISTING_NOID_DB } from '../modules/local/pyspectrafuse/build_noid_cluster_db/main'
include { GENERATE_NOID_MSP } from '../modules/local/pyspectrafuse/generate_noid_msp/main'
include { EXTRACT_REPS_DAT } from '../modules/local/pyspectrafuse/extract_representatives_dat/main'


workflow SPECTRAFUSE_NOID {
    take:
    ch_mzml_dir

    main:
    ch_versions = channel.empty()
    def has_existing = params.existing_cluster_db != null

    MZML_TO_DAT(ch_mzml_dir)
    ch_versions = ch_versions.mix(MZML_TO_DAT.out.versions)

    ch_new_partitions = MZML_TO_DAT.out.partition_manifest
        .splitCsv(header: true, sep: '\t')
        .combine(MZML_TO_DAT.out.dat_output_dir)
        .map { row, dat_dir ->
            def meta = [
                id: "partition__${row.partition_id}__${row.charge}",
                partition_id: row.partition_id,
                species: row.species,
                instrument: row.instrument ?: '',
                charge: row.charge,
            ]
            return [
                meta,
                file("${dat_dir}/${row.dat_path}"),
                file("${dat_dir}/${row.scaninfo_path}"),
                file("${dat_dir}/${row.titles_path}"),
                file("${dat_dir}/${row.spectra_path}"),
            ]
        }

    if (has_existing) {
        ch_existing_db = channel.fromPath("${params.existing_cluster_db}/**/cluster_metadata.parquet")
            .map { meta_file ->
                def membership = meta_file.parent.resolve('spectrum_cluster_membership.parquet')
                if (!membership.exists()) {
                    error "existing_cluster_db is not a no-ID DB: missing spectrum_cluster_membership.parquet beside ${meta_file}"
                }
                def charge = meta_file.parent.name
                def instrument = params.skip_instrument ? '' : meta_file.parent.parent.name
                def species = params.skip_instrument
                    ? meta_file.parent.parent.name
                    : meta_file.parent.parent.parent.name
                def parts = params.skip_instrument ? [species] : [species, instrument]
                def partition_id = parts.collect {
                    it.replaceAll(/[^A-Za-z0-9]+/, '_').replaceAll(/^_+|_+$/, '') ?: 'Unknown'
                }.join('__')
                def meta = [
                    id: "partition__${partition_id}__${charge}",
                    partition_id: partition_id,
                    species: species,
                    instrument: instrument,
                    charge: charge,
                ]
                return [meta.id, meta, meta_file, membership]
            }

        EXTRACT_REPS_DAT(ch_existing_db.map { _id, meta, meta_file, _membership -> [meta, meta_file] })
        ch_versions = ch_versions.mix(EXTRACT_REPS_DAT.out.versions)

        ch_new_by_id = ch_new_partitions.map { meta, dat, si, titles, spectra ->
            [meta.id, meta, dat, si, titles, spectra]
        }
        ch_rep_dat = EXTRACT_REPS_DAT.out.dat_files.map { meta, dat -> [meta.id, dat] }
        ch_rep_si = EXTRACT_REPS_DAT.out.scaninfo_files.map { meta, si -> [meta.id, si] }
        ch_rep_titles = EXTRACT_REPS_DAT.out.scan_titles.map { meta, titles -> [meta.id, titles] }

        ch_concat_input = ch_new_by_id
            .join(ch_rep_dat)
            .join(ch_rep_si)
            .join(ch_rep_titles)
            .join(ch_existing_db.map { id, meta, mf, mem -> [id, mf, mem] })
            .map { id, meta, new_dat, new_si, new_titles, spectra, rep_dat, rep_si, rep_titles, existing_meta, existing_membership ->
                return [
                    meta,
                    [rep_dat, new_dat],
                    [rep_si, new_si],
                    [rep_titles, new_titles],
                    spectra,
                    existing_meta,
                    existing_membership,
                ]
            }

        CONCAT_NOID_DAT_FILES(ch_concat_input.map { meta, dats, sis, titles, _spectra, _em, _eMem ->
            [meta, dats, sis, titles]
        })
        ch_versions = ch_versions.mix(CONCAT_NOID_DAT_FILES.out.versions)

        ch_ready_for_split = CONCAT_NOID_DAT_FILES.out.combined
        ch_context = CONCAT_NOID_DAT_FILES.out.combined
            .map { meta, dat, si, titles -> [meta.id, meta, titles] }
            .join(ch_concat_input.map { meta, _dats, _sis, _titles, spectra, existing_meta, existing_membership ->
                [meta.id, spectra, existing_meta, existing_membership]
            })
            .map { id, meta, titles, spectra, existing_meta, existing_membership ->
                [id, meta, [titles], spectra, existing_meta, existing_membership]
            }
    } else {
        ch_ready_for_split = ch_new_partitions.map { meta, dat, si, titles, _spectra ->
            [meta, dat, si, titles]
        }
        ch_context = ch_new_partitions.map { meta, _dat, _si, titles, spectra ->
            [meta.id, meta, [titles], spectra, null, null]
        }
    }

    SPLIT_MZ_WINDOWS(ch_ready_for_split)
    ch_versions = ch_versions.mix(SPLIT_MZ_WINDOWS.out.versions)

    SPLIT_MZ_WINDOWS.out.window_dat_files
        .transpose()
        .map { meta, dat_file ->
            def w_match = (dat_file.name =~ /_w(\d+)\.(scan_info\.)?dat$/)
            if (!w_match) return null
            return ["${meta.id}__w${w_match[0][1]}", meta, dat_file]
        }
        .filter { it != null }
        .groupTuple(by: 0)
        .map { key, metas, dat_files ->
            def meta = metas[0] + [id: key]
            [meta, dat_files]
        }
        .set { ch_window_dat }

    SPLIT_MZ_WINDOWS.out.window_titles
        .transpose()
        .map { meta, titles_file ->
            def w_match = (titles_file.name =~ /_w(\d+)\.scan_titles\.txt$/)
            if (!w_match) return null
            return ["${meta.id}__w${w_match[0][1]}", titles_file]
        }
        .filter { it != null }
        .groupTuple()
        .set { ch_window_titles }

    ch_window_dat
        .map { meta, dat_files -> [meta.id, meta, dat_files] }
        .join(ch_window_titles)
        .map { _key, meta, dat_files, titles -> [meta, dat_files, titles] }
        .set { ch_mara_input }

    RUN_MARACLUSTER_DAT(ch_mara_input)
    ch_versions = ch_versions.mix(RUN_MARACLUSTER_DAT.out.versions)

    RUN_MARACLUSTER_DAT.out.maracluster_results
        .map { meta, tsv ->
            def base_id = meta.id.replaceAll(/__w\d+$/, '')
            [base_id, meta + [id: base_id], tsv]
        }
        .groupTuple(by: 0)
        .join(
            SPLIT_MZ_WINDOWS.out.window_manifest
                .map { meta, manifest -> [meta.id, manifest] }
        )
        .map { id, metas, tsvs, manifest ->
            [metas[0], tsvs, manifest]
        }
        .set { ch_merge_windows_input }

    MERGE_MZ_WINDOWS(ch_merge_windows_input)
    ch_versions = ch_versions.mix(MERGE_MZ_WINDOWS.out.versions)

    MERGE_MZ_WINDOWS.out.merged_clusters
        .map { meta, tsv -> [meta.id, meta, tsv] }
        .join(ch_ready_for_split.map { meta, dat, _si, _titles -> [meta.id, dat] })
        .map { _id, meta, tsv, dat -> [meta, tsv, dat] }
        .set { ch_filter_input }

    FILTER_NOID_CLUSTERS(ch_filter_input)
    ch_versions = ch_versions.mix(FILTER_NOID_CLUSTERS.out.versions)

    ch_db_joined = FILTER_NOID_CLUSTERS.out.filtered
        .map { meta, filtered_tsv, metrics_tsv -> [meta.id, meta, filtered_tsv, metrics_tsv] }
        .join(ch_context)

    ch_db_joined
        .branch {
            fresh: it[7] == null
            incremental: it[7] != null
        }
        .set { ch_db_branches }

    BUILD_NOID_CLUSTER_DB(
        ch_db_branches.fresh.map { _id, meta, filtered_tsv, metrics_tsv, _ctx_meta, _titles, spectra, _em, _eMem ->
            [meta, filtered_tsv, metrics_tsv, spectra]
        }
    )
    MERGE_INTO_EXISTING_NOID_DB(
        ch_db_branches.incremental.map { _id, meta, filtered_tsv, metrics_tsv, _ctx_meta, titles, spectra, existing_meta, existing_membership ->
            [meta, filtered_tsv, metrics_tsv, titles, spectra, existing_meta, existing_membership]
        }
    )
    ch_versions = ch_versions.mix(BUILD_NOID_CLUSTER_DB.out.versions).mix(MERGE_INTO_EXISTING_NOID_DB.out.versions)

    ch_noid_cluster_meta = BUILD_NOID_CLUSTER_DB.out.cluster_metadata
        .mix(MERGE_INTO_EXISTING_NOID_DB.out.cluster_metadata)

    GENERATE_NOID_MSP(ch_noid_cluster_meta)
    ch_versions = ch_versions.mix(GENERATE_NOID_MSP.out.versions)

    emit:
    filtered_clusters = FILTER_NOID_CLUSTERS.out.filtered
    cluster_parquet = ch_noid_cluster_meta
    msp_files = GENERATE_NOID_MSP.out.msp_files
    versions = ch_versions
}


process CONCAT_NOID_DAT_FILES {
    label 'process_low'
    tag { meta.id }

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.6' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.6' }"

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
data_dats = [f for f in dat_files if '.scan_info.' not in f.name]

stem = '${meta.id}'.replace('partition__', '')
out_dat = open(f'combined/{stem}.dat', 'wb')
out_si = open(f'combined/{stem}.scan_info.dat', 'wb')
out_titles = open(f'combined/{stem}.scan_titles.txt', 'w')
global_scannr = 0

for dat_path in data_dats:
    dat_stem = dat_path.stem
    si_path = dat_path.parent / f"{dat_stem}.scan_info.dat"
    titles_path = dat_path.parent / f"{dat_stem}.scan_titles.txt"
    dat_data = dat_path.read_bytes()
    si_data = si_path.read_bytes()
    n_spectra = len(dat_data) // SPECTRUM_SIZE

    for i in range(n_spectra):
        fields = list(SPECTRUM_STRUCT.unpack_from(dat_data, i * SPECTRUM_SIZE))
        fields[0] = 0
        fields[1] = global_scannr
        out_dat.write(SPECTRUM_STRUCT.pack(*fields))

        si_fields = list(SCANINFO_STRUCT.unpack_from(si_data, i * SCANINFO_SIZE))
        si_fields[0] = 0
        si_fields[1] = global_scannr
        out_si.write(SCANINFO_STRUCT.pack(*si_fields))
        global_scannr += 1

    with open(titles_path) as f:
        local_scannr = 0
        for line in f:
            parts = line.rstrip('\\n').split('\\t', 2)
            if len(parts) == 3:
                new_scannr = global_scannr - n_spectra + local_scannr
                out_titles.write(f"0\\t{new_scannr}\\t{parts[2]}\\n")
                local_scannr += 1

out_dat.close()
out_si.close()
out_titles.close()
PYEOF

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.6")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

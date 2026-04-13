/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    SPECTRAFUSE WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Spectral clustering using MaRaCluster.

    Two modes (params.mode):
    - Full (default): Parquet → .dat → MaRaCluster → Cluster DB
    - Incremental: Extract reps → cluster with new data → merge back

    All modes partition by species/instrument/charge.
----------------------------------------------------------------------------------------
*/

// ── Full mode: parquet-to-dat ──
include { PARQUET_TO_DAT }       from '../modules/local/pyspectrafuse/parquet_to_dat/main'
include { RUN_MARACLUSTER_DAT }  from '../modules/local/maracluster/run_maracluster_dat/main'
include { BUILD_CLUSTER_DB }     from '../modules/local/pyspectrafuse/build_cluster_db/main'
include { GENERATE_MSP_FORMAT }  from '../modules/local/pyspectrafuse/generate_msp_format/main'

// ── Incremental mode ──
include { GENERATE_MGF_FILES }       from '../modules/local/quantmsio2mgf/generate_mgf_files/main'
include { RUN_MARACLUSTER }          from '../modules/local/maracluster/run_maracluster/main'
include { EXTRACT_REPRESENTATIVES }  from '../modules/local/pyspectrafuse/extract_representatives/main'
include { MERGE_INCREMENTAL }        from '../modules/local/pyspectrafuse/merge_incremental/main'


workflow SPECTRAFUSE_FULL {
    /*
     * FULL MODE
     * Parquet → .dat → MaRaCluster -D → Cluster DB
     */
    take:
    ch_projects
    ch_parquet_dir

    main:
    ch_versions = channel.empty()

    // Step 1: Convert each project to .dat
    ch_projects
        .map { project_dir ->
            [ [ id: project_dir.baseName, file_idx: 0 ], project_dir ]
        }
        .set { ch_projects_for_dat }

    PARQUET_TO_DAT(ch_projects_for_dat)
    ch_versions = ch_versions.mix(PARQUET_TO_DAT.out.versions)

    // Step 2: Group .dat files by charge
    PARQUET_TO_DAT.out.dat_files
        .transpose()
        .map { meta, dat_file ->
            def charge_match = (dat_file.name =~ /_charge(\d+)\.dat$/)
            if (!charge_match) return null
            def charge = "charge${charge_match[0][1]}"
            return [charge, meta.id, dat_file]
        }
        .filter { it != null }
        .groupTuple(by: 0)
        .map { charge, _ids, dat_files ->
            def meta = [
                id: "partition__${charge}",
                species: params.default_species ?: 'Unknown',
                instrument: params.default_instrument ?: 'Unknown',
                charge: charge
            ]
            return [meta, dat_files]
        }
        .set { ch_grouped_dat }

    PARQUET_TO_DAT.out.scan_titles
        .transpose()
        .map { meta, titles_file ->
            def charge_match = (titles_file.name =~ /_charge(\d+)\.scan_titles\.txt$/)
            if (!charge_match) return null
            def charge = "charge${charge_match[0][1]}"
            return [charge, titles_file]
        }
        .filter { it != null }
        .groupTuple()
        .set { ch_grouped_titles }

    // Step 3: MaRaCluster with -D
    ch_grouped_dat
        .map { meta, dat_files -> [meta.charge, meta, dat_files] }
        .join(ch_grouped_titles)
        .map { _charge, meta, dat_files, titles -> [meta, dat_files, titles] }
        .set { ch_mara_input }

    RUN_MARACLUSTER_DAT(ch_mara_input)
    ch_versions = ch_versions.mix(RUN_MARACLUSTER_DAT.out.versions)

    // Step 4: Build cluster DB
    RUN_MARACLUSTER_DAT.out.maracluster_results
        .map { meta, tsv -> [meta.charge, meta, tsv] }
        .join(
            RUN_MARACLUSTER_DAT.out.scan_titles_out
                .map { meta, titles -> [meta.charge, titles] }
        )
        .map { _charge, meta, tsv, titles ->
            [meta, tsv, titles]
        }
        .set { ch_db_input }

    // Collect all project dirs as a list for BUILD_CLUSTER_DB
    ch_project_dirs = ch_projects.collect()

    BUILD_CLUSTER_DB(
        ch_db_input.combine(ch_project_dirs.map { dirs -> [dirs] })
    )
    ch_versions = ch_versions.mix(BUILD_CLUSTER_DB.out.versions)

    // Step 5: Generate MSP consensus spectral library
    GENERATE_MSP_FORMAT(ch_parquet_dir, RUN_MARACLUSTER_DAT.out.maracluster_results)
    ch_versions = ch_versions.mix(GENERATE_MSP_FORMAT.out.versions)

    emit:
    maracluster_results = RUN_MARACLUSTER_DAT.out.maracluster_results
    cluster_parquet     = BUILD_CLUSTER_DB.out.cluster_metadata
    msp_files           = GENERATE_MSP_FORMAT.out.msp_files
    versions            = ch_versions
}


workflow SPECTRAFUSE_INCREMENTAL {
    /*
     * INCREMENTAL MODE
     * Extract reps from existing DB → cluster with new data → merge
     */
    take:
    ch_projects
    ch_parquet_dir

    main:
    ch_versions = channel.empty()

    // Load existing cluster DB
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

    // Step 1: Extract representatives
    EXTRACT_REPRESENTATIVES(
        ch_existing_db.map { meta, meta_file, _mem -> [meta, meta_file] }
    )
    ch_versions = ch_versions.mix(EXTRACT_REPRESENTATIVES.out.versions)

    // Step 2: Convert new data to spectrum files
    ch_projects
        .map { project_dir -> [ [ id: project_dir.baseName ], project_dir ] }
        .set { ch_projects_with_meta }

    GENERATE_MGF_FILES(ch_projects_with_meta)
    ch_versions = ch_versions.mix(GENERATE_MGF_FILES.out.versions)

    // Step 3: Group new spectra by partition key
    GENERATE_MGF_FILES.out.mgf_files
        .flatten()
        .map { file ->
            def pathParts = file.toString().split('/')
            def idx = pathParts.findIndexOf { it == 'mgf_output' }
            if (idx == -1) return null
            def species = pathParts[idx + 1]
            def instrument = params.skip_instrument ? 'all_instruments' : pathParts[idx + 2]
            def charge = pathParts[idx + 3]
            def key = "${species}__${instrument}__${charge}"
                .replaceAll(/[\\/]/, '_').replaceAll(/\s+/, '_')
            return [key, file]
        }
        .filter { it != null }
        .groupTuple()
        .set { ch_new_spectra }

    // Combine reps + new spectra
    EXTRACT_REPRESENTATIVES.out.representative_mgf
        .map { meta, mgf -> [meta.id, meta, mgf] }
        .join(ch_new_spectra)
        .map { _id, meta, rep_mgf, new_files ->
            [meta, [rep_mgf, new_files].flatten()]
        }
        .set { ch_combined_spectra }

    RUN_MARACLUSTER(ch_combined_spectra)
    ch_versions = ch_versions.mix(RUN_MARACLUSTER.out.versions)

    // Step 4: Merge
    RUN_MARACLUSTER.out.maracluster_results
        .map { meta, tsv -> [meta.id, meta, tsv] }
        .join(
            EXTRACT_REPRESENTATIVES.out.representative_mgf.map { meta, mgf -> [meta.id, mgf] }
        )
        .join(
            ch_existing_db.map { meta, mf, mem -> [meta.id, mf, mem] }
        )
        .map { _id, meta, tsv, rep_mgf, mf, mem -> [meta, tsv, rep_mgf, mf, mem] }
        .set { ch_merge_input }

    MERGE_INCREMENTAL(ch_parquet_dir, ch_merge_input)
    ch_versions = ch_versions.mix(MERGE_INCREMENTAL.out.versions)

    emit:
    maracluster_results = RUN_MARACLUSTER.out.maracluster_results
    cluster_parquet     = MERGE_INCREMENTAL.out.cluster_parquet_files
    versions            = ch_versions
}


workflow SPECTRAFUSE {
    /*
     * Router workflow — dispatches to the correct sub-workflow based on params.
     */
    take:
    ch_projects
    ch_parquet_dir

    main:

    if (params.mode == 'incremental') {
        SPECTRAFUSE_INCREMENTAL(ch_projects, ch_parquet_dir)
        ch_results = SPECTRAFUSE_INCREMENTAL.out.maracluster_results
        ch_parquet = SPECTRAFUSE_INCREMENTAL.out.cluster_parquet
        ch_msp     = channel.empty()
        ch_versions = SPECTRAFUSE_INCREMENTAL.out.versions
    } else {
        SPECTRAFUSE_FULL(ch_projects, ch_parquet_dir)
        ch_results = SPECTRAFUSE_FULL.out.maracluster_results
        ch_parquet = SPECTRAFUSE_FULL.out.cluster_parquet
        ch_msp     = SPECTRAFUSE_FULL.out.msp_files
        ch_versions = SPECTRAFUSE_FULL.out.versions
    }

    emit:
    maracluster_results = ch_results
    cluster_parquet     = ch_parquet
    msp_files           = ch_msp
    versions            = ch_versions
}

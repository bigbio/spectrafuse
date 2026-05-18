#!/usr/bin/env nextflow
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    bigbio/spectrafuse
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Github : https://github.com/bigbio/spectrafuse
----------------------------------------------------------------------------------------
*/

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT FUNCTIONS / MODULES / SUBWORKFLOWS / WORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { SPECTRAFUSE } from './workflows/spectrafuse'
include { SPECTRAFUSE_NOID } from './workflows/spectrafuse_noid'
include { UTILS_NEXTFLOW_PIPELINE } from './subworkflows/nf-core/utils_nextflow_pipeline'

//
// WORKFLOW: Run main bigbio/spectrafuse analysis pipeline
//
workflow BIGBIO_SPECTRAFUSE {
    take:
    ch_projects  // channel: [ path(project_dir) ]

    main:

    // Create a value channel with the parquet_dir for MSP generation
    // Use Channel.value() to create a value channel that broadcasts to all MSP tasks
    ch_parquet_dir = Channel.value(file(params.parquet_dir))

    SPECTRAFUSE(ch_projects, ch_parquet_dir)

    emit:
    maracluster_results = SPECTRAFUSE.out.maracluster_results
    cluster_parquet     = SPECTRAFUSE.out.cluster_parquet
    versions            = SPECTRAFUSE.out.versions
}


workflow BIGBIO_SPECTRAFUSE_NOID {
    take:
    ch_mzml_dir

    main:
    SPECTRAFUSE_NOID(ch_mzml_dir)

    emit:
    filtered_clusters = SPECTRAFUSE_NOID.out.filtered_clusters
    cluster_parquet   = SPECTRAFUSE_NOID.out.cluster_parquet
    versions          = SPECTRAFUSE_NOID.out.versions
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN ALL WORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

//
// WORKFLOW: Execute a single named workflow for the pipeline
//
workflow {

    main:

    // Validate input parameters
    if (!!params.parquet_dir == !!params.mzml_dir) {
        error "Provide exactly one input mode: --parquet_dir or --mzml_dir"
    }

    // Dump parameters to JSON file for documenting the pipeline settings
    UTILS_NEXTFLOW_PIPELINE (
        false,
        true,
        params.outdir,
        false
    )

    if (params.mzml_dir) {
        if (!params.dataset_name) {
            error "Please provide --dataset_name when using --mzml_dir"
        }
        params.strategytype = params.strategytype ?: 'most'
        if (params.strategytype == 'best') {
            error "No-ID mzML mode does not support --strategytype best"
        }
        ch_mzml_dir = Channel.value(file(params.mzml_dir))
        BIGBIO_SPECTRAFUSE_NOID(ch_mzml_dir)
    } else {
        params.strategytype = params.strategytype ?: 'best'
        // Create channels for all items to be clustered
        // Exclude output directories (msp/, cluster_db/) that may exist inside project dirs
        ch_projects = channel.fromPath("${params.parquet_dir}/*", type: 'dir')
            .filter { dir -> !(dir.name in ['msp', 'cluster_db', 'msp_output', 'dat_output']) }
        BIGBIO_SPECTRAFUSE(ch_projects)
    }
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/


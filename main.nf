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
include { UTILS_NEXTFLOW_PIPELINE } from './subworkflows/nf-core/utils_nextflow_pipeline'

//
// WORKFLOW: Run main bigbio/spectrafuse analysis pipeline
//
workflow BIGBIO_SPECTRAFUSE {
    take:
    ch_projects  // channel: [ path(project_dir) ]

    main:

    // Create a channel with the parquet_dir for MSP generation
    ch_parquet_dir = channel.fromPath(params.parquet_dir)

    SPECTRAFUSE(ch_projects, ch_parquet_dir)

    emit:
    maracluster_results = SPECTRAFUSE.out.maracluster_results
    msp_files          = SPECTRAFUSE.out.msp_files
    versions           = SPECTRAFUSE.out.versions
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
    if (!params.parquet_dir) {
        error "Please provide a folder containing the files that will be clustered (--parquet_dir)"
    }

    // Create channels for all items to be clustered
    // Use glob pattern to find directories - more idiomatic Nextflow than file system operations
    ch_projects = channel.fromPath("${params.parquet_dir}/*", type: 'dir')

    // Dump parameters to JSON file for documenting the pipeline settings
    UTILS_NEXTFLOW_PIPELINE (
        false,
        true,
        params.outdir,
        false
    )

    // Run main pipeline
    BIGBIO_SPECTRAFUSE(ch_projects)
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/


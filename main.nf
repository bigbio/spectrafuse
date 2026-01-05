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

    main:

    SPECTRAFUSE(ch_projects)

    emit:
    maracluster_results = SPECTRAFUSE.out.maracluster_results
    versions            = SPECTRAFUSE.out.versions
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
    ch_projects = channel.fromPath(params.parquet_dir)
        .map { path -> 
            new File(path.toString()).listFiles()?.findAll { file -> file.isDirectory() }?.collect { file -> file.path } ?: []
        }
        .flatten()

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


#!/usr/bin/env nextflow.enable.dsl=2

process run_maracluster {
    label 'process_low'

    publishDir "${params.maracluster_output}/${fileInput.baseName}", mode: 'copy', overwrite: false, emitDirs: true

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://containers.biocontainers.pro/s3/SingImgsRepo/maracluster/1.02.1_cv1/maracluster:1.02.1_cv1' :
        'biocontainers/maracluster:1.02.1_cv1' }"

    input:
    path file_input

    output:
    path "maracluster_output/*.tsv", emit: maracluster_results

    script:

    verbose = params.maracluster_verbose ? "-v" : ""

    """
    maracluster batch -b "${file_input}" -t ${params.maracluster_pvalue_threshold} -p ${params.maracluster_precursor_tolerance} ${verbose}
    """
}

// validate the input parameters
if (!params.maracluster_files_list_folder) {
    error "Please provide a folder containing the files that will be clustered"
}

workflow {
    file_list = Channel.fromPath("${params.maracluster_files_list_folder}/files_list_*.txt")
    file_list.view()
    run_maracluster(file_list)
}
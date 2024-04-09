#!/usr/bin/env nextflow.enable.dsl=2

params.files_list_folder = "./"
params.maracluster_output = "./"
params.pvalue = "-5.0"

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
    """
    maracluster batch -b "${file_input}" -t ${params.pvalue}
    """
}

workflow {
    file_list = Channel.fromPath("${params.files_list_folder}/files_list_*.txt")
    file_list.view()
    runMaRaCluster(file_list)
}
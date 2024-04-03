#!/usr/bin/env nextflow.enable.dsl=2

params.files_list_folder = "./"
params.maracluster_output = "./"
params.pvalue = "-5.0"

process runMaRaCluster {
     publishDir "${params.maracluster_output}/${fileInput.baseName}", mode: 'copy', overwrite: false, emitDirs: true

    input:
    path fileInput

    output:
    path "maracluster_output/*.tsv", emit: maracluster_results

    script:
    """
    maracluster batch -b "${fileInput}" -t ${params.pvalue}
    """
}

workflow {
    file_list = Channel.fromPath("${params.files_list_folder}/files_list_*.txt")
    file_list.view()
    runMaRaCluster(file_list)
}
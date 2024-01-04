#!/usr/bin/env 
nextflow.enable.dsl=2

params.files_list_folder = "./"
params.maracluster_output = "./"

process runMaRaCluster {

    container 'biocontainers/maracluster:1.02.1_cv1'
    publishDir "${params.maracluster_output}", mode: 'copy', overwrite: true

    input:
    path fileInput 

    output:
    path "maracluster_output/*.tsv", emit: maracluster_results

    script:
    """
    maracluster batch -b "${fileInput}" -t -10
    """
}

workflow {
      file_list = Channel.fromPath("${params.files_list_folder}/files_list_*.txt")
      file_list.view()
      runMaRaCluster(file_list)
}
#!/usr/bin/env nextflow.enable.dsl=2

// This process needs a lot of work, first we need to capture all the project folders that will contains a parquet
// file and a sdrf file. Then independently we will run this process before the maracluster process for every project folder.
// The output of this process will be a folder with the mgf files that will be used by the maracluster process.
// TODO: @PengShu Can you develop that logic.

process generate_mgf_files{
    label 'process_low'

    publishDir "${params.mgf_output}/${fileInput.baseName}", mode: 'copy', overwrite: false, emitDirs: true

    // Here we have to define a container that have all the dependencies needed by quantmsio2mgf
    conda "conda-forge::pandas_schema conda-forge::lzstring bioconda::pmultiqc=0.0.21"
    if (workflow.containerEngine == 'singularity' && !params.singularity_pull_docker_container) {
        container "https://depot.galaxyproject.org/singularity/pmultiqc:0.0.22--pyhdfd78af_0"
    } else {
        container "biocontainers/pmultiqc:0.0.22--pyhdfd78af_0"
    }

    input:
    path file_input

    output:
    path "mgf_output/*.mgf", emit: mgf_files

    script:

    verbose = params.mgf_verbose ? "-v" : ""

    """
    quantmsio2mgf --parquet_dir "${parquet_dir}" --sdrf_file_path "${sdrf_file_path}" --output_path "${output_path}"
    """
}

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
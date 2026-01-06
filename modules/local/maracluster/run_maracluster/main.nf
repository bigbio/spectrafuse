process RUN_MARACLUSTER {
    label 'process_low'

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://containers.biocontainers.pro/s3/SingImgsRepo/maracluster/1.04.1_cv1/maracluster_1.04.1_cv1.sif' :
        'biocontainers/maracluster:1.04.1_cv1' }"

    input:
    tuple val(meta), path(mgf_files_path)

    output:
    tuple val(meta), path("maracluster_output/*${params.cluster_threshold}.tsv"), emit: maracluster_results
    path "versions.yml", emit: versions

    script:
    def verbose = params.maracluster_verbose ? "-v 3" : "-v 0"
    def args = task.ext.args ?: ''

    """
    echo "\${mgf_files_path.join('\n')}" > files_list.txt
    
    maracluster batch -b files_list.txt -t ${params.maracluster_pvalue_threshold} -p '${params.maracluster_precursor_tolerance}' ${verbose} ${args}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        maracluster: \$(maracluster --version 2>&1 | head -n 1 | sed 's/.*version //g' || echo "1.04.1")
    END_VERSIONS
    """
}


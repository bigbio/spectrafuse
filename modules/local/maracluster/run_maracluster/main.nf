process RUN_MARACLUSTER {
    label 'process_low'
    tag { meta.id }

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://containers.biocontainers.pro/s3/SingImgsRepo/maracluster/1.04.1_cv1/maracluster_1.04.1_cv1.sif' :
        'docker.io/biocontainers/maracluster:1.04.1_cv1' }"

    input:
    tuple val(meta), path(mgf_files)  // Collection of MGF files grouped by species/instrument/charge

    output:
    tuple val(meta), path("maracluster_output/*_p${params.cluster_threshold}.tsv"), emit: maracluster_results
    path "versions.yml", emit: versions

    script:
    def verbose = params.maracluster_verbose ? "-v 3" : "-v 0"
    def args = task.ext.args ?: ''
    def files_list = mgf_files.join('\n')

    """
    cat <<EOF > files_list.txt
${files_list}
EOF
    
    maracluster batch -b files_list.txt -t ${params.maracluster_pvalue_threshold} -p '${params.maracluster_precursor_tolerance}' ${verbose} ${args}

    # Verify that MaRaCluster produced output files matching the expected pattern
    # Expected pattern: maracluster_output/*_p${params.cluster_threshold}.tsv
    if [ ! -d maracluster_output ]; then
        echo "ERROR: MaRaCluster did not create maracluster_output directory"
        exit 1
    fi
    
    output_count=\$(find maracluster_output -name "*_p${params.cluster_threshold}.tsv" -type f | wc -l)
    if [ "\$output_count" -eq 0 ]; then
        echo "ERROR: MaRaCluster did not produce any output files matching pattern *_p${params.cluster_threshold}.tsv"
        echo "Found files in maracluster_output:"
        ls -la maracluster_output/ || echo "Directory is empty or does not exist"
        exit 1
    fi

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        maracluster: \$(maracluster --version 2>&1 | head -n 1 | sed 's/.*version //g' || echo "1.04.1")
    END_VERSIONS
    """
}


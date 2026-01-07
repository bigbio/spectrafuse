process GENERATE_MSP_FORMAT {
    label 'process_low'

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.2' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.2' }"

    input:
    path parquet_dir
    tuple val(meta), path(cluster_tsv_file)

    output:
    path "*.msp", emit: msp_files
    path "versions.yml", emit: versions

    script:
    def verbose = params.mgf_verbose ? "-v" : ""
    def args = task.ext.args ?: ''

    """
    # Run MSP format generation using pyspectrafuse_cli from the pyspectrafuse container
    # Use !{} syntax for safe shell escaping to prevent command injection
    pyspectrafuse_cli msp \
        --parquet_dir !{parquet_dir} \
        --method_type "${params.strategytype}" \
        --cluster_tsv_file !{cluster_tsv_file} \
        --species "${meta.species}" \
        --instrument "${meta.instrument}" \
        --charge "${meta.charge}" \
        --sim "${params.sim}" \
        --fragment_mz_tolerance "${params.fragment_mz_tolerance}" \
        --min_mz "${params.min_mz}" \
        --max_mz "${params.max_mz}" \
        --bin_size "${params.bin_size}" \
        --peak_quorum "${params.peak_quorum}" \
        --edge_case_threshold "${params.edge_case_threshold}" \
        --diff_thresh "${params.diff_thresh}" \
        --dyn_range "${params.dyn_range}" \
        --min_fraction "${params.min_fraction}" \
        --pepmass "${params.pepmass}" \
        --msms_avg "${params.msms_avg}" \
        ${verbose} ${args}

    # Get pyspectrafuse version dynamically
    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //' | sed 's/[^0-9.].*//' || pyspectrafuse_cli --version 2>&1 | sed 's/.*version //' | sed 's/[^0-9.].*//' || echo "unknown")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}


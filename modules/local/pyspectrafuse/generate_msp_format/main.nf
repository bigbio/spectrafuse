process GENERATE_MSP_FORMAT {
    label 'process_low'
    tag { meta.id }
    publishDir "${params.outdir}/msp_files", mode: params.publish_dir_mode, pattern: "**/*.msp{,.gz}"

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.4' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.4' }"

    // Additional environment variables for Numba in containers
    // Disable JIT and caching to prevent issues when running as non-root user
    containerOptions = workflow.containerEngine == 'singularity' ?
        '--env NUMBA_DISABLE_JIT=1 --env NUMBA_DISABLE_CACHING=1 --env NUMBA_CACHE_DIR=/tmp' :
        '-e NUMBA_DISABLE_JIT=1 -e NUMBA_DISABLE_CACHING=1 -e NUMBA_CACHE_DIR=/tmp'

    input:
    tuple val(meta), path(cluster_tsv_file), path(scan_titles_files)
    path parquet_dir

    output:
    path "**/*.msp{,.gz}", emit: msp_files, optional: true
    path "versions.yml", emit: versions

    script:
    def args = task.ext.args ?: ''
    // Build --scan_titles arguments from the list of scan_titles files
    def titles_args = scan_titles_files instanceof List ?
        scan_titles_files.collect { "--scan_titles ${it}" }.join(' ') :
        "--scan_titles ${scan_titles_files}"

    """
    # Disable Numba JIT and caching to prevent container issues when running as non-root user
    export NUMBA_DISABLE_JIT=1
    export NUMBA_DISABLE_CACHING=1
    export NUMBA_CACHE_DIR=/tmp

    pyspectrafuse msp \
        --parquet_dir ${parquet_dir} \
        --method_type "${params.strategytype}" \
        --cluster_tsv_file ${cluster_tsv_file} \
        ${titles_args} \
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
        ${args}

    # Get pyspectrafuse version dynamically
    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.4")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

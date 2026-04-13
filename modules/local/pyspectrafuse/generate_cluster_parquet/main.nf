process GENERATE_CLUSTER_PARQUET {
    label 'process_low'
    tag { meta.id }
    publishDir "${params.outdir}/cluster_parquet", mode: params.publish_dir_mode, pattern: "cluster_db/**/*.parquet"

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.3' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.3' }"

    containerOptions = workflow.containerEngine == 'singularity' ?
        '--env NUMBA_DISABLE_JIT=1 --env NUMBA_DISABLE_CACHING=1 --env NUMBA_CACHE_DIR=/tmp' :
        '-e NUMBA_DISABLE_JIT=1 -e NUMBA_DISABLE_CACHING=1 -e NUMBA_CACHE_DIR=/tmp'

    input:
    path parquet_dir
    tuple val(meta), path(cluster_tsv_file)

    output:
    path "cluster_db/**/*.parquet", emit: cluster_parquet_files
    path "versions.yml", emit: versions

    shell:
    def args = task.ext.args ?: ''

    """
    export NUMBA_DISABLE_JIT=1
    export NUMBA_DISABLE_CACHING=1
    export NUMBA_CACHE_DIR=/tmp

    pyspectrafuse cluster-parquet \
        --parquet_dir !{parquet_dir} \
        --method_type "${params.strategytype}" \
        --cluster_tsv_file !{cluster_tsv_file} \
        --species "${meta.species}" \
        --instrument "${meta.instrument}" \
        --charge "${meta.charge}" \
        --output_dir cluster_db \
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

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.3")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

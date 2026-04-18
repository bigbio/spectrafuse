process BUILD_CLUSTER_DB {
    label 'process_medium'
    tag { meta.id }
    publishDir path: {
        params.skip_instrument
            ? "${params.outdir}/cluster_db/${meta.species}/${meta.charge}"
            : "${params.outdir}/cluster_db/${meta.species}/${meta.instrument}/${meta.charge}"
    }, mode: params.publish_dir_mode, pattern: "cluster_db/*.parquet"

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.4' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.4' }"

    containerOptions = workflow.containerEngine == 'singularity' ?
        '--env NUMBA_DISABLE_JIT=1 --env NUMBA_DISABLE_CACHING=1 --env NUMBA_CACHE_DIR=/tmp' :
        '-e NUMBA_DISABLE_JIT=1 -e NUMBA_DISABLE_CACHING=1 -e NUMBA_CACHE_DIR=/tmp'

    input:
    tuple val(meta), path(cluster_tsv), path(scan_titles), path(parquet_dirs)

    output:
    tuple val(meta), path("cluster_db/cluster_metadata.parquet"),         emit: cluster_metadata
    tuple val(meta), path("cluster_db/psm_cluster_membership.parquet"),   emit: psm_membership
    path "versions.yml",                                                   emit: versions

    script:
    def titles_args = scan_titles instanceof List ?
        scan_titles.collect { "--scan_titles ${it}" }.join(' ') :
        "--scan_titles ${scan_titles}"
    def dir_args = parquet_dirs instanceof List ?
        parquet_dirs.collect { "--parquet_dir ${it}" }.join(' ') :
        "--parquet_dir ${parquet_dirs}"
    def name_args = parquet_dirs instanceof List ?
        parquet_dirs.collect { "--dataset_name ${it.baseName}" }.join(' ') :
        "--dataset_name ${parquet_dirs.baseName}"
    def args = task.ext.args ?: ''

    """
    export NUMBA_DISABLE_JIT=1
    export NUMBA_DISABLE_CACHING=1
    export NUMBA_CACHE_DIR=/tmp

    pyspectrafuse build-cluster-db \
        --cluster_tsv ${cluster_tsv} \
        ${titles_args} \
        ${dir_args} \
        ${name_args} \
        --species "${meta.species}" \
        --instrument "${meta.instrument}" \
        --charge "${meta.charge}" \
        --method_type "${params.strategytype}" \
        --output_dir cluster_db \
        ${args}

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.4")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}


process MERGE_INTO_EXISTING_DB {
    label 'process_medium'
    tag { meta.id }
    publishDir path: {
        params.skip_instrument
            ? "${params.outdir}/cluster_db/${meta.species}/${meta.charge}"
            : "${params.outdir}/cluster_db/${meta.species}/${meta.instrument}/${meta.charge}"
    }, mode: params.publish_dir_mode, pattern: "cluster_db/*.parquet"

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.4' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.4' }"

    containerOptions = workflow.containerEngine == 'singularity' ?
        '--env NUMBA_DISABLE_JIT=1 --env NUMBA_DISABLE_CACHING=1 --env NUMBA_CACHE_DIR=/tmp' :
        '-e NUMBA_DISABLE_JIT=1 -e NUMBA_DISABLE_CACHING=1 -e NUMBA_CACHE_DIR=/tmp'

    input:
    tuple val(meta), path(cluster_tsv), path(scan_titles), path(existing_metadata), path(existing_membership), path(parquet_dirs)

    output:
    tuple val(meta), path("cluster_db/cluster_metadata.parquet"),         emit: cluster_metadata
    tuple val(meta), path("cluster_db/psm_cluster_membership.parquet"),   emit: psm_membership
    path "versions.yml",                                                   emit: versions

    script:
    def titles_args = scan_titles instanceof List ?
        scan_titles.collect { "--scan_titles ${it}" }.join(' ') :
        "--scan_titles ${scan_titles}"
    def dir_args = parquet_dirs instanceof List ?
        parquet_dirs.collect { "--parquet_dir ${it}" }.join(' ') :
        "--parquet_dir ${parquet_dirs}"
    def name_args = parquet_dirs instanceof List ?
        parquet_dirs.collect { "--dataset_name ${it.baseName}" }.join(' ') :
        "--dataset_name ${parquet_dirs.baseName}"
    def args = task.ext.args ?: ''

    """
    export NUMBA_DISABLE_JIT=1
    export NUMBA_DISABLE_CACHING=1
    export NUMBA_CACHE_DIR=/tmp

    pyspectrafuse build-cluster-db \
        --cluster_tsv ${cluster_tsv} \
        ${titles_args} \
        ${dir_args} \
        ${name_args} \
        --species "${meta.species}" \
        --instrument "${meta.instrument}" \
        --charge "${meta.charge}" \
        --method_type "${params.strategytype}" \
        --existing_metadata ${existing_metadata} \
        --existing_membership ${existing_membership} \
        --output_dir cluster_db \
        ${args}

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.4")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

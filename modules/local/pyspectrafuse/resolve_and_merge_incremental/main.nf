process RESOLVE_AND_MERGE_INCREMENTAL {
    label 'process_medium'
    tag { meta.id }
    publishDir "${params.outdir}/cluster_db/${meta.charge}", mode: params.publish_dir_mode, pattern: "cluster_db/*.parquet"

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.4' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.4' }"

    containerOptions = workflow.containerEngine == 'singularity' ?
        '--env NUMBA_DISABLE_JIT=1 --env NUMBA_DISABLE_CACHING=1 --env NUMBA_CACHE_DIR=/tmp' :
        '-e NUMBA_DISABLE_JIT=1 -e NUMBA_DISABLE_CACHING=1 -e NUMBA_CACHE_DIR=/tmp'

    input:
    tuple val(meta), path(cluster_tsv), path(scan_titles), path(existing_metadata), path(existing_membership), path(parquet_dirs)

    output:
    path "cluster_db/**/*.parquet", emit: cluster_parquet_files
    path "versions.yml",           emit: versions

    script:
    def args = task.ext.args ?: ''

    """
    export NUMBA_DISABLE_JIT=1
    export NUMBA_DISABLE_CACHING=1
    export NUMBA_CACHE_DIR=/tmp

    # Stage all scan_titles into a single directory for the CLI
    mkdir -p scan_titles_collected
    for st in ${scan_titles}; do
        cp "\$st" scan_titles_collected/
    done

    pyspectrafuse incremental merge-clusters \
        --cluster_tsv ${cluster_tsv} \
        --scan_titles_dir scan_titles_collected \
        --existing_metadata ${existing_metadata} \
        --existing_membership ${existing_membership} \
        --new_parquet_dir ${parquet_dirs} \
        --species "${meta.species}" \
        --instrument "${meta.instrument}" \
        --charge "${meta.charge}" \
        --method_type "${params.strategytype}" \
        --output_dir cluster_db/${meta.species}/${meta.instrument}/${meta.charge} \
        ${args}

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.4")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

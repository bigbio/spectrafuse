process MERGE_INCREMENTAL {
    label 'process_low'
    tag { meta.id }

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.3' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.3' }"

    containerOptions = workflow.containerEngine == 'singularity' ?
        '--env NUMBA_DISABLE_JIT=1 --env NUMBA_DISABLE_CACHING=1 --env NUMBA_CACHE_DIR=/tmp' :
        '-e NUMBA_DISABLE_JIT=1 -e NUMBA_DISABLE_CACHING=1 -e NUMBA_CACHE_DIR=/tmp'

    input:
    path parquet_dir
    tuple val(meta), path(cluster_tsv_file), path(rep_mgf), path(existing_metadata), path(existing_membership)

    output:
    path "cluster_db/**/*.parquet", emit: cluster_parquet_files
    path "versions.yml", emit: versions

    shell:
    def args = task.ext.args ?: ''

    """
    export NUMBA_DISABLE_JIT=1
    export NUMBA_DISABLE_CACHING=1
    export NUMBA_CACHE_DIR=/tmp

    pyspectrafuse incremental merge-clusters \
        --cluster_tsv !{cluster_tsv_file} \
        --rep_mgf !{rep_mgf} \
        --existing_metadata !{existing_metadata} \
        --existing_membership !{existing_membership} \
        --new_parquet_dir !{parquet_dir} \
        --species "${meta.species}" \
        --instrument "${meta.instrument}" \
        --charge "${meta.charge}" \
        --method_type "${params.strategytype}" \
        --output_dir cluster_db/${meta.species}/${meta.instrument}/${meta.charge} \
        ${args}

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.3")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

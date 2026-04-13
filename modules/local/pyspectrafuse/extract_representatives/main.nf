process EXTRACT_REPRESENTATIVES {
    label 'process_low'
    tag { meta.id }

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.3' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.3' }"

    containerOptions = workflow.containerEngine == 'singularity' ?
        '--env NUMBA_DISABLE_JIT=1 --env NUMBA_DISABLE_CACHING=1 --env NUMBA_CACHE_DIR=/tmp' :
        '-e NUMBA_DISABLE_JIT=1 -e NUMBA_DISABLE_CACHING=1 -e NUMBA_CACHE_DIR=/tmp'

    input:
    tuple val(meta), path(cluster_metadata_parquet)

    output:
    tuple val(meta), path("representatives/*.mgf"), emit: representative_mgf
    path "versions.yml", emit: versions

    shell:
    def args = task.ext.args ?: ''

    """
    export NUMBA_DISABLE_JIT=1
    export NUMBA_DISABLE_CACHING=1
    export NUMBA_CACHE_DIR=/tmp

    mkdir -p representatives

    pyspectrafuse incremental extract-reps \
        --cluster_metadata !{cluster_metadata_parquet} \
        --output_mgf representatives/!{meta.id}_representatives.mgf \
        ${args}

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.3")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

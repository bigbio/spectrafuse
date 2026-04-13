process PARQUET_TO_DAT {
    label 'process_low'
    tag { meta.id }

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.4' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.4' }"

    containerOptions = workflow.containerEngine == 'singularity' ?
        '--env NUMBA_DISABLE_JIT=1 --env NUMBA_DISABLE_CACHING=1 --env NUMBA_CACHE_DIR=/tmp' :
        '-e NUMBA_DISABLE_JIT=1 -e NUMBA_DISABLE_CACHING=1 -e NUMBA_CACHE_DIR=/tmp'

    input:
    tuple val(meta), path(project_dir)

    output:
    tuple val(meta), path("dat_output/*.dat"),              emit: dat_files
    tuple val(meta), path("dat_output/*.scan_info.dat"),    emit: scaninfo_files
    tuple val(meta), path("dat_output/*.scan_titles.txt"),  emit: scan_titles
    path "versions.yml",                                    emit: versions

    script:
    def args = task.ext.args ?: ''
    def charge_opt = meta.charge_int ? "-c ${meta.charge_int}" : ""

    """
    export NUMBA_DISABLE_JIT=1
    export NUMBA_DISABLE_CACHING=1
    export NUMBA_CACHE_DIR=/tmp

    pyspectrafuse convert-dat \
        -p ${project_dir} \
        -o dat_output \
        ${charge_opt} \
        --file_idx ${meta.file_idx ?: 0} \
        ${args}

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.4")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

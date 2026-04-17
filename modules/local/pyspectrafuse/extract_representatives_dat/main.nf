process EXTRACT_REPS_DAT {
    label 'process_low'
    tag { meta.id }

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.4' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.4' }"

    containerOptions = workflow.containerEngine == 'singularity' ?
        '--env NUMBA_DISABLE_JIT=1 --env NUMBA_DISABLE_CACHING=1 --env NUMBA_CACHE_DIR=/tmp' :
        '-e NUMBA_DISABLE_JIT=1 -e NUMBA_DISABLE_CACHING=1 -e NUMBA_CACHE_DIR=/tmp'

    input:
    tuple val(meta), path(cluster_metadata_parquet)

    output:
    tuple val(meta), path("reps_output/*.dat"),              emit: dat_files
    tuple val(meta), path("reps_output/*.scan_info.dat"),    emit: scaninfo_files
    tuple val(meta), path("reps_output/*.scan_titles.txt"),  emit: scan_titles
    path "versions.yml",                                      emit: versions

    script:
    def charge_int = meta.charge.toString().replace('charge', '')
    def args = task.ext.args ?: ''

    """
    export NUMBA_DISABLE_JIT=1
    export NUMBA_DISABLE_CACHING=1
    export NUMBA_CACHE_DIR=/tmp

    pyspectrafuse incremental extract-reps-dat \
        --cluster_metadata ${cluster_metadata_parquet} \
        --output_dir reps_output \
        --charge_filter ${charge_int} \
        --file_idx 0 \
        ${args}

    # Verify output
    dat_count=\$(find reps_output -name "*.dat" -not -name "*.scan_info.dat" 2>/dev/null | wc -l)
    if [ "\$dat_count" -eq 0 ]; then
        echo "ERROR: No .dat files produced"
        exit 1
    fi

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.4")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

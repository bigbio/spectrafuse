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
    tuple val(meta), path("dat_output/*_charge*.dat"),              emit: dat_files
    tuple val(meta), path("dat_output/*_charge*.scan_info.dat"),    emit: scaninfo_files
    tuple val(meta), path("dat_output/*_charge*.scan_titles.txt"),  emit: scan_titles
    path "versions.yml",                                            emit: versions

    script:
    def args = task.ext.args ?: ''

    """
    export NUMBA_DISABLE_JIT=1
    export NUMBA_DISABLE_CACHING=1
    export NUMBA_CACHE_DIR=/tmp

    # Convert per charge state (2-6) to produce charge-specific .dat files
    for charge in 2 3 4 5 6; do
        pyspectrafuse convert-dat \
            -p ${project_dir} \
            -o dat_output \
            -c \$charge \
            --file_idx ${meta.file_idx ?: 0} \
            ${args} || true
    done

    # Verify at least some output was produced
    dat_count=\$(find dat_output -name "*_charge*.dat" -not -name "*.scan_info.dat" 2>/dev/null | wc -l)
    if [ "\$dat_count" -eq 0 ]; then
        echo "ERROR: No charge-specific .dat files produced"
        exit 1
    fi
    echo "Produced \$dat_count charge-specific .dat files"

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.4")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

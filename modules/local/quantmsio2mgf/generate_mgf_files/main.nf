process GENERATE_MGF_FILES {
    label 'process_low'

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.2' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.2' }"

    input:
    path file_input

    output:
    path "${file_input}/mgf_output/**/*.mgf", emit: mgf_files  // Recursive pattern to match all MGF files in subdirectories
    path "versions.yml", emit: versions

    script:
    def verbose = params.mgf_verbose ? "-v" : ""
    def args = task.ext.args ?: ''

    """
    # Run the conversion using quantmsio2mgf from the pyspectrafuse container
    # The script creates mgf_output directory inside the parquet_dir (file_input)
    quantmsio2mgf convert --parquet_dir ${file_input} ${verbose} ${args}

    # Get pyspectrafuse version dynamically
    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //' | sed 's/[^0-9.].*//' || pyspectrafuse_cli --version 2>&1 | sed 's/.*version //' | sed 's/[^0-9.].*//' || echo "unknown")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}


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
    # Try quantmsio2mgf command first, fallback to python -m quantmsio2mgf if needed
    # Use standard Nextflow interpolation (${file_input}) so the value is safely escaped in the shell command
    if command -v quantmsio2mgf &> /dev/null; then
        quantmsio2mgf convert --parquet_dir ${file_input} ${verbose} ${args}
    else
        python -m quantmsio2mgf convert --parquet_dir ${file_input} ${verbose} ${args}
    fi

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: 0.0.2
        python: \$(python --version 2>&1 | sed 's/Python //g')
        pyarrow: \$(python -c "import pyarrow; print(pyarrow.__version__)" 2>&1)
        pandas: \$(python -c "import pandas; print(pandas.__version__)" 2>&1)
        numpy: \$(python -c "import numpy; print(numpy.__version__)" 2>&1)
    END_VERSIONS
    """
}


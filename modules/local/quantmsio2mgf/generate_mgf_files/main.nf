process GENERATE_MGF_FILES {
    label 'process_low'

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://python:3.11-slim' :
        'python:3.11-slim' }"

    input:
    path file_input

    output:
    path "${file_input}/mgf_output/**/*.mgf", emit: mgf_files
    path "versions.yml", emit: versions

    script:
    def verbose = params.mgf_verbose ? "-v" : ""
    def args = task.ext.args ?: ''

    """
    # Install required Python packages
    pip install --quiet pyarrow pandas numpy click

    # Copy the quantmsio2mgf.py script to work directory
    cp \${projectDir}/bin/quantmsio2mgf.py ./

    # Run the conversion
    # The script creates mgf_output directory inside the parquet_dir (file_input)
    python quantmsio2mgf.py convert --parquet_dir "${file_input}" ${verbose} ${args}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python --version 2>&1 | sed 's/Python //g')
        pyarrow: \$(python -c "import pyarrow; print(pyarrow.__version__)" 2>&1)
        pandas: \$(python -c "import pandas; print(pandas.__version__)" 2>&1)
        numpy: \$(python -c "import numpy; print(numpy.__version__)" 2>&1)
    END_VERSIONS
    """
}


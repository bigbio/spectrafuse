process COMBINE_PROJECT_MSP {
    label 'process_low'
    tag { meta.id }

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.2' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.2' }"
    
    // Additional environment variables for Numba in containers
    // Disable JIT and caching to prevent issues when running as non-root user
    containerOptions = workflow.containerEngine == 'singularity' ? 
        '--env NUMBA_DISABLE_JIT=1 --env NUMBA_DISABLE_CACHING=1 --env NUMBA_CACHE_DIR=/tmp' : 
        '-e NUMBA_DISABLE_JIT=1 -e NUMBA_DISABLE_CACHING=1 -e NUMBA_CACHE_DIR=/tmp'

    input:
    tuple val(meta), path(project_dir)  // Project directory containing parquet and SDRF files
    path(msp_files)  // Collection of MSP files from all species/instrument/charge combinations within the project

    output:
    path "${meta.id}_project.msp", emit: project_msp_file
    path "versions.yml", emit: versions

    script:
    """
    # Disable Numba JIT and caching to prevent container issues when running as non-root user
    export NUMBA_DISABLE_JIT=1
    export NUMBA_DISABLE_CACHING=1
    export NUMBA_CACHE_DIR=/tmp
    
    # Combine all MSP files from the project into a single project-level MSP file
    # MSP files have entries separated by blank lines (\\n\\n), so we need to ensure proper separation
    
    OUTPUT_FILE="${meta.id}_project.msp"
    FIRST_FILE=true
    
    # Process each MSP file and combine them (only .msp files)
    for msp_file in \$(find !{msp_files} -name "*.msp" -type f | sort); do
        if [ "\$FIRST_FILE" = true ]; then
            # For the first file, just copy it (removing any trailing blank lines)
            awk 'NF' "\$msp_file" > "\$OUTPUT_FILE"
            echo "" >> "\$OUTPUT_FILE"
            FIRST_FILE=false
        else
            # For subsequent files, append with a blank line separator
            echo "" >> "\$OUTPUT_FILE"
            awk 'NF' "\$msp_file" >> "\$OUTPUT_FILE"
            echo "" >> "\$OUTPUT_FILE"
        fi
    done
    
    # Ensure the file ends with a newline and clean up trailing blank lines
    if [ -s "\$OUTPUT_FILE" ]; then
        # Remove trailing blank lines
        awk 'NF || p++' "\$OUTPUT_FILE" > "\$OUTPUT_FILE.tmp" && mv "\$OUTPUT_FILE.tmp" "\$OUTPUT_FILE"
        echo "" >> "\$OUTPUT_FILE"
    fi

    # Get pyspectrafuse version dynamically
    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.2")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

process MZML_TO_DAT {
    label 'process_low'
    tag { params.dataset_name ?: 'mzml' }

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.6' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.6' }"

    input:
    path mzml_dir

    output:
    path "dat_output",                         emit: dat_output_dir
    path "dat_output/partitions.tsv",          emit: partition_manifest
    path "versions.yml",                       emit: versions

    script:
    def sdrf_arg = params.sdrf ? "--sdrf ${params.sdrf}" : ''
    def species_arg = params.default_species ? "--default_species \"${params.default_species}\"" : ''
    def instrument_arg = params.default_instrument ? "--default_instrument \"${params.default_instrument}\"" : ''
    def skip_inst_arg = params.skip_instrument ? '--skip_instrument' : ''
    def args = task.ext.args ?: ''

    """
    pyspectrafuse convert-mzml-dat \
        --mzml_dir ${mzml_dir} \
        --output_dir dat_output \
        --dataset_name "${params.dataset_name}" \
        --charges "${params.charges}" \
        --workers ${params.mzml_workers} \
        --partitioned \
        ${sdrf_arg} \
        ${species_arg} \
        ${instrument_arg} \
        ${skip_inst_arg} \
        ${args}

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.6")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

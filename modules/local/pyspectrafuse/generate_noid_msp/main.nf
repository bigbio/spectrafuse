process GENERATE_NOID_MSP {
    label 'process_low'
    tag { meta.id }
    publishDir path: {
        params.skip_instrument
            ? "${params.outdir}/noid_msp_files/${meta.species}/${meta.charge}"
            : "${params.outdir}/noid_msp_files/${meta.species}/${meta.instrument}/${meta.charge}"
    }, mode: params.publish_dir_mode, pattern: "*.msp.gz"

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.5' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.5' }"

    input:
    tuple val(meta), path(cluster_metadata)

    output:
    path "*.msp.gz", emit: msp_files
    path "versions.yml", emit: versions

    script:
    """
    pyspectrafuse msp-noid \
        --cluster_metadata ${cluster_metadata} \
        --output_dir . \
        --dataset_name "${params.dataset_name}"

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.5")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

process BUILD_NOID_CLUSTER_DB {
    label 'process_medium'
    tag { meta.id }
    publishDir path: {
        params.skip_instrument
            ? "${params.outdir}/noid_cluster_db/${meta.species}/${meta.charge}"
            : "${params.outdir}/noid_cluster_db/${meta.species}/${meta.instrument}/${meta.charge}"
    }, mode: params.publish_dir_mode, pattern: "noid_cluster_db/*.parquet"

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.5' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.5' }"

    input:
    tuple val(meta), path(cluster_tsv), path(metrics_tsv), path(spectra_parquet)

    output:
    tuple val(meta), path("noid_cluster_db/cluster_metadata.parquet"),           emit: cluster_metadata
    tuple val(meta), path("noid_cluster_db/spectrum_cluster_membership.parquet"), emit: spectrum_membership
    path "versions.yml",                                                           emit: versions

    script:
    def method_type = params.strategytype ?: 'most'
    """
    pyspectrafuse build-noid-cluster-db \
        --cluster_tsv ${cluster_tsv} \
        --metrics_tsv ${metrics_tsv} \
        --spectra_parquet ${spectra_parquet} \
        --species "${meta.species}" \
        --instrument "${meta.instrument}" \
        --charge "${meta.charge}" \
        --method_type "${method_type}" \
        --output_dir noid_cluster_db

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.5")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}


process MERGE_INTO_EXISTING_NOID_DB {
    label 'process_medium'
    tag { meta.id }
    publishDir path: {
        params.skip_instrument
            ? "${params.outdir}/noid_cluster_db/${meta.species}/${meta.charge}"
            : "${params.outdir}/noid_cluster_db/${meta.species}/${meta.instrument}/${meta.charge}"
    }, mode: params.publish_dir_mode, pattern: "noid_cluster_db/*.parquet"

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.5' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.5' }"

    input:
    tuple val(meta), path(cluster_tsv), path(metrics_tsv), path(scan_titles), path(spectra_parquet), path(existing_metadata), path(existing_membership)

    output:
    tuple val(meta), path("noid_cluster_db/cluster_metadata.parquet"),           emit: cluster_metadata
    tuple val(meta), path("noid_cluster_db/spectrum_cluster_membership.parquet"), emit: spectrum_membership
    path "versions.yml",                                                           emit: versions

    script:
    def titles_args = scan_titles instanceof List ?
        scan_titles.collect { "--scan_titles ${it}" }.join(' ') :
        "--scan_titles ${scan_titles}"
    def method_type = params.strategytype ?: 'most'

    """
    pyspectrafuse build-noid-cluster-db \
        --cluster_tsv ${cluster_tsv} \
        --metrics_tsv ${metrics_tsv} \
        --spectra_parquet ${spectra_parquet} \
        ${titles_args} \
        --species "${meta.species}" \
        --instrument "${meta.instrument}" \
        --charge "${meta.charge}" \
        --method_type "${method_type}" \
        --existing_metadata ${existing_metadata} \
        --existing_membership ${existing_membership} \
        --output_dir noid_cluster_db

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.5")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

process FILTER_NOID_CLUSTERS {
    label 'process_low'
    tag { meta.id }

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.6' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.6' }"

    input:
    tuple val(meta), path(cluster_tsv), path(dat_file)

    output:
    tuple val(meta), path("filtered/*.filtered.tsv"), path("filtered/*.quality.tsv"), emit: filtered
    path "versions.yml", emit: versions

    script:
    """
    mkdir -p filtered

    pyspectrafuse filter-noid-clusters \
        --cluster_tsv ${cluster_tsv} \
        --dat ${dat_file} \
        --output_tsv filtered/${meta.id}.filtered.tsv \
        --metrics_tsv filtered/${meta.id}.quality.tsv \
        --min_quality_ratio ${params.noid_min_quality_ratio} \
        --min_pair_similarity ${params.noid_min_pair_similarity} \
        --min_cluster_size ${params.noid_min_cluster_size} \
        --max_pairs_per_cluster ${params.noid_max_pairs_per_cluster}

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.6")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

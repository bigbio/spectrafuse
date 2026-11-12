process REDUCE_PSM {
    label 'process_high'
    tag { meta.id }
    publishDir "${params.outdir}/reduced", mode: params.publish_dir_mode, pattern: "${meta.id}.psm.parquet*", saveAs: { it.endsWith('.keep.npy') ? null : it }

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.6' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.6' }"

    input:
    tuple val(meta), path(project_dir), path(cluster_tsvs, stageAs: 'clusters/clusters_*.tsv'), path(scan_titles, stageAs: 'titles/*')

    output:
    tuple val(meta), path("${meta.id}.psm.parquet"),                                   emit: psm
    tuple val(meta), path("${meta.id}.psm.parquet.build.json"), path("${meta.id}.psm.parquet.verify.json"), emit: reports
    path "versions.yml",                                                               emit: versions

    script:
    def tsv_args    = (cluster_tsvs instanceof List ? cluster_tsvs : [cluster_tsvs]).collect { "--clusters ${it}" }.join(' ')
    def title_args  = (scan_titles instanceof List ? scan_titles : [scan_titles]).collect { "--titles ${it}" }.join(' ')
    def duckdb_mem  = Math.max(1, (task.memory.toGiga() * 0.7) as int)
    def weak        = params.reduce_weak_filter ? '--weak-filter' : '--no-weak-filter'
    def args        = task.ext.args ?: ''
    """
    export SPECTRAFUSE_DUCKDB_TEMP_DIR=\$PWD/duckdb_tmp
    pyspectrafuse reduce-psm \\
        --psm-dir ${project_dir} \\
        ${tsv_args} \\
        ${title_args} \\
        --output ${meta.id}.psm.parquet \\
        --max-rows-per-pfz ${params.reduce_max_rows_per_pfz} \\
        --majority ${params.reduce_majority} \\
        --weak-max-runs ${params.reduce_weak_max_runs} \\
        --weak-min-series ${params.reduce_weak_min_series} \\
        ${weak} \\
        --method "MaRaCluster 1.04.1 via spectrafuse, p${params.cluster_threshold}, per charge" \\
        --memory ${duckdb_mem}GB \\
        --threads ${task.cpus} \\
        ${args}
    rm -rf duckdb_tmp

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \$(pyspectrafuse --version 2>&1 | sed 's/.*version //g')
    END_VERSIONS
    """
}

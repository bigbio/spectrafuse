process MERGE_MZ_WINDOWS {
    label 'process_low'
    tag { meta.id }

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.6' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.6' }"

    input:
    tuple val(meta), path(cluster_tsvs), path(window_manifest)

    output:
    tuple val(meta), path("merged/clusters_p${params.cluster_threshold}.tsv"), emit: merged_clusters
    path "versions.yml",                                                        emit: versions

    script:
    def duckdb_mem = Math.max(1, (task.memory.toGiga() * 0.7) as int)
    """
    mkdir -p merged
    export SPECTRAFUSE_DUCKDB_MEMORY=${duckdb_mem}GB SPECTRAFUSE_DUCKDB_THREADS=${task.cpus}

    python3 << 'PYEOF'
import pandas as pd
from pyspectrafuse.common.duckdb_ops import merge_mz_window_clusters

# Read window manifest for m/z ranges
manifest = pd.read_csv('${window_manifest}', sep='\\t')
window_mz_ranges = list(zip(manifest['mz_min'], manifest['mz_max']))

# Collect cluster TSV paths in window order
import glob, re
tsv_files = [f for f in glob.glob('*.tsv') if f != '${window_manifest}']
tsv_files.sort(key=lambda f: int(re.search(r'__w(\\d+)', f).group(1)))

result = merge_mz_window_clusters(tsv_files, window_mz_ranges)

# Write in MaRaCluster TSV format (mgf_path, scannr, cluster_id)
result.to_csv('merged/clusters_p${params.cluster_threshold}.tsv',
              sep='\\t', header=False, index=False)
print(f'Merged {len(result)} spectra from {len(tsv_files)} windows')
PYEOF

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.6")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

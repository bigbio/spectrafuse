process MERGE_MZ_WINDOWS {
    label 'process_low'
    tag { meta.id }

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.4' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.4' }"

    input:
    tuple val(meta), path(cluster_tsvs), path(window_manifest)

    output:
    tuple val(meta), path("merged/clusters_p${params.cluster_threshold}.tsv"), emit: merged_clusters
    path "versions.yml",                                                        emit: versions

    script:
    """
    mkdir -p merged

    python3 << 'PYEOF'
import pandas as pd
from pyspectrafuse.common.duckdb_ops import merge_mz_window_clusters

# Read window manifest for m/z ranges
manifest = pd.read_csv('${window_manifest}', sep='\\t')
window_mz_ranges = list(zip(manifest['mz_min'], manifest['mz_max']))

# Collect cluster TSV paths in window order
import glob
tsv_files = sorted(glob.glob('*.tsv'))
# Exclude the manifest
tsv_files = [f for f in tsv_files if f != '${window_manifest}']

result = merge_mz_window_clusters(tsv_files, window_mz_ranges)

# Write in MaRaCluster TSV format (mgf_path, scannr, cluster_id)
result.to_csv('merged/clusters_p${params.cluster_threshold}.tsv',
              sep='\\t', header=False, index=False)
print(f'Merged {len(result)} spectra from {len(tsv_files)} windows')
PYEOF

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.4")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

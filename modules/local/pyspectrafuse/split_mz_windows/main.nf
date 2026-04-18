process SPLIT_MZ_WINDOWS {
    label 'process_low'
    tag { meta.id }

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'docker://ghcr.io/bigbio/pyspectrafuse:0.0.4' :
        'ghcr.io/bigbio/pyspectrafuse:0.0.4' }"

    input:
    tuple val(meta), path(dat_file), path(scaninfo_file), path(titles_file)

    output:
    tuple val(meta), path("windows/*.dat"),              emit: window_dat_files
    tuple val(meta), path("windows/*.scan_info.dat"),    emit: window_scaninfo_files
    tuple val(meta), path("windows/*.scan_titles.txt"),  emit: window_titles
    tuple val(meta), path("windows/window_manifest.tsv"), emit: window_manifest
    path "versions.yml",                                  emit: versions

    script:
    def window_size = params.mz_window_size ?: 300
    def window_overlap = params.mz_window_overlap ?: 1.0

    """
    python3 << 'PYEOF'
import json
from pyspectrafuse.maracluster_dat import split_dat_by_mz_window

results = split_dat_by_mz_window(
    dat_path='${dat_file}',
    scaninfo_path='${scaninfo_file}',
    titles_path='${titles_file}',
    output_dir='windows',
    window_size=${window_size},
    window_overlap=${window_overlap},
)

# Write manifest for downstream grouping
with open('windows/window_manifest.tsv', 'w') as f:
    f.write('window_idx\\tmz_min\\tmz_max\\tdat_path\\ttitles_path\\tn_spectra\\n')
    for r in results:
        f.write(f'{r["window_idx"]}\\t{r["mz_min"]}\\t{r["mz_max"]}\\t{r["dat_path"]}\\t{r["titles_path"]}\\t{r["n_spectra"]}\\n')
PYEOF

    PYSPECTRAFUSE_VERSION=\$(pyspectrafuse --version 2>&1 | sed 's/.*version //g' || echo "0.0.4")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pyspectrafuse: \${PYSPECTRAFUSE_VERSION}
    END_VERSIONS
    """
}

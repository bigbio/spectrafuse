process RUN_MARACLUSTER_DAT {
    label 'process_medium'
    tag { meta.id }

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://containers.biocontainers.pro/s3/SingImgsRepo/maracluster/1.04.1_cv1/maracluster_1.04.1_cv1.sif' :
        'docker.io/biocontainers/maracluster:1.04.1_cv1' }"

    input:
    tuple val(meta), path(dat_files), path(scan_titles)

    output:
    tuple val(meta), path("results/*_p${params.cluster_threshold}.tsv"), emit: maracluster_results
    tuple val(meta), path("scan_titles/*"),                               emit: scan_titles_out
    path "versions.yml",                                                  emit: versions

    script:
    def verbose = params.maracluster_verbose ? "-v 3" : "-v 0"
    def args = task.ext.args ?: ''

    """
    # Clean up any leftover MaRaCluster cache from failed retries
    rm -rf maracluster_output

    # ── Dummy MGF workaround ──
    # MaRaCluster's "batch" command requires a file list (-b) of .mgf paths —
    # this is hardcoded in its CLI parser and cannot be bypassed. However, when
    # the -D flag points to a directory containing pre-existing .dat files,
    # MaRaCluster skips its own file-conversion step (Step 1) and reads spectra
    # directly from the .dat binaries instead.
    #
    # Our pipeline produces .dat files from parquet (pyspectrafuse convert-dat),
    # not from MGF. To make MaRaCluster accept them we:
    #   1. Place our .dat files in dat_dir/
    #   2. Create a minimal dummy .mgf per .dat (one fake spectrum, ~100 bytes)
    #   3. Pass the dummy .mgf list via -b and our .dat dir via -D
    #
    # MaRaCluster matches each .mgf basename to a .dat basename in the -D dir
    # (e.g. dummy_mgf/foo.mgf → dat_dir/foo.dat) and uses the .dat data.
    # The dummy .mgf content is never read for actual spectra.
    #
    # This workaround can be removed if MaRaCluster adds native support for
    # a .dat file list (e.g. --datFNfile).
    mkdir -p dummy_mgf dat_dir scan_titles

    for dat in ${dat_files}; do
        if [[ "\$dat" == *.scan_info.dat ]]; then
            cp "\$dat" dat_dir/
            continue
        fi
        basename=\$(basename "\$dat" .dat)
        echo -e "BEGIN IONS\\nTITLE=dummy\\nPEPMASS=500.0\\nCHARGE=2+\\n100 1000\\nEND IONS" > "dummy_mgf/\${basename}.mgf"
        cp "\$dat" dat_dir/
    done

    # Copy scan_titles for downstream cluster DB building
    for st in ${scan_titles}; do
        cp "\$st" scan_titles/
    done

    # Build batch file with dummy MGF paths
    ls dummy_mgf/*.mgf | sed 's|^|./|' > files_list.txt

    # Run MaRaCluster — reads spectra from .dat files (via -D), not from the dummy MGFs
    maracluster batch -b files_list.txt -t ${params.maracluster_pvalue_threshold} \
        -p '${params.maracluster_precursor_tolerance}' \
        -D dat_dir \
        ${verbose} ${args}

    # Verify output
    if [ ! -d maracluster_output ]; then
        echo "ERROR: MaRaCluster did not create maracluster_output directory"
        exit 1
    fi

    output_count=\$(find maracluster_output -name "*_p${params.cluster_threshold}.tsv" -type f | wc -l)
    if [ "\$output_count" -eq 0 ]; then
        echo "ERROR: No output files matching *_p${params.cluster_threshold}.tsv"
        ls -la maracluster_output/
        exit 1
    fi

    # Rename outputs with meta.id to avoid name collisions when collected downstream
    mkdir -p results
    for tsv in maracluster_output/*_p${params.cluster_threshold}.tsv; do
        mv "\$tsv" "results/${meta.id}.\$(basename \$tsv)"
    done

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        maracluster: \$(maracluster --version 2>&1 | head -n 1 | sed 's/.*version //g' || echo "1.04.1")
    END_VERSIONS
    """
}

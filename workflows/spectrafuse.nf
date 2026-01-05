/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    SPECTRAFUSE WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Main workflow for incremental spectral clustering using MaRaCluster
----------------------------------------------------------------------------------------
*/

include { GENERATE_MGF_FILES } from '../modules/local/quantmsio2mgf/generate_mgf_files/main'
include { RUN_MARACLUSTER     } from '../modules/local/maracluster/run_maracluster/main'

workflow SPECTRAFUSE {
    take:
    ch_projects  // channel: [ path(project_dir) ]

    main:

    ch_versions = channel.empty()

    //
    // MODULE: Generate MGF files from parquet files
    //
    GENERATE_MGF_FILES(ch_projects)
    ch_versions = ch_versions.mix(GENERATE_MGF_FILES.out.versions)

    //
    // Process MGF files: Group by species, instrument, and charge
    //
    GENERATE_MGF_FILES.out.mgf_files
        .flatten()
        .map { file ->
            def pathParts = file.toString().split('/')
            def mgfOutputIndex = pathParts.findIndexOf { part -> part == 'mgf_output' }

            if (mgfOutputIndex == -1) {
                log.warn "Warning: Could not find 'mgf_output' in path: ${file}"
                return null
            }

            // Create keys based on species, instrument, and charge
            def species = pathParts[mgfOutputIndex + 1]
            def instrument = pathParts[mgfOutputIndex + 2]
            def charge = pathParts[mgfOutputIndex + 3]

            def key = "${species}_${instrument}_${charge}"

            return [key, file]
        }
        .filter { it != null }
        .groupTuple(by: 0)
        .map { key, files -> files }
        .set { ch_grouped_mgf_files }

    //
    // MODULE: Run MaRaCluster on grouped MGF files
    //
    RUN_MARACLUSTER(ch_grouped_mgf_files)
    ch_versions = ch_versions.mix(RUN_MARACLUSTER.out.versions)

    emit:
    maracluster_results = RUN_MARACLUSTER.out.maracluster_results
    versions            = ch_versions
}


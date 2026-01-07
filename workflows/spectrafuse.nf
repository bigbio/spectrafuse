/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    SPECTRAFUSE WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Main workflow for incremental spectral clustering using MaRaCluster
----------------------------------------------------------------------------------------
*/

include { GENERATE_MGF_FILES } from '../modules/local/quantmsio2mgf/generate_mgf_files/main'
include { RUN_MARACLUSTER     } from '../modules/local/maracluster/run_maracluster/main'
include { GENERATE_MSP_FORMAT } from '../modules/local/pyspectrafuse/generate_msp_format/main'

workflow SPECTRAFUSE {
    take:
    ch_projects  // channel: [ path(project_dir) ]
    ch_parquet_dir  // channel: [ path(parquet_dir) ] - base directory for all projects

    main:

    ch_versions = channel.empty()

    //
    // MODULE: Generate MGF files from parquet files
    //
    ch_projects
        .map { project_dir -> [ [ id: project_dir.baseName ], project_dir ] }
        .set { ch_projects_with_meta }

    GENERATE_MGF_FILES(ch_projects_with_meta)
    ch_versions = ch_versions.mix(GENERATE_MGF_FILES.out.versions)

    //
    // Process MGF files: Group by species, instrument, and charge
    //
    GENERATE_MGF_FILES.out.mgf_files
        .flatten()
        .map { file ->
            def pathParts = file.toString().split('/')
            def mgf_output_index = pathParts.findIndexOf { pathPart -> pathPart == 'mgf_output' }

            if (mgf_output_index == -1) {
                log.warn "Warning: Could not find 'mgf_output' in path: ${file}"
                return null
            }

            // Create keys based on species, instrument, and charge
            // Use slash format to match main branch behavior
            def species = pathParts[mgf_output_index + 1]
            def instrument = pathParts[mgf_output_index + 2]
            def charge = pathParts[mgf_output_index + 3]

            def key = "${species}/${instrument}/${charge}"

            // Create a meta map (nf-core style) including a stable id for tags/logs
            // NOTE: Keep id filesystem-friendly (no slashes, minimal whitespace)
            def id = "${species}__${instrument}__${charge}"
                .replaceAll(/[\\/]/, '_')
                .replaceAll(/\s+/, '_')

            def meta = [
                id: id,
                species: species,
                instrument: instrument,
                charge: charge
            ]

            return [key, meta, file]
        }
        .filter { item ->
            item != null
        }
        .groupTuple(by: 0)
        .map { _key, meta_list, files ->
            // All items in meta_list should be identical for the same key
            def meta = meta_list[0]
            return [meta, files]
        }
        .set { ch_grouped_mgf_files_with_meta }

    //
    // MODULE: Run MaRaCluster on grouped MGF files
    // Pass metadata through as tuple to match main branch behavior
    //
    RUN_MARACLUSTER(ch_grouped_mgf_files_with_meta)
    ch_versions = ch_versions.mix(RUN_MARACLUSTER.out.versions)

    //
    // Process MaRaCluster results: pass meta map through for MSP generation
    //
    RUN_MARACLUSTER.out.maracluster_results.set { ch_maracluster_with_meta }

    //
    // MODULE: Generate MSP format files from clustering results
    //
    // Pass parquet_dir directly - Nextflow automatically broadcasts single-value channels
    // to each item in ch_maracluster_with_meta, ensuring parquet_dir is available for all MSP tasks
    GENERATE_MSP_FORMAT(
        ch_parquet_dir,
        ch_maracluster_with_meta
    )
    ch_versions = ch_versions.mix(GENERATE_MSP_FORMAT.out.versions)

    emit:
    maracluster_results = RUN_MARACLUSTER.out.maracluster_results
    msp_files          = GENERATE_MSP_FORMAT.out.msp_files
    versions           = ch_versions
}


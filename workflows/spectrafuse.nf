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
    GENERATE_MGF_FILES(ch_projects)
    ch_versions = ch_versions.mix(GENERATE_MGF_FILES.out.versions)

    //
    // Process MGF files: Group by species, instrument, and charge
    //
    GENERATE_MGF_FILES.out.mgf_files
        .flatten()
        .map { file ->
            def pathParts = file.toString().split('/')
            def mgfOutputIndex = pathParts.findIndexOf { pathPart -> pathPart == 'mgf_output' }

            if (mgfOutputIndex == -1) {
                log.warn "Warning: Could not find 'mgf_output' in path: ${file}"
                return null
            }

            // Create keys based on species, instrument, and charge
            // Use slash format to match main branch behavior
            def species = pathParts[mgfOutputIndex + 1]
            def instrument = pathParts[mgfOutputIndex + 2]
            def charge = pathParts[mgfOutputIndex + 3]

            def key = "${species}/${instrument}/${charge}"
            
            // Create metadata string in the format used by main branch
            def meta_string = key

            return [key, meta_string, file]
        }
        .filter { item ->
            item != null
        }
        .groupTuple(by: 0)
        .map { key, meta_list, files ->
            // Get the first metadata string (all should be the same for the same key)
            def meta_string = meta_list[0]
            return [meta_string, files]
        }
        .set { ch_grouped_mgf_files_with_meta }

    //
    // MODULE: Run MaRaCluster on grouped MGF files
    // Pass metadata through as tuple to match main branch behavior
    //
    RUN_MARACLUSTER(ch_grouped_mgf_files_with_meta)
    ch_versions = ch_versions.mix(RUN_MARACLUSTER.out.versions)

    //
    // Process MaRaCluster results: Convert metadata string to map for MSP generation
    // Uses getMetaMap function similar to main branch
    //
    RUN_MARACLUSTER.out.maracluster_results
        .map { meta_string, tsv_file ->
            // Convert metadata string (format: "species/instrument/charge") to map
            def parts = meta_string.split('/')
            def meta = [
                species: parts.size() >= 1 ? parts[0] : 'unknown',
                instrument: parts.size() >= 2 ? parts[1] : 'unknown',
                charge: parts.size() >= 3 ? parts[2] : 'unknown'
            ]
            
            return [meta, tsv_file]
        }
        .set { ch_maracluster_with_meta }

    //
    // MODULE: Generate MSP format files from clustering results
    //
    ch_parquet_dir
        .first()
        .set { ch_single_parquet_dir }
    
    GENERATE_MSP_FORMAT(
        ch_single_parquet_dir,
        ch_maracluster_with_meta
    )
    ch_versions = ch_versions.mix(GENERATE_MSP_FORMAT.out.versions)

    emit:
    maracluster_results = RUN_MARACLUSTER.out.maracluster_results
    msp_files          = GENERATE_MSP_FORMAT.out.msp_files
    versions           = ch_versions
}


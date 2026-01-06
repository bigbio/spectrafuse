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
            def species = pathParts[mgfOutputIndex + 1]
            def instrument = pathParts[mgfOutputIndex + 2]
            def charge = pathParts[mgfOutputIndex + 3]

            def key = "${species}_${instrument}_${charge}"
            
            // Create metadata map for later use
            def meta = [
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
        .map { key, meta_list, files ->
            // Get the first metadata (all should be the same for the same key)
            def meta = meta_list[0]
            return [meta, files]
        }
        .set { ch_grouped_mgf_files_with_meta }

    //
    // MODULE: Run MaRaCluster on grouped MGF files
    // Note: We need to pass files to maracluster but preserve metadata
    // Since maracluster outputs don't preserve the input structure,
    // we'll extract metadata from the output filenames
    //
    ch_grouped_mgf_files_with_meta
        .map { meta, files -> files }
        .set { ch_grouped_mgf_files }
    
    RUN_MARACLUSTER(ch_grouped_mgf_files)
    ch_versions = ch_versions.mix(RUN_MARACLUSTER.out.versions)

    //
    // Process MaRaCluster results: Extract metadata from filenames and prepare for MSP generation
    // The TSV files are named based on the input grouping, so we can extract metadata
    //
    RUN_MARACLUSTER.out.maracluster_results
        .map { tsv_file ->
            // Extract metadata from the TSV file path/filename
            // MaRaCluster output format may vary, so we try to extract from the path structure
            def pathParts = tsv_file.toString().split('/')
            def fileName = pathParts[-1]
            
            // Try to extract from filename pattern: {species}_{instrument}_{charge}_*.tsv
            // or from path if it contains the structure
            def species = 'unknown'
            def instrument = 'unknown'
            def charge = 'unknown'
            
            // Look for the pattern in the filename
            def nameParts = fileName.replace('.tsv', '').split('_')
            if (nameParts.size() >= 3) {
                species = nameParts[0]
                instrument = nameParts[1]
                charge = nameParts[2]
            }
            
            // Create metadata map
            def meta = [
                species: species,
                instrument: instrument,
                charge: charge
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


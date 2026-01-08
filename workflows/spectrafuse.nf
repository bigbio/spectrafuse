/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    SPECTRAFUSE WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Main workflow for incremental spectral clustering using MaRaCluster
----------------------------------------------------------------------------------------
*/

include { GENERATE_MGF_FILES } from '../modules/local/quantmsio2mgf/generate_mgf_files/main'
include { RUN_MARACLUSTER as RUN_MARACLUSTER_PROJECT } from '../modules/local/maracluster/run_maracluster/main'
include { RUN_MARACLUSTER as RUN_MARACLUSTER_GLOBAL } from '../modules/local/maracluster/run_maracluster/main'
include { GENERATE_MSP_FORMAT as GENERATE_MSP_FORMAT_PROJECT } from '../modules/local/pyspectrafuse/generate_msp_format/main'
include { GENERATE_MSP_FORMAT as GENERATE_MSP_FORMAT_GLOBAL } from '../modules/local/pyspectrafuse/generate_msp_format/main'
include { COMBINE_PROJECT_MSP } from '../modules/local/pyspectrafuse/combine_project_msp/main'

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
    // PROJECT-LEVEL CLUSTERING: Group MGF files within each project by species, instrument, and charge
    //
    // Extract project info from MGF file paths and group by project first
    GENERATE_MGF_FILES.out.mgf_files
        .flatten()
        .map { file ->
            def pathParts = file.toString().split('/')
            def mgf_output_index = pathParts.findIndexOf { pathPart -> pathPart == 'mgf_output' }
            
            if (mgf_output_index == -1) {
                return null
            }
            
            // Extract project info from path (project directory is before mgf_output)
            def project_dir_index = mgf_output_index - 1
            def project_dir = project_dir_index >= 0 ? pathParts[0..<mgf_output_index].join('/') : null
            def project_id = project_dir ? new File(project_dir).getName() : null
            
            def species = pathParts[mgf_output_index + 1]
            def instrument = pathParts[mgf_output_index + 2]
            def charge = pathParts[mgf_output_index + 3]
            def key = "${species}/${instrument}/${charge}"
            
            def id = "${project_id}__${species}__${instrument}__${charge}"
                .replaceAll(/[\\/]/, '_')
                .replaceAll(/\s+/, '_')
            
            def meta = [
                id: id,
                project_id: project_id,
                species: species,
                instrument: instrument,
                charge: charge,
                project_dir: project_dir
            ]
            
            // Return [project_id, key, meta, file] for grouping
            return [project_id, key, meta, file]
        }
        .filter { item -> item != null }
        .groupTuple(by: [0, 1])  // Group by project_id and key
        .map { project_id, key, meta_list, files_list ->
            // All items in meta_list should have same species/instrument/charge
            def meta = meta_list[0]
            def files = files_list.flatten()
            return [meta, files]
        }
        .set { ch_project_mgf_files_with_meta }

    //
    // PROJECT-LEVEL: Run MaRaCluster on grouped MGF files within each project
    //
    RUN_MARACLUSTER_PROJECT(ch_project_mgf_files_with_meta)
    ch_versions = ch_versions.mix(RUN_MARACLUSTER_PROJECT.out.versions)
    
    // Pass through project metadata
    RUN_MARACLUSTER_PROJECT.out.maracluster_results
        .set { ch_project_maracluster_results }

    //
    // PROJECT-LEVEL: Generate MSP format files per project (per species/instrument/charge)
    //
    GENERATE_MSP_FORMAT_PROJECT(
        ch_parquet_dir,
        ch_project_maracluster_results
    )
    ch_versions = ch_versions.mix(GENERATE_MSP_FORMAT_PROJECT.out.versions)
    
    // Group MSP files by project for combination
    // Combine MSP files with their corresponding metadata from maracluster_results
    GENERATE_MSP_FORMAT_PROJECT.out.msp_files
        .combine(ch_project_maracluster_results)
        .map { msp_files_collection, meta, cluster_tsv_file ->
            // Flatten msp_files_collection and filter for .msp files only
            def msp_files_list = msp_files_collection.flatten().findAll { file ->
                file.toString().endsWith('.msp')
            }
            if (msp_files_list.isEmpty()) {
                return null
            }
            // Extract project_id from meta (meta.id format: "project_id__species__instrument__charge")
            def project_id = meta.project_id ?: (meta.id ? meta.id.split('__')[0] : null)
            def project_dir = meta.project_dir
            if (!project_id || !project_dir) {
                return null
            }
            return [project_id, project_dir, msp_files_list]
        }
        .filter { it != null }
        .groupTuple(by: 0)  // Group by project_id to combine all MSP files from the same project
        .map { project_id, project_dir_list, msp_files_list_list ->
            // All items should have the same project_dir
            def project_dir_final = project_dir_list[0]
            // Flatten all MSP files from all charge/instrument combinations
            def all_msp_files = msp_files_list_list.flatten().findAll { file ->
                file.toString().endsWith('.msp')
            }
            if (all_msp_files.isEmpty()) {
                return null
            }
            def project_meta_combined = [
                id: project_id,
                project_dir: project_dir_final.toString()
            ]
            return [project_meta_combined, project_dir_final, all_msp_files]
        }
        .filter { it != null }
        .set { ch_project_msp_files_for_combine }
    
    // Split into two channels - Nextflow will automatically synchronize them since they come from the same source
    // The msp_files list will be passed as a collection to the path() input
    ch_project_info_for_combine = ch_project_msp_files_for_combine.map { project_meta, project_dir, msp_files -> [project_meta, project_dir] }
    ch_project_msp_files_list = ch_project_msp_files_for_combine.map { project_meta, project_dir, msp_files -> msp_files }
    
    COMBINE_PROJECT_MSP(
        ch_project_info_for_combine,
        ch_project_msp_files_list
    )
    ch_versions = ch_versions.mix(COMBINE_PROJECT_MSP.out.versions)

    //
    // GLOBAL CLUSTERING: Process MGF files: Group by species, instrument, and charge ACROSS projects
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
    // GLOBAL: Run MaRaCluster on grouped MGF files across all projects
    // Pass metadata through as tuple to match main branch behavior
    //
    RUN_MARACLUSTER_GLOBAL(ch_grouped_mgf_files_with_meta)
    ch_versions = ch_versions.mix(RUN_MARACLUSTER_GLOBAL.out.versions)
    
    // Get global clustering results
    RUN_MARACLUSTER_GLOBAL.out.maracluster_results
        .set { ch_global_maracluster_results }

    //
    // GLOBAL: Generate MSP format files from global clustering results
    //
    // Pass parquet_dir directly - Nextflow automatically broadcasts single-value channels
    // to each item in RUN_MARACLUSTER_GLOBAL.out.maracluster_results, ensuring parquet_dir is available for all MSP tasks
    GENERATE_MSP_FORMAT_GLOBAL(
        ch_parquet_dir,
        ch_global_maracluster_results
    )
    ch_versions = ch_versions.mix(GENERATE_MSP_FORMAT_GLOBAL.out.versions)

    emit:
    project_msp_files    = COMBINE_PROJECT_MSP.out.project_msp_file
    maracluster_results  = ch_global_maracluster_results
    msp_files            = GENERATE_MSP_FORMAT_GLOBAL.out.msp_files
    versions             = ch_versions
}

#!/usr/bin/env nextflow.enable.dsl=2

// This process needs a lot of work, first we need to capture all the project folders that will contains a parquet
// file and a sdrf file. Then independently we will run this process before the maracluster process for every project folder.
// The output of this process will be a folder with the mgf files that will be used by the maracluster process.
// TODO: @PengShu Can you develop that logic.

process generate_mgf_files{
    label 'process_low'

    // Here we have to define a container that have all the dependencies needed by quantmsio2mgf
    // conda "conda-forge::pandas_schema conda-forge::lzstring bioconda::pmultiqc=0.0.21"
    // if (workflow.containerEngine == 'singularity' && !params.singularity_pull_docker_container) {
    //     container "https://depot.galaxyproject.org/singularity/pmultiqc:0.0.22--pyhdfd78af_0"
    // } else {
    //     container "biocontainers/pmultiqc:0.0.22--pyhdfd78af_0"
    // }
    input:
    path file_input

    output:
    path "${file_input}/**/mgf files/*.mgf", emit: mgf_files 

    script:

    verbose = params.mgf_verbose ? "-v" : ""

    """
    python /mnt/nfs/shupeng/spectrafuse/bin/quantmsio2mgf.py convert --parquet_dir "${file_input}"
    """
}


process run_maracluster {
    label 'process_low'

    // publishDir "${parquet_dir}/${mgf_files_dir}/", mode: 'copy', overwrite: false, emitDirs: true

    if (workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container) {
        container 'https://containers.biocontainers.pro/s3/SingImgsRepo/maracluster/1.02.1_cv1/maracluster:1.02.1_cv1'
    }
    else {
        container 'biocontainers/maracluster:1.02.1_cv1'
    }

    input:
    path mgf_files_path 

    output:
    path "maracluster_output/*.tsv", emit: maracluster_results

    script:

    verbose = params.maracluster_verbose ? "-v" : ""

    """
    echo ${mgf_files_path.join('\n')} > files_list.txt
    maracluster batch -b files_list.txt -t ${params.maracluster_pvalue_threshold} -p ${params.maracluster_precursor_tolerance} ${verbose} 
    """
}

//validate the input parameters
if (!params.parquet_dir) {
    error "Please provide a folder containing the files that will be clustered"
}

//Create channels for all items to be clustered
def createSubDirsChannel(String folderPath) {
    return Channel.fromPath(folderPath)
        .map { path ->
            new File(path.toString()).listFiles()?.findAll { it.isDirectory() }?.collect { it.path } ?: []
        }
        .flatten()
}

workflow {
    cluster_projects_channel = createSubDirsChannel(params.parquet_dir)
    generate_mgf_files(cluster_projects_channel)

    //Create an empty hash map to store the split file
    def fileMap = [:]
    generate_mgf_files.out.mgf_files.flatten()
        .map { file ->
            def pathParts = file.toString().split('/')
            def mgfOutputIndex = pathParts.findIndexOf { it == 'mgf_output' }

            //Create keys based on species, instrument, and charge
            def species = pathParts[mgfOutputIndex + 1]
            def instrument = pathParts[mgfOutputIndex + 2]
            def charge = pathParts[mgfOutputIndex + 3]

            def key = "mgf_output/${species}/${instrument}/${charge}"

            // If the key is not in the map, a new list is created
            if (!fileMap[key]) {
                fileMap[key] = []
            }
            // Add the file to the corresponding list
            fileMap[key].add(file)

            return fileMap[key]
        }
        .set { splitFiles }

    
    splitFiles.view()

    run_maracluster(splitFiles)

    //TODO: Filtering is performed in the maracluster channel
    }


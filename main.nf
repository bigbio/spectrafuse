#!/usr/bin/env nextflow.enable.dsl=2

// This process needs a lot of work, first we need to capture all the project folders that will contains a parquet
// file and a sdrf file. Then independently we will run this process before the maracluster process for every project folder.
// The output of this process will be a folder with the mgf files that will be used by the maracluster process.
// TODO: @PengShu Can you develop that logic.

process generate_mgf_files{
    label 'process_low'

    input:
    path file_input

    output:
    path "${file_input}/**/mgf files/*.mgf", emit: mgf_files 

    script:

    verbose = params.mgf_verbose ? "-v" : ""

    """
    pyspectrafuse_cli convert-mgf --parquet_dir "${file_input}"
    """
}


process run_maracluster{
    label 'process_low'

    // publishDir "${params.parquet_dir}/mgf_output/", mode: 'copy', overwrite: false, emitDirs: true,  pattern: "*_p30.tsv"

    if (workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container) {
        container 'https://containers.biocontainers.pro/s3/SingImgsRepo/maracluster/1.02.1_cv1/maracluster:1.02.1_cv1'
    }
    else {
        container 'biocontainers/maracluster:1.02.1_cv1'
    }

    input:
    tuple val(meta), path(mgf_files)


    output:
    tuple val(meta), path("maracluster_output/*${params.cluster_threshold}.tsv") ,emit: meta_tsv //// meta:  Homo sapiens/Q Exactive/charge2
    script:

    verbose = params.maracluster_verbose ? "-v 3" : "-v 0"

    """
    echo ${mgf_files.join('\n')} > files_list.txt
    maracluster batch -b files_list.txt -t ${params.maracluster_pvalue_threshold} -p '${params.maracluster_precursor_tolerance}' ${verbose}
    """
}

process get_msp_format{
    label 'process_low'
    
    input:
    path file_input
    tuple val(meta), path(tsv_path)

    script:
    verbose = params.mgf_verbose ? "-v" : ""

    """
    pyspectrafuse_cli msp\\
        --parquet_dir "${file_input}" \\
        --method_type "${params.strategytype}" \\
        --cluster_tsv_file "${tsv_path}"\\
        --species "${meta.species}"\\
        --instrument "${meta.instrument}"\\
        --charge "${meta.charge}"\\
        --sim "${params.sim}"\\
        --fragment_mz_tolerance "${params.fragment_mz_tolerance}"\\
        --min_mz "${params.min_mz}"\\
        --max_mz "${params.max_mz}"\\
        --bin_size "${params.bin_size}"\\
        --peak_quorum "${params.peak_quorum}"\\
        --edge_case_threshold "${params.edge_case_threshold}"\\
        --diff_thresh "${params.diff_thresh}" \\
        --dyn_range "${params.dyn_range}" \\
        --min_fraction "${params.min_fraction}" \\
        --pepmass "${params.pepmass}" \\
        --msms_avg "${params.msms_avg}"  
    """
}

//validate the input parameters
if (!params.parquet_dir) {
    error "Please provide a folder containing the files that will be clustered"
}


workflow {
    // cluster_projects_channel = createSubDirsChannel(params.parquet_dir)
    // cluster_projects_channel.view()
    generate_mgf_files(params.parquet_dir)

    // 创建一个空的字典来存储分割的文件
    //Create an empty hash map to store the split file
    // generate_mgf_files.out.mgf_files.view()
    generate_mgf_files.out.mgf_files.flatten()
        .map { file ->                                                            
            getkv(file)
        }.set{t}

    t.groupTuple()  //  按物种/仪器/带电荷分组文件
        .set { splitFiles }
    splitFiles.view()

    run_maracluster(splitFiles)
    run_maracluster.out.meta_tsv
        .map {meta, file ->
            [getMetaMap(meta), file]
        }.set{k}
    
    k.view()
    get_msp_format(params.parquet_dir, k)

    //TODO: Filtering is performed in the maracluster channel
}



def getkv(file) {
    pathParts = file.toString().split('/')
    mgfOutputIndex = pathParts.findIndexOf { it == 'mgf_output' }

    species = pathParts[mgfOutputIndex + 1]
    instrument = pathParts[mgfOutputIndex + 2]
    charge = pathParts[mgfOutputIndex + 3]
    key = "${species}/${instrument}/${charge}"

    return [key, file]

}

def getMetaMap(meta) {
    res = [:]
    parts = meta.split('/')
    species = parts[0]
    instrument = parts[1]
    charge = parts[2]

    res.species = species
    res.instrument = instrument
    res.charge = charge
    return res
}

def getChargeKv(file) {
    //把同个电荷的聚类文件放在放在一起
    pathParts = file.toString().split('-') // maracluster_output/Homo_sapiens-Q_Exactive-charge2.clusters_p30.tsv

}

//Create channels for all items to be clustered
def createSubDirsChannel(String folderPath) {
    return Channel.fromPath(folderPath)
        .map { path ->
            new File(path.toString()).listFiles()?.findAll { it.isDirectory() }?.collect { it.path } ?: []
        }
        .flatten()
}

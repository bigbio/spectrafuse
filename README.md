# spectrafuse

Incremental clustering pipeline from [quantms data](quamts.org). quantms is a workflow for reanalysis of public proteomics data. The quantms not only release a workflow to the public but also perform reanalysis of public proteomics data in a systematic way for TMT, LFQ, ITRAQ and other DDA methods.

quantms has reanalyzed an extensive number of datasets with almost 1 billion MS/MS (Mass Spectrometry/Mass Spectrometry) MS2 analyzed, comprising nearly 100 million PSMs (Peptide-Spectrum Matches) derived from various tissues, cell lines, and diseases. In light of this vast wealth of data,The spectrafuse aims to apply spectral clustering techniques to organize this data and construct spectral libraries. 

[spectrafuse](https://github.com/bigbio/spectrafuse) is a nextflow workflow that perform incremental clustering of quantms and is based in the tool [MaRaCluster](https://github.com/statisticalbiotechnology/maracluster).  

The workflow in a nutshell:

![image](https://github.com/bigbio/spectrafuse/assets/52113/24a7c9ac-7287-421c-b8f1-6319764ed29c)

Reference: https://github.com/bigbio/spectrafuse/blob/main/docs/algorithm.png

The workflow is designed to be run in a high-performance computing environment, and it is built using [Nextflow](https://www.nextflow.io/). It uses Docker/Singularity containers making installation trivial and results highly reproducible. The [Nextflow DSL2](https://www.nextflow.io/docs/latest/dsl2.html) implementation of this pipeline uses one container per process which makes it much easier to maintain and update software dependencies.

### Workflow steps

This workflow mainly consists of the following processes:

1. **Tool mgf-converter**:A tool for converting each project file analyzed by QuantMS into an MGF file.
2. **Incremental Maracluster Algorithm** : where we will utilize the incremental clustering method of Maracluster to cluster MGF files from the same species, instrument, and charge within each project.
3. **Library converter**: - After all the clustering is done we should have a folder with the corresponding structure. 

The pipeline is built using [Nextflow](https://www.nextflow.io/), a workflow tool to run tasks across multiple compute infrastructures in a very portable manner. It uses Docker/Singularity containers making installation trivial and results highly reproducible. The [Nextflow DSL2](https://www.nextflow.io/docs/latest/dsl2.html) implementation of this pipeline uses one container per process which makes it much easier to maintain and update software dependencies. 

## Usage:

First, you should generate a flat text file directory containing the absolute/relative paths to each MS2 spectrum file in all projects.

Now, you can run the pipeline using:

```shell
nextflow run main.nf \
	--files_list_folder <FILE_LIST_FOLER>  
	--maracluster_output <OUTDIR> 
```


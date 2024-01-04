# spectrafuse
Incremental clustesting pipeline from quantms data. 

## Introduction

Quantms, a cutting-edge workflow, has reanalyzed an extensive dataset of almost 1 billion MS/MS (Mass Spectrometry/Mass Spectrometry) scans, comprising nearly 100 million PSMs (Peptide-Spectrum Matches) derived from various tissues, cell lines, and diseases. In light of this vast wealth of data,The spectrafuse aims to apply spectral clustering techniques to organize this data and construct spectral libraries.  Spectrafuse is a Incremental  clustesting pipeline from quantms data. 

This workflow mainly consists of the following processes:

1. **Tool mgf-converter**:A tool for converting each project file analyzed by QuantMS into an MGF file.
2. **Incremental Maracluster Algorithm** : where we will utilize the incremental clustering method of Maracluster to cluster MGF files from the same species, instrument, and charge within each project.
3. **Library converter**: - After all the clustering is done we should have a folder with the corresponding structure. 

The pipeline is built using [Nextflow](https://www.nextflow.io/), a workflow tool to run tasks across multiple compute infrastructures in a very portable manner. It uses Docker/Singularity containers making installation trivial and results highly reproducible. The [Nextflow DSL2](https://www.nextflow.io/docs/latest/dsl2.html) implementation of this pipeline uses one container per process which makes it much easier to maintain and update software dependencies. 

## Usage:

First, you should generate a flat text file directory containing the absolute/relative paths to each MS2 spectrum file in all projects.

Now, you can run the pipeline using:

```shell
nextflow run run_maracluster.nf \
	--files_list_folder <FILE_LIST_FOLER>  
	--maracluster_output <OUTDIR> 
	-with-docker biocontainers/maracluster:1.02.1_cv1
```


## EMUCAT All-sky Catalogue Assembly

EMU’s all-sky tiling strategy places adjacent ASKAP fields close enough so as to obtain full
sensitivity in the overlapping regions. Therefore, mosaicking of adjacent fields is essential to
meeting the survey’s sensitivity requirements over the full sky. The goal is to gather a set of
adjacent fields from CASDA, merge them together into a larger mosaic or region, run
source-finding software on the larger mosaic, and then integrate the results into an all-sky
catalogue with no duplication of sources

The size of the regions are determined based on the performance of the source extraction
software, Selavy, which is estimated at about 100 square degrees to maintain efficiency.
Prerequisites are the set of all ASKAP observations that include any portion of the region (and
possibly some additional buffer). Once the processed observations for a full region are
completed and uploaded to CASDA, the merge workflow can be executed. The merge workflow
consists of the following steps:

1. All prerequisite ASKAP field images downloaded from CASDA
2. Combine tiles into mosaic region using linmos
3. Selavy run on mosaic
4. Selavy components and islands added to EMUCat database

The merge workflow has been developed to execute once per region (Source Extraction Region, SER), however multiple merge
workflows can run in parallel on separate regions. The full EMU survey will eventually cover ~30,000 square
degrees, so it is expected that the workflow (e.g. gathering adjacent fields, mosaicking, and
running Selavy) will need to be run ~300 times over the duration of the full 5-year survey.

Refer to [Aussrc Design Study](https://aussrc.org/wp-content/uploads/2022/12/DSPFinalReport_29112022.pdf) for more detail. 


## Configuration

Create a Nextflow profile within `nextflow.config` that is approporate for the SLURM Cluster in which the workflow is deployed. 
Ensure that the correct SLURM config is set for the MPI process `run_selavy`. The `run_lhr` is not MPI based but it must match the 
number of processes being deployed. This is set to `32` by default and can be reconfigured by changing `export LHR_CPU=32` in the `run_lhr_components` process.
A good example can be found under the `sentonix` profile. 

Next, the `SCRATCH_ROOT` and `IMAGES` variables needs to be set in `nextflow.config`

```
params {
    SCRATCH_ROOT = "<project root dir>"
    IMAGES = "<images dir>"
}

workDir = "<work dir>"
tmpDir = "<temp dir>"

docker.enabled = false
```

* `SCRATCH_ROOT` is the root directory which all files are referenced
* `IMAGES` is the directory where the singularity files downloaded and stored. 
* `workDir` Nextflow work directory
* `tmpDir` Nextflow temp directory

### Credentials File

Create a credentials file, `<SCRATCH_ROOT>/emucat/cred.ini` with the following entries:

```
[casda]
user: <user>
password: <password>

[emucat_database]
user=<user>
password=<password>
database=emucat
host=<host>

[noao]
user=<user>
password=<password>
```

* casda: A CASDA account that can access images products for the EMU ASKAP project
* emucat_database: connection details to the emucat database
* noao: An acccount at [NOIRLab](https://noirlab.edu/public/) that is required for remote cross matching emucat sources.


## Requirements
 * SLURM HPC Cluster
 * Nextflow >= 21.04
 * OpenMPI >= 4.1
 * Singularity

## Running

```
module load <nextflow>
module load <mpi>
module load <singularity>

sbatch nextflow run ASKAP-EMUCat/emucat_workflows  -profile <profile> --ser=<SER> 
```

* SER: Predefined source extraction regions which can be found in emucat database within the table `emucat.regions` under the column `name` eg: EMU_1704-4408.  
* Profile: Nextflow profile to use defined in `nextflow.config`


## Mosaic Prerequisites

The prerequisites images that go into forming a source extraction region must have the associated field name with an SBID. The predefined fields for each SER is defined in `emucat.scheduling_blocks`. In order for the pipeline to download the fields as part of an SER, the filename of a new CASDA image must be parsed, the field name extracted then the SBID updated against the name in `emucat.scheduling_blocks`. Note that all the mosaic prerequisites (ASKAP footprints) must exist before the SER can be processed. To determine that all the footprints exist for an SER, the following SQL query can be run (same query in `get_sched_blocks`):

```
SELECT sb.sb_num 
FROM emucat.regions as ser, 
emucat.mosaic_prerequisites as mp, 
emucat.scheduling_blocks as sb 
WHERE ser.id = mp.ser_id 
and mp.sb_id = sb.id 
and ser.name = <SER>
and sb.sb_num is not null
```

There is a daemon that can be run that will update these entries as they are added to CASDA and trigger a pipeline if all the SER prerequisites are met: [AUSSRC services](https://github.com/AusSRC/aussrc_services)

# EMUCAT Pipeline

EMUCAT science pipeline.

## Requirements
 * Nextflow 21.04
 * OpenMPI 4.1
 * Singularity

## Running

```
module load <nextflow>
module load <mpi>
module load <singularity>

sbatch nextflow run ASKAP-EMUCat/emucat_workflows  -profile <profile> --ser=<SER> 
```

Note: There is a credentials file (cred.ini) that is required
#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

// ----------------------------------------------------------------------------------------
// Processes
// ----------------------------------------------------------------------------------------

process objectstore_upload_directory {

    errorStrategy 'retry'
    maxErrors 3

    input:
        val upload_dir
        val bucket

    output:
        stdout emit: output

    script:
        """
        singularity run \
                   --bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT} \
                   ${params.IMAGES}/rclone-rclone-latest.img \
                   copy $upload_dir $bucket
        """
}
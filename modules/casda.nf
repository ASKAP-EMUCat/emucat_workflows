#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

// ----------------------------------------------------------------------------------------
// Processes
// ----------------------------------------------------------------------------------------

process casda_download {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    errorStrategy 'retry'
    maxErrors 3

    input:
        val obs_list
        val output
        val conf

    output:
        path 'manifest.json', emit: file_manifest

    script:
        """
        python3 /scripts/casda.py --list $obs_list -o $output \
        -m manifest.json -p $conf/cred.ini -c true
        """
}

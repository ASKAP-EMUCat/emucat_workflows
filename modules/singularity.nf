#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

// ----------------------------------------------------------------------------------------
// Processes
// ----------------------------------------------------------------------------------------

process download_singularity {
    executor = 'local'
    debug true

    input:
        val pass_through

    output:
        val pass_through, emit: pass_though_output

    shell:
        '''
        #!/bin/bash
        lock_acquire() {
            # Open a file descriptor to lock file
            exec {LOCKFD}>!{params.IMAGES}/container.lock || return 1
            # Block until an exclusive lock can be obtained on the file descriptor
            flock -x $LOCKFD
        }
        lock_release() {
            test "$LOCKFD" || return 1
            # Close lock file descriptor, thereby releasing exclusive lock
            exec {LOCKFD}>&- && unset LOCKFD
        }
        lock_acquire || { echo >&2 "Error: failed to acquire lock"; exit 1; }
        singularity pull !{params.IMAGES}/aussrc-emucat_scripts-latest.img docker://aussrc/emucat_scripts:latest
        singularity pull !{params.IMAGES}/aussrc-emucat_lhr-parallel.img docker://aussrc/emucat_lhr:parallel
        singularity pull !{params.IMAGES}/aussrc-emucat_double_sources-latest.img docker://aussrc/emucat_double_sources:latest
        singularity pull !{params.IMAGES}/aussrc-emucat_diffusefilter-latest.img docker://aussrc/emucat_diffusefilter:latest
        singularity pull !{params.IMAGES}/csirocass-askapsoft.img docker://csirocass/askapsoft:1.9.1-casacore3.5.0-mpich
        singularity pull !{params.IMAGES}/rclone-rclone-latest.img docker://rclone/rclone:latest

        lock_release
        '''
}

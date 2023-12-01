#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

include { download_singularity } from './modules/singularity'
include { casda_download } from './modules/casda'
include { objectstore_upload_directory } from './modules/objectstore'

params.INPUT_CONF = "${params.SCRATCH_ROOT}/emucat"
params.OUTPUT_RAW = "${params.SCRATCH_ROOT}/data/raw_diffuse/${params.SBID}/"
params.BUCKET = "ja3:aussrc/emu/diffuse"

process setup {
    
    input:
        val pass_though

    output:
        val pass_though, emit: pass_though_output

    script:
        """
        #!/bin/bash

        mkdir -p ${params.OUTPUT_RAW}
        """
}

process get_diffuse_input_path {
    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        path manifest

    output:
        stdout emit: out_image_path

    script:
        """
        #!python3

        import json

        with open('${manifest.toRealPath()}') as f:
            data = json.loads(f.read())
            matching = [s for s in data.get("images") if "cont.taylor.0.restored.conv.fits" in s]
            if matching:
                print(matching[0])
            else:
                raise Exception("image not found")
        """
}


process run_diffuse {
    container = "aussrc/emucat_diffusefilter:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        val image_path

    output:
        val image_path, emit: out_image_path

    script:
        """
        EMU_filter.py $image_path
        """
}


process cleanup {
    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        val image_path

    script:
        """
        #!python3
        import os
        import glob

        dir = "${image_path}"
        direct = os.path.dirname(dir)

        for f in glob.glob(f"{direct}/*"):
            if "diffuse" not in os.path.basename(f):
                os.remove(f)
        """
}


workflow {
    sbid = "${params.SBID}"
    output_raw = "${params.OUTPUT_RAW}"
    input_conf = "${params.INPUT_CONF}"
    bucket = "${params.BUCKET}"

    main:
        download_singularity(sbid)
        setup(download_singularity.out.pass_though_output)
        casda_download(setup.out.pass_though_output, output_raw, input_conf)
        get_diffuse_input_path(casda_download.out.file_manifest)
        run_diffuse(get_diffuse_input_path.out.out_image_path.trim())
        run_diffuse.out.out_image_path.view()
        cleanup(run_diffuse.out.out_image_path)
}

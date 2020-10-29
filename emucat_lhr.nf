nextflow.enable.dsl=2

params.ser = 'EMU_2052-5300'
params.emu_vo_url = 'http://146.118.67.65:8080/tap'

params.INPUT_CONF = "${params.SCRATCH_ROOT}/data/emu/emucat"
params.OUTPUT_RAW = "${params.SCRATCH_ROOT}/data/emu/data/raw"
params.OUTPUT_LINMOS = "${params.SCRATCH_ROOT}/data/emu/data/linmos"
params.OUTPUT_SELAVY = "${params.SCRATCH_ROOT}/data/emu/data/selavy/${params.ser}/"
params.OUTPUT_LHR = "${params.SCRATCH_ROOT}/data/emu/data/lhr/${params.ser}/"
params.OUTPUT_LOG_DIR = "${params.SCRATCH_ROOT}/data/emu/log"


process setup {

    input:
        val ser

    output:
        val ser, emit: ser_output

    script:
        """
        #!/bin/bash

        mkdir -p ${params.OUTPUT_RAW}
        mkdir -p ${params.OUTPUT_LINMOS}
        mkdir -p ${params.OUTPUT_SELAVY}
        mkdir -p ${params.OUTPUT_LHR}
        mkdir -p ${params.OUTPUT_LOG_DIR}
        """
}


process get_sched_blocks {

    container = "${params.IMAGES}/emucat_scripts.sif"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    errorStrategy 'retry'
    maxErrors 3

    input:
        val ser

    output:
        stdout emit: obs_list

    script:
        """
        #!python3

        import pyvo as vo

        query = f"SELECT sb.sb_num " \
                f"FROM emucat.source_extraction_regions as ser, " \
                f"emucat.mosaic_prerequisites as mp, " \
                f"emucat.scheduling_blocks as sb " \
                f"WHERE ser.id = mp.ser_id and mp.sb_id = sb.id and ser.name = '${ser}'"

        service = vo.dal.TAPService('${params.emu_vo_url}')
        rowset = service.search(query)
        print(' '.join(str(x['sb_num']) for x in rowset), end='')
        """
}


process casda_download {

    container = "${params.IMAGES}/emucat_scripts.sif"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    errorStrategy 'retry'
    maxErrors 3

    input:
        val obs_list

    output:
        path 'manifest.json', emit: file_manifest

    script:
        """
        python3 /scripts/casda.py --list $obs_list -o ${params.OUTPUT_RAW} \
        -m manifest.json -p ${params.INPUT_CONF}/cred.ini -c true
        """
}


process generate_linmos_conf {

    container = "${params.IMAGES}/emucat_scripts.sif"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        path file_manifest
        val ser

    output:
        path 'linmos.conf', emit: linmos_conf

    script:
        """
        #!python3

        import json
        from jinja2 import Environment, FileSystemLoader
        from pathlib import Path

        with open('${file_manifest.toRealPath()}') as o:
           data = json.loads(o.read())

        images = [Path(image).with_suffix('') for image in data['images'] if '.0.' in image]
        weights = [Path(weight).with_suffix('') for weight in data['weights'] if '.0.' in weight]
        image_out = Path('${params.OUTPUT_LINMOS}/${ser}.image.taylor.0')
        weight_out = Path('${params.OUTPUT_LINMOS}/${ser}.weights.taylor.0')

        j2_env = Environment(loader=FileSystemLoader('${params.INPUT_CONF}/templates'), trim_blocks=True)
        result = j2_env.get_template('linmos.j2').render(images=images, weights=weights, \
        image_out=image_out, weight_out=weight_out)

        with open('linmos.conf', 'w') as f:
            print(result, file=f)
        """
}


process run_linmos {

    input:
        path linmos_conf
        val ser

    output:
        val "${params.OUTPUT_LINMOS}/${ser}.image.taylor.0.fits", emit: image_out
        val "${params.OUTPUT_LINMOS}/${ser}.weights.taylor.0.fits", emit: weight_out

    script:
        """
        #!/bin/bash
        mpirun singularity exec --bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT} \
        ${params.IMAGES}/yandasoft_devel_focal.sif linmos-mpi -c ${linmos_conf.toRealPath()}
        """
}


process generate_selavy_conf {

    container = "${params.IMAGES}/emucat_scripts.sif"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        path image_input
        path weight_input
        val ser

    output:
        path 'selavy.conf', emit: selavy_conf
        path 'yandasoft.log_cfg', emit: selavy_log_conf

    script:
        """
        #!python3
        from jinja2 import Environment, FileSystemLoader
        from pathlib import Path
        import os

        ser = '${ser}'
        output_path = Path('${params.OUTPUT_SELAVY}')
        image = Path('${image_input.toRealPath()}')
        weight = Path('${weight_input.toRealPath()}')
        log = Path('${params.OUTPUT_LOG_DIR}/${ser}_selavy.log')
        results = Path('${params.OUTPUT_SELAVY}/${ser}_results.txt')
        votable = Path('${params.OUTPUT_SELAVY}/${ser}_votable.xml')
        annotations = Path('${params.OUTPUT_SELAVY}/${ser}_annotations.ann')

        j2_env = Environment(loader=FileSystemLoader('${params.INPUT_CONF}/templates'), trim_blocks=True)
        result = j2_env.get_template('selavy.j2').render(ser=ser, output_path=output_path, image=image, weight=weight, \
                 results=results, votable=votable, annotations=annotations)

        with open('selavy.conf', 'w') as f:
            print(result, file=f)

        result = j2_env.get_template('yandasoft_log.j2').render(log=log)

        with open('yandasoft.log_cfg', 'w') as f:
            print(result, file=f)
        """
}


process run_selavy {

    executor = 'slurm'
    clusterOptions = '--nodes=20 --ntasks-per-node=5'

    input:
        path selavy_conf
        path selavy_log_conf
        val ser

    output:
        val "${params.OUTPUT_SELAVY}/${ser}_results.components.xml", emit: cat_out

    script:
        """
        #!/bin/bash
        mpirun singularity exec --bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT} \
        ${params.IMAGES}/yandasoft_devel_focal.sif selavy -c ${selavy_conf.toRealPath()} \
        -l ${selavy_log_conf.toRealPath()}
        """
}

process remove_mosaic_from_emucat {

    container = "${params.IMAGES}/emucat_scripts.sif"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        path cat_input
        val ser

    output:
        path cat_input, emit: cat_out
        val ser, emit: ser_output

    script:
        """
        python3 /scripts/catalog.py delete_components -s ${ser} -c ${params.INPUT_CONF}/cred.ini
        """
}


process insert_selavy_into_emucat {

    container = "${params.IMAGES}/emucat_scripts.sif"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        path cat_input
        val ser

    output:
        val ser, emit: ser_output

    script:
        """
        python3 /scripts/catalog.py import_selavy -s ${ser} -c ${params.INPUT_CONF}/cred.ini \
        -i ${cat_input.toRealPath()}
        """
}


process get_component_sources {

    container = "${params.IMAGES}/emucat_scripts.sif"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    errorStrategy 'retry'
    maxErrors 3

    input:
        val ser

    output:
        val "${params.OUTPUT_LHR}/${ser}_components.xml", emit: component_cat

    script:
        """
        #!python3
        import pyvo as vo

        query = f"SELECT c.id, c.flux_int, c.flux_int_err, c.ra_deg_cont, c.dec_deg_cont " \
                f"FROM emucat.components c, emucat.mosaics m, emucat.source_extraction_regions s "\
                f"WHERE c.mosaic_id=m.id AND m.ser_id=s.id AND s.name='${ser}' ORDER BY id ASC"

        service = vo.dal.TAPService('${params.emu_vo_url}')
        rowset = service.search(query, maxrec=service.hardlimit)
        with open("${params.OUTPUT_LHR}/${ser}_components.xml", "w") as f:
            rowset.to_table().write(output=f, format="votable")
        """
}


process get_allwise_sources {

    container = "${params.IMAGES}/emucat_scripts.sif"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    errorStrategy 'retry'
    maxErrors 3

    input:
        val ser

    output:
        val "${params.OUTPUT_LHR}/${ser}_allwise.xml", emit: allwise_cat

    script:
        """
        #!python3
        import pyvo as vo

        query = f"SELECT designation, ra, dec, w1mpro, w1sigmpro FROM emucat.allwise as a, " \
                f"(SELECT extent FROM emucat.source_extraction_regions WHERE name = '${ser}') as sr " \
                f"WHERE 1 = INTERSECTS(a.ra_dec, sr.extent) ORDER BY ra ASC"

        service = vo.dal.TAPService('${params.emu_vo_url}')
        rowset = service.search(query, maxrec=service.hardlimit)
        with open("${params.OUTPUT_LHR}/${ser}_allwise.xml", "w") as f:
            rowset.to_table().write(output=f, format="votable")
        """
}


process generate_lhr_conf {

    container = "${params.IMAGES}/emucat_scripts.sif"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        val ser

    output:
        path 'lr_config.conf', emit: lhr_conf

    script:
        """
        #!python3

        from jinja2 import Environment, FileSystemLoader
        from pathlib import Path

        output = Path('${params.OUTPUT_LHR}')
        j2_env = Environment(loader=FileSystemLoader('${params.INPUT_CONF}/templates'), trim_blocks=True)
        result = j2_env.get_template('lr_config.j2').render(output=output)
        with open('lr_config.conf', 'w') as f:
            print(result, file=f)
        """
}


process run_lhr {

    container = "${params.IMAGES}/emucat_lhr.sif"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        val mwcat
        val radcat
        val conf

    output:
        val "${params.OUTPUT_LHR}/w1_LR_matches.xml", emit: w1_lr_matches

    script:
        """
        python3 /scripts/lr_wrapper_emucat.py --mwcat ${mwcat} --radcat ${radcat} --config ${conf} \
        > ${params.OUTPUT_LOG_DIR}/${params.ser}_lhr.log
        """
}


process insert_lhr_into_emucat {

    container = "${params.IMAGES}/emucat_scripts.sif"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        path w1_lr_matches
        val ser

    output:
        val ser, emit: ser_output

    script:
        """
        python3 /scripts/catalog.py import_lhr -c ${params.INPUT_CONF}/cred.ini \
        -i ${w1_lr_matches.toRealPath()}
        """

}


process import_des_from_lhr {
    echo true

    container = "${params.IMAGES}/emucat_scripts.sif"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        val ser

    output:
        val ser, emit: ser_output

    script:
        """
        python3 -u /scripts/catalog.py import_des_from_lhr -s ${ser} -c ${params.INPUT_CONF}/cred.ini \
        > ${params.OUTPUT_LOG_DIR}/${ser}_des_dr1.log
        """
}


workflow emucat_lhr {
    take:
        ser

    main:
        setup(ser)
        get_sched_blocks(setup.out.ser_output)
        casda_download(get_sched_blocks.out.obs_list)
        generate_linmos_conf(casda_download.out.file_manifest, ser)
        run_linmos(generate_linmos_conf.out.linmos_conf, ser)
        generate_selavy_conf(run_linmos.out.image_out, run_linmos.out.weight_out, ser)
        run_selavy(generate_selavy_conf.out.selavy_conf, generate_selavy_conf.out.selavy_log_conf, ser)
        remove_mosaic_from_emucat(run_selavy.out.cat_out, ser)
        insert_selavy_into_emucat(remove_mosaic_from_emucat.out.cat_out, ser)
        generate_lhr_conf(insert_selavy_into_emucat.out.ser_output)
        get_allwise_sources(insert_selavy_into_emucat.out.ser_output)
        get_component_sources(insert_selavy_into_emucat.out.ser_output)
        run_lhr(get_allwise_sources.out.allwise_cat, get_component_sources.out.component_cat, generate_lhr_conf.out.lhr_conf)
        insert_lhr_into_emucat(run_lhr.out.w1_lr_matches, ser)
        import_des_from_lhr(insert_lhr_into_emucat.out.ser_output)
}


workflow {
    main:
        emucat_lhr(params.ser)
}
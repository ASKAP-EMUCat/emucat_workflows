nextflow.enable.dsl=2

params.ser = 'EMU_2137-5300'
params.emu_vo_url = 'http://146.118.67.65:8080/tap'

params.INPUT_CONF = "${params.SCRATCH_ROOT}/data/emu/emucat"
params.OUTPUT_RAW = "${params.SCRATCH_ROOT}/data/emu/data/raw"
params.OUTPUT_LINMOS = "${params.SCRATCH_ROOT}/data/emu/data/linmos"
params.OUTPUT_SELAVY = "${params.SCRATCH_ROOT}/data/emu/data/selavy"
params.OUTPUT_LOG_DIR = "${params.SCRATCH_ROOT}/data/emu/log"


process get_sched_blocks {

    container = "${params.IMAGES}/general.sif"
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

    container = "${params.IMAGES}/general.sif"
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

    container = "${params.IMAGES}/general.sif"
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
        ${params.IMAGES}/yandasoft.sif linmos-mpi -c ${linmos_conf.toRealPath()}
        """
}


process generate_selavy_conf {

    container = "${params.IMAGES}/general.sif"
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

        image = Path('${image_input.toRealPath()}')
        weight = Path('${weight_input.toRealPath()}')
        log = Path('${params.OUTPUT_LOG_DIR}/${ser}_selavy.log')
        results = Path('${params.OUTPUT_SELAVY}/${ser}_results.txt')
        votable = Path('${params.OUTPUT_SELAVY}/${ser}_votable.xml')
        annotations = Path('${params.OUTPUT_SELAVY}/${ser}_annotations.ann')

        j2_env = Environment(loader=FileSystemLoader('${params.INPUT_CONF}/templates'), trim_blocks=True)
        result = j2_env.get_template('selavy.j2').render(image=image, weight=weight, \
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
    clusterOptions = '--nodes=20 --ntasks-per-node=6'

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
        ${params.IMAGES}/yandasoft.sif selavy -c ${selavy_conf.toRealPath()} -l ${selavy_log_conf.toRealPath()}
        """
}


process insert_selavy_into_emucat {

    container = "${params.IMAGES}/general.sif"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        path cat_input
        val ser

    output:

    script:
        """
        python3 /scripts/catalog.py import_selavy -s ${ser} -c ${params.INPUT_CONF}/cred.ini \
        -i ${cat_input.toRealPath()}
        """
}


process get_sched_blocks {

    container = "${params.IMAGES}/general.sif"
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


workflow {
    get_sched_blocks(params.ser)
    casda_download(get_sched_blocks.out.obs_list)
    generate_linmos_conf(casda_download.out.file_manifest, params.ser)
    run_linmos(generate_linmos_conf.out.linmos_conf, params.ser)
    generate_selavy_conf(run_linmos.out.image_out, run_linmos.out.weight_out, params.ser)
    run_selavy(generate_selavy_conf.out.selavy_conf, generate_selavy_conf.out.selavy_log_conf, params.ser)
    insert_selavy_into_emucat(run_selavy.out.cat_out, params.ser)
}

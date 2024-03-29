nextflow.enable.dsl=2

params.ser = ''
params.emu_vo_url = 'https://emucat.aussrc.org/tap'

params.INPUT_CONF = "${params.SCRATCH_ROOT}/emucat"
params.OUTPUT_RAW = "${params.SCRATCH_ROOT}/data/raw/${params.ser}/"
params.OUTPUT_LINMOS = "${params.SCRATCH_ROOT}/data/linmos/${params.ser}/"
params.OUTPUT_SELAVY = "${params.SCRATCH_ROOT}/data/selavy/${params.ser}/"
params.OUTPUT_LHR = "${params.SCRATCH_ROOT}/data/lhr/${params.ser}/"
params.OUTPUT_LHR_ISLAND = "${params.SCRATCH_ROOT}/data/lhr_island/${params.ser}/"
params.OUTPUT_EXTENDED_DOUBLES = "${params.SCRATCH_ROOT}/data/extended_doubles/${params.ser}/"
params.OUTPUT_LOG_DIR = "${params.SCRATCH_ROOT}/log"


include {
    generate_lhr_conf as generate_lhr_conf;
    generate_lhr_conf as generate_lhr_island_conf;
    get_allwise_sources as get_lhr_allwise_sources;
    get_allwise_sources as get_lhr_island_allwise_sources;
} from './modules/lhr'


process setup {
    
    input:
        val ser

    output:
        val ser, emit: ser_output

    script:
        """
        #!/bin/bash

        if [ -z "${ser}" ]
        then
            echo "ser is empty"
            exit -1
        fi

        rm -rf ${params.OUTPUT_RAW}
        rm -rf ${params.OUTPUT_LINMOS}
        rm -rf ${params.OUTPUT_SELAVY}
        rm -rf ${params.OUTPUT_LHR}
        rm -ef ${params.OUTPUT_LHR_ISLAND}
        rm -rf ${params.OUTPUT_LOG_DIR}
        rm -rf ${params.OUTPUT_EXTENDED_DOUBLES}

        mkdir -p ${params.OUTPUT_RAW}
        mkdir -p ${params.OUTPUT_LINMOS}
        mkdir -p ${params.OUTPUT_SELAVY}
        mkdir -p ${params.OUTPUT_LHR}
        mkdir -p ${params.OUTPUT_LHR_ISLAND}
        mkdir -p ${params.OUTPUT_LOG_DIR}
        mkdir -p ${params.OUTPUT_EXTENDED_DOUBLES}
        """
}


process get_sched_blocks {

    container = "aussrc/emucat_scripts:latest"
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

        ser = '${ser}'
        if not ser:
            raise ValueError('SER is empty')

        query = f"SELECT sb.sb_num " \
                f"FROM emucat.regions as ser, " \
                f"emucat.mosaic_prerequisites as mp, " \
                f"emucat.scheduling_blocks as sb " \
                f"WHERE ser.id = mp.ser_id " \
                f"and mp.sb_id = sb.id " \
                f"and ser.name = '${ser}' " \
                f"and sb.sb_num is not null"

        service = vo.dal.TAPService('${params.emu_vo_url}')
        rowset = service.run_async(query)
        print(' '.join(str(x['sb_num']) for x in rowset), end='')
        """
}


process casda_download {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    errorStrategy 'retry'
    maxErrors 4

    input:
        val obs_list

    output:
        val "${params.OUTPUT_RAW}/manifest.json", emit: file_manifest

    script:
        """
        python3 /scripts/casda.py --list $obs_list -o ${params.OUTPUT_RAW} \
        -m ${params.OUTPUT_RAW}/manifest.json -p ${params.INPUT_CONF}/cred.ini -c false
        """
}


process generate_linmos_conf {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        path file_manifest
        val ser

    output:
        val "${params.OUTPUT_LINMOS}/linmos.conf", emit: linmos_conf
        val "${params.OUTPUT_LINMOS}/linmos.log_cfg", emit: linmos_log_conf

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
        log = Path('${params.OUTPUT_LOG_DIR}/${ser}_linmos.log')

        j2_env = Environment(loader=FileSystemLoader('$baseDir/templates'), trim_blocks=True)
        result = j2_env.get_template('linmos.j2').render(images=images, weights=weights, \
        image_out=image_out, weight_out=weight_out)

        with open('${params.OUTPUT_LINMOS}/linmos.conf', 'w') as f:
            print(result, file=f)

        result = j2_env.get_template('yandasoft_log.j2').render(log=log)

        with open('${params.OUTPUT_LINMOS}/linmos.log_cfg', 'w') as f:
            print(result, file=f)
        """
}


process run_linmos {

    container = "csirocass/askapsoft:1.13.0-setonix-lustrempich"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        path linmos_conf
        path linmos_log_conf
        val ser

    output:
        val "${params.OUTPUT_LINMOS}/${ser}.image.taylor.0.fits", emit: image_out
        val "${params.OUTPUT_LINMOS}/${ser}.weights.taylor.0.fits", emit: weight_out

    script:
        """
        #!/bin/bash

        linmos -c ${linmos_conf.toRealPath()} -l ${linmos_log_conf.toRealPath()}
        """
}


process generate_selavy_conf {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        path image_input
        path weight_input
        val ser

    output:
        val "${params.OUTPUT_SELAVY}/selavy.conf", emit: selavy_conf
        val "${params.OUTPUT_SELAVY}/selavy.log_cfg", emit: selavy_log_conf

    script:
        """
        #!python3
        from jinja2 import Environment, FileSystemLoader
        from pathlib import Path
        import os
        from astropy.io import fits

        # Correct header
        #fits.setval('${image_input.toRealPath()}', 'BUNIT', value='Jy/beam ')

        ser = '${ser}'
        output_path = Path('${params.OUTPUT_SELAVY}')
        image = Path('${image_input.toRealPath()}')
        weight = Path('${weight_input.toRealPath()}')
        log = Path('${params.OUTPUT_LOG_DIR}/${ser}_selavy.log')
        results = Path('${params.OUTPUT_SELAVY}/${ser}_results.txt')
        votable = Path('${params.OUTPUT_SELAVY}/${ser}_votable.xml')
        annotations = Path('${params.OUTPUT_SELAVY}/${ser}_annotations.ann')

        j2_env = Environment(loader=FileSystemLoader('$baseDir/templates'), trim_blocks=True)
        result = j2_env.get_template('selavy.j2').render(ser=ser, output_path=output_path, image=image, weight=weight, \
                 results=results, votable=votable, annotations=annotations)

        with open('${params.OUTPUT_SELAVY}/selavy.conf', 'w') as f:
            print(result, file=f)

        result = j2_env.get_template('yandasoft_log.j2').render(log=log)

        with open('${params.OUTPUT_SELAVY}/selavy.log_cfg', 'w') as f:
            print(result, file=f)
        """
}


process run_selavy {

    input:
        path selavy_conf
        path selavy_log_conf
        val ser

    output:
        val "${params.OUTPUT_SELAVY}/${ser}_results.components.xml", emit: cat_out
        val "${params.OUTPUT_SELAVY}/${ser}_results.islands.xml", emit: island_out

    script:
        """
        #!/bin/bash

        srun --export=ALL --mpi=pmi2 -n 64 \
                singularity exec \
                --bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT} \
                ${params.IMAGES}/csirocass-askapsoft-1.13.0-setonix-lustrempich.img \
                selavy -c ${selavy_conf.toRealPath()} -l ${selavy_log_conf.toRealPath()}
        """
}

process remove_mosaic_from_emucat {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    errorStrategy { sleep(Math.pow(2, task.attempt) * 200 as long); return 'retry' }
    maxErrors 3

    input:
        path cat_input
        path island_input
        val ser

    output:
        path cat_input, emit: cat_out
        path island_input, emit: island_out
        val ser, emit: ser_output

    script:
        """
        #!/bin/bash

        python3 /scripts/catalog.py delete_components -s ${ser} -c ${params.INPUT_CONF}/cred.ini
        """
}


process insert_selavy_components_into_emucat {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        path cat_input
        val ser

    output:
        val ser, emit: ser_output

    script:
        """
        #!/bin/bash

        python3 /scripts/catalog.py import_selavy -s ${ser} -c ${params.INPUT_CONF}/cred.ini -i ${cat_input.toRealPath()}
        """
}

process insert_selavy_islands_into_emucat {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        path island_input
        val ser

    output:
        val ser, emit: ser_output

    script:
        """
        #!/bin/bash

        python3 /scripts/catalog.py import_selavy_island -s ${ser} -c ${params.INPUT_CONF}/cred.ini -i ${island_input.toRealPath()}
        """
}


process match_nearest_neighbour_with_allwise {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        val ser

    output:
        val ser, emit: ser_output

    script:
        """
        #!/bin/bash

        python3 /scripts/catalog.py match_nearest_neighbour_with_allwise -s ${ser} -c ${params.INPUT_CONF}/cred.ini
        """
}


process get_component_sources {

    container = "aussrc/emucat_scripts:latest"
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
        import time
        import pyvo as vo

        query = f"SELECT c.id, c.flux_int, c.flux_int_err, c.ra_deg_cont, c.dec_deg_cont " \
                f"FROM emucat.components c, emucat.mosaics m, emucat.regions s "\
                f"WHERE c.mosaic_id=m.id AND m.ser_id=s.id AND s.name='${ser}' ORDER BY id ASC"

        service = vo.dal.TAPService('${params.emu_vo_url}')
        job = service.submit_job(query, maxrec=service.hardlimit)
        job.run()

        while True:
            if job.phase == 'EXECUTING':
                time.sleep(10)
            else:
                break

        with open("${params.OUTPUT_LHR}/${ser}_components.xml", "w") as f:
            job.fetch_result().to_table().write(output=f, format="votable")
        """
}


process get_island_sources {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    errorStrategy 'retry'
    maxErrors 3

    input:
        val ser

    output:
        val "${params.OUTPUT_LHR_ISLAND}/${ser}_islands.xml", emit: island_cat

    script:
        """
        #!python3
        import time
        import pyvo as vo

        query = f"SELECT i.id, i.flux_int, i.flux_int_err, i.ra_deg_cont, i.dec_deg_cont " \
                f"FROM emucat.islands i, emucat.mosaics m, emucat.regions s "\
                f"WHERE i.mosaic_id=m.id AND m.ser_id=s.id AND s.name='${ser}' AND i.n_components > 1 ORDER BY id ASC"

        service = vo.dal.TAPService('${params.emu_vo_url}')
        job = service.submit_job(query, maxrec=service.hardlimit)
        job.run()

        while True:
            if job.phase == 'EXECUTING':
                time.sleep(10)
            else:
                break

        with open("${params.OUTPUT_LHR_ISLAND}/${ser}_islands.xml", "w") as f:
            job.fetch_result().to_table().write(output=f, format="votable")
        """
}



process run_lhr_components {

    container = "aussrc/emucat_lhr:parallel"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        val mwcat
        val radcat
        val conf

    output:
        val "${params.OUTPUT_LHR}/w1_LR_matches.csv", emit: w1_lr_matches

    script:
        """
        #!/bin/bash

        mkdir -p ${params.OUTPUT_LHR}/astropy
        export XDG_CACHE_HOME=${params.OUTPUT_LHR}
        export MPLCONFIGDIR=${params.OUTPUT_LHR}
        export LHR_CPU=32
        python3 -u /scripts/lr_wrapper_emucat.py --mwcat ${mwcat} --radcat ${radcat} --config ${conf} > ${params.OUTPUT_LOG_DIR}/${params.ser}_lhr.log
        cp ${params.OUTPUT_LHR}/w1_LR_matches.csv ${params.OUTPUT_LHR}/w1_LR_matches_components.csv
        cp ${params.OUTPUT_LHR}/w1_LR_matches.fits ${params.OUTPUT_LHR}/w1_LR_matches_components.fits      
        """
}


process run_lhr_islands {

    container = "aussrc/emucat_lhr:parallel"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        val mwcat
        val radcat
        val conf

    output:
        val "${params.OUTPUT_LHR_ISLAND}/w1_LR_matches_islands.csv", emit: w1_lr_matches

    script:
        """
        #!/bin/bash

        mkdir -p ${params.OUTPUT_LHR_ISLAND}/astropy
        export XDG_CACHE_HOME=${params.OUTPUT_LHR_ISLAND}
        export MPLCONFIGDIR=${params.OUTPUT_LHR_ISLAND}
        export LHR_CPU=32
        python3 -u /scripts/lr_wrapper_emucat.py --mwcat ${mwcat} --radcat ${radcat} --config ${conf} > ${params.OUTPUT_LOG_DIR}/${params.ser}_lhr.log
        cp ${params.OUTPUT_LHR_ISLAND}/w1_LR_matches.csv ${params.OUTPUT_LHR_ISLAND}/w1_LR_matches_islands.csv
        cp ${params.OUTPUT_LHR_ISLAND}/w1_LR_matches.fits ${params.OUTPUT_LHR_ISLAND}/w1_LR_matches_islands.fits
        """
}


process insert_lhr_into_emucat {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        path w1_lr_matches
        val ser

    output:
        val ser, emit: ser_output

    script:
        """
        python3 /scripts/catalog.py import_lhr -c ${params.INPUT_CONF}/cred.ini -i ${w1_lr_matches.toRealPath()}
        """
}


process insert_lhr_islands_into_emucat {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        path w1_lr_matches
        val ser

    output:
        val ser, emit: ser_output

    script:
        """
        #!/bin/bash

        python3 /scripts/catalog.py import_lhr_islands -c ${params.INPUT_CONF}/cred.ini -i ${w1_lr_matches.toRealPath()}
        """
}


process import_des_dr1_from_lhr {
    
    errorStrategy 'retry'
    maxErrors 3

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        val ser

    output:
        val ser, emit: ser_output

    script:
        """
        #!/bin/bash

        export HOME=${params.SCRATCH_ROOT}
        python3 -u /scripts/noao.py import_des_dr1_from_lhr -s ${ser} -c ${params.INPUT_CONF}/cred.ini \
        -o ${params.OUTPUT_LHR} > ${params.OUTPUT_LOG_DIR}/${ser}_des_dr1.log
        """
}


process import_des_dr2_from_lhr {
    
    errorStrategy 'retry'
    maxErrors 3

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        val ser

    output:
        val ser, emit: ser_output

    script:
        """
        export HOME=${params.SCRATCH_ROOT}
        python3 -u /scripts/noao.py import_des_dr2_from_lhr -s ${ser} -c ${params.INPUT_CONF}/cred.ini \
        -o ${params.OUTPUT_LHR} > ${params.OUTPUT_LOG_DIR}/${ser}_des_dr2.log
        """
}


process import_vhs_from_lhr {
    
    errorStrategy 'retry'
    maxErrors 3

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        val ser

    output:
        val ser, emit: ser_output

    script:
        """
        #!/bin/bash

        export HOME=${params.SCRATCH_ROOT}
        python3 -u /scripts/noao.py import_vhs_from_lhr -s ${ser} -c ${params.INPUT_CONF}/cred.ini \
        -o ${params.OUTPUT_LHR} > ${params.OUTPUT_LOG_DIR}/${ser}_vhs_from_lhr.log
        """
}


process get_extended_double_components {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    errorStrategy 'retry'
    maxErrors 3

    input:
        val ser

    output:
        val ser, emit: ser_output
        val "${params.OUTPUT_EXTENDED_DOUBLES}/${ser}_double_components.xml", emit: comp_cat

    script:
        """
        #!python3
        import time
        import pyvo as vo

        query = f"SELECT c.id, c.ra_deg_cont, c.dec_deg_cont, c.flux_peak, c.flux_int, " \
        f"maj_axis_deconv, min_axis_deconv, pos_ang_deconv " \
        f"FROM emucat.components c, emucat.mosaics m, emucat.regions s " \
        f"WHERE c.mosaic_id=m.id AND m.ser_id=s.id AND s.name='${ser}' " \
        f"AND c.id NOT IN " \
        f"(SELECT co.id FROM emucat.components co, emucat.mosaics mo, emucat.regions se, " \
        f"emucat.sources_nearest_allwise n WHERE n.component_id=co.id AND co.mosaic_id=mo.id " \
        f"AND mo.ser_id=se.id AND se.name='${ser}')"

        service = vo.dal.TAPService('${params.emu_vo_url}')
        job = service.submit_job(query, maxrec=service.hardlimit)
        job.run()

        while True:
            if job.phase == 'EXECUTING':
                time.sleep(10)
            else:
                break

        with open("${params.OUTPUT_EXTENDED_DOUBLES}/${ser}_double_components.xml", "w") as f:
            job.fetch_result().to_table().write(output=f, format="votable")
        """
}

process generate_extended_double_conf {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        val ser

    output:
        path 'ed_config.conf', emit: ed_conf

    script:
        """
        #!python3

        from jinja2 import Environment, FileSystemLoader
        from pathlib import Path

        output = Path('${params.OUTPUT_EXTENDED_DOUBLES}')
        j2_env = Environment(loader=FileSystemLoader('$baseDir/templates'), trim_blocks=True)
        result = j2_env.get_template('ed_config.j2').render(output=output)
        with open('ed_config.conf', 'w') as f:
            print(result, file=f)
        """
}


process run_extended_doubles {

    container = "aussrc/emucat_double_sources:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        val ser
        val ed_conf
        val comp_cat

    output:
        val ser, emit: ser_output
        val "${params.OUTPUT_EXTENDED_DOUBLES}/${ser}_double_components_pairs.xml", emit: source_cat

    script:
        """
        #!/bin/bash

        rm -f ${params.OUTPUT_EXTENDED_DOUBLES}/${ser}_double_components_pairs.xml && \
        python3 -u /scripts/emu_doubles.py --config ${ed_conf} ${comp_cat}
        """
}


process insert_extended_doubles_into_emucat {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        val ser
        val source_cat

    output:
        val ser, emit: ser_output
        
    script:
        """
        #!/bin/bash

        python3 -u /scripts/catalog.py import_extended_doubles -i ${source_cat} -c ${params.INPUT_CONF}/cred.ini
        """
}


process insert_properties_into_emucat {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        val ser

    output:
        val ser, emit: ser_output

    script:
        """
        #!/bin/bash

        python3 -u /scripts/properties.py import_properties -s ${ser} -c ${params.INPUT_CONF}/cred.ini
        """
}


workflow emucat_ser {
    take:
        ser

    main:
        setup(ser)

        // Download Image data
        get_sched_blocks(setup.out.ser_output)
        casda_download(get_sched_blocks.out.obs_list)

        // Mosaic
        generate_linmos_conf(casda_download.out.file_manifest, ser)
        run_linmos(generate_linmos_conf.out.linmos_conf, generate_linmos_conf.out.linmos_log_conf, ser)
        
        // Source extraction
        generate_selavy_conf(run_linmos.out.image_out, run_linmos.out.weight_out, ser)
        run_selavy(generate_selavy_conf.out.selavy_conf, generate_selavy_conf.out.selavy_log_conf, ser)
        remove_mosaic_from_emucat(run_selavy.out.cat_out, run_selavy.out.island_out, ser)
        insert_selavy_components_into_emucat(remove_mosaic_from_emucat.out.cat_out, ser)
        insert_selavy_islands_into_emucat(remove_mosaic_from_emucat.out.island_out, insert_selavy_components_into_emucat.out.ser_output)
        
        // Matching
        match_nearest_neighbour_with_allwise(insert_selavy_components_into_emucat.out.ser_output)
        
        // LHR algorithm for components
        generate_lhr_conf("${params.OUTPUT_LHR}", match_nearest_neighbour_with_allwise.out.ser_output)
        get_lhr_allwise_sources("${params.OUTPUT_LHR}/${ser}_allwise.xml", run_linmos.out.image_out, ser)
        get_component_sources(insert_selavy_components_into_emucat.out.ser_output)
        run_lhr_components(get_lhr_allwise_sources.out.allwise_cat, get_component_sources.out.component_cat, generate_lhr_conf.out.lhr_conf)
        insert_lhr_into_emucat(run_lhr_components.out.w1_lr_matches, ser)

        // LHR algorithm for islands
        generate_lhr_island_conf("${params.OUTPUT_LHR_ISLAND}", match_nearest_neighbour_with_allwise.out.ser_output)
        get_lhr_island_allwise_sources("${params.OUTPUT_LHR_ISLAND}/${ser}_allwise.xml", run_linmos.out.image_out, ser)
        get_island_sources(insert_selavy_islands_into_emucat.out.ser_output)
        run_lhr_islands(get_lhr_island_allwise_sources.out.allwise_cat, get_island_sources.out.island_cat, generate_lhr_island_conf.out.lhr_conf)
        insert_lhr_islands_into_emucat(run_lhr_islands.out.w1_lr_matches, ser)

        // Extended doubles algorithm
        get_extended_double_components(insert_lhr_into_emucat.out.ser_output)
        generate_extended_double_conf(get_extended_double_components.out.ser_output)
        run_extended_doubles(get_extended_double_components.out.ser_output, generate_extended_double_conf.out.ed_conf, get_extended_double_components.out.comp_cat)
        insert_extended_doubles_into_emucat(run_extended_doubles.out.ser_output, run_extended_doubles.out.source_cat)

        // Properties
        insert_properties_into_emucat(insert_extended_doubles_into_emucat.out.ser_output)

        // Value add processes
        import_des_dr1_from_lhr(insert_lhr_into_emucat.out.ser_output)
        import_des_dr2_from_lhr(insert_lhr_into_emucat.out.ser_output)
        import_vhs_from_lhr(insert_lhr_into_emucat.out.ser_output)
}


workflow {
    main:
        emucat_ser(params.ser)
}

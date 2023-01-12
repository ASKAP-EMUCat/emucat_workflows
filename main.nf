nextflow.enable.dsl=2

params.ser = ''
params.emu_vo_url = 'https://emucat.aussrc.org/tap'

params.INPUT_CONF = "${params.SCRATCH_ROOT}/emucat"
params.OUTPUT_RAW = "${params.SCRATCH_ROOT}/data/raw/${params.ser}/"
params.OUTPUT_LINMOS = "${params.SCRATCH_ROOT}/data/linmos/${params.ser}/"
params.OUTPUT_SELAVY = "${params.SCRATCH_ROOT}/data/selavy/${params.ser}/"
params.OUTPUT_LHR = "${params.SCRATCH_ROOT}/data/lhr/${params.ser}/"
params.OUTPUT_EXTENDED_DOUBLES = "${params.SCRATCH_ROOT}/data/extended_doubles/${params.ser}/"
params.OUTPUT_LOG_DIR = "${params.SCRATCH_ROOT}/log"


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

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        path file_manifest
        val ser

    output:
        path 'linmos.conf', emit: linmos_conf
        path 'linmos.log_cfg', emit: linmos_log_conf

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

        with open('linmos.conf', 'w') as f:
            print(result, file=f)

        result = j2_env.get_template('yandasoft_log.j2').render(log=log)

        with open('linmos.log_cfg', 'w') as f:
            print(result, file=f)
        """
}


process run_linmos {

    container = "csirocass/askapsoft:1.9.1-casacore3.5.0-mpich"
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

        if [ ! -f "${params.OUTPUT_LINMOS}/${ser}.image.taylor.0.fits" ]; then
            linmos -c ${linmos_conf.toRealPath()} -l ${linmos_log_conf.toRealPath()}
        fi
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
        path 'selavy.conf', emit: selavy_conf
        path 'selavy.log_cfg', emit: selavy_log_conf

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

        with open('selavy.conf', 'w') as f:
            print(result, file=f)

        result = j2_env.get_template('yandasoft_log.j2').render(log=log)

        with open('selavy.log_cfg', 'w') as f:
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

        if [ ! -f "${params.OUTPUT_SELAVY}/${ser}_results.components.xml" ]; then
            export SINGULARITY_PULLDIR=${params.IMAGES}
            singularity pull askapsoft_1.9.1-mpich.sif docker://csirocass/askapsoft:1.9.1-casacore3.5.0-mpich
            srun -N 12 --ntasks-per-node 6 \
                   singularity exec \
                   --bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT} \
                   ${params.IMAGES}/askapsoft_1.9.1-mpich.sif \
                   selavy -c ${selavy_conf.toRealPath()} -l ${selavy_log_conf.toRealPath()}
        fi
        """
}

process remove_mosaic_from_emucat {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

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
        if [ ! -f "${params.OUTPUT_SELAVY}/${ser}_results.components.xml" ]; then
            python3 /scripts/catalog.py delete_components -s ${ser} -c ${params.INPUT_CONF}/cred.ini
        fi
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
        if [ ! -f "${params.OUTPUT_SELAVY}/${ser}_results.components.xml" ]; then
            python3 /scripts/catalog.py import_selavy -s ${ser} -c ${params.INPUT_CONF}/cred.ini -i ${cat_input.toRealPath()}
        fi
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
        if [ ! -f "${params.OUTPUT_SELAVY}/${ser}_results.components.xml" ]; then
            python3 /scripts/catalog.py import_selavy_island -s ${ser} -c ${params.INPUT_CONF}/cred.ini -i ${island_input.toRealPath()}
        fi
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
        if [ ! -f "${params.OUTPUT_SELAVY}/${ser}_results.components.xml" ]; then
            python3 /scripts/catalog.py match_nearest_neighbour_with_allwise -s ${ser} -c ${params.INPUT_CONF}/cred.ini
        fi
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


process get_allwise_sources {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    errorStrategy 'retry'
    maxErrors 3

    input:
        val mosaic
        val ser

    output:
        val "${params.OUTPUT_LHR}/${ser}_allwise.xml", emit: allwise_cat

    script:
        """
        #!python3
        import pyvo as vo
        from astropy.io import fits
        from astropy.wcs import WCS

        with fits.open('${mosaic}') as hdu:
            naxis1 = float(hdu[0].header['NAXIS1'])
            naxis2 = float(hdu[0].header['NAXIS2'])
            w = WCS(hdu[0].header)
            a = w.pixel_to_world_values(0, 0, 0, 0)
            b = w.pixel_to_world_values(naxis1, naxis2, 0, 0)
            x0, y0 = a[0].item(), a[1].item()
            x1, y1 = b[0].item(), b[1].item()

        query = f"SELECT designation, ra, dec, w1mpro, w1sigmpro FROM emucat.allwise as a " \
                f"WHERE 1 = INTERSECTS(CIRCLE(a.ra_dec, 0), POLYGON({x0},{y0},{x0},{y1},{x1},{y1},{x1},{y0})) ORDER BY ra ASC"

        service = vo.dal.TAPService('${params.emu_vo_url}')
        job = service.submit_job(query, maxrec=service.hardlimit)
        job.run()

        while True:
            if job.phase == 'EXECUTING':
                time.sleep(10)
            else:
                break

        with open("${params.OUTPUT_LHR}/${ser}_allwise.xml", "w") as f:
            job.fetch_result().to_table().write(output=f, format="votable")
        """
}


process generate_lhr_conf {

    container = "aussrc/emucat_scripts:latest"
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
        j2_env = Environment(loader=FileSystemLoader('$baseDir/templates'), trim_blocks=True)
        result = j2_env.get_template('lr_config.j2').render(output=output)
        with open('lr_config.conf', 'w') as f:
            print(result, file=f)
        """
}


process run_lhr {

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
        if [ ! -f "${params.OUTPUT_LHR}/w1_LR_matches.csv" ]; then
            mkdir -p ${params.OUTPUT_LHR}/astropy
            export XDG_CACHE_HOME=${params.OUTPUT_LHR}
            export MPLCONFIGDIR=${params.OUTPUT_LHR}
            export LHR_CPU=32
            python3 -u /scripts/lr_wrapper_emucat.py --mwcat ${mwcat} --radcat ${radcat} --config ${conf} \
            > ${params.OUTPUT_LOG_DIR}/${params.ser}_lhr.log
        fi
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
        if [ ! -f "${params.OUTPUT_LHR}/w1_LR_matches.csv" ]; then
            python3 /scripts/catalog.py import_lhr -c ${params.INPUT_CONF}/cred.ini -i ${w1_lr_matches.toRealPath()}
        fi
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
        export HOME=${params.SCRATCH_ROOT}
        python3 -u /scripts/noao.py import_vhs_from_lhr -s ${ser} -c ${params.INPUT_CONF}/cred.ini \
        -o ${params.OUTPUT_LHR} > ${params.OUTPUT_LOG_DIR}/${ser}_des_dr2.log
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
        
        // LHR algorithm
        generate_lhr_conf(match_nearest_neighbour_with_allwise.out.ser_output)
        get_allwise_sources(run_linmos.out.image_out, ser)
        get_component_sources(insert_selavy_components_into_emucat.out.ser_output)
        run_lhr(get_allwise_sources.out.allwise_cat, get_component_sources.out.component_cat, generate_lhr_conf.out.lhr_conf)
        insert_lhr_into_emucat(run_lhr.out.w1_lr_matches, ser)

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

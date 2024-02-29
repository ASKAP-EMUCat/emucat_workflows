#!/usr/bin/env nextflow

nextflow.enable.dsl = 2


process generate_lhr_conf {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    input:
        val output_dir
        val ser

    output:
        path 'lr_config.conf', emit: lhr_conf

    script:
        """
        #!python3

        from jinja2 import Environment, FileSystemLoader
        from pathlib import Path

        output = Path('${output_dir}')
        j2_env = Environment(loader=FileSystemLoader('$baseDir/templates'), trim_blocks=True)
        result = j2_env.get_template('lr_config.j2').render(output=output)
        with open('lr_config.conf', 'w') as f:
            print(result, file=f)
        """
}


process get_allwise_sources {

    container = "aussrc/emucat_scripts:latest"
    containerOptions = "--bind ${params.SCRATCH_ROOT}:${params.SCRATCH_ROOT}"

    errorStrategy 'retry'
    maxErrors 3

    input:
        val output
        val mosaic
        val ser

    output:
        val "${output}", emit: allwise_cat

    script:
        """
        #!python3
        import time
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
                f"WHERE 1 = INTERSECTS(a.ra_dec, POLYGON({x0},{y0},{x0},{y1},{x1},{y1},{x1},{y0}))"

        service = vo.dal.TAPService('${params.emu_vo_url}')
        job = service.submit_job(query, maxrec=service.hardlimit)
        job.run()

        while True:
            if job.phase == 'EXECUTING':
                time.sleep(10)
            else:
                break

        with open("${output}", "w") as f:
            job.fetch_result().to_table().write(output=f, format="votable")
        """
}
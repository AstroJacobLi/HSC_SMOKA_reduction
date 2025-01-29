def ingest_files(name, expid_ranges, filetype='flat'):
    base_path = f"/scratch/gpfs/JENNYG/jiaxuanl/HSC/raw_smoka/{name}"
    file_dir = f"{base_path}/{filetype}"
    log_path = f"$LOGDIR/{name}_ingest_{filetype}.log"
    repo_path = "$REPO"

    # Generate mkdir command
    mkdir_cmd = f"mkdir -p {file_dir}"

    # Generate cp commands using ranges
    expid_cmds = []
    for start, end in expid_ranges:
        expid_cmds.append(f"{{{start}..{end}}}")
    expid_list = " ".join(expid_cmds)
    cp_cmd = (
        f"for expid in {expid_list}; do\n"
        f"    mv {base_path}/HSCA${{expid}}*.fits \\\n"
        f"       {file_dir}/\n"
        f"done"
    )

    # Generate butler ingest commands
    ingest_cmd = (
        f"LOGFILE={log_path}; \\\n"
        f"FILES={file_dir}/*.fits; \\\n"
        f"date | tee $LOGFILE; \\\n"
        f"butler ingest-raws {repo_path} $FILES --transfer link \\\n"
        f"2>&1 | tee -a $LOGFILE; \\\n"
        f"date | tee -a $LOGFILE"
    )

    # Combine all commands
    print(f'######## Ingesting {filetype}... ##########')
    full_script = f"{mkdir_cmd}\n{cp_cmd}\n\n{ingest_cmd}" + '\n'
    return full_script

def calibration_command(name, expid_dict, filetype="bias", njobs=36, clobber=False, checkNoData=False):
    """
    Generate shell commands for calibration processing (e.g., bias, flat, dark).

    Args:
        name (str): Galaxy name.
        exp_ids (list of int): List of exposure IDs for the calibration type.
        filetype (str): Type of calibration data (e.g., 'bias', 'flat', 'dark'). Default is 'bias'.
        log_file_suffix (str): Suffix for the log file name (default is filetype).

    Returns:
        str: Shell script as a string.
    """
    # Set default log file suffix
    log_file_suffix = filetype

    # Generate the exposure ID string
    expid_dict['sky'] = expid_dict['science']
    exp_ids = [exp_id for start, end in expid_dict[filetype] for exp_id in range(start, end + 1) if exp_id % 2 == 0]
    exp_id_str = f"({', '.join(map(str, exp_ids))})"

    # Define paths and variables
    log_path = f"$LOGDIR/{name}_{log_file_suffix}.log"
    repo_path = "$REPO"
    calib_output = f"HSC/calib/{name}/{filetype}"
    pipeline_path = f"$CP_PIPE_DIR/pipelines/HSC/cp{filetype.capitalize()}.yaml"

    calib_collection = f",HSC/calib/{name}" if filetype != 'bias' else ''
    clobber_command = f"--clobber-outputs --extend-run --skip-existing \\\n" if clobber else ''

    if filetype != 'sky':
        checkNoDataCommand = f"-c cp{filetype.title()}Combine:checkNoData=False \\\n" if checkNoData == False else ''
    else:
        checkNoDataCommand = ''
    
    # Generate the command
    command = (
        f"export {filetype.upper()}EXPS=\"{exp_id_str}\"\n"
        f"LOGFILE={log_path}; \\\n"
        f"date | tee $LOGFILE; \\\n"
        f"pipetask --long-log run --register-dataset-types -j {njobs} \\\n"
        f"-b {repo_path} --instrument lsst.obs.subaru.HyperSuprimeCam \\\n"
        f"-i HSC/raw/all,HSC/calib,HSC/calib/unbounded{calib_collection},HSC/calib/curated/19700101T000000Z \\\n"
        f"-o {calib_output} \\\n"
        f"-p {pipeline_path} \\\n"
        f"-d \"instrument='HSC' AND exposure IN ${filetype.upper()}EXPS AND detector!=9\" \\\n"
        f"{clobber_command}"
        f"{checkNoDataCommand}"
        f"2>&1 | tee -a $LOGFILE; \\\n"
        f"date | tee -a $LOGFILE"
        f"\\\n"
        "\n"
        f"butler certify-calibrations $REPO HSC/calib/{name}/{filetype} HSC/calib/{name} {filetype} --begin-date 2016-01-01T00:00:00 --end-date 2024-06-30T23:59:59"
        "\n"
    )

    print(f'######## Generating {filetype} calibration command... ##########')
    return command

def reduction_command(name, expid_dict, step="step1", tracts=None, njobs=36, clobber=False):
    """
    Generate shell commands for redunction processing, from step1 to step3a.

    Args:
        name (str): Galaxy name.
        exp_ids (list of int): List of exposure IDs for the calibration type.
        filetype (str): Type of calibration data (e.g., 'bias', 'flat', 'dark'). Default is 'bias'.
        log_file_suffix (str): Suffix for the log file name (default is filetype).

    Returns:
        str: Shell script as a string.
    """
    if step == 'step3a' and tracts is None:
        raise ValueError('Must provide tracts for step3a')
    # Set default log file suffix
    log_file_suffix = step

    # Generate the exposure ID string
    filetype = 'science'
    exp_ids = [exp_id for start, end in expid_dict[filetype] for exp_id in range(start, end + 1) if exp_id % 2 == 0]
    exp_id_str = f"({', '.join(map(str, exp_ids))})"

    # Define paths and variables
    log_path = f"$LOGDIR/{name}_{log_file_suffix}.log"
    repo_path = "$REPO"
    OUTPUT = f"HSC/runs/{name}"

    pipeline_path_dict = {'step1': "$DRP_PIPE_DIR/pipelines/HSC/DRP-RC2.yaml#step1",
                          'step2a': "$DRP_PIPE_DIR/pipelines/HSC/DRP-RC2.yaml#step2a",
                          'step2b': "$REPO/pipelines/step2b.yaml#step2b", 
                          'step2c': "$REPO/pipelines/step2ce.yaml#step2c",
                          'step2e': "$REPO/pipelines/step2ce.yaml#step2e", 
                          'step3a': "$REPO/pipelines/step3a.yaml#step3a"}
    pipeline_path = pipeline_path_dict[step]

    collections = f'HSC/raw/all,HSC/calib/{name},HSC/calib,HSC/calib/unbounded,refcats/gen2,skymaps'
    clobber_command = f"--clobber-outputs --extend-run --skip-existing \\\n" if clobber else ''

    BADCCDS = "(9, 33, 104, 105, 106, 107, 108, 109, 110, 111)" if step == 'step2a' else "(9, 33)"

    
    step2b_command = ("-d \"instrument='HSC' AND skymap='hsc_rings_v1'\" \\\n"
                      "-c jointcal:connections.astrometryRefCat='gaia_dr3_20230707' \\\n"
                      "-c jointcal:maxAstrometrySteps=50 \\\n"
                      "-c jointcal:doPhotometry=True \\\n") if step == 'step2b' else ""

    step2c_command = ("-c updateVisitSummary:wcs_provider='input_summary' \\\n"
                      "-c updateVisitSummary:photo_calib_provider='input_summary' \\\n"
                      "-c updateVisitSummary:background_provider='input_summary' \\\n"
                      "-d \"instrument='HSC' AND skymap='hsc_rings_v1' AND visit IN $SCIEXPS\" \\\n") if step == 'step2c' else ""

    step2e_command = "-d \"instrument='HSC' AND skymap='hsc_rings_v1'\" \\\n" if step == 'step2e' else ""
    step3a_command = "-d \"instrument='HSC' AND skymap='hsc_rings_v1 AND tract in $TRACTS'\" \\\n" if step == 'step3a' else ""
    
    where = f"-d \"instrument='HSC' AND exposure IN $SCIEXPS AND detector NOT IN $BADCCDS\" \\\n" if step in ['step1', 'step2a'] else ""
    
    TRACTS = f"TRACTS=\"{tracts}\"; \\\n" if (tracts is not None and step == 'step3a') else ""
    
    # Generate the command
    command = (
        f"SCIEXPS=\"{exp_id_str}\"; \\\n"
        f"LOGFILE={log_path}; \\\n"
        f"BADCCDS=\"{BADCCDS}\"; \\\n"
        f"{TRACTS}"
        f"date | tee $LOGFILE; \\\n"
        f"pipetask --long-log run --register-dataset-types -j {njobs} \\\n"
        f"-b {repo_path} --instrument lsst.obs.subaru.HyperSuprimeCam \\\n"
        f"-i {collections} \\\n"
        f"-o {OUTPUT} \\\n"
        f"-p {pipeline_path} \\\n"
        f"{where}"
        f"{step2b_command}"
        f"{step2c_command}"
        f"{step2e_command}"
        f"{step3a_command}"
        f"{clobber_command}"
        f"2>&1 | tee -a $LOGFILE; \\\n"
        f"date | tee -a $LOGFILE"
        f"\\\n"
    )

    print(f'######## ...{step} command... #########')
    return command

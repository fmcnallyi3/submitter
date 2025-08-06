#!/usr/bin/env python

#############################################################################
## A python wrapper for submitting executables. To use, import pysubmit    ##
## and call in a python script.                                            ##
##                                                                         ##
## Arguments:                                                              ##
## - executable - string with the executable argument                        ##
##    - ex: '/home/user/test.py -a [argument]'                             ##
## - jobID - title for the job in condor and exec/out/log/error files      ##
## - outdir - location for exec/out/log/error files                        ##
## - test - run executable off cluster as a test                           ##
## - sublines - location for additional submission options                 ##
##    - replace eventually with actual options (like 'universe')           ##
#############################################################################

import subprocess
import stat, random
from pathlib import Path


def pysubmit(executable, jobID=None, outdir=None,
              test=False, universe='vanilla',
              header=['#!/bin/bash'],
              notification='never', sublines=None):

    # Default storage for exec/out/log/error files is in this directory
    if outdir == None:
        outdir = Path(__file__).parent.resolve()

    # Option for testing off cluster
    if test:
        result = subprocess.run(executable.split(' '))
        # Checking for errors
        if result.returncode != 0:
            print(f'Error: {result.stderr}')
        return

    # Default naming for jobIDs if not specified
    if jobID == None:
        jobID = f'npx4-{random.randint(0,999999):06d}' 
        print(jobID)

    # Ensure output directories exist
    outdir.mkdir(parents=True, exist_ok=True)
    for condor_out in ['execs','logs','out','error']:
        condor_dir = outdir / f'npx4-{condor_out}'
        condor_dir.mkdir(exist_ok=True)

    # Create execution script
    exelines = header + [
        'date',
        'hostname',
        '',
        f'{executable}',
        '',
        'date',
        'echo "Fin"'
    ]
    exelines = [l+'\n' for l in exelines]

    exe_out = Path(f'{outdir}/npx4-execs/{jobID}.sh')
    with open(exe_out, 'w') as f:
        f.writelines(exelines)

    # Make file executable
    mode = exe_out.stat().st_mode
    exe_out.chmod(mode | stat.S_IEXEC)

    # Condor submission script
    lines = [
        f'universe = {universe}',
        f'executable = {outdir}/npx4-execs/{jobID}.sh',
        f'log = {outdir}/npx4-logs/{jobID}.log',
        f'output = {outdir}/npx4-out/{jobID}.out',
        f'error = {outdir}/npx4-error/{jobID}.error',
        f'notification = {notification}',
        'queue'
    ]
    lines = [l+'\n' for l in lines]

    # Option for additional lines to submission script
    if sublines != None:
        for l in sublines:
            lines.insert(-1, f'{l}\n')

    # Submission script for condor
    condor_script = f'{outdir}/2sub.sub'
    with open(condor_script, 'w') as f:
        f.writelines(lines)

    # Submit
    result = subprocess.run(['condor_submit',condor_script])



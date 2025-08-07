#!/usr/bin/env python

#######################################################
# Runs through error files, returns a list of files where the process ran
# correctly, and can remove files associated with good runs
#######################################################

import sys, argparse
from pathlib import Path

from get_time import get_time


def main():

    parser = argparse.ArgumentParser(
            description='Scan npx4 files and remove good runs')
    parser.add_argument('--npxdir', dest='npxdir',
            default=None,
            help='Location of npx4 folders')
    parser.add_argument('--purge', dest='purge',
            default=False, action='store_true',
            help='Remove all npx4 files')
    parser.add_argument('--strict', dest='strict',
            default=False, action='store_true',
            help='ONLY look at lines with the word "Error" in them')
    parser.add_argument('--rerun', dest='rerun',
            default=False, action='store_true',
            help='Rerun bad run files')
    parser.add_argument('--orphans', dest='orphans',
            default=False, action='store_true',
            help='Mark orphaned executable files as bad')
    args = parser.parse_args()

    # Default location for npx4 files is in same directory
    current = Path(__file__).parent.resolve()
    if args.npxdir == None:
        args.npxdir = str(current)

    # List of all npx4 files and jobIDs
    files = Path(args.npxdir).glob('npx4-*/*')
    jobIDs = sorted(set([f.stem for f in files]))

    # Exit if no files found
    if len(jobIDs) == 0:
        sys.exit(f'No npx4 files found in {args.npxdir}. Quitting...')

    # Early opportunity to back out on rerun command
    if args.rerun:
        yn = input('Rerun all bad and held executables? [y/N]: ')
        if yn != 'y':
            sys.exit('Rerun canceled. Aborting...')

    # Option to purge all npx4 files
    if args.purge:
        yn = input(f'Purge all {len(files)} npx4 files? [y/N]: ')
        if yn == 'y':
            for f in files:
                f.unlink()
            sys.exit(f'{len(files)} successfully deleted')
        else:
            sys.exit('Purge canceled')

    job_status = ['good','orphaned','held','aborted','bad']
    status = {status_type:[] for status_type in job_status}
    tList = []
    t = 0.

    for jobID in jobIDs:

        # Unstarted jobs will not have output files
        exe = Path(f'{args.npxdir}/npx4-execs/{jobID}.sh')
        out = Path(f'{args.npxdir}/npx4-out/{jobID}.out')
        err = Path(f'{args.npxdir}/npx4-error/{jobID}.error')
        log = Path(f'{args.npxdir}/npx4-logs/{jobID}.log')

        # All jobs (started or not) should have an exe and a log file
        if not exe.is_file() or not log.is_file():
            status['orphaned'].append(jobID)
            continue

        # Handle user-aborted cases separately
        with open(log, 'r') as f:
            lines = f.readlines()
        if any(['Job was aborted' in l for l in lines]) and \
                not any(['Job was held' in l for l in lines]):
            status['aborted'].append(jobID)
            continue

        # Jobs that have not yet begun won't have a .out file
        if not out.is_file():
            continue

        # Jobs that have started should have an err file
        if not err.is_file():
            status['orphaned'].append(jobID)
            continue

        # Mark held jobs
        with open(log, 'r') as f:
            lines = f.readlines()
        log_messages = ['Job was held', 'Job was evicted']
        if any([m in l for l in lines for m in log_messages]) and \
                not any(['Normal termination' in l for l in lines]):
            if any(['Job was aborted' in l for l in lines]):
                status['held'].append(jobID)
            continue

        # Make sure there's some text in the output file
        with open(out, 'r') as f:
            lines = f.readlines()
        if lines == []:
            print('{} is empty!'.format(out))
            continue

        # Check the error file for any lines that aren't harmless
        is_good = good_file(err, args.strict)

        # Calculate run time from log file
        t0 = get_time(log)

        # Jobs with a runtime of None are still running
        if t0 == None:
            continue

        # Finished files get added to good or bad run lists
        if is_good:
            status['good'].append(jobID)
            t += t0
            tList += [t0]
        else:
            status['bad'].append(jobID)


    # jobIDs without an assigned status are running
    nFinished = sum([len(s_list) for s_type, s_list in status.items()])
    nRunning = len(jobIDs) - nFinished
    if nRunning != 0:
        print(f'Running ({nRunning} file(s)):')
        for jobID in jobIDs:
            if all([jobID not in s_list for s_type, s_list in status.items()]):
                print(f'  - {jobID}')
        print('')

    # Print information on all other jobs, sorted by status
    for s_type, s_list in status.items():

        if len(s_list) != 0:
            print(f'{s_type.capitalize()} runs ({len(s_list)} file(s)):')
            for jobID in s_list:
                print(f' --- {jobID} ---')

                # Option to resubmit bad, held, or user-aborted runs
                if s_type in ['bad','held','aborted'] and args.rerun:
                    resubmit(args.npxdir, jobID)

            # Print basic runtime information for good runs
            if s_type == 'good':
                print(f'  Average time per job: {t/len(s_list):.01f} seconds')
                tList.sort()
                print(f'    Min time: {tList[0]:.01f}')
                print(f'    Max time: {tList[-1]:.01f}')

        print('')


    # Option to remove good runs
    if len(status['good']) != 0 and not args.rerun:
        yn = input('Do you want to remove the good runs? [y/N]: ')
        if yn == 'y':
            for jobID in status['good']:
                rm_files = Path(args.npxdir).glob('npx4-*/{jobID}.*')
                for f in rmFiles:
                    f.unlink()



def resubmit(npxdir, jobID):

    ## NOTE: Assumes the last jobs you submitted involved the same options
    ## (example: request_memory), and that the sub script is '2sub.sub'

    # Condor submission script
    d = {}
    d['executable'] = f'{npxdir}/npx4-execs/{jobID}.sh\n'
    d['log'] = f'{npxdir}/npx4-logs/{jobID}.log\n'
    d['output'] = f'{npxdir}/npx4-out/{jobID}.out\n'
    d['error'] = f'{npxdir}/npx4-error/{jobID}.error\n'

    # Read in previous submission script
    with open('2sub.sub','r') as f:
        lines = f.readlines()

    for key, value in d.items():

        # Replace exe/log/out/err filenames
        for i, l in enumerate(lines):
            if l.split(' ')[0].lower() == key:
                lines[i] = f'{key} = {value}'

        # Remove all log, output, and error files before resubmitting
        file_path = Path(value.strip())
        if file_path.is_file() and 'exec' not in key:
            print(f'File {value} found! Removing...')
            file_path.unlink()

    with open('2sub.sub', 'w') as f:
        f.writelines(lines)

    result = subprocess.run(['condor_submit','2sub.sub'])



# List of any safe errors you want to ignore
def getErrorList():
    safeErrors = []
    # General safe errors
    safeErrors.append('Error in <TSystem::ExpandFileName>: input: $HOME/.root.mimes, output: $HOME/.root.mimes')
    # Time-scrambling errors assoc. w/ file already exists?
    safeErrors.append('FITS error')
    safeErrors.append("terminate called after throwing an instance of 'Message_error'")
    safeErrors.append("/cvmfs/icecube.opensciencegrid.org/py3-v4.1.1/RHEL_7_x86_64/lib/python3.7/site-packages/astropy/config/configuration.py:532: ConfigurationMissingWarning: Configuration defaults will be used due to OSError:Could not find unix home directory to search for astropy config dir on None")
    return safeErrors



def good_file(filename, strict):

    # Check the error file for any lines that aren't harmless
    with open(filename, 'r') as f:
        err = f.readlines()
        err = [l.strip() for l in err]

    # Remove non-error-related I3Tray output
    oks = ['NOTICE','INFO','WARN']
    err = [l for l in err if l.split(' ')[0] not in oks]

    # Option: ignore all lines that don't explicity list as "Error"
    problem_words = ['error','Error','Break']
    if strict:
        err = [l for l in err if any([e in l for e in problem_words])]

    safeErrors = getErrorList()
    if any([l not in safeErrors for l in err]):
        return False

    return True



if __name__ == "__main__":

    main()

#!/usr/bin/env python

import re
from datetime import datetime
from pathlib import Path

def extract_time(line, end=False):

    date_format = re.compile('\d{4}-\d{2}-\d{2}')
    time_format = re.compile('\d{2}:\d{2}:\d{2}')
    d0 = date_format.findall(line)[0]
    t0 = time_format.findall(line)[0]

    # Extract month, day, and time
    yy, mm, dd = int(d0[:4]), int(d0[5:7]), int(d0[8:])
    hrs, min, sec = int(t0[:2]), int(t0[3:5]), int(t0[6:])

    t = datetime(yy, mm, dd, hrs, min, sec)

    return t
    
    

def getTime(filename):

    if not Path(filename).is_file():
        print(f'Log file {str(filename)} not found')
        return None

    with open(filename, 'r') as f:
        lines = f.readlines()

    start_lines = [line for line in lines if 'Job executing on host:' in line]
    end_lines = [line for line in lines if 'Job terminated' in line or \
                'Job was aborted' in line]
    if start_lines == [] or end_lines == []:
        return None

    start_line = start_lines[0]
    end_line = end_lines[0]       # Time for last occurrence appears broken
    start_time = extract_time(start_line)
    end_time = extract_time(end_line, end=True)
    dt = end_time - start_time
    dt = dt.total_seconds()

    return dt


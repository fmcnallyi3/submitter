This project is designed to help with submitting and monitoring jobs via
condor. It contains the following files:
- `cleaner.py` : checks npx4 executable, log, output, and error files. Primary
  tool for batch assessment and clean up.
- `get_time.py` : calculates job runtime from log file
- `pysubmit.py` : python wrapper script for submission of executable to cluster
- `README.md` : this file

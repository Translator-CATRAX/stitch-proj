# stitch
Some tools for building a Translator KG (experimental! not yet finished!) 

# Requirements
- CPython 3.12, available in your path as `python3.12`
- Tested on Ubuntu 24.04 (on x86_64) and MacOS 14.6.1 (on ARM64)
- At least 32 GiB of system memory
- At least 150 GiB of free file system storage space

# Ubuntu notes
- Make sure to install Ubuntu package `python3.12-venv`

# Setup
- `ssh ubuntu@stitch.rtx.ai` (if running in AWS); else just create a new `bash` session
- `git clone https://github.com/Translator-CATRAX/stitch.git`
- `cd stitch`
- `python3.12 -m venv venv`
- `source venv/bin/activate`
- `pip3 install -r requirements.txt`

# How to run the Babel ingest

- `ssh ubuntu@stitch.rtx.ai` (if running in AWS); else just create a new `bash` session
- `cd stitch`
- `screen`
- `source venv/bin/activate`
- `python3.12 -u ingest_babel.py > ingest_babel.log 2>&1`
- `ctrl-X D` (to exit the screen session)
- `tail -f ingest_babel.log` (so you can watch progress)
- In another terminal session, watch memory usage using `top`

# Running the mypy checks:
These checks should be run before any commit to `ingest_babel.py`:
```
mypy --ignore-missing-imports ingest_babel.py
```

# Schema

![stitch Babel sqlite3 database schema](schema.png)


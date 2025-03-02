# stitch
Some tools for building a Translator BigKG (experimental!) 

# Requirements
- CPython 3.12
- Tested on Ubuntu 24.04 (on x86_64) and MacOS 14.6.1 (on ARM64)

# Ubuntu notes
- Make sure to install Ubuntu package `python3.12-venv`

# How to run the Babel ingest
- `ssh ubuntu@stitch.rtx.ai`
- `cd stitch`
- `screen`
- `source venv/bin/activate`
- `python3.12 -u ingest_babel.py > ingest_babel.log 2>&1`
- `ctrl-X D` (to exit the screen session)
- `tail -f ingest_babel.log` (so you can watch progress)
- In another terminal session, watch memory usage using `top`

# Running the mypy checks:

```
mypy --ignore-missing-imports ingest_babel.py
```

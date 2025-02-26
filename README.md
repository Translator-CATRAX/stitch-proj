# stitch
Some tools for building a Translator BigKG (experimental!) 

# Requirements
CPython 3.12
So far, only tested on MacOS Sonoma on ARM64

# Ubuntu/ARM64 notes
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

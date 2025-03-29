# stitch
Some tools for building a Translator KG (experimental! not yet finished!) 

# Requirements
- CPython 3.12, available in your path as `python3.12`
- Ubuntu or Macos
- At least 32 GiB of system memory
- At least 500 GiB of free file system storage space (400 GiB might be sufficient but I am not certain)

# Systems tested

## AWS 
- Ubuntu 24.04
- `c7g.4xlarge` instance (Graviton3 processor), 32 GiB of memory
- `gp3` root volume (24 GiB)
- `io1` data volume (400 GiB); mounted with `noatime` (standard 3000 IOPS and 125 MiBS)
- The following packages `apt` installed: 
  - `sqlite3`
  - `build-essential` 
  - `gcc` 
  - `g++` 
  - `make` 
  - `libffi-dev` 
  - `libssl-dev` 
  - `zlib1g-dev` 
  - `libbz2-dev` 
  - `libreadline-dev` 
  - `libsqlite3-dev` 
  - `libncursesw5-dev` 
  - `tk-dev` 
  - `libgdbm-dev` 
  - `libnss3-dev` 
  - `liblzma-dev`
  - `uuid-dev`
  - `python3-dev`
  - `gfortran` 
  - `libopenblas-dev` 
  - `liblapack-dev`
  - `libfreetype6-dev` 
  - `libpng-dev` 
  - `libjpeg-dev`
  - `libtiff-dev` 
  - `libffi-dev` 
  - `liblzma-dev`
  - `pkg-config` 
  - `cmake`
  - `python3.12-venv`
- CPython, Numpy, and Pandas compiled locally using gcc/g++ with the following CFLAGS:
```-mcpu=neoverse-v1 -mtune=neoverse-v1 -march=armv8.4-a+crypto -O3 -pipe```

## MacOS
For reasons I don't fully understand, `ingest_babel.py` runs quite fast on the M1 Max, compared to
the Graviton3 processor. I've tested on the following MacOS system:
- MacOS 14.6.1
- Apple M1 Max processor, 64 GiB of memory
- Apple SSD AP2048R Media SSD (2 TiB)
- `python3.12` installed via Homebrew
- `openblas` installed via Homebrew

# Setup
- `ssh ubuntu@stitch2.rtx.ai` (if running in AWS); else just create a new `bash` session
- `git clone https://github.com/Translator-CATRAX/stitch.git`
- `cd stitch`
- `python3.12 -m venv venv`
- `source venv/bin/activate`
- `pip3 install -r requirements.txt`

# How to run the Babel ingest in AWS
- `ssh ubuntu@stitch2.rtx.ai` (if running in AWS); else just create a new `bash` session
- `cd stitch`
- `screen`
- `source venv/bin/activate`
- `./run-ingest-aws.sh`
- `ctrl-X D` (to exit the screen session)
- `tail -f ingest-babel.log` (so you can watch progress)
- In another terminal session, watch memory usage using `top`

After approximately 60 hours, the ingest script should save the database as a file
`/home/ubuntu/stitch/babel.sqlite`.

# Running the mypy checks:
These checks should be run before any commit to `ingest_babel.py`:
```
mypy --ignore-missing-imports ingest_babel.py
```

# Schema
This schema diagram was generated using DbVisualizer Free version 24.3.3.
![stitch Babel sqlite3 database schema](schema.png)

# How to use

1. When you create a connection, make sure to set `PRAGMA foreign_keys = ON;`


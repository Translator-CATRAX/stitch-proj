# stitch-proj
Some tools for building a Translator BigKG. This software project is experimental and unfinished.

# Introduction 
There are two types of intended users for this software suite, someone who is
tasked with ingesting the
[Babel concept identifier normalization database](https://github.com/TranslatorSRI/Babel)
into a local sqlite databse (an "ingester") and someone developing a
application, such as a BigKG build system, that wants to programmatically query
a local Babel sqlite database for node normalization, etc. ("querier"). The
"ingester" type user will need to rea this entire README document, in order to
be able to set up and run the `ingest_babel.py` program to carry out an ingest
of Babel into a local sqlite database. The "querier" type user can skip over the
sections of this document that discuss ingesting Babel, and focus on the
sections about downloading the pre-built Babel sqlite database from S3 and using
the `local_babel.py` python module that provides functions for querying the
local Babel sqlite database.

# Tools
- `ingest_babel.py`: downloads and ingests the Babel concept identifier synonymization database into a local sqlite3 relational database
- `local_babel.py`: functions for querying the local Babel sqlite database
- `row_counts.py`: a script that prints out the row counts of the tables in the local Babel sqlite database

# Requirements
- CPython 3.12, which needs to be available in your path as `python3.12`, with the `venv` library installed and in the python path
- At least 32 GiB of system memory
- Sufficient disk space in wherever filesystem hosts your `stitch-proj` directory, which will depend on your use-case:
  - To build `babel.sqlite`, at least 600 GiB of free file system storage space (usage transiently spikes to ~522 GiB and then the final database size is ~172 GiB).
  - To use a local `babel.sqlite` in your application, 200 GiB of free system storage space to store the sqlite file.
- Linux or MacOS (this software has not been tested on Windows; see "Systems on which this software has been tested").
- If you want to download the pre-built Babel sqlite database file, you will need to have `curl` or `wget` installed.
- Optionally, you can install `sqlite3_analyzer`, if you want to obtain detailed database statistics (see instructions below in this page).

# Systems on which this software has been tested
The `stitch-proj` project's module `ingest_babel.py` has been tested in three compute environments:

## Ubuntu/Xeon
- We have tested a full run of `ingest_babel.py` on this system ([release `babel-sqlite-20250331`](https://github.com/Translator-CATRAX/stitch-proj/releases/tag/babel-20250331)). This instance has instance name `stitch2.rtx.ai` and is in the `us-west-1` AWS region.
- Ubuntu 24.04
- `i4i.2xlarge` instance (Intel Xeon 8375C processor, which is x86_64 architecture), 64 GiB of memory
- `gp3` root volume (500 GiB)
- `Nitro SSD` volume (1.7 TiB)

## Ubuntu/Graviton 
- We have tested a full run of `ingest_babel.py` on this system ([release `babel-sqlite-20250123`](https://github.com/Translator-CATRAX/stitch-proj/releases/tag/babel-20250123)).
- Ubuntu 24.04
- `c7g.4xlarge` instance (Graviton3 processor, which is ARM64 architecture), 32 GiB of memory
- `gp3` root volume (800 GiB)
- CPython, Numpy, and Pandas were compiled locally using gcc/g++ with the following CFLAGS:
```-mcpu=neoverse-v1 -mtune=neoverse-v1 -march=armv8.4-a+crypto -O3 -pipe```
- To enable local compilation of CPython, Numpy, and Pandas, the following packages were `apt` installed: `sqlite3`, `build-essential`, `gcc`, `g++`, `make`, `libffi-dev`, `libssl-dev`, `zlib1g-dev`, `libbz2-dev`, `libreadline-dev`, `libsqlite3-dev`, `libncursesw5-dev`, `tk-dev`, `libgdbm-dev`, `libnss3-dev`, `liblzma-dev`, `uuid-dev`, `python3-dev`, `gfortran`, `libopenblas-dev`, `liblapack-dev`, `libfreetype6-dev`, `libpng-dev`, `libjpeg-dev`, `libtiff-dev`, `libffi-dev`, `liblzma-dev`, `pkg-config`, `cmake`, `python3.12-venv`.

## MacOS/Apple Silicon
- We have tested only _partial_ ingests of Babel on this system type. For reasons 
I don't fully understand, `ingest_babel.py` runs quite fast on the M1 Max, compared to
the Graviton3 processor. I've tested on the following MacOS system:
- MacOS 14.6.1
- Apple M1 Max processor, 64 GiB of memory
- Apple SSD AP2048R Media SSD (2 TiB)
- `python3.12` installed via Homebrew
- `openblas` installed via Homebrew

# Python distribution package requirements 
All external PyPI distribution package requirements for the `stitch-proj` project are listed in the
[`requirements.txt`](https://github.com/Translator-CATRAX/stitch-proj/blob/main/requirements.txt) file.  
The `run-checks.sh` script (see section "Running
the type checks, lint checks, ..." below) depends on the packages `pytest`,
`ruff`, `vulture`, and `pylint`.  For a "querying" type user that is just using
`local_babel.py`, only three PyPI distribution packages are needed, `requests`,
`numpy`, and the Biolink Model Toolkit (`bmt`). Additionally, for an "ingester"
type user who wants to run `ingest_babel.py` to build a local Babel sqlite
database from scratch, the PyPI packages `pandas`, `ray`, and
`htmllistparse` are needed. The `requirements.txt` file contains
the full set of dependencies.

# Setup of a python virtualenv for using the `stich` software
You can just run
```
cd stitch-proj
./run-setup-venv.sh
```
Or if you are using AWS,
- `ssh ubuntu@stitch2.rtx.ai` (if running in AWS); else just create a new `bash` session
- `git clone https://github.com/Translator-CATRAX/stitch-proj.git`
- `cd stitch-proj` (this is the directory that contains `requirements.txt`)
- `./run-setup-venv.sh`
The last step above (i.e., the `pip3 install -e .` step) sets up some symbolic
links within your virtualenv, so that `stitchutils` can be imported
without manipulating the PYTHONPATH, no matter what the current working
directory is. You will need this in order for the unit test module
`tests/test_ingest_babel.py` to run successfully.

# How to run the `stich-proj` Babel sqlite ingest in AWS
- `ssh ubuntu@stitch2.rtx.ai` (if running in AWS); else just create a new `bash` session
- `cd stitch-proj`
- `screen` (to enter a screen session)
- `./instance-memory-tracker.sh`
- `ctrl-X D` (to exit the screen session)
- `screen` (to enter a second screen session)
- `./run-ingest-aws.sh`
- `ctrl-X D` (to exit the second screen session)
- `tail -f ingest-babel.log` (so you can watch progress)
- In another terminal session, watch memory usage using `top`

After approximately 28 hours, the ingest script should save the database as a file
`/home/ubuntu/stitch-proj/babel.sqlite`; as of the March 31, 2025 release of Babel, the
`babel.sqlite` file produced by the `ingest_babel.py` script is 172 GiB.

# What if you don't want to use `run-ingest-aws.sh`, for ingesting Babel?
If you decide to run `ingest_babel.py` by invoking it directly from
the command-line (rather than by using the `run-ingest-aws.sh` script), you will
want to ensure that whatever location you specify (or, alternatively, the
default location you opt to leave in place) for the `ingest_babel.py` temporary
file directory will have at least 600 GiB of free space available (although upon
script completion, `ingest_babel.py` will not need any temp directory space). In
most cases, the easiest way to ensure this is to specify, in calling
`ingest_babel.py`, the location that you choose for a temporary file directory
using the `--temp-dir` command-line option, and further, to specify a temporary
file directory location that is _in the same filesystem_ as the location where
you are configuring `ingest_babel.py` to output the Babel sqlite file. This way,
the space on the filesystem is "shared" between the temp directory and the final
output database. The `run-ingest-aws.sh` script takes care of this, in an
idempotent way, by creating a local temp dir and then configuring
`ingest_babel.py` to use that temp dir (and ensuring that the final output Babel
sqlite file goes into the same filesystem).

# Downloading a pre-built Babel sqlite database file
[`babel-20250331.sqlite`](https://rtx-kg2-public.s3.us-west-2.amazonaws.com/babel-20250331.sqlite)
(173 GiB) is available for download from AWS S3.  For details and an MD5
checksum hash, see the (Releases
page)[https://github.com/Translator-CATRAX/stitch-proj/releases] for the stich
project. You will need to download (or, alternatively, build from scratch using
`ingest_babel.py`) this file in order to be able to run the unit test 
suite.

# The local Babel sqlite database schema
This schema diagram was generated using [DbVisualizer](https://www.dbvis.com) Free version 24.3.3.
![stitch Babel sqlite3 database schema with conflation](schema.png)

In the `cliques` table, the combination of columns `primary_identifier_id` and
`type_id` are unique, as confirmed by this SQL query returning no rows:
```
sqlite> SELECT primary_identifier_id, type_id, COUNT(*) as count
   ...> FROM cliques
   ...> GROUP BY primary_identifier_id, type_id
   ...> HAVING COUNT(*) > 1 LIMIT 10;
```
In contrast, the column `primary_identifier_id` on the `cliques` table by itself
is not unique; there can be more than one clique with the same
`primary_identifier_id` and different `type_id` values.  In theory, I should
probably add a two-column uniqueness constraint to the `cliques` table, but I
have not yet done so.  See issue 16:
https://github.com/Translator-CATRAX/stitch-proj/issues/16

# Analyzing the local Babel sqlite database
Consider installing and compiling `sqlite3_analyzer`, which is available
from the [sqlite software project area on GitHub](https://github.com/sqlite/sqlite).
On Ubuntu, you can just perform the following steps to have
`sqlite3_analyzer` available in `/usr/local/bin`:
```
cd stitch-prod
git clone https://github.com/sqlite/sqlite.git
cd sqlite
./configure --prefix=/usr/local
make sqlite3_analyzer
sudo cp sqlite3_analyzer /usr/local/bin
sudo chmod a+x /usr/local/bin/sqlite3_analyzer
```
On MacOS, you can just use Homebrew to install `sqlite3_analyzer`:
```
brew install sqlite-analyzer
```
which will install the program in `/opt/homebrew/bin/sqlite3_analyzer`.

One analyzes the database like this:
```
sqlite3_analyzer babel.sqlite > babel-sqlite-analysis.txt
```
The analysis should take less than an hour.

# How to use the local Babel sqlite database
For now, see the module `tests/test_local_babel.py` for examples.

# Setting up local Babel sqlite database so you can run the unit tests:
First, download `babel-20250331.sqlite` from S3 as described above, 
and ensure that in the top-level `stitch-proj` directory, there is a symbolic link `db` or a subdirectory `db`
such that if the current working directory is the top-level `stitch-proj` directory,
the relative path `db/babel-2025331.sqlite` can open the database file.
Something like this should do it:
```
cd stitch-proj
mkdir -p db
curl -s -L https://rtx-kg2-public.s3.us-west-2.amazonaws.com/babel-20250331.sqlite > \
    db/babel-20250331.sqlite
```

# Running the type checks, lint checks, dead code checks, and unit tests:
These checks should be run before any commit:
```
cd stitch-proj
./run-checks.sh
```
which will run type checks (using `mypy`), lint checks (using `ruff`),
dead code tests (using `vulture`), and unit tests (using `pytest`).

# How to run just the unit test suite
First, you need to make sure that underneath the top-level
"stich" directory, there is a subdirectory "db" containing 
the `babel-20250331.sqlite` file (see section 
"Downloading a pre-built Babel sqlite database file").
Then you can run the unit test suite, like this:
```
cd stitch-proj
venv/bin/pytest -v
```
You should _not_ try to run the unit tests like this:
```
cd stitch-proj/tests
../venv/bin/pytest -v
```
because if you do it that way, the `test_local_babel.py` module
won't be able to find the sqlite database that it depends on.

# How to run the integration tests of ingest_babel.py
Running all three integration tests may take up to an hour:
```
cd stitch-proj
./run-integration-tests.sh
```

# How to regenerate the schema diagram

Use the `ingest_babel.py` script to generate the `ddl.sql` file as follows: 
```
cd stitch-proj
venv/bin/python3 stitch/ingest_babel.sql --print-ddl --dry-run 2>ddl.sql 
``` 
On macOS, run the DbVisualizer application (free version
24.3.3). Under the "File" menu select "Open File...", then navigate to the new
`ddl.sql` file.  In the treeview control under "SQLite" on the left, open
"Schema" and click on "Tables". In the "Tables" view in the main application
pane, click on the "References" tab. Use macOS system screen-capture tool to
obtain a PNG of the schema diagram.

# To print out the table row counts:

Run these steps:
```
cd stitch-proj
venv/bin/python3 stitch/row_counts.py babel.sqlite
```

# Special instructions for running `ingest_babel.py` in an `i4i.2xlarge` instance
The first time you start the instance:
```
ln -s /mnt/localssd /home/ubuntu/stitch-proj
```
Then, every time you start the instance:
```
sudo mkdir -p /mnt/localssd
sudo lsblk
```
The last command (`sudo lsblk`) should provide the name of the 1.7 TiB local SSD device,
like `/dev/nvme1n1`. Use that in place of "`/dev/nvme1n1`" below.
```
sudo mkfs.ext4 /dev/nvme1n1
sudo mount /dev/nvme1n1 /mnt/localssd
```

# How to cite Babel in a publication
Please see the [Babel `CITATION.cff` file](https://github.com/TranslatorSRI/Babel/blob/master/CITATION.cff).

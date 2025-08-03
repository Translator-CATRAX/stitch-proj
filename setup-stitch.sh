#!/usr/bin/env bash
# setup-stitch.sh:  setup the environment for building the stitch KG2pre->KG2c processor
# Copyright 2025 Stephen A. Ramsey <stephen.ramsey@oregonstate.edu>

# Options:
# ./setup-stitch.sh

set -o nounset -o pipefail -o errexit

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    echo Usage: "$0" 
    exit 2
fi

# Usage: setup-stitch.sh

## setup the shell variables for various directories
config_dir=`dirname "$0"`
source ${config_dir}/master-config.shinc

mkdir -p ${BUILD_DIR}
setup_log_file=${BUILD_DIR}/setup-stitch.log
touch ${setup_log_file}

{
echo "================= starting setup-stitch.sh ================="
date

echo `hostname`

## sym-link into RTX-KG2/
if [ ! -L ${CODE_DIR} ]; then
    ln -sf ~/stitch ${CODE_DIR}
fi

## install the Linux distro packages that we need (python3-minimal is for docker installations)
sudo apt-get update

## handle weird tzdata install (this makes UTC the timezone)
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata

# install various other packages used by the build system
#  - curl is generally used for HTTP downloads
#  - wget is used by the neo4j installation script (some special "--no-check-certificate" mode)
sudo DEBIAN_FRONTEND=noninateractive apt-get install -y \
     zip \
     curl \
     wget \
     gtk-doc-tools \
     libtool \
     automake \
     git \
     libssl-dev \
     make \
     jq \
     unzip \

sudo DEBIAN_FRONTEND=noninateractive apt-get install -y awscli

# we want python3.13 (also need python3.13-dev or else pip cannot install the python package "mysqlclient")
source ${CODE_DIR}/setup-python313-with-pip3-in-ubuntu.shinc
${VENV_DIR}/bin/pip3 install -r ${CODE_DIR}/requirements.txt
} > ${setup_log_file} 2>&1

if ! ${s3_cp_cmd} s3://${s3_bucket}/test-file-do-not-delete /tmp/; then
    aws configure
else
    rm -f /tmp/test-file-do-not-delete
fi

{
${s3_cp_cmd} s3://${s3_bucket_public}/${kg2_nodes_filename} ${BUILD_DIR}/${kg2_nodes_filename}
${s3_cp_cmd} s3://${s3_bucket_public}/${kg2_edges_filename} ${BUILD_DIR}/${kg2_edges_filename}

date
echo "================= setup-stitch.sh finished ================="
} >> ${setup_log_file} 2>&1
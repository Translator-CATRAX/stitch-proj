#!/usr/bin/env bash
# setup-i4i-instance.sh
#
# Idempotently prepare an AWS `i4i.2xlarge` (or similar) instance for running
# `ingest_babel.py` against Babel: locate the local ephemeral NVMe SSD,
# format and mount it at /mnt/localssd, create the symlink
# /home/ubuntu/stitch-proj -> /mnt/localssd/stitch-proj, then (re-)clone the
# stitch-proj repo and set up its virtualenv.
#
# The local NVMe on i4i instances is *ephemeral* and is wiped on every
# stop/start, so this script is safe (and expected) to re-run on every boot.
# The symlink lives on the persistent EBS root volume and survives restarts.
#
# Steps performed (each is gated so re-running is a no-op when already done):
#   1. find the local NVMe disk (largest unmounted non-root disk, >= 1 TiB)
#   2. mkfs.ext4 + mount at /mnt/localssd  (only if not already mounted)
#   3. chown the mount to ubuntu, mkdir stitch-proj subdir
#   4. symlink ~/stitch-proj -> /mnt/localssd/stitch-proj  (only if missing)
#   5. git clone the repo into the symlink target  (only if no .git there)
#   6. run ./run-setup-venv.sh inside the clone  (only if venv/ missing)
#
# Usage (as the `ubuntu` user, with passwordless sudo):
#     ./tools/setup-i4i-instance.sh
#
# Or drop the body of this script into EC2 user-data to run automatically on
# every boot.  See the README section
# "Special instructions for running ingest_babel.py in an i4i.2xlarge instance".

set -o nounset -o pipefail -o errexit

LOCAL_MOUNT=/mnt/localssd
LINK_PATH=/home/ubuntu/stitch-proj
PROJECT_SUBDIR=stitch-proj
REPO_URL=https://github.com/Translator-CATRAX/stitch-proj.git
MIN_SSD_BYTES=$((1024 * 1024 * 1024 * 1024))   # 1 TiB floor; i4i.2xlarge has ~1.7

log() { printf '%s %s\n' "$(date '+%FT%T%z')" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

# ---- 1. locate the local NVMe SSD ----------------------------------------
# Pick the largest unmounted disk that is NOT the parent of the root device,
# subject to a >=1 TiB sanity floor.

for cmd in lsblk mkfs.ext4 mountpoint findmnt numfmt git; do
    command -v "$cmd" >/dev/null || die "required command not found: $cmd"
done

# Parent device of the root filesystem (e.g. "nvme0n1" given root on
# "/dev/nvme0n1p1").
ROOT_SOURCE=$(findmnt -no SOURCE /)
ROOT_DEV=$(lsblk -no PKNAME "${ROOT_SOURCE}" | head -n 1)
[[ -n "${ROOT_DEV}" ]] || die "could not determine root device parent from ${ROOT_SOURCE}"
log "root device parent: ${ROOT_DEV}"

# Candidates: TYPE=disk, no current mountpoint, not the root parent.
# Sort by size descending, take the first that meets the size floor.
SSD_DEV=""
while read -r name size; do
    [[ "${name}" == "${ROOT_DEV}" ]] && continue
    (( size < MIN_SSD_BYTES )) && continue
    SSD_DEV="/dev/${name}"
    log "found local SSD candidate: ${SSD_DEV} ($(numfmt --to=iec --suffix=B "${size}"))"
    break
done < <(lsblk -dn -bo NAME,SIZE,TYPE,MOUNTPOINT \
         | awk '$3=="disk" && $4=="" {print $1, $2}' \
         | sort -k2 -n -r)

[[ -n "${SSD_DEV}" ]] || die "no unmounted local disk >= $(numfmt --to=iec ${MIN_SSD_BYTES}) found"

# ---- 2. format and mount (only if not already mounted) -------------------
sudo mkdir -p "${LOCAL_MOUNT}"

if mountpoint -q "${LOCAL_MOUNT}"; then
    CURRENT_DEV=$(findmnt -no SOURCE "${LOCAL_MOUNT}")
    log "${LOCAL_MOUNT} already mounted (${CURRENT_DEV}); skipping mkfs and mount"
else
    log "formatting ${SSD_DEV} as ext4"
    sudo mkfs.ext4 -F "${SSD_DEV}"
    log "mounting ${SSD_DEV} at ${LOCAL_MOUNT}"
    sudo mount "${SSD_DEV}" "${LOCAL_MOUNT}"
fi

# ---- 3. own the mount, create the project subdir -------------------------
sudo chown ubuntu:ubuntu "${LOCAL_MOUNT}"
mkdir -p "${LOCAL_MOUNT}/${PROJECT_SUBDIR}"

# ---- 4. symlink ~/stitch-proj -> /mnt/localssd/stitch-proj (one-shot) ----
if [[ -L "${LINK_PATH}" ]]; then
    log "symlink ${LINK_PATH} already exists; leaving in place"
elif [[ -e "${LINK_PATH}" ]]; then
    die "${LINK_PATH} exists but is not a symlink; refusing to clobber"
else
    ln -s "${LOCAL_MOUNT}/${PROJECT_SUBDIR}" "${LINK_PATH}"
    log "created symlink ${LINK_PATH} -> ${LOCAL_MOUNT}/${PROJECT_SUBDIR}"
fi

# ---- 5. clone the repo onto the SSD (re-done on every restart) -----------
if [[ -d "${LINK_PATH}/.git" ]]; then
    log "${LINK_PATH} already contains a git checkout; skipping clone"
else
    log "cloning ${REPO_URL} into ${LINK_PATH}"
    git clone "${REPO_URL}" "${LINK_PATH}"
fi

# ---- 6. set up the venv (re-done on every restart, since SSD is wiped) ---
if [[ -d "${LINK_PATH}/venv" ]]; then
    log "${LINK_PATH}/venv already present; skipping run-setup-venv.sh"
else
    log "running ./run-setup-venv.sh"
    ( cd "${LINK_PATH}" && ./run-setup-venv.sh )
fi

log "done; project ready at ${LINK_PATH}"

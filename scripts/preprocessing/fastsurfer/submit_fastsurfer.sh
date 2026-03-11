#!/bin/bash

set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# =============================================================================
# submit_fastsurfer.sh
#
# description: slurm submission script for fastsurfer. processes t1-weighted
#              mri data from a bids-formatted cohort directory, producing
#              fastsurfer segmentations including surface reconstruction
#              (equivalent to FreeSurfer's recon-all).
#
#              the script:
#                1. discovers all t1w images under <cohort>/raw/
#                2. creates a flat directory of symlinks named by bids entities
#                3. submits the full cohort to fastsurfer via srun_fastsurfer.sh
#
# usage:       ./submit_fastsurfer.sh -c <cohort> -f <sif> -l <license>
#                                     -t <tools_dir> -w <work_dir>
#
# author:      hannah baumeister
# email:       hannah.baumeister[at]med.lu.se
# =============================================================================


# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------

# print a timestamped info message to stdout
info()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]  $*"; }

# print a timestamped warning to stderr (non-fatal)
warn()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN]  $*" >&2; }

# print a timestamped error message to stderr and exit
error() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; exit 1; }

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  -c <cohort>      Name of the cohort to process (must exist under PROJ_DIR)
  -f <filepath>    Path to the FastSurfer Singularity image (.sif)
  -l <filepath>    Path to the FreeSurfer license file
  -p <directory>   Root project directory (cohorts live as subdirectories)
  -t <directory>   Directory containing FastSurfer tools
                   (brun_fastsurfer.sh, srun_fastsurfer.sh, stools.sh)
  -w <directory>   Work directory for intermediate and log files
  -h               Show this help message and exit

Example:
  $(basename "$0") -c ADNI -p /to/project/directory \\
                   -f /sw/fastsurfer.sif -l ~/.license \\
                   -t /sw/fastsurfer -w /scratch/$USER
EOF
  exit 0
}


# -----------------------------------------------------------------------------
# argument parsing and validation
# -----------------------------------------------------------------------------

parse_args() {
  while getopts ":c:f:l:p:t:w:h" opt; do
    case $opt in
      c) COHORT="$OPTARG" ;;
      f) FASTSURFER_SIF="$OPTARG" ;;
      l) FREESURFER_LICENSE="$OPTARG" ;;
      p) PROJ_DIR="$OPTARG" ;;
      t) TO_FASTSURFER_TOOLS="$OPTARG" ;;
      w) WORK_DIR="$OPTARG" ;;
      h) usage ;;
      :) error "Option -$OPTARG requires an argument. Run with -h for help." ;;
      \?) error "Unknown option: -$OPTARG. Run with -h for help." ;;
    esac
  done

  # Validate that all required arguments were provided
  [[ -z "${COHORT:-}"              ]] && error "Missing required argument: -c <cohort>"
  [[ -z "${FASTSURFER_SIF:-}"      ]] && error "Missing required argument: -f <sif>"
  [[ -z "${FREESURFER_LICENSE:-}"  ]] && error "Missing required argument: -l <license>"
  [[ -z "${TO_FASTSURFER_TOOLS:-}" ]] && error "Missing required argument: -t <tools_dir>"
  [[ -z "${PROJ_DIR:-}"            ]] && error "Missing required argument: -p <proj_dir>"
  [[ -z "${WORK_DIR:-}"            ]] && error "Missing required argument: -w <work_dir>"

  # Validate that provided paths exist
  [[ ! -f "$FASTSURFER_SIF"        ]] && error "FastSurfer SIF not found: $FASTSURFER_SIF"
  [[ ! -f "$FREESURFER_LICENSE"    ]] && error "FreeSurfer license not found: $FREESURFER_LICENSE"
  [[ ! -d "$TO_FASTSURFER_TOOLS"   ]] && error "FastSurfer tools directory not found: $TO_FASTSURFER_TOOLS"
  [[ ! -d "$PROJ_DIR"              ]] && error "Project directory not found: $PROJ_DIR"
}

parse_args "$@"


# -----------------------------------------------------------------------------
# directory setup
# -----------------------------------------------------------------------------

COHORT_DIR="${PROJ_DIR}/${COHORT}"        # raw + derivatives for this cohort
COHORT_WORK_DIR="${WORK_DIR}/${COHORT}"   # intermediate files and logs for this cohort

[[ ! -d "$COHORT_DIR" ]] && error "Cohort directory not found: $COHORT_DIR"

info "Setting up work directory: ${COHORT_WORK_DIR}"
mkdir -p "${COHORT_WORK_DIR}"


# -----------------------------------------------------------------------------
# discover t1w images
# -----------------------------------------------------------------------------

IMAGE_PATH_LIST="${COHORT_WORK_DIR}/images_to_process.txt"

info "Searching for T1w images under ${COHORT_DIR}/raw/ ..."
find "${COHORT_DIR}/raw" -maxdepth 4 \
    \( -name "*t1w.nii" -o -name "*t1w.nii.gz" \) \
    | sort > "$IMAGE_PATH_LIST"

n_images=$(wc -l < "$IMAGE_PATH_LIST")
[[ "$n_images" -eq 0 ]] && error "No T1w images found under ${COHORT_DIR}/raw/"
info "Found ${n_images} T1w image(s). Image list saved to: ${IMAGE_PATH_LIST}"


# -----------------------------------------------------------------------------
# create flat symlink directory
#
# srun_fastsurfer.sh expects a flat directory where each file is named
# <sid>_t1w.nii[.gz]. We build this from the bids paths by extracting the
# sub- and ses- entities from the directory structure.
# -----------------------------------------------------------------------------

SYMLINK_DIR="${COHORT_WORK_DIR}/t1w_links"
mkdir -p "$SYMLINK_DIR"
info "Creating symlinks in: ${SYMLINK_DIR}"

n_links=0
n_skipped=0

while IFS= read -r t1_path; do
    [[ -z "$t1_path" ]] && continue

    # Extract BIDS entities from the path (e.g. sub-01, ses-baseline)
    sub=$(echo "$t1_path" | grep -oP 'sub-[^/]+' | head -1)
    ses=$(echo "$t1_path" | grep -oP 'ses-[^/]+' | head -1)
    ext="${t1_path#*t1w}"  # preserves .nii or .nii.gz

    if [[ -z "$sub" ]]; then
        warn "Could not extract subject ID from path, skipping: $t1_path"
        (( n_skipped++ )) || true
        continue
    fi

    # Build a unique session ID; append session if present
    sid="${sub}${ses:+_${ses}}"
    link_name="${sid}_t1w${ext}"

    ln -sf "$t1_path" "${SYMLINK_DIR}/${link_name}"
    (( n_links++ )) || true

done < "$IMAGE_PATH_LIST"

info "Created ${n_links} symlink(s)."
[[ "$n_skipped" -gt 0 ]] && warn "Skipped ${n_skipped} image(s) due to missing BIDS entities."
[[ "$n_links"   -eq 0 ]] && error "No symlinks were created — nothing to submit."


# -----------------------------------------------------------------------------
# prepare fastsurfer output and work directories
# -----------------------------------------------------------------------------

# final outputs go into the bids derivatives folder alongside the raw data
COHORT_FASTSURFER_DIR="${COHORT_DIR}/derivatives/fastsurfer"
[[ ! -d "${COHORT_FASTSURFER_DIR}" ]] && mkdir -p "${COHORT_FASTSURFER_DIR}"
info "FastSurfer output directory: ${COHORT_FASTSURFER_DIR}"

# clear any leftover intermediate files from a previous run to avoid conflicts
FASTSURFER_WORK_DIR="${COHORT_WORK_DIR}/fastsurfer"
if [[ -d "$FASTSURFER_WORK_DIR" ]]; then
    warn "Removing existing FastSurfer work directory: ${FASTSURFER_WORK_DIR}"
    rm -rf "${FASTSURFER_WORK_DIR}"
fi
mkdir -p "${FASTSURFER_WORK_DIR}"
info "FastSurfer work directory: ${FASTSURFER_WORK_DIR}"


# -----------------------------------------------------------------------------
# submit jobs
# -----------------------------------------------------------------------------

info "Submitting ${n_links} scan(s) to FastSurfer via srun_fastsurfer.sh ..."

# TODO: update --partition_seg and --partition_surf to match cluster
srun_fastsurfer.sh \
    --partition_seg=GPU_Partition \
    --partition_surf=CPU_Partition \
    --sd "${COHORT_FASTSURFER_DIR}" \
    --work "${FASTSURFER_WORK_DIR}" \
    --data "${SYMLINK_DIR}" \
    --fslicense "${FREESURFER_LICENSE}" \
    --singularity_image "${FASTSURFER_SIF}" \
    --3T

info "Job submission complete. Monitor with: squeue -u \$USER"


# -----------------------------------------------------------------------------
# cleanup
# -----------------------------------------------------------------------------

# TODO: add cleanup once output structure is confirmed
#       candidates: remove FASTSURFER_WORK_DIR and SYMLINK_DIR on success

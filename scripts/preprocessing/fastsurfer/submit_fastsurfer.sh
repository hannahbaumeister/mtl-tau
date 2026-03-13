#!/bin/bash

set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# =============================================================================
# submit_fastsurfer.sh
#
# description: slurm submission script for fastsurfer. processes t1-weighted
#              mri data from a bids-formatted cohort directory, producing
#              fastsurfer segmentations including surface reconstruction
#              (equivalent to freesurfer's recon-all).
#
#              the script:
#                1. discovers all t1w images under <cohort>/raw/
#                2. creates a flat directory of symlinks named by bids entities
#                3. submits two dependent slurm job arrays:
#                     - job 1: gpu-based segmentation (seg_only)
#                     - job 2: cpu-based surface reconstruction (surf_only)
#                4. submits a cleanup job that moves outputs from the work
#                   directory to the bids derivatives folder
#
# usage:       ./submit_fastsurfer.sh -c <cohort> -f <sif> -l <license>
#                                     -p <proj_dir> -t <tools_dir> -w <work_dir>
#                                     [-n <cases_per_task>] [-d] [-x]
#
# author:      hannah baumeister
# email:       hannah.baumeister[at]med.lu.se
# =============================================================================


# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------

info()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]  $*"; }
warn()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN]  $*" >&2; }
error() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; exit 1; }

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  -c <cohort>      Name of the cohort to process (must exist under PROJ_DIR)
  -f <filepath>    Path to the FastSurfer Singularity image (.sif)
  -l <filepath>    Path to the FreeSurfer license file
  -p <directory>   Root project directory (cohorts live as subdirectories)
  -t <directory>   Directory containing brun_fastsurfer.sh and stools.sh
  -w <directory>   Work directory for intermediate and log files
  -n <integer>     Number of cases per job array task (default: 16)
  -d               Dry run — generate scripts but do not submit to SLURM
  -x               Enable debug output
  -h               Show this help message and exit

Example:
  $(basename "$0") -c ADNI -p /path/to/project \\
                   -f /sw/fastsurfer.sif -l ~/.license \\
                   -t /sw/fastsurfer -w /scratch/\$USER
EOF
  exit 0
}


# -----------------------------------------------------------------------------
# defaults
# -----------------------------------------------------------------------------

CASES_PER_TASK=16
DRY_RUN=false
DEBUG=false

# SLURM resource defaults (edit to match your cluster)
PARTITION_SEG="gpu"          # partition with GPUs for segmentation
PARTITION_SURF="core"        # partition for surface reconstruction
NUM_CPUS_SEG=16              # cpus per task for segmentation
NUM_CPUS_SURF=2              # cpus per case for surface reconstruction
MEM_SEG_GB=7                 # memory (GB) for gpu segmentation
MEM_SURF_GB=6                # memory (GB) per surface case
TIMELIMIT_SEG_MIN=15         # per-case time limit for segmentation (minutes)
TIMELIMIT_SURF_MIN=240       # per-case time limit for surface recon (minutes)


# -----------------------------------------------------------------------------
# argument parsing and validation
# -----------------------------------------------------------------------------

parse_args() {
  while getopts ":c:f:l:p:t:w:n:dxh" opt; do
    case $opt in
      c) COHORT="$OPTARG" ;;
      f) FASTSURFER_SIF="$OPTARG" ;;
      l) FREESURFER_LICENSE="$OPTARG" ;;
      p) PROJ_DIR="$OPTARG" ;;
      t) TO_FASTSURFER_TOOLS="$OPTARG" ;;
      w) WORK_DIR="$OPTARG" ;;
      n) CASES_PER_TASK="$OPTARG" ;;
      d) DRY_RUN=true ;;
      x) DEBUG=true ;;
      h) usage ;;
      :) error "Option -$OPTARG requires an argument. Run with -h for help." ;;
      \?) error "Unknown option: -$OPTARG. Run with -h for help." ;;
    esac
  done

  [[ -z "${COHORT:-}"              ]] && error "Missing required argument: -c <cohort>"
  [[ -z "${FASTSURFER_SIF:-}"      ]] && error "Missing required argument: -f <sif>"
  [[ -z "${FREESURFER_LICENSE:-}"  ]] && error "Missing required argument: -l <license>"
  [[ -z "${TO_FASTSURFER_TOOLS:-}" ]] && error "Missing required argument: -t <tools_dir>"
  [[ -z "${PROJ_DIR:-}"            ]] && error "Missing required argument: -p <proj_dir>"
  [[ -z "${WORK_DIR:-}"            ]] && error "Missing required argument: -w <work_dir>"

  [[ ! -f "$FASTSURFER_SIF"        ]] && error "FastSurfer SIF not found: $FASTSURFER_SIF"
  [[ ! -f "$FREESURFER_LICENSE"    ]] && error "FreeSurfer license not found: $FREESURFER_LICENSE"
  [[ ! -d "$TO_FASTSURFER_TOOLS"   ]] && error "FastSurfer tools directory not found: $TO_FASTSURFER_TOOLS"
  [[ ! -d "$PROJ_DIR"              ]] && error "Project directory not found: $PROJ_DIR"
}

parse_args "$@"

$DEBUG && info "Debug mode enabled."
$DRY_RUN && info "Dry-run mode: SLURM scripts will be written but not submitted."


# -----------------------------------------------------------------------------
# directory setup
# -----------------------------------------------------------------------------

COHORT_DIR="${PROJ_DIR}/${COHORT}"
COHORT_WORK_DIR="${WORK_DIR}/${COHORT}"

[[ ! -d "$COHORT_DIR" ]] && error "Cohort directory not found: $COHORT_DIR"

info "Setting up work directory: ${COHORT_WORK_DIR}"
mkdir -p "${COHORT_WORK_DIR}/logs"
mkdir -p "${COHORT_WORK_DIR}/scripts"
mkdir -p "${COHORT_WORK_DIR}/images"


# -----------------------------------------------------------------------------
# discover t1w images
# -----------------------------------------------------------------------------

IMAGE_PATH_LIST="${COHORT_WORK_DIR}/scripts/images_to_process.txt"

info "Searching for T1w images under ${COHORT_DIR}/raw/ ..."
find "${COHORT_DIR}/raw" -maxdepth 4 \
    \( -name "*T1w.nii" -o -name "*T1w.nii.gz" \) \
    | sort > "$IMAGE_PATH_LIST"

n_images=$(wc -l < "$IMAGE_PATH_LIST")
[[ "$n_images" -eq 0 ]] && error "No T1w images found under ${COHORT_DIR}/raw/"
info "Found ${n_images} T1w image(s). Image list saved to: ${IMAGE_PATH_LIST}"

# dev option: only process 2 images
head -n 2 "$IMAGE_PATH_LIST" > "${IMAGE_PATH_LIST}.tmp" \
    && mv "${IMAGE_PATH_LIST}.tmp" "$IMAGE_PATH_LIST"


# -----------------------------------------------------------------------------
# create flat symlink directory
# -----------------------------------------------------------------------------

SYMLINK_DIR="${COHORT_WORK_DIR}/t1w_links"
mkdir -p "$SYMLINK_DIR"
info "Creating symlinks in: ${SYMLINK_DIR}"

n_links=0

while IFS= read -r t1_path; do
    [[ -z "$t1_path" ]] && continue
    link_name=$(basename "$t1_path")
    ln -sf "$t1_path" "${SYMLINK_DIR}/${link_name}"
    (( n_links++ )) || true
done < "$IMAGE_PATH_LIST"

info "Created ${n_links} symlink(s)."
[[ "$n_links" -eq 0 ]] && error "No symlinks were created — nothing to submit."


# -----------------------------------------------------------------------------
# prepare fastsurfer output and work directories
# -----------------------------------------------------------------------------

COHORT_FASTSURFER_DIR="${COHORT_DIR}/derivatives/fastsurfer"
mkdir -p "${COHORT_FASTSURFER_DIR}"
info "FastSurfer output directory: ${COHORT_FASTSURFER_DIR}"

FASTSURFER_WORK_DIR="${COHORT_WORK_DIR}/fastsurfer"
if [[ -d "$FASTSURFER_WORK_DIR" ]]; then
    warn "Removing existing FastSurfer work directory: ${FASTSURFER_WORK_DIR}"
    rm -rf "${FASTSURFER_WORK_DIR}"
fi
mkdir -p "${FASTSURFER_WORK_DIR}"
info "FastSurfer work directory: ${FASTSURFER_WORK_DIR}"


# -----------------------------------------------------------------------------
# stage singularity image and support files into work directory
# -----------------------------------------------------------------------------

info "Staging singularity image and support files..."

if ! $DRY_RUN; then
    cp "$FASTSURFER_SIF"       "${COHORT_WORK_DIR}/images/fastsurfer.sif"
    cp "$FREESURFER_LICENSE"   "${COHORT_WORK_DIR}/scripts/.fs_license"
    cp "${TO_FASTSURFER_TOOLS}/brun_fastsurfer.sh" \
       "${TO_FASTSURFER_TOOLS}/stools.sh" \
       "${COHORT_WORK_DIR}/scripts/"
fi


# -----------------------------------------------------------------------------
# compute job array dimensions
# -----------------------------------------------------------------------------

# build a subject_list file in srun_fastsurfer format: sid=/path/to/t1.nii.gz
SUBJECT_LIST="${COHORT_WORK_DIR}/scripts/subject_list"
> "$SUBJECT_LIST"

while IFS= read -r t1_path; do
    [[ -z "$t1_path" ]] && continue
    fname=$(basename "$t1_path")
    # derive a subject id from the filename by stripping the T1w suffix
    sid="${fname%%_T1w*}"
    echo "${sid}=${t1_path}" >> "$SUBJECT_LIST"
done < "$IMAGE_PATH_LIST"

n_cases=$(wc -l < "$SUBJECT_LIST")
n_tasks=$(( (n_cases + CASES_PER_TASK - 1) / CASES_PER_TASK ))
real_cases_per_task=$(( (n_cases + n_tasks - 1) / n_tasks ))

info "Cases: ${n_cases} | Tasks: ${n_tasks} | Cases/task: ${real_cases_per_task}"

if [[ "$n_tasks" -gt 1 ]]; then
    ARRAY_OPT="--array=1-${n_tasks}"
    ARRAY_DEPEND="aftercorr"
else
    ARRAY_OPT=""
    ARRAY_DEPEND="afterok"
fi


# -----------------------------------------------------------------------------
# write segmentation job script
# -----------------------------------------------------------------------------

SEG_SCRIPT="${COHORT_WORK_DIR}/scripts/slurm_seg.sh"
SEG_TIMELIMIT=$(( TIMELIMIT_SEG_MIN * real_cases_per_task + 5 ))

cat > "$SEG_SCRIPT" <<SEGSCRIPT
#!/bin/bash
#SBATCH --job-name=FastSurfer-Seg
#SBATCH --partition=${PARTITION_SEG}
#SBATCH --cpus-per-task=${NUM_CPUS_SEG}
#SBATCH --mem=${MEM_SEG_GB}G
#SBATCH --gpus-per-task=1
#SBATCH --time=${SEG_TIMELIMIT}
${ARRAY_OPT:+#SBATCH ${ARRAY_OPT}}
#SBATCH --output=${COHORT_WORK_DIR}/logs/seg_%A_%a.log

module load singularity

singularity exec --nv \\
  -B "${COHORT_WORK_DIR}:/data,${SYMLINK_DIR}:/source:ro" \\
  --no-mount home,cwd --cleanenv \\
  --env TQDM_DISABLE=1 \\
  --env SLURM_ARRAY_TASK_ID=\$SLURM_ARRAY_TASK_ID \\
  --env SLURM_ARRAY_TASK_COUNT=\$SLURM_ARRAY_TASK_COUNT \\
  "${COHORT_WORK_DIR}/images/fastsurfer.sif" \\
  /data/scripts/brun_fastsurfer.sh \\
    --subject_list /data/scripts/subject_list \\
    --statusfile   /data/scripts/subject_success \\
    --batch        "slurm_task_id/${n_tasks}" \\
    --sd           /data/fastsurfer \\
    --threads      ${NUM_CPUS_SEG} \\
    --seg_only \\
    --fs_license   /data/scripts/.fs_license \\
    --3T

# exit 0 so aftercorr dependency triggers the surface job even on partial failure
exit 0
SEGSCRIPT

chmod +x "$SEG_SCRIPT"
$DEBUG && info "Segmentation script written to: ${SEG_SCRIPT}"


# -----------------------------------------------------------------------------
# write surface reconstruction job script
# -----------------------------------------------------------------------------

SURF_SCRIPT="${COHORT_WORK_DIR}/scripts/slurm_surf.sh"
MEM_PER_CPU_SURF=$(( (MEM_SURF_GB + NUM_CPUS_SURF - 1) / NUM_CPUS_SURF ))

cat > "$SURF_SCRIPT" <<SURFSCRIPT
#!/bin/bash
#SBATCH --job-name=FastSurfer-Surf
#SBATCH --partition=${PARTITION_SURF}
#SBATCH --ntasks=${real_cases_per_task}
#SBATCH --nodes=1-${real_cases_per_task}
#SBATCH --cpus-per-task=${NUM_CPUS_SURF}
#SBATCH --mem-per-cpu=${MEM_PER_CPU_SURF}G
#SBATCH --hint=nomultithread
#SBATCH --time=${TIMELIMIT_SURF_MIN}
${ARRAY_OPT:+#SBATCH ${ARRAY_OPT}}
#SBATCH --output=${COHORT_WORK_DIR}/logs/surf_%A_%a.log

module load singularity

run_fastsurfer=(
  srun
    -J singularity-surf
    -o "${COHORT_WORK_DIR}/logs/surf_%A_%a_%s.log"
    --ntasks=1
    --time=${TIMELIMIT_SURF_MIN}
    --nodes=1
    --cpus-per-task=${NUM_CPUS_SURF}
    --mem=${MEM_SURF_GB}G
    --hint=nomultithread
  singularity exec
    --no-mount home,cwd --cleanenv
    -B "${COHORT_WORK_DIR}:/data,${SYMLINK_DIR}:/source:ro"
    "${COHORT_WORK_DIR}/images/fastsurfer.sif"
    /fastsurfer/run_fastsurfer.sh
)

${COHORT_WORK_DIR}/scripts/brun_fastsurfer.sh \\
  --run_fastsurfer "\${run_fastsurfer[*]}" \\
  --subject_list   "${COHORT_WORK_DIR}/scripts/subject_list" \\
  --statusfile     "${COHORT_WORK_DIR}/scripts/subject_success" \\
  --parallel       max \\
  --sd             /data/fastsurfer \\
  --surf_only \\
  --fs_license     /data/scripts/.fs_license \\
  --threads        ${NUM_CPUS_SURF} \\
  --3T
SURFSCRIPT

chmod +x "$SURF_SCRIPT"
$DEBUG && info "Surface script written to: ${SURF_SCRIPT}"


# -----------------------------------------------------------------------------
# write cleanup job script
# -----------------------------------------------------------------------------

CLEANUP_SCRIPT="${COHORT_WORK_DIR}/scripts/slurm_cleanup.sh"

cat > "$CLEANUP_SCRIPT" <<CLEANSCRIPT
#!/bin/bash
#SBATCH --job-name=FastSurfer-Cleanup
#SBATCH --partition=${PARTITION_SURF}
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=60
#SBATCH --output=${COHORT_WORK_DIR}/logs/cleanup_%j.log

echo "Moving FastSurfer outputs to derivatives..."
mv "${FASTSURFER_WORK_DIR}"/* "${COHORT_FASTSURFER_DIR}/"

echo "Removing symlink directory..."
rm -rf "${SYMLINK_DIR}"

echo "Cleanup complete. Results in: ${COHORT_FASTSURFER_DIR}"
CLEANSCRIPT

chmod +x "$CLEANUP_SCRIPT"
$DEBUG && info "Cleanup script written to: ${CLEANUP_SCRIPT}"


# -----------------------------------------------------------------------------
# submit jobs
# -----------------------------------------------------------------------------

info "Submitting jobs to SLURM..."

if $DRY_RUN; then
    info "[DRY RUN] Would submit: sbatch ${SEG_SCRIPT}"
    info "[DRY RUN] Would submit: sbatch --dependency=<seg_jobid> ${SURF_SCRIPT}"
    info "[DRY RUN] Would submit: sbatch --dependency=afterany:<surf_jobid> ${CLEANUP_SCRIPT}"
    info "Scripts written to: ${COHORT_WORK_DIR}/scripts/"
    exit 0
fi

SEG_JOBID=$(sbatch --parsable "$SEG_SCRIPT")
info "Submitted segmentation job: ${SEG_JOBID}"

SURF_JOBID=$(sbatch --parsable \
    --dependency="${ARRAY_DEPEND}:${SEG_JOBID}" \
    "$SURF_SCRIPT")
info "Submitted surface job: ${SURF_JOBID} (depends on ${SEG_JOBID})"

CLEANUP_JOBID=$(sbatch --parsable \
    --dependency="afterany:${SURF_JOBID}" \
    "$CLEANUP_SCRIPT")
info "Submitted cleanup job: ${CLEANUP_JOBID} (depends on ${SURF_JOBID})"

info "All jobs submitted. Monitor with: squeue -u \$USER"
info "Logs: ${COHORT_WORK_DIR}/logs/"
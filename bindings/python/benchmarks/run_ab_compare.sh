#!/usr/bin/env bash
# A/B comparison driver for the GetFileStatus hot-path optimisations.
#
# Builds two wheels — one from the baseline tag and one from the current
# branch — installs each into its own throwaway venv, runs the same e2e
# benchmark against the local cluster, and prints the side-by-side delta.
#
# Prerequisites:
#   * a local GooseFS cluster reachable at $GOOSEFS_MASTER_ADDR
#     (default: 127.0.0.1:9200)
#   * uv on PATH
#   * a clean working tree (the script aborts otherwise so you don't lose work)
#
# Usage:
#   cd bindings/python
#   ./benchmarks/run_ab_compare.sh                       # default sweep
#   PATHS=200 CALLS=4000 THREADS="1,4,16,64,256" \
#     ./benchmarks/run_ab_compare.sh
#   BASELINE_REF=v0.1.5 ./benchmarks/run_ab_compare.sh   # override baseline

set -euo pipefail

# ---------------------------------------------------------------------------
# Config (override via env)
# ---------------------------------------------------------------------------
BASELINE_REF="${BASELINE_REF:-8a14e9e}"
MASTER_ADDR="${GOOSEFS_MASTER_ADDR:-127.0.0.1:9200}"
PATHS="${PATHS:-200}"
CALLS="${CALLS:-4000}"
THREADS="${THREADS:-1,4,16,64,256}"
WORK_DIR="${WORK_DIR:-/tmp/goosefs-ab-$(date +%Y%m%d-%H%M%S)}"

# ---------------------------------------------------------------------------
# Locate the project (this script lives in bindings/python/benchmarks/)
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYBIND_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${PYBIND_DIR}/../.." && pwd)"
BENCH_SCRIPT="${SCRIPT_DIR}/bench_get_status_hotpath.py"

if [[ ! -f "${BENCH_SCRIPT}" ]]; then
    echo "FATAL: bench script not found at ${BENCH_SCRIPT}" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Safety: refuse to run on a dirty tree (we will be checking out other refs)
# ---------------------------------------------------------------------------
cd "${REPO_ROOT}"
if [[ -n "$(git status --porcelain)" ]]; then
    echo "FATAL: working tree is dirty. Commit or stash first." >&2
    git status --short >&2
    exit 1
fi

ORIGINAL_REF="$(git rev-parse --abbrev-ref HEAD)"
if [[ "${ORIGINAL_REF}" == "HEAD" ]]; then
    ORIGINAL_REF="$(git rev-parse HEAD)"
fi
echo "original ref = ${ORIGINAL_REF}"
echo "baseline ref = ${BASELINE_REF}"
echo "master       = ${MASTER_ADDR}"
echo "paths        = ${PATHS}"
echo "calls        = ${CALLS}"
echo "threads      = ${THREADS}"
echo "work dir     = ${WORK_DIR}"

mkdir -p "${WORK_DIR}"
BASELINE_JSON="${WORK_DIR}/baseline.json"
OPTIMIZED_JSON="${WORK_DIR}/optimized.json"

# ---------------------------------------------------------------------------
# Always restore the original branch on exit
# ---------------------------------------------------------------------------
cleanup() {
    echo
    echo "restoring git ref: ${ORIGINAL_REF}"
    git -C "${REPO_ROOT}" checkout --quiet "${ORIGINAL_REF}" || true
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Helper: build a wheel at the current ref into $1, install into a fresh venv
# at $2, then run the bench writing JSON to $3 with label $4.
# ---------------------------------------------------------------------------
build_install_run() {
    local stage_dir="$1"
    local venv_dir="$2"
    local out_json="$3"
    local label="$4"

    echo
    echo "===== [${label}] building wheel @ $(git -C "${REPO_ROOT}" rev-parse --short HEAD) ====="

    rm -rf "${stage_dir}" "${venv_dir}"
    mkdir -p "${stage_dir}"

    cd "${PYBIND_DIR}"
    # Build a release wheel into ${stage_dir}.
    # We use `uvx maturin` so a pinned maturin version is picked up without
    # depending on the in-tree pyproject (which may not exist on every ref).
    uvx --from "maturin>=1.5,<2" \
        maturin build --release --out "${stage_dir}" \
        --interpreter python3

    local wheel
    wheel="$(ls -1 "${stage_dir}"/goosefs-*.whl | head -n1)"
    if [[ -z "${wheel}" ]]; then
        echo "FATAL: no wheel produced in ${stage_dir}" >&2
        exit 1
    fi
    echo "built wheel: ${wheel}"

    # Throwaway venv. We install only the wheel + nothing else; the bench
    # script has no extra deps beyond the stdlib + goosefs.
    uv venv --quiet "${venv_dir}"
    # `uv venv` deliberately does not ship pip; use `uv pip` against the venv
    # python directly — it is the supported install path and stays offline-fast.
    uv pip install --quiet --python "${venv_dir}/bin/python" "${wheel}"

    # Run the bench. We always run *the current branch's* bench script (copied
    # to ${WORK_DIR}) so that the baseline run also produces the same JSON
    # schema, even though the script itself doesn't exist on the baseline ref.
    echo "running bench (label=${label}) ..."
    GOOSEFS_MASTER_ADDR="${MASTER_ADDR}" \
        "${venv_dir}/bin/python" "${WORK_DIR}/bench_get_status_hotpath.py" \
            --paths "${PATHS}" \
            --calls "${CALLS}" \
            --threads "${THREADS}" \
            --label "${label}" \
            --json "${out_json}"
}

# ---------------------------------------------------------------------------
# Stage the bench script in a stable location (it lives only on the optimised
# branch, but we want to run identical code in both runs).
# ---------------------------------------------------------------------------
cp "${BENCH_SCRIPT}" "${WORK_DIR}/bench_get_status_hotpath.py"

# ---------------------------------------------------------------------------
# Run A: baseline
# ---------------------------------------------------------------------------
git -C "${REPO_ROOT}" checkout --quiet "${BASELINE_REF}"
build_install_run \
    "${WORK_DIR}/wheels-baseline" \
    "${WORK_DIR}/venv-baseline" \
    "${BASELINE_JSON}" \
    "baseline-${BASELINE_REF}"

# ---------------------------------------------------------------------------
# Run B: optimised (back to the original branch)
# ---------------------------------------------------------------------------
git -C "${REPO_ROOT}" checkout --quiet "${ORIGINAL_REF}"
build_install_run \
    "${WORK_DIR}/wheels-optimized" \
    "${WORK_DIR}/venv-optimized" \
    "${OPTIMIZED_JSON}" \
    "optimized-${ORIGINAL_REF}"

# ---------------------------------------------------------------------------
# Compare
# ---------------------------------------------------------------------------
echo
echo "===== A/B comparison ====="
"${WORK_DIR}/venv-optimized/bin/python" "${WORK_DIR}/bench_get_status_hotpath.py" \
    --compare "${BASELINE_JSON}" "${OPTIMIZED_JSON}"

echo
echo "Raw JSON results:"
echo "  ${BASELINE_JSON}"
echo "  ${OPTIMIZED_JSON}"

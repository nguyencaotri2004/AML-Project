# config.R — Portable global config for AML Detective project
# ══════════════════════════════════════════════════════════════════════
# HOW TO USE: source("config.R") at the top of any script
# This file auto-detects the project root and machine environment.
# ══════════════════════════════════════════════════════════════════════

# ── 1. Auto-detect project root ───────────────────────────────────────
# Works regardless of where the script is run from.
# Priority: (a) env var DSR_ROOT, (b) script's own directory, (c) getwd()
if (nchar(Sys.getenv("DSR_ROOT")) > 0) {
  ROOT_DIR <- normalizePath(Sys.getenv("DSR_ROOT"), mustWork = FALSE)
} else {
  # Try to find the project root by looking for a marker file (config.R itself)
  # When sourced from any subdirectory, walk up until we find config.R
  find_root <- function() {
    candidates <- c(
      getwd(),
      dirname(sys.frame(1)$ofile %||% ""),
      normalizePath(".")
    )
    for (d in candidates) {
      if (file.exists(file.path(d, "config.R"))) return(normalizePath(d))
      parent <- dirname(d)
      if (file.exists(file.path(parent, "config.R"))) return(normalizePath(parent))
    }
    getwd()  # fallback
  }
  # Helper: NULL-coalescing for base R
  `%||%` <- function(a, b) if (!is.null(a) && length(a) > 0 && nchar(a) > 0) a else b
  ROOT_DIR <- tryCatch(find_root(), error = function(e) getwd())
}

cat(sprintf("[Config] Project root: %s\n", ROOT_DIR))

# ── 2. Standard sub-paths (all relative to ROOT_DIR) ─────────────────
PROC_DIR  <- file.path(ROOT_DIR, "processed")
OUT_DIR   <- file.path(ROOT_DIR, "outputs")
PLOTS_DIR <- file.path(ROOT_DIR, "outputs", "plots")
DATA_DIR  <- file.path(ROOT_DIR, "Dataset")
MODELS_DIR<- file.path(ROOT_DIR, "outputs", "wallet_models")
REPORT_DIR<- file.path(ROOT_DIR, "report")

# ── 3. R Library — scan all common user library locations ────────────
r_ver_short <- paste0(R.version$major, ".",
                      substr(R.version$minor, 1, 1))
user_lib_candidates <- unique(c(
  Sys.getenv("R_LIBS_USER"),
  file.path(Sys.getenv("USERPROFILE"), "R", "win-library", r_ver_short),
  file.path(path.expand("~"), "R", "win-library", r_ver_short),
  file.path(Sys.getenv("APPDATA"), "R", "win-library", r_ver_short),
  file.path(Sys.getenv("LOCALAPPDATA"), "R", "win-library", r_ver_short)
))
existing_libs <- user_lib_candidates[nchar(user_lib_candidates) > 0 &
                                       dir.exists(user_lib_candidates)]
if (length(existing_libs) > 0) {
  .libPaths(unique(c(existing_libs, .libPaths())))
}
cat(sprintf("[Config] R lib paths (%d):\n", length(.libPaths())))
for (p in .libPaths()) cat(sprintf("    %s\n", p))


# ── 4. Required packages — auto-install if missing ───────────────────
REQUIRED_PKGS <- c(
  "shiny", "data.table", "plotly", "jsonlite",
  "randomForest", "xgboost", "ggplot2", "scales",
  "gridExtra", "reshape2", "DBI", "RSQLite"
)

install_if_missing <- function(pkgs) {
  missing_pkgs <- pkgs[!sapply(pkgs, requireNamespace, quietly = TRUE)]
  if (length(missing_pkgs) > 0) {
    cat(sprintf("[Config] Installing missing packages: %s\n",
                paste(missing_pkgs, collapse = ", ")))
    install.packages(missing_pkgs,
                     repos = "https://cloud.r-project.org",
                     quiet = TRUE)
  } else {
    cat("[Config] All required packages installed \u2713\n")
  }
}

install_if_missing(REQUIRED_PKGS)

# ── 5. GPU / CPU auto-detection ───────────────────────────────────────
library(xgboost)

GPU_DEVICE <- tryCatch({
  Sys.setenv(CUDA_VISIBLE_DEVICES = "0")
  dm_test <- xgb.DMatrix(matrix(rnorm(50), 5, 10),
                          label = rep(0:1, length.out = 5))
  suppressWarnings(
    xgb.train(list(tree_method = "hist", device = "cuda",
                   objective   = "binary:logistic"),
              dm_test, nrounds = 1, verbose = 0)
  )
  cat("[Config] GPU: CUDA available — using GPU\n")
  "cuda"
}, error = function(e) {
  cat("[Config] GPU: CUDA unavailable — using CPU\n")
  "cpu"
})

N_THREADS <- parallel::detectCores(logical = FALSE)
cat(sprintf("[Config] CPU threads: %d physical cores\n", N_THREADS))

# ── 6. XGBoost default params ─────────────────────────────────────────
DEFAULT_XGB_PARAMS <- list(
  tree_method = "hist",
  device      = GPU_DEVICE,
  nthread     = N_THREADS
)

make_xgb_params <- function(...) modifyList(DEFAULT_XGB_PARAMS, list(...))

# ── 7. Summary ────────────────────────────────────────────────────────
cat(sprintf("[Config] Ready | GPU=%s | Threads=%d | Root=%s\n",
            GPU_DEVICE, N_THREADS, ROOT_DIR))

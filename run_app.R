# run_app.R — One-click launcher for AML Detective Shiny Dashboard
# ══════════════════════════════════════════════════════════════════════
# USAGE (from any terminal / RStudio):
#   Rscript run_app.R
#   Rscript run_app.R --port 8080
#   Rscript run_app.R --no-browser
# ══════════════════════════════════════════════════════════════════════

# ── 0. Locate project root ────────────────────────────────────────────
# When run as: Rscript run_app.R, the --file= argument gives us the path.
args_full <- commandArgs(FALSE)
file_arg  <- grep("^--file=", args_full, value = TRUE)
if (length(file_arg) > 0) {
  ROOT_DIR <- normalizePath(dirname(sub("^--file=", "", file_arg[1])))
} else {
  ROOT_DIR <- getwd()
}

setwd(ROOT_DIR)
cat(sprintf("[run_app] Project root: %s\n", ROOT_DIR))

# ── 1. Source config (auto-installs packages, sets lib paths, GPU) ───
source(file.path(ROOT_DIR, "config.R"))

# ── 2. Parse command-line arguments ──────────────────────────────────
args <- commandArgs(trailingOnly = TRUE)
get_arg <- function(flag, default) {
  idx <- which(args == flag)
  if (length(idx) > 0 && idx < length(args)) args[idx + 1] else default
}

PORT         <- as.integer(get_arg("--port", "7777"))
LAUNCH_BROWSER <- !("--no-browser" %in% args)
HOST         <- get_arg("--host", "127.0.0.1")

# ── 3. Sanity-check required output files exist ───────────────────────
required_files <- c(
  file.path(OUT_DIR, "eda_summary.json"),
  file.path(OUT_DIR, "wallet_metrics.json"),
  file.path(PROC_DIR, "wallet", "wallet_nodes.csv"),
  file.path(PROC_DIR, "wallet", "wallet_features_full.csv"),
  file.path(MODELS_DIR, "best_model.rds")
)

missing <- required_files[!file.exists(required_files)]
if (length(missing) > 0) {
  cat("\n[run_app] ⚠ Missing required files:\n")
  for (f in missing) cat(sprintf("    ✗ %s\n", f))
  cat("\n[run_app] Please run the pipeline scripts first:\n")
  cat("    Rscript R/2_tien_xu_ly_du_lieu.R\n")
  cat("    Rscript R/3_analysis.R\n")
  cat("    Rscript R/5_train_model.R\n\n")
  stop("Missing required data files. See messages above.")
}
cat("[run_app] All required files found ✓\n")

# ── 4. Load required libraries for Shiny ─────────────────────────────
library(shiny)

# ── 5. Launch the app ────────────────────────────────────────────────
cat(sprintf("\n[run_app] Starting AML Detective Dashboard...\n"))
cat(sprintf("[run_app] URL: http://%s:%d\n", HOST, PORT))
cat(sprintf("[run_app] Press Ctrl+C to stop.\n\n"))

shiny::runApp(
  appDir        = file.path(ROOT_DIR, "R", "app"),
  host          = HOST,
  port          = PORT,
  launch.browser= LAUNCH_BROWSER
)

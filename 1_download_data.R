# 1_download_data.R
# Elliptic++ Dataset Validator
# ─────────────────────────────────────────────────────────────────
# Elliptic++ is NOT on Kaggle. Download manually from Google Drive:
#   https://drive.google.com/drive/folders/1MRPXz79Lu_JGLlJ21MDfML44dKN9R08l
#
# Place all CSV files inside:  d:/dsr/Dataset/
# Then run this script to validate everything is in place.
# ─────────────────────────────────────────────────────────────────

DATASET_DIR <- "Dataset"

required_tx <- c(
  "txs_features.csv",
  "txs_classes.csv",
  "txs_edgelist.csv"
)

optional_wallet <- c(
  "wallets_features.csv",
  "wallets_classes.csv",
  "AddrTx_edgelist.csv",
  "TxAddr_edgelist.csv"
)

cat("=======================================================\n")
cat("  Elliptic++ Dataset Validator\n")
cat("=======================================================\n\n")

if (!dir.exists(DATASET_DIR)) {
  stop(paste0(
    "Directory '", DATASET_DIR, "' not found!\n",
    "Download from: https://drive.google.com/drive/folders/1MRPXz79Lu_JGLlJ21MDfML44dKN9R08l\n",
    "Then place all CSV files under: ", normalizePath(DATASET_DIR, mustWork = FALSE)
  ))
}

cat("Checking directory:", normalizePath(DATASET_DIR), "\n\n")

# ── Check required transaction files ──────────────────────────────
cat("Phase 1 — Transaction Files (Required):\n")
all_ok <- TRUE
for (fname in required_tx) {
  fpath <- file.path(DATASET_DIR, fname)
  if (file.exists(fpath)) {
    mb <- round(file.info(fpath)$size / 1024 / 1024, 0)
    cat(sprintf("  [OK] %-35s (%d MB)\n", fname, mb))
  } else {
    cat(sprintf("  [MISSING] %s\n", fname))
    all_ok <- FALSE
  }
}

# ── Check optional wallet files ────────────────────────────────────
cat("\nPhase 2 — Wallet/Actor Files (Optional):\n")
wallet_ok <- TRUE
for (fname in optional_wallet) {
  fpath <- file.path(DATASET_DIR, fname)
  if (file.exists(fpath)) {
    mb <- round(file.info(fpath)$size / 1024 / 1024, 0)
    cat(sprintf("  [OK] %-35s (%d MB)\n", fname, mb))
  } else {
    cat(sprintf("  [--] %-35s (not found, optional)\n", fname))
    wallet_ok <- FALSE
  }
}

cat("\n")
if (all_ok) {
  cat("All required files present!\n")
  cat("  -> Next: Rscript R/2_preprocessing.R\n")
  if (wallet_ok) cat("  -> Wallet data also available for Phase 2.\n")
} else {
  stop("Missing required files. Please download from Google Drive.")
}

# 1_download_data.R
# ══════════════════════════════════════════════════════════════════════════════
# KIỂM TRA DỮ LIỆU — Elliptic++ Dataset (8 files bắt buộc)
# ══════════════════════════════════════════════════════════════════════════════
# Dự án: Wallet-Level AML Detection
# Dataset: Elliptic++ (KHÔNG có trên Kaggle)
#
# Tải thủ công từ Google Drive:
#   https://drive.google.com/drive/folders/1MRPXz79Lu_JGLlJ21MDfML44dKN9R08l
#
# Đặt tất cả file CSV vào:  d:/dsr/Dataset/
# Sau đó chạy script này để kiểm tra đủ 8 files chưa.
# ══════════════════════════════════════════════════════════════════════════════

DATASET_DIR <- "Dataset"

# Tất cả 8 files bắt buộc cho dự án
REQUIRED_FILES <- list(

  # ── NHÓM 1: WALLET (Dùng cho mô hình chính — Wallet-Level AML) ──────────────
  "wallets_features_classes_combined.csv" = list(
    group = "WALLET",
    desc  = "55 behavioral features + label của 822K wallets",
    min_mb = 200
  ),
  "wallets_classes.csv" = list(
    group = "WALLET",
    desc  = "Label (illicit/licit) của từng wallet — dùng để validate",
    min_mb = 5
  ),
  "wallets_features.csv" = list(
    group = "WALLET",
    desc  = "Features wallet theo time step (dự phòng / cross-check)",
    min_mb = 100
  ),

  # ── NHÓM 2: TRANSACTION (Dùng để tính TX Risk Score) ─────────────────────────
  "txs_features.csv" = list(
    group = "TX",
    desc  = "183 features của 203K transactions",
    min_mb = 100
  ),
  "txs_classes.csv" = list(
    group = "TX",
    desc  = "Label (illicit/licit) của từng transaction",
    min_mb = 1
  ),
  "txs_edgelist.csv" = list(
    group = "TX",
    desc  = "Cạnh tx→tx (mạng giao dịch), 234K edges",
    min_mb = 1
  ),

  # ── NHÓM 3: BIPARTITE GRAPH (Wallet ↔ Transaction) ────────────────────────────
  "AddrTx_edgelist.csv" = list(
    group = "BIPARTITE",
    desc  = "address→txId (wallet gửi tiền VÀO tx), 477K edges",
    min_mb = 5
  ),
  "TxAddr_edgelist.csv" = list(
    group = "BIPARTITE",
    desc  = "txId→address (tx gửi tiền RA wallet), 837K edges",
    min_mb = 5
  )
)

# ── BANNER ────────────────────────────────────────────────────────────────────
cat("╔══════════════════════════════════════════════════════╗\n")
cat("║  Wallet-Level AML Detection — Kiểm tra Dataset      ║\n")
cat("╚══════════════════════════════════════════════════════╝\n\n")

if (!dir.exists(DATASET_DIR)) {
  stop(paste0(
    "Không tìm thấy thư mục '", DATASET_DIR, "'\n",
    "Tải dataset từ: https://drive.google.com/drive/folders/1MRPXz79Lu_JGLlJ21MDfML44dKN9R08l\n",
    "Đặt tất cả file CSV vào: ", normalizePath(DATASET_DIR, mustWork=FALSE)
  ))
}

cat(sprintf("Thư mục dataset: %s\n\n", normalizePath(DATASET_DIR)))

# ── CHECK TỪNG FILE ───────────────────────────────────────────────────────────
groups   <- c("WALLET", "TX", "BIPARTITE")
grp_label <- c(
  WALLET    = "NHÓM 1 — WALLET (Mô hình chính)",
  TX        = "NHÓM 2 — TRANSACTION (TX Risk Score)",
  BIPARTITE = "NHÓM 3 — BIPARTITE GRAPH (Wallet ↔ TX)"
)

all_ok   <- TRUE
total_mb <- 0

for (grp in groups) {
  cat(sprintf("📁 %s\n", grp_label[grp]))
  for (fname in names(REQUIRED_FILES)) {
    info <- REQUIRED_FILES[[fname]]
    if (info$group != grp) next
    fpath <- file.path(DATASET_DIR, fname)
    if (file.exists(fpath)) {
      mb  <- round(file.info(fpath)$size / 1024 / 1024, 0)
      ok  <- mb >= info$min_mb
      tag <- if (ok) "[OK]" else "[TOO SMALL?]"
      cat(sprintf("  %s %-43s %4d MB\n", tag, fname, mb))
      cat(sprintf("       → %s\n", info$desc))
      total_mb <- total_mb + mb
      if (!ok) all_ok <- FALSE
    } else {
      cat(sprintf("  [MISSING] %-43s\n", fname))
      cat(sprintf("       → %s\n", info$desc))
      all_ok <- FALSE
    }
  }
  cat("\n")
}

# ── KẾT QUẢ ──────────────────────────────────────────────────────────────────
cat(sprintf("Tổng dung lượng: ~%d MB\n\n", total_mb))

if (all_ok) {
  cat("╔══════════════════════════════════════════════════════╗\n")
  cat("║  ✅ ĐỦ 8 FILES — SẴN SÀNG TIỀN XỬ LÝ               ║\n")
  cat("╚══════════════════════════════════════════════════════╝\n")
  cat("  Bước tiếp theo: Rscript R/2_tien_xu_ly_du_lieu.R\n\n")
} else {
  cat("╔══════════════════════════════════════════════════════╗\n")
  cat("║  ❌ THIẾU FILE — Vui lòng tải đủ trước khi tiếp tục ║\n")
  cat("╚══════════════════════════════════════════════════════╝\n")
  cat("  Tải từ: https://drive.google.com/drive/folders/1MRPXz79Lu_JGLlJ21MDfML44dKN9R08l\n\n")
  stop("Thiếu file dữ liệu. Không thể tiếp tục.")
}

# 2_tien_xu_ly_du_lieu.R
# ══════════════════════════════════════════════════════════════════════════════
# TIỀN XỬ LÝ DỮ LIỆU — Elliptic++ (8 files gốc)
# ══════════════════════════════════════════════════════════════════════════════
# INPUT  : 8 files trong Dataset/
# OUTPUT :
#   processed/wallet/wallet_nodes.csv          — wallet metadata + label
#   processed/wallet/wallet_features_full.csv  — behavioral + graph features
#   processed/wallet/train/val/test_idx.csv    — split indices
#   processed/tx/tx_nodes.csv                  — tx metadata + label
#   processed/tx/tx_features.csv              — 184 tx features (scaled)
#   processed/tx/tx_edgelist.csv              — tx→tx graph
#   outputs/preprocessing_report.json          — báo cáo tóm tắt
# ══════════════════════════════════════════════════════════════════════════════

.libPaths(c("C:/Users/Trinc/R/win-library/4.5", .libPaths()))
library(data.table)
library(jsonlite)
set.seed(42)

DATASET <- "Dataset"
PROC_W  <- "processed/wallet"
PROC_T  <- "processed/tx"
OUT     <- "outputs"
dir.create(PROC_W, recursive=TRUE, showWarnings=FALSE)
dir.create(PROC_T, recursive=TRUE, showWarnings=FALSE)
dir.create(OUT,    recursive=TRUE, showWarnings=FALSE)

sec <- function(title) {
  cat(sprintf("\n╔══════════════════════════════════════════╗\n"))
  cat(sprintf("║  %-40s║\n", title))
  cat(sprintf("╚══════════════════════════════════════════╝\n"))
}

bugs_log    <- list()
anomaly_log <- list()

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 1: ĐỌC TẤT CẢ 8 FILES
# ══════════════════════════════════════════════════════════════════════════════
sec("BƯỚC 1: Đọc 8 files gốc")

t0 <- proc.time()["elapsed"]

wal_combined <- fread(file.path(DATASET, "wallets_features_classes_combined.csv"))
wal_classes  <- fread(file.path(DATASET, "wallets_classes.csv"))
wal_feat_raw <- fread(file.path(DATASET, "wallets_features.csv"))
tx_feat      <- fread(file.path(DATASET, "txs_features.csv"))
tx_cls       <- fread(file.path(DATASET, "txs_classes.csv"))
tx_edge      <- fread(file.path(DATASET, "txs_edgelist.csv"))
addr_tx      <- fread(file.path(DATASET, "AddrTx_edgelist.csv"))
tx_addr      <- fread(file.path(DATASET, "TxAddr_edgelist.csv"))

cat(sprintf("  Load time: %.1fs\n", proc.time()["elapsed"] - t0))
for (nm in c("wal_combined","wal_classes","wal_feat_raw",
             "tx_feat","tx_cls","tx_edge","addr_tx","tx_addr")) {
  dt <- get(nm)
  cat(sprintf("  %-30s: %s rows × %d cols\n",
      nm, format(nrow(dt), big.mark=","), ncol(dt)))
}

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 2: FIX BUGS TÊN CỘT
# ══════════════════════════════════════════════════════════════════════════════
sec("BƯỚC 2: Fix bugs tên cột")

fix_col_spaces <- function(dt, src) {
  bad <- names(dt)[grepl(" ", names(dt))]
  if (length(bad)) {
    setnames(dt, names(dt), gsub(" ", "_", names(dt)))
    cat(sprintf("  [%s] FIX %d cột: %s\n", src, length(bad), paste(bad, collapse=", ")))
    bugs_log[[src]] <<- bad
  } else {
    cat(sprintf("  [%s] OK — không có space\n", src))
  }
}

fix_col_spaces(wal_combined, "wallets_features_classes_combined")
fix_col_spaces(wal_feat_raw, "wallets_features")
fix_col_spaces(wal_classes,  "wallets_classes")
fix_col_spaces(tx_feat,      "txs_features")
fix_col_spaces(tx_cls,       "txs_classes")
fix_col_spaces(tx_edge,      "txs_edgelist")
fix_col_spaces(addr_tx,      "AddrTx_edgelist")
fix_col_spaces(tx_addr,      "TxAddr_edgelist")

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 3: XỬ LÝ WALLET FEATURES (file 1 + 2)
# ══════════════════════════════════════════════════════════════════════════════
sec("BƯỚC 3: Wallet Behavioral Features")

# Xác định cột features
WAL_ID    <- "address"
WAL_TIME  <- "Time_step"
WAL_LABEL <- "class"
WAL_FEAT  <- setdiff(names(wal_combined), c(WAL_ID, WAL_TIME, WAL_LABEL))
cat(sprintf("  Wallet feature cols: %d\n", length(WAL_FEAT)))

# Map label: 1=illicit, 2=licit, 3=unknown → 1/0/-1
wal_combined[, label := fcase(class==1L, 1L, class==2L, 0L, default=-1L)]

# Validate label với wallets_classes.csv
setnames(wal_classes, names(wal_classes), c("address","class_ref"))
wal_lbl_ref <- wal_classes[, .(label_ref = fcase(
  class_ref==1L, 1L, class_ref==2L, 0L, default=-1L)), by=address]
merged_lbl <- merge(
  wal_combined[, .(label_from_combined=max(label)), by=address],
  wal_lbl_ref, by="address", all=TRUE)
mismatch <- sum(merged_lbl$label_from_combined != merged_lbl$label_ref, na.rm=TRUE)
cat(sprintf("  Label mismatch (combined vs classes): %d %s\n",
    mismatch, ifelse(mismatch==0, "✓ KHỚP", "⚠ CÓ SAI")))
anomaly_log[["label_mismatch"]] <- mismatch

# NaN check
n_nan_wal <- sum(is.na(wal_combined[, ..WAL_FEAT]))
cat(sprintf("  NaN trong wallet features: %d %s\n",
    n_nan_wal, ifelse(n_nan_wal==0, "✓", "⚠")))

# Winsorize BTC + timing features
btc_cols    <- grep("^btc_|^fees_", WAL_FEAT, value=TRUE)
timing_cols <- grep("^blocks_btwn_", WAL_FEAT, value=TRUE)
count_cols  <- setdiff(WAL_FEAT, c(btc_cols, timing_cols))
winsorize   <- function(x) { lo<-quantile(x,.005,na.rm=T); hi<-quantile(x,.995,na.rm=T); pmin(pmax(x,lo),hi) }
for (cn in c(btc_cols, timing_cols)) wal_combined[[cn]] <- winsorize(wal_combined[[cn]])
cat(sprintf("  Winsorized: %d BTC/fees + %d timing cols\n",
    length(btc_cols), length(timing_cols)))

# Aggregate → 1 row per wallet
cat("  Aggregating to 1 row per wallet...\n")
wal_agg <- wal_combined[, lapply(.SD, mean, na.rm=TRUE), .SDcols=WAL_FEAT, by=address]
wal_meta <- wal_combined[, .(
  label      = max(label),
  time_first = min(Time_step),
  time_last  = max(Time_step),
  n_tsteps   = uniqueN(Time_step)
), by=address]
wal_dt <- merge(wal_agg, wal_meta, by="address")
cat(sprintf("  Kết quả: %s wallets | %d features\n",
    format(nrow(wal_dt), big.mark=","), length(WAL_FEAT)))
cat(sprintf("  Labeled: %d illicit | %d licit | %d unknown\n",
    sum(wal_dt$label==1L), sum(wal_dt$label==0L), sum(wal_dt$label==-1L)))

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 4: BIPARTITE GRAPH FEATURES (file 7 + 8 + 5)
# ══════════════════════════════════════════════════════════════════════════════
sec("BƯỚC 4: Bipartite Graph Features cho Wallet")
# Mục tiêu: với mỗi wallet, tính:
#   - n_input_txs       : số tx wallet gửi tiền vào
#   - n_output_txs      : số tx wallet nhận tiền ra
#   - total_txs_bipart  : tổng số tx liên quan
#   - n_illicit_nbr_txs : số tx lân cận có label illicit
#   - pct_illicit_nbr   : % tx lân cận illicit (KEY AML SIGNAL)
#   - n_licit_nbr_txs   : số tx lân cận licit

# Cần tx labels để tính illicit neighborhood
tx_cls[, tx_label := fcase(class==1L, 1L, class==2L, 0L, default=-1L)]
tx_cls[, txId := as.character(txId)]
addr_tx[, txId := as.character(txId)]
tx_addr[, txId := as.character(txId)]

# Tên cột
setnames(addr_tx, names(addr_tx), c("address","txId"))  # input: wallet sends to tx
setnames(tx_addr, names(tx_addr), c("txId","address"))  # output: tx sends to wallet

# Join tx labels lên bipartite edges
addr_tx_lbl <- merge(addr_tx, tx_cls[, .(txId, tx_label)], by="txId", all.x=TRUE)
tx_addr_lbl <- merge(tx_addr, tx_cls[, .(txId, tx_label)], by="txId", all.x=TRUE)

# Aggregate input side (wallet → tx)
in_agg <- addr_tx_lbl[, .(
  n_input_txs         = .N,
  n_illicit_in_txs    = sum(tx_label==1L, na.rm=TRUE),
  n_licit_in_txs      = sum(tx_label==0L, na.rm=TRUE),
  n_unknown_in_txs    = sum(tx_label==-1L, na.rm=TRUE)
), by=address]

# Aggregate output side (tx → wallet)
out_agg <- tx_addr_lbl[, .(
  n_output_txs        = .N,
  n_illicit_out_txs   = sum(tx_label==1L, na.rm=TRUE),
  n_licit_out_txs     = sum(tx_label==0L, na.rm=TRUE),
  n_unknown_out_txs   = sum(tx_label==-1L, na.rm=TRUE)
), by=address]

# Merge cả 2
graph_feat <- merge(in_agg, out_agg, by="address", all=TRUE)
for (cn in names(graph_feat)[-1]) graph_feat[[cn]][is.na(graph_feat[[cn]])] <- 0L

# Tính features tổng hợp
graph_feat[, `:=`(
  total_connected_txs  = n_input_txs + n_output_txs,
  total_illicit_txs    = n_illicit_in_txs + n_illicit_out_txs,
  total_licit_txs      = n_licit_in_txs + n_licit_out_txs,
  pct_illicit_neighbors = (n_illicit_in_txs + n_illicit_out_txs) /
                          pmax(n_input_txs + n_output_txs, 1)
)]

GRF_FEAT <- setdiff(names(graph_feat), "address")
cat(sprintf("  Graph features tạo ra: %d\n", length(GRF_FEAT)))
cat(sprintf("  Wallets có ít nhất 1 tx kết nối: %s (%.1f%%)\n",
    format(sum(graph_feat$total_connected_txs > 0), big.mark=","),
    sum(graph_feat$total_connected_txs > 0) / nrow(graph_feat) * 100))

# Kiểm tra illicit signal
labeled_g <- merge(graph_feat, wal_dt[label>=0, .(address,label)], by="address")
cat(sprintf("  Mean pct_illicit_neighbors — illicit wallets: %.4f\n",
    mean(labeled_g[label==1]$pct_illicit_neighbors)))
cat(sprintf("  Mean pct_illicit_neighbors — licit wallets  : %.4f\n",
    mean(labeled_g[label==0]$pct_illicit_neighbors)))

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 5: GỘP WALLET FEATURES ĐẦY ĐỦ
# ══════════════════════════════════════════════════════════════════════════════
sec("BƯỚC 5: Merge Wallet Features (behavioral + graph)")

wal_full <- merge(wal_dt, graph_feat, by="address", all.x=TRUE)
for (cn in GRF_FEAT) wal_full[[cn]][is.na(wal_full[[cn]])] <- 0

ALL_WAL_FEAT <- c(WAL_FEAT, GRF_FEAT)
cat(sprintf("  Features tổng cộng: %d (= %d behavioral + %d graph)\n",
    length(ALL_WAL_FEAT), length(WAL_FEAT), length(GRF_FEAT)))

# Kiểm tra sạch
n_nan_full <- sum(is.na(wal_full[, ..ALL_WAL_FEAT]))
n_inf_full <- sum(sapply(wal_full[, ..ALL_WAL_FEAT], function(x) sum(is.infinite(x))))
cat(sprintf("  NaN: %d | Inf: %d %s\n", n_nan_full, n_inf_full,
    ifelse(n_nan_full==0 && n_inf_full==0, "✓ CLEAN", "⚠ VẤN ĐỀ")))

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 6: XỬ LÝ TRANSACTION FEATURES (file 4 + 5)
# ══════════════════════════════════════════════════════════════════════════════
sec("BƯỚC 6: Transaction Features")

TX_ID    <- "txId"
TX_FEAT  <- setdiff(names(tx_feat), TX_ID)
tx_feat[, txId := as.character(txId)]
tx_cls[, txId  := as.character(txId)]
tx_edge[, txId1 := as.character(txId1)]
tx_edge[, txId2 := as.character(txId2)]

cat(sprintf("  TX features: %d cols\n", length(TX_FEAT)))
tx_nodes <- merge(tx_cls[, .(txId, tx_label, class)],
                  tx_feat[, .(txId)], by="txId", all.x=TRUE)
cat(sprintf("  TX nodes: %s | illicit=%d | licit=%d\n",
    format(nrow(tx_cls), big.mark=","),
    sum(tx_cls$tx_label==1L), sum(tx_cls$tx_label==0L)))

# NaN check
n_nan_tx <- sum(is.na(tx_feat[, ..TX_FEAT]))
cat(sprintf("  NaN trong tx features: %d %s\n",
    n_nan_tx, ifelse(n_nan_tx==0, "✓", "⚠")))

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 7: SCALE TẤT CẢ FEATURES
# ══════════════════════════════════════════════════════════════════════════════
sec("BƯỚC 7: Feature Scaling")

log1p_minmax <- function(x, x_ref=x) {
  lx <- log1p(pmax(x,0)); lr <- log1p(pmax(x_ref,0))
  lo <- min(lr,na.rm=T); hi <- max(lr,na.rm=T)
  if (hi==lo) return(rep(0.5, length(lx)))
  pmin(pmax((lx-lo)/(hi-lo), 0), 1)
}
robust_scale <- function(x, x_ref=x) {
  md <- median(x_ref,na.rm=T); iq <- IQR(x_ref,na.rm=T)
  if (iq>0) return(pmin(pmax((x-md)/iq, -5), 5))
  sg <- sd(x_ref,na.rm=T)
  if (sg>0) return(pmin(pmax((x-md)/sg, -5), 5))
  rep(0, length(x))
}

# --- Scale wallet features ---
labeled_ref <- wal_full[label >= 0]
X_wal <- as.data.frame(wal_full[, ..ALL_WAL_FEAT])
X_ref <- as.data.frame(labeled_ref[, ..ALL_WAL_FEAT])

X_wal_sc <- X_wal
for (cn in c(btc_cols, timing_cols))
  X_wal_sc[[cn]] <- log1p_minmax(X_wal[[cn]], X_ref[[cn]])
for (cn in count_cols)
  X_wal_sc[[cn]] <- robust_scale(X_wal[[cn]], X_ref[[cn]])
# Graph features: cả 2 loại
int_grf <- c("n_input_txs","n_output_txs","total_connected_txs",
             "n_illicit_in_txs","n_illicit_out_txs","total_illicit_txs",
             "n_licit_in_txs","n_licit_out_txs","total_licit_txs",
             "n_unknown_in_txs","n_unknown_out_txs")
rat_grf <- c("pct_illicit_neighbors")
for (cn in intersect(int_grf, GRF_FEAT))
  X_wal_sc[[cn]] <- log1p_minmax(X_wal[[cn]], X_ref[[cn]])
for (cn in intersect(rat_grf, GRF_FEAT))
  X_wal_sc[[cn]] <- pmin(pmax(X_wal[[cn]], 0), 1)  # ratio: already [0,1]

# Fill NaN còn lại
for (cn in ALL_WAL_FEAT) X_wal_sc[[cn]][is.na(X_wal_sc[[cn]])] <- 0
cat(sprintf("  Wallet features scaled: %d | NaN=%d ✓\n",
    length(ALL_WAL_FEAT), sum(is.na(X_wal_sc))))

# --- Scale TX features (MinMax đơn giản) ---
X_tx <- as.data.frame(tx_feat[, ..TX_FEAT])
X_tx_sc <- as.data.frame(lapply(X_tx, function(x) {
  lo <- min(x, na.rm=T); hi <- max(x, na.rm=T)
  if (hi==lo) return(rep(0, length(x)))
  (x-lo)/(hi-lo)
}))
for (cn in TX_FEAT) X_tx_sc[[cn]][is.na(X_tx_sc[[cn]])] <- 0
cat(sprintf("  TX features scaled: %d | NaN=%d ✓\n",
    length(TX_FEAT), sum(is.na(X_tx_sc))))

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 8: FEATURE SELECTION (Wallet)
# ══════════════════════════════════════════════════════════════════════════════
sec("BƯỚC 8: Feature Selection")

# Chỉ làm feature selection trên LABELED wallets để tránh bias từ unlabeled
labeled_sc_idx <- which(wal_full$label >= 0)
X_lab_sc       <- X_wal_sc[labeled_sc_idx, , drop=FALSE]
y_lab          <- wal_full$label[labeled_sc_idx]

# ── 8a: Loại ZERO-VARIANCE features ──────────────────────────────────────────
cat("[8a] Zero-variance features:\n")
feat_var <- sapply(X_wal_sc, var, na.rm=TRUE)
zero_var_feats <- names(feat_var)[feat_var == 0]
if (length(zero_var_feats) > 0) {
  cat(sprintf("  ⚠ Loại %d zero-variance: %s\n",
      length(zero_var_feats), paste(zero_var_feats, collapse=", ")))
} else {
  cat("  ✓ Không có zero-variance feature\n")
}

# ── 8b: Loại NEAR-ZERO VARIANCE (< 0.0001) ───────────────────────────────────
nzv_feats <- names(feat_var)[feat_var < 0.0001 & feat_var > 0]
if (length(nzv_feats) > 0) {
  cat(sprintf("  ⚠ Near-zero variance: %d features\n", length(nzv_feats)))
} else {
  cat("  ✓ Không có near-zero variance feature\n")
}
feats_to_remove <- unique(c(zero_var_feats, nzv_feats))

# ── 8c: Loại HIGH CORRELATION (|r| > 0.95) ───────────────────────────────────
cat("[8b] High correlation (|r| > 0.95):\n")
remaining_feats <- setdiff(ALL_WAL_FEAT, feats_to_remove)
X_corr <- X_lab_sc[, remaining_feats, drop=FALSE]
corr_mat <- cor(X_corr, use="pairwise.complete.obs")
corr_mat[is.na(corr_mat)] <- 0

high_corr_remove <- c()
checked <- c()
for (i in seq_len(ncol(corr_mat) - 1)) {
  fi <- remaining_feats[i]
  if (fi %in% high_corr_remove) next
  for (j in seq(i + 1, ncol(corr_mat))) {
    fj <- remaining_feats[j]
    if (fj %in% high_corr_remove) next
    if (abs(corr_mat[fi, fj]) > 0.95) {
      # Giữ feature có discriminative power cao hơn
      disc_i <- abs(mean(X_lab_sc[y_lab==1, fi]) - mean(X_lab_sc[y_lab==0, fi]))
      disc_j <- abs(mean(X_lab_sc[y_lab==1, fj]) - mean(X_lab_sc[y_lab==0, fj]))
      remove_feat <- if (disc_i >= disc_j) fj else fi
      high_corr_remove <- unique(c(high_corr_remove, remove_feat))
      checked <- c(checked, sprintf("%s↔%s (r=%.2f)", fi, fj, corr_mat[fi,fj]))
    }
  }
}
cat(sprintf("  Cặp tương quan cao: %d | Loại: %d features\n",
    length(checked), length(high_corr_remove)))
if (length(high_corr_remove) > 0 && length(high_corr_remove) <= 20)
  cat(sprintf("  Loại: %s\n", paste(high_corr_remove, collapse=", ")))
feats_to_remove <- unique(c(feats_to_remove, high_corr_remove))

# ── 8d: Discriminative Power Ranking ─────────────────────────────────────────
cat("[8c] Discriminative power (top vs bottom features):\n")
remaining_feats2 <- setdiff(ALL_WAL_FEAT, feats_to_remove)
ill_mean <- colMeans(X_lab_sc[y_lab==1, remaining_feats2, drop=FALSE], na.rm=TRUE)
lic_mean <- colMeans(X_lab_sc[y_lab==0, remaining_feats2, drop=FALSE], na.rm=TRUE)
pool_sd  <- apply(X_lab_sc[, remaining_feats2, drop=FALSE], 2, sd, na.rm=TRUE)
disc     <- abs(ill_mean - lic_mean) / pmax(pool_sd, 1e-8)
disc_sorted <- sort(disc, decreasing=TRUE)
cat("  Top 10 features (phân biệt tốt nhất):\n")
for (i in seq_len(min(10, length(disc_sorted))))
  cat(sprintf("    %2d. %-42s %.4f\n", i, names(disc_sorted)[i], disc_sorted[i]))
cat("  Bottom 5 features (ít phân biệt nhất):\n")
bot <- tail(disc_sorted, 5)
for (i in seq_along(bot))
  cat(sprintf("    %2d. %-42s %.4f\n", i, names(bot)[i], bot[i]))

# ── 8e: Tổng kết và cập nhật feature list ────────────────────────────────────
SELECTED_WAL_FEAT <- remaining_feats2  # Giữ tất cả sau khi loại zero-var + high-corr
cat(sprintf("\n[8d] Tổng kết Feature Selection:\n"))
cat(sprintf("  Ban đầu      : %d features\n", length(ALL_WAL_FEAT)))
cat(sprintf("  Loại zero-var: %d features\n", length(zero_var_feats) + length(nzv_feats)))
cat(sprintf("  Loại high-cor: %d features\n", length(high_corr_remove)))
cat(sprintf("  Còn lại (FINAL): %d features ✓\n", length(SELECTED_WAL_FEAT)))

# Cập nhật X_wal_sc chỉ giữ selected features
X_wal_sc <- X_wal_sc[, SELECTED_WAL_FEAT, drop=FALSE]
ALL_WAL_FEAT <- SELECTED_WAL_FEAT

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 9: SINGLE-PASS TEMPORAL SPLIT (WALLET)
# ══════════════════════════════════════════════════════════════════════════════
sec("BƯỚC 9: Temporal Split")

# Wallet — theo time_last
labeled_wal   <- wal_full[label >= 0][order(time_last)]
n_lab         <- nrow(labeled_wal)
n_tr <- floor(0.70 * n_lab); n_va <- floor(0.10 * n_lab)
tr_wal <- labeled_wal[1:n_tr]
va_wal <- labeled_wal[(n_tr+1):(n_tr+n_va)]
te_wal <- labeled_wal[(n_tr+n_va+1):n_lab]

get_row_idx <- function(addrs) which(wal_full$address %in% addrs) - 1L
tr_idx <- get_row_idx(tr_wal$address)
va_idx <- get_row_idx(va_wal$address)
te_idx <- get_row_idx(te_wal$address)

ir_train <- sum(tr_wal$label==0) / max(1, sum(tr_wal$label==1))

cat(sprintf("  Wallet — Train: %s | Val: %s | Test: %s\n",
    format(length(tr_idx),big.mark=","),
    format(length(va_idx),big.mark=","),
    format(length(te_idx),big.mark=",")))
cat(sprintf("  Train illicit=%.2f%% | Test illicit=%.2f%% | IR=%.1f:1\n",
    mean(tr_wal$label==1)*100, mean(te_wal$label==1)*100, ir_train))

# TX — theo txId order (no time info → random split 70/10/20)
tx_lab <- tx_cls[tx_label >= 0]
set.seed(42)
tx_shuf <- sample(nrow(tx_lab))
n_tr_t  <- floor(0.70*nrow(tx_lab)); n_va_t <- floor(0.10*nrow(tx_lab))
tr_tx_id <- tx_lab$txId[tx_shuf[1:n_tr_t]]
va_tx_id <- tx_lab$txId[tx_shuf[(n_tr_t+1):(n_tr_t+n_va_t)]]
te_tx_id <- tx_lab$txId[tx_shuf[(n_tr_t+n_va_t+1):nrow(tx_lab)]]
tr_tx_idx <- which(tx_feat$txId %in% tr_tx_id) - 1L
va_tx_idx <- which(tx_feat$txId %in% va_tx_id) - 1L
te_tx_idx <- which(tx_feat$txId %in% te_tx_id) - 1L
cat(sprintf("  TX — Train: %s | Val: %s | Test: %s\n",
    format(length(tr_tx_idx),big.mark=","),
    format(length(va_tx_idx),big.mark=","),
    format(length(te_tx_idx),big.mark=",")))


# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 9: SAVE TẤT CẢ FILES
# ══════════════════════════════════════════════════════════════════════════════
sec("BƯỚC 9: Save Files")

# --- Wallet ---
wal_nodes_out <- data.table(
  row_idx    = seq_len(nrow(wal_full)) - 1L,
  address    = wal_full$address,
  label      = wal_full$label,
  time_first = wal_full$time_first,
  time_last  = wal_full$time_last,
  n_tsteps   = wal_full$n_tsteps
)
fwrite(wal_nodes_out,         file.path(PROC_W, "wallet_nodes.csv"))
fwrite(as.data.table(X_wal_sc), file.path(PROC_W, "wallet_features_full.csv"))
fwrite(data.table(idx=as.integer(tr_idx)), file.path(PROC_W, "train_idx.csv"))
fwrite(data.table(idx=as.integer(va_idx)), file.path(PROC_W, "val_idx.csv"))
fwrite(data.table(idx=as.integer(te_idx)), file.path(PROC_W, "test_idx.csv"))
write_json(as.list(ALL_WAL_FEAT), file.path(PROC_W,"feature_names.json"), auto_unbox=TRUE)
cat(sprintf("  Wallet:\n"))
cat(sprintf("    wallet_nodes.csv        : %s rows\n", format(nrow(wal_nodes_out),big.mark=",")))
cat(sprintf("    wallet_features_full.csv: %s rows × %d cols\n",
    format(nrow(X_wal_sc),big.mark=","), ncol(X_wal_sc)))
cat(sprintf("    train/val/test_idx.csv  : %d/%d/%d\n",
    length(tr_idx), length(va_idx), length(te_idx)))

# --- TX ---
tx_nodes_out <- merge(tx_cls[, .(txId, label=tx_label)],
                      data.table(txId=tx_feat$txId,
                                 row_idx=seq_len(nrow(tx_feat))-1L),
                      by="txId", all.x=TRUE)
fwrite(tx_nodes_out,          file.path(PROC_T, "tx_nodes.csv"))
fwrite(as.data.table(X_tx_sc), file.path(PROC_T, "tx_features.csv"))
fwrite(tx_edge,               file.path(PROC_T, "tx_edgelist.csv"))
fwrite(data.table(idx=as.integer(tr_tx_idx)), file.path(PROC_T,"train_idx.csv"))
fwrite(data.table(idx=as.integer(va_tx_idx)), file.path(PROC_T,"val_idx.csv"))
fwrite(data.table(idx=as.integer(te_tx_idx)), file.path(PROC_T,"test_idx.csv"))
cat(sprintf("  TX:\n"))
cat(sprintf("    tx_nodes.csv  : %s rows\n", format(nrow(tx_nodes_out),big.mark=",")))
cat(sprintf("    tx_features.csv: %s rows × %d cols\n",
    format(nrow(X_tx_sc),big.mark=","), ncol(X_tx_sc)))
cat(sprintf("    tx_edgelist.csv: %s edges\n", format(nrow(tx_edge),big.mark=",")))

# --- Report ---
rpt <- list(
  files_used = c("wallets_features_classes_combined.csv","wallets_classes.csv",
                 "wallets_features.csv","txs_features.csv","txs_classes.csv",
                 "txs_edgelist.csv","AddrTx_edgelist.csv","TxAddr_edgelist.csv"),
  wallet = list(
    unique_wallets=uniqueN(wal_full$address),
    n_labeled=nrow(labeled_wal), n_illicit=sum(wal_dt$label==1L),
    n_licit=sum(wal_dt$label==0L), imbalance=round(ir_train,1),
    behavioral_features=length(WAL_FEAT), graph_features=length(GRF_FEAT),
    total_features=length(ALL_WAL_FEAT),
    split=list(train=length(tr_idx), val=length(va_idx), test=length(te_idx)),
    scale_pos_weight=round(ir_train,0)
  ),
  tx = list(
    total_txs=nrow(tx_cls), n_illicit=sum(tx_cls$tx_label==1L),
    n_licit=sum(tx_cls$tx_label==0L), n_features=length(TX_FEAT),
    n_edges=nrow(tx_edge),
    split=list(train=length(tr_tx_idx),val=length(va_tx_idx),test=length(te_tx_idx))
  ),
  bugs_fixed=bugs_log,
  label_mismatch=anomaly_log$label_mismatch,
  nan_final_wallet=sum(is.na(X_wal_sc)),
  nan_final_tx=sum(is.na(X_tx_sc))
)
write_json(rpt, file.path(OUT, "preprocessing_report.json"), pretty=TRUE, auto_unbox=TRUE)
cat(sprintf("  preprocessing_report.json: OK\n"))

# ══════════════════════════════════════════════════════════════════════════════
# BÁO CÁO CUỐI
# ══════════════════════════════════════════════════════════════════════════════
cat("\n╔══════════════════════════════════════════╗\n")
cat("║  KẾT QUẢ — 8 FILES ĐÃ XỬ LÝ ĐẦY ĐỦ     ║\n")
cat("╚══════════════════════════════════════════╝\n")
cat(sprintf("  ✅ WALLET: %s wallets | %d features (%d behavioral + %d graph)\n",
    format(nrow(wal_full),big.mark=","), length(ALL_WAL_FEAT),
    length(WAL_FEAT), length(GRF_FEAT)))
cat(sprintf("  ✅ TX    : %s transactions | %d features\n",
    format(nrow(tx_feat),big.mark=","), length(TX_FEAT)))
cat(sprintf("  ✅ GRAPH : %s tx→tx edges | %s addr→tx | %s tx→addr\n",
    format(nrow(tx_edge),big.mark=","),
    format(nrow(addr_tx),big.mark=","),
    format(nrow(tx_addr),big.mark=",")))
cat(sprintf("  ✅ NaN wallet: %d | NaN tx: %d\n",
    sum(is.na(X_wal_sc)), sum(is.na(X_tx_sc))))
cat(sprintf("  ✅ scale_pos_weight (XGBoost wallet): %.0f\n\n", ir_train))
cat("=== TIỀN XỬ LÝ HOÀN THÀNH ===\n")
cat("Next: Rscript R/w2_wallet_train.R\n\n")

# === 36-WINDOW EXPANDING TEMPORAL SPLITS ===
# Tao dinh nghia 36 windows, luu vao processed/wallet/windows.json
# Moi window: train<=t_cut, val=t_cut+1, test=t_cut+2..t_cut+3
cat("\n[STEP 8] Generating 36-window temporal splits...\n")
if (exists("wal_dt") && !is.null(wal_dt)) {
  T_MAX_W <- max(wal_dt$time_last, na.rm=TRUE)
  win_list <- vector("list", 36L)
  for (.i in 1:36) {
    .t <- 7L + .i
    .te1 <- .t + 2L; .te2 <- min(.t + 3L, T_MAX_W)
    win_list[[.i]] <- list(
      window_id=.i, train_max=.t, val_time=.t+1L,
      test_min=.te1, test_max=.te2,
      n_illicit_tr=sum(wal_dt$time_last<=.t & wal_dt$label==1L, na.rm=TRUE),
      n_illicit_te=sum(wal_dt$time_last>=.te1 & wal_dt$time_last<=.te2 &
                       wal_dt$label==1L, na.rm=TRUE),
      n_val=sum(wal_dt$time_last==(.t+1L) & wal_dt$label%in%c(0L,1L), na.rm=TRUE),
      usable=TRUE)
  }
  write_json(win_list, file.path(PROC_W,"windows.json"), pretty=TRUE, auto_unbox=TRUE)
  cat(sprintf("  windows.json: 36 windows saved (t08-t43)\n"))
}

# === SINH FOLDER-BASED WINDOW SPLITS ===
# Tao 36 folder windows/win_XX/ voi train/val/test_idx.csv + meta.json
cat("[STEP 8b] Generating window folders in processed/wallet/windows/...\n")
if (exists("wal_dt") && exists("PROC_W")) {
  .wdir <- file.path(PROC_W, "windows")
  dir.create(.wdir, showWarnings=FALSE, recursive=TRUE)
  .wins <- fromJSON(file.path(PROC_W, "windows.json"), simplifyDataFrame=FALSE)
  .y    <- wal_dt$label
  for (.w in .wins) {
    .d <- file.path(.wdir, sprintf("win_%02d", .w$window_id))
    dir.create(.d, showWarnings=FALSE)
    .tr <- which(wal_dt$time_last<=.w$train_max & .y%in%c(0L,1L))-1L
    .va <- which(wal_dt$time_last==.w$val_time  & .y%in%c(0L,1L))-1L
    .te <- which(wal_dt$time_last>=.w$test_min  & wal_dt$time_last<=.w$test_max & .y%in%c(0L,1L))-1L
    fwrite(data.table(idx=.tr), file.path(.d,"train_idx.csv"))
    fwrite(data.table(idx=.va), file.path(.d,"val_idx.csv"))
    fwrite(data.table(idx=.te), file.path(.d,"test_idx.csv"))
    write_json(list(window_id=.w$window_id,train_max=.w$train_max,
                    val_time=.w$val_time,test_min=.w$test_min,test_max=.w$test_max,
                    n_train=length(.tr),n_val=length(.va),n_test=length(.te)),
               file.path(.d,"meta.json"), pretty=TRUE, auto_unbox=TRUE)
  }
  cat(sprintf("  36 window folders saved: processed/wallet/windows/win_01/ ... win_36/\n"))
}

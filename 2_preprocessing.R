# 2_preprocessing.R — Elliptic++ Preprocessing Pipeline (FIXED v2)
# ─────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURE (từ exploration):
#   txs_features.csv: 184 cols = txId + time + 93 Local + 72 Aggregate + 17 Augmented
#
#   - Local_feature_1..93    : ĐÃ pre-scaled bởi Elliptic++ (range ~-7 đến +265)
#   - Aggregate_feature_1..72: ĐÃ pre-scaled bởi Elliptic++ (range ~-4 đến +252)
#   - Augmented (17 cols)    : BTC raw values (total_BTC, fees, size, BTC amounts...)
#                              965 rows có NaN → fill median
#   - Graph features (9)     : Tính thêm (degree, pagerank, neighbor ratio...)
#
# SCALING STRATEGY (FIXED):
#   1. Local + Aggregate → KHÔNG scale lại (đã pre-scaled) — chỉ clip outlier nhẹ
#   2. Augmented         → Fill NaN + Log1p + Min-Max [0,1]
#   3. Graph features    → Robust scale với fallback std (fix IQR=0 bug)
# ─────────────────────────────────────────────────────────────────────────────

.libPaths(c("C:/Users/Trinc/R/win-library/4.5", .libPaths()))

library(data.table)
library(igraph)
library(jsonlite)
library(DBI)
library(RSQLite)

set.seed(42)

DATASET_DIR <- "Dataset"
PROC_DIR    <- "processed"
SPLITS_DIR  <- file.path(PROC_DIR, "splits")
dir.create(PROC_DIR,   showWarnings = FALSE, recursive = TRUE)
dir.create(SPLITS_DIR, showWarnings = FALSE, recursive = TRUE)

cat("=======================================================\n")
cat("  Elliptic++ Preprocessing v2 — Smart Scaling Fix\n")
cat("=======================================================\n\n")

# ═══════════════════════════════════════════════════════════════════════════
# BƯỚC 1: ĐỌC FEATURES
# ═══════════════════════════════════════════════════════════════════════════
cat("[1/9] Reading txs_features.csv ...\n")
feats <- fread(file.path(DATASET_DIR, "txs_features.csv"))
setnames(feats, "Time step", "time")

tx_ids     <- feats$txId
time_steps <- feats$time
feat_cols  <- setdiff(names(feats), c("txId", "time"))
X_raw      <- as.data.frame(feats[, ..feat_cols])

# Phân loại 3 nhóm features
local_cols     <- grep("^Local_feature", feat_cols, value = TRUE)      # 93 cols
agg_cols       <- grep("^Aggregate_feature", feat_cols, value = TRUE)  # 72 cols
augmented_cols <- c("in_txs_degree", "out_txs_degree", "total_BTC",
                    "fees", "size", "num_input_addresses", "num_output_addresses",
                    "in_BTC_min", "in_BTC_max", "in_BTC_mean", "in_BTC_median",
                    "in_BTC_total", "out_BTC_min", "out_BTC_max", "out_BTC_mean",
                    "out_BTC_median", "out_BTC_total")

cat(sprintf("   Total features        : %d\n", ncol(X_raw)))
cat(sprintf("   Local features        : %d (pre-scaled, NO re-scale)\n", length(local_cols)))
cat(sprintf("   Aggregate features    : %d (pre-scaled, NO re-scale)\n", length(agg_cols)))
cat(sprintf("   Augmented BTC cols    : %d (raw values, need scaling)\n", length(augmented_cols)))
cat(sprintf("   Transactions          : %s\n", format(nrow(X_raw), big.mark = ",")))

# ═══════════════════════════════════════════════════════════════════════════
# BƯỚC 2: XỬ LÝ NaN — CHỈ AUGMENTED COLS
# ═══════════════════════════════════════════════════════════════════════════
cat("\n[2/9] Handling Missing Values (NaN) ...\n")

nan_total <- sum(is.na(X_raw))
nan_rows  <- sum(rowSums(is.na(X_raw)) > 0)
cat(sprintf("   Total NaN cells       : %s in %d rows\n",
            format(nan_total, big.mark = ","), nan_rows))
cat("   Strategy: fill NaN with column MEDIAN (only augmented cols have NaN)\n")

# Fill median của cột (chỉ augmented cols có NaN)
for (cn in augmented_cols) {
  if (any(is.na(X_raw[[cn]]))) {
    med <- median(X_raw[[cn]], na.rm = TRUE)
    X_raw[[cn]][is.na(X_raw[[cn]])] <- med
    cat(sprintf("   Filled %-30s with median=%.4f\n", cn, med))
  }
}
cat(sprintf("   Remaining NaN         : %d (should be 0)\n", sum(is.na(X_raw))))

# ═══════════════════════════════════════════════════════════════════════════
# BƯỚC 3: WINSORIZE — CHỈ AUGMENTED COLS (BTC values có outliers nặng)
# ═══════════════════════════════════════════════════════════════════════════
cat("\n[3/9] Winsorizing Augmented BTC features (1st-99th percentile) ...\n")

winsorize <- function(x, lo = 0.01, hi = 0.99) {
  q <- quantile(x, probs = c(lo, hi), na.rm = TRUE)
  pmax(pmin(x, q[2]), q[1])
}

# Chỉ winsorize augmented cols — Local/Aggregate đã được xử lý bởi Elliptic
for (cn in augmented_cols) {
  X_raw[[cn]] <- winsorize(X_raw[[cn]])
}
cat(sprintf("   Winsorized %d augmented cols\n", length(augmented_cols)))

# ═══════════════════════════════════════════════════════════════════════════
# BƯỚC 4: ĐỌC NHÃN
# ═══════════════════════════════════════════════════════════════════════════
cat("\n[4/9] Reading labels ...\n")
classes <- fread(file.path(DATASET_DIR, "txs_classes.csv"))
classes[, label := fcase(
  class == 1L,  1L,   # ILLICIT
  class == 2L,  0L,   # LICIT
  default      = -1L  # UNKNOWN
)]

y_df <- merge(data.table(txId = tx_ids), classes[, .(txId, label)],
              by = "txId", all.x = TRUE)
y_df[is.na(label), label := -1L]
y <- y_df$label

lbl_names <- c("-1" = "unknown", "0" = "licit", "1" = "illicit")
tbl <- table(y)
for (lv in names(tbl)) {
  cat(sprintf("   %2s (%-9s): %8s  (%.1f%%)\n",
              lv, lbl_names[lv], format(tbl[[lv]], big.mark = ","),
              tbl[[lv]] / length(y) * 100))
}

# ═══════════════════════════════════════════════════════════════════════════
# BƯỚC 5: GRAPH FEATURE ENGINEERING
# ═══════════════════════════════════════════════════════════════════════════
cat("\n[5/9] Building graph + computing graph features ...\n")

edges_raw  <- fread(file.path(DATASET_DIR, "txs_edgelist.csv"))
id2idx     <- setNames(seq_along(tx_ids) - 1L, as.character(tx_ids))
valid      <- as.character(edges_raw$txId1) %in% names(id2idx) &
              as.character(edges_raw$txId2) %in% names(id2idx)
edges_filt <- edges_raw[valid]
src_idx    <- id2idx[as.character(edges_filt$txId1)]
dst_idx    <- id2idx[as.character(edges_filt$txId2)]
cat(sprintf("   Edges kept            : %s / %s\n",
            format(sum(valid), big.mark = ","), format(nrow(edges_raw), big.mark = ",")))

n_nodes <- length(tx_ids)
g <- graph_from_data_frame(
  d        = data.frame(from = src_idx + 1L, to = dst_idx + 1L),
  directed = TRUE,
  vertices = data.frame(name = seq_len(n_nodes))
)

in_deg  <- degree(g, mode = "in")
out_deg <- degree(g, mode = "out")
pr      <- page_rank(g, directed = TRUE)$vector

# ── 1-hop neighbor illicit ratio (vectorized) ──────────────────────────────
cat("   Computing 1-hop neighbor illicit ratio...\n")
node_label_dt <- data.table(node = seq_len(n_nodes), label = y)
all_edges_dt  <- rbind(
  data.table(src = src_idx + 1L, dst = dst_idx + 1L),
  data.table(src = dst_idx + 1L, dst = src_idx + 1L)
)
nbr_dt  <- merge(all_edges_dt, node_label_dt, by.x = "dst", by.y = "node")
nbr_dt  <- nbr_dt[label >= 0]
nbr_agg <- nbr_dt[, .(
  nbr_illicit = mean(label == 1L),
  nbr_licit   = mean(label == 0L)
), by = src]

nbr_illicit <- numeric(n_nodes)
nbr_licit   <- numeric(n_nodes)
nbr_illicit[nbr_agg$src] <- nbr_agg$nbr_illicit
nbr_licit[nbr_agg$src]   <- nbr_agg$nbr_licit

cat(sprintf("   nbr_illicit: mean=%.4f, max=%.4f, nonzero=%.1f%%\n",
            mean(nbr_illicit), max(nbr_illicit), mean(nbr_illicit > 0) * 100))

# ── 2-hop neighbor illicit ratio ───────────────────────────────────────────
cat("   Computing 2-hop neighbor illicit ratio...\n")
hop2_dt    <- merge(all_edges_dt[, .(src, hop1 = dst)],
                    all_edges_dt[, .(hop1 = src, hop2 = dst)],
                    by = "hop1", allow.cartesian = TRUE)
hop2_dt    <- hop2_dt[src != hop2]
hop2_label <- merge(hop2_dt[, .(src, hop2)],
                    node_label_dt[, .(node, label)],
                    by.x = "hop2", by.y = "node")
hop2_label <- hop2_label[label >= 0]
hop2_agg   <- hop2_label[, .(nbr2_illicit = mean(label == 1L)), by = src]
nbr2_illicit <- numeric(n_nodes)
nbr2_illicit[hop2_agg$src] <- hop2_agg$nbr2_illicit
rm(hop2_dt, hop2_label, hop2_agg); gc(verbose = FALSE)

# ── Clustering + Triangle count ────────────────────────────────────────────
cat("   Computing clustering coefficient + triangle count...\n")
clust_coef     <- transitivity(g, type = "local", isolates = "zero")
clust_coef[is.nan(clust_coef)] <- 0
triangle_count <- count_triangles(as.undirected(g, mode = "collapse"))

graph_feats <- data.frame(
  gf_in_degree              = as.numeric(in_deg),
  gf_out_degree             = as.numeric(out_deg),
  gf_total_degree           = as.numeric(in_deg + out_deg),
  gf_pagerank               = pr,
  gf_neighbor_illicit_ratio = nbr_illicit,
  gf_neighbor_licit_ratio   = nbr_licit,
  gf_2hop_illicit_ratio     = nbr2_illicit,
  gf_clustering_coef        = clust_coef,
  gf_triangle_count         = as.numeric(triangle_count)
)
cat(sprintf("   Graph features added  : %d\n", ncol(graph_feats)))

# ═══════════════════════════════════════════════════════════════════════════
# BƯỚC 6: SCALING — SMART 3-TIER STRATEGY
# ═══════════════════════════════════════════════════════════════════════════
cat("\n[6/9] Smart Scaling (3-tier strategy) ...\n")

# ── Helper functions ─────────────────────────────────────────────────────

# Robust scale với fallback sang std khi IQR=0
robust_scale_fixed <- function(x) {
  med <- median(x, na.rm = TRUE)
  iqr <- IQR(x, na.rm = TRUE)
  if (iqr > 0) return((x - med) / iqr)
  s <- sd(x, na.rm = TRUE)
  if (s > 0)  return((x - med) / s)
  return(rep(0.0, length(x)))   # truly constant
}

# Min-Max scale [0,1]
minmax_scale <- function(x) {
  lo <- min(x, na.rm = TRUE)
  hi <- max(x, na.rm = TRUE)
  if (hi == lo) return(rep(0.0, length(x)))
  (x - lo) / (hi - lo)
}

# Log1p + Min-Max (cho sparse ratio features: neighbor_illicit, 2hop, pagerank)
log1p_minmax <- function(x) {
  minmax_scale(log1p(pmax(x, 0)))
}

# ── Tier 1: Local + Aggregate — NO SCALING ──────────────────────────────
cat("   [Tier 1] Local + Aggregate features: KEEPING AS-IS (pre-scaled by Elliptic++)\n")
X_local_agg <- X_raw[, c(local_cols, agg_cols), drop = FALSE]
# Chỉ clip extreme outliers ở 0.1%-99.9% (phòng ngừa data corruption)
for (cn in names(X_local_agg)) {
  q <- quantile(X_local_agg[[cn]], c(0.001, 0.999), na.rm = TRUE)
  X_local_agg[[cn]] <- pmax(pmin(X_local_agg[[cn]], q[2]), q[1])
}
cat(sprintf("   Kept %d cols (clipped 0.1%%-99.9%% only)\n", ncol(X_local_agg)))

# ── Tier 2: Augmented BTC features — Log1p + Min-Max [0,1] ─────────────
cat("   [Tier 2] Augmented BTC features: Log1p + Min-Max [0,1]\n")
X_aug <- X_raw[, augmented_cols, drop = FALSE]
# Convert all to numeric first
X_aug <- as.data.frame(lapply(X_aug, as.numeric))
# Apply log1p for heavy-tailed BTC distributions, then minmax
X_aug_scaled <- as.data.frame(lapply(X_aug, function(x) {
  x_log <- log1p(pmax(x, 0))   # log1p handles zeros safely
  minmax_scale(x_log)
}))
cat(sprintf("   Scaled %d cols: range check...\n", ncol(X_aug_scaled)))
ranges_ok <- all(sapply(X_aug_scaled, function(x) min(x) >= -0.01 & max(x) <= 1.01))
cat(sprintf("   All in [0,1]: %s\n", if(ranges_ok) "YES ✓" else "NO ✗"))

# ── Tier 3: Graph features — Robust/Std fallback, sparse → Log1p+MinMax ──
cat("   [Tier 3] Graph features: smart scale per feature type\n")
# Sparse ratio features (0..1 range, mostly 0): log1p + minmax
sparse_gf <- c("gf_neighbor_illicit_ratio", "gf_neighbor_licit_ratio",
                "gf_2hop_illicit_ratio", "gf_clustering_coef")
# Count-based features: winsorize + robust scale
count_gf   <- c("gf_in_degree", "gf_out_degree", "gf_total_degree", "gf_triangle_count")
# Probability feature: log1p + minmax
prob_gf    <- c("gf_pagerank")

# Winsorize count features first
for (cn in count_gf) graph_feats[[cn]] <- winsorize(graph_feats[[cn]])

X_graph_scaled <- graph_feats
for (cn in sparse_gf) {
  X_graph_scaled[[cn]] <- log1p_minmax(graph_feats[[cn]])
  cat(sprintf("   %-35s log1p+minmax: range [%.4f, %.4f] nonzero=%.1f%%\n",
              cn, min(X_graph_scaled[[cn]]), max(X_graph_scaled[[cn]]),
              mean(X_graph_scaled[[cn]] > 0) * 100))
}
for (cn in c(count_gf, prob_gf)) {
  X_graph_scaled[[cn]] <- robust_scale_fixed(graph_feats[[cn]])
  cat(sprintf("   %-35s robust: range [%.4f, %.4f]\n",
              cn, min(X_graph_scaled[[cn]]), max(X_graph_scaled[[cn]])))
}

# ── Combine all ───────────────────────────────────────────────────────────
cat("\n   Combining all feature tiers...\n")
X_final <- cbind(X_local_agg, X_aug_scaled, X_graph_scaled)

# Force everything to double (prevent integer columns)
X_final <- as.data.frame(lapply(X_final, as.double))

# Fill any remaining NaN with 0
n_remaining_nan <- sum(is.na(X_final))
if (n_remaining_nan > 0) {
  cat(sprintf("   Filling %d remaining NaN with 0\n", n_remaining_nan))
  X_final[is.na(X_final)] <- 0.0
}

cat(sprintf("   Final feature matrix  : %s x %d\n",
            format(nrow(X_final), big.mark = ","), ncol(X_final)))

# ═══════════════════════════════════════════════════════════════════════════
# BƯỚC 7: VALIDATION CHECKS
# ═══════════════════════════════════════════════════════════════════════════
cat("\n[7/9] Validation checks ...\n")

# Check 1: No NaN
n_nan <- sum(is.na(X_final))
cat(sprintf("   [%s] NaN cells: %d\n", if(n_nan == 0) "✓" else "✗", n_nan))

# Check 2: All double
n_non_double <- sum(sapply(X_final, function(x) !is.double(x)))
cat(sprintf("   [%s] Non-double cols: %d\n", if(n_non_double == 0) "✓" else "✗", n_non_double))

# Check 3: No all-zero columns (var > 0)
col_vars  <- sapply(X_final, var)
n_zerocol <- sum(col_vars == 0)
cat(sprintf("   [%s] All-zero cols: %d\n", if(n_zerocol == 0) "✓" else "✗", n_zerocol))
if (n_zerocol > 0) {
  zero_cols <- names(col_vars[col_vars == 0])
  cat("   Zero-var cols:", paste(zero_cols, collapse = ", "), "\n")
}

# Check 4: IQR=0 count (should be << previous 58)
n_iqr0 <- sum(sapply(X_final, function(x) IQR(x) == 0))
cat(sprintf("   [i] IQR=0 cols remaining: %d (was 58 before fix)\n", n_iqr0))

# Check 5: Graph features sanity
cat(sprintf("   [i] gf_neighbor_illicit_ratio variance: %.6f\n",
            var(X_final$gf_neighbor_illicit_ratio)))
cat(sprintf("   [i] gf_neighbor_illicit_ratio nonzero: %.1f%%\n",
            mean(X_final$gf_neighbor_illicit_ratio > 0) * 100))

# Check 6: Augmented features in [0,1]
aug_ok <- all(sapply(X_aug_scaled, function(x) min(x) >= -0.001 & max(x) <= 1.001))
cat(sprintf("   [%s] Augmented cols in [0,1]\n", if(aug_ok) "✓" else "✗"))

cat(sprintf("\n   Validation: %s\n",
            if(n_nan == 0 & n_non_double == 0 & n_zerocol == 0)
              "ALL PASSED ✓" else "SOME FAILED — check above"))

# ═══════════════════════════════════════════════════════════════════════════
# BƯỚC 8: TEMPORAL SPLITS
# ═══════════════════════════════════════════════════════════════════════════
cat("\n[8/9] Building temporal splits ...\n")

labeled_idx    <- which(y >= 0) - 1L
labeled_times  <- time_steps[y >= 0]
order_idx      <- order(labeled_times)
labeled_sorted <- labeled_idx[order_idx]

n       <- length(labeled_sorted)
n_test  <- floor(0.20 * n)
n_val   <- floor(0.10 * n)
n_train <- n - n_test - n_val
test_idx  <- tail(labeled_sorted, n_test)
val_idx   <- labeled_sorted[(n_train + 1):(n_train + n_val)]
train_idx <- head(labeled_sorted, n_train)

cat(sprintf("   Train : %s | Val : %s | Test : %s\n",
            format(length(train_idx), big.mark = ","),
            format(length(val_idx),   big.mark = ","),
            format(length(test_idx),  big.mark = ",")))

# Class balance in train
y_train    <- y[train_idx + 1]
y_test_set <- y[test_idx + 1]
cat(sprintf("   Train  illicit rate : %.2f%%\n", mean(y_train == 1) * 100))
cat(sprintf("   Test   illicit rate : %.2f%%\n", mean(y_test_set == 1) * 100))

# Rolling temporal windows
unique_times    <- sort(unique(time_steps[y >= 0]))
VAL_STEPS  <- 2L; TEST_STEPS <- 2L; MIN_TRAIN <- 10L
windows_created <- 0L
for (i in seq(MIN_TRAIN + 1L, length(unique_times) - VAL_STEPS - TEST_STEPS + 1L)) {
  tr_t <- unique_times[1:(i - 1)]
  va_t <- unique_times[i:(i + VAL_STEPS - 1)]
  te_t <- unique_times[(i + VAL_STEPS):(i + VAL_STEPS + TEST_STEPS - 1)]
  w_tr <- which((y >= 0) & (time_steps %in% tr_t)) - 1L
  w_va <- which((y >= 0) & (time_steps %in% va_t)) - 1L
  w_te <- which((y >= 0) & (time_steps %in% te_t)) - 1L
  if (!length(w_tr) || !length(w_va) || !length(w_te)) next
  windows_created <- windows_created + 1L
  wdir <- file.path(SPLITS_DIR, sprintf("window_%02d", windows_created))
  dir.create(wdir, showWarnings = FALSE)
  fwrite(data.table(idx = as.integer(w_tr)), file.path(wdir, "train_idx.csv"))
  fwrite(data.table(idx = as.integer(w_va)), file.path(wdir, "val_idx.csv"))
  fwrite(data.table(idx = as.integer(w_te)), file.path(wdir, "test_idx.csv"))
  write_json(list(window_id      = windows_created,
                  train_time_min = min(tr_t), train_time_max = max(tr_t),
                  val_times      = as.list(va_t), test_times = as.list(te_t),
                  n_train = length(w_tr), n_val = length(w_va), n_test = length(w_te)),
             file.path(wdir, "meta.json"), pretty = TRUE, auto_unbox = TRUE)
}
cat(sprintf("   Rolling windows created : %d\n", windows_created))

# ═══════════════════════════════════════════════════════════════════════════
# BƯỚC 9: LƯU FILES
# ═══════════════════════════════════════════════════════════════════════════
cat("\n[9/9] Saving processed files ...\n")

nodes_out <- data.table(
  node_idx = seq_len(n_nodes) - 1L,
  txId     = as.character(tx_ids),
  time     = as.integer(time_steps),
  label    = as.integer(y)
)
edges_out <- data.table(
  src_idx = as.integer(src_idx),
  dst_idx = as.integer(dst_idx)
)

fwrite(nodes_out,                   file.path(PROC_DIR, "nodes.csv"))
fwrite(edges_out,                   file.path(PROC_DIR, "edges.csv"))
fwrite(as.data.table(X_final),      file.path(PROC_DIR, "features_std.csv"))
fwrite(data.table(idx = as.integer(train_idx)), file.path(PROC_DIR, "train_idx.csv"))
fwrite(data.table(idx = as.integer(val_idx)),   file.path(PROC_DIR, "val_idx.csv"))
fwrite(data.table(idx = as.integer(test_idx)),  file.path(PROC_DIR, "test_idx.csv"))

# ── SQLite DATABASE ────────────────────────────────────────────────────────
cat("\n[9b] Building SQLite database ...\n")
db_path <- file.path(PROC_DIR, "elliptic_aml.db")
if (file.exists(db_path)) file.remove(db_path)
con <- dbConnect(RSQLite::SQLite(), db_path)

nodes_sql <- copy(nodes_out)
nodes_sql[, split := "unlabeled"]
nodes_sql[node_idx %in% train_idx, split := "train"]
nodes_sql[node_idx %in% val_idx,   split := "val"]
nodes_sql[node_idx %in% test_idx,  split := "test"]

dbWriteTable(con, "nodes",  as.data.frame(nodes_sql), overwrite = TRUE)
dbWriteTable(con, "edges",  as.data.frame(edges_out),  overwrite = TRUE)

feat_colnames <- c(paste0("f_", seq_len(ncol(X_final))))
feats_sql     <- as.data.frame(X_final)
names(feats_sql) <- feat_colnames
feats_sql <- cbind(node_idx = seq_len(n_nodes) - 1L, feats_sql)
dbWriteTable(con, "features", feats_sql, overwrite = TRUE)

dbWriteTable(con, "split_train", data.frame(idx = as.integer(train_idx)), overwrite = TRUE)
dbWriteTable(con, "split_val",   data.frame(idx = as.integer(val_idx)),   overwrite = TRUE)
dbWriteTable(con, "split_test",  data.frame(idx = as.integer(test_idx)),  overwrite = TRUE)

dbExecute(con, "CREATE INDEX idx_nodes_node_idx ON nodes(node_idx)")
dbExecute(con, "CREATE INDEX idx_nodes_label    ON nodes(label)")
dbExecute(con, "CREATE INDEX idx_nodes_split    ON nodes(split)")
dbExecute(con, "CREATE INDEX idx_nodes_time     ON nodes(time)")
dbExecute(con, "CREATE INDEX idx_edges_src      ON edges(src_idx)")
dbExecute(con, "CREATE INDEX idx_edges_dst      ON edges(dst_idx)")
dbExecute(con, "CREATE INDEX idx_features_nidx  ON features(node_idx)")

dbExecute(con, "DROP VIEW IF EXISTS v_labeled_nodes")
dbExecute(con, "CREATE VIEW v_labeled_nodes AS SELECT * FROM nodes WHERE label >= 0")

dbExecute(con, "DROP VIEW IF EXISTS v_illicit_by_time")
dbExecute(con, "
  CREATE VIEW v_illicit_by_time AS
  SELECT time,
         COUNT(*) AS total,
         SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END) AS n_illicit,
         SUM(CASE WHEN label = 0 THEN 1 ELSE 0 END) AS n_licit,
         ROUND(SUM(CASE WHEN label = 1 THEN 1.0 ELSE 0 END) /
               NULLIF(SUM(CASE WHEN label >= 0 THEN 1 ELSE 0 END), 0), 4) AS illicit_ratio
  FROM nodes GROUP BY time ORDER BY time
")

dbExecute(con, "DROP VIEW IF EXISTS v_label_summary")
dbExecute(con, "
  CREATE VIEW v_label_summary AS
  SELECT label,
         CASE label WHEN -1 THEN 'unknown' WHEN 0 THEN 'licit' WHEN 1 THEN 'illicit' END AS label_name,
         COUNT(*) AS count,
         ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM nodes), 2) AS pct
  FROM nodes GROUP BY label
")

dbExecute(con, "DROP VIEW IF EXISTS v_nodes_preview")
dbExecute(con, "
  CREATE VIEW v_nodes_preview AS
  SELECT n.node_idx, n.txId, n.time, n.label, n.split, f.f_1, f.f_2, f.f_3, f.f_4, f.f_5
  FROM nodes n JOIN features f ON n.node_idx = f.node_idx
")

dbDisconnect(con)
db_size_mb <- round(file.info(db_path)$size / 1024 / 1024, 1)
cat(sprintf("   SQLite DB: %s (%.1f MB)\n", db_path, db_size_mb))

# ── EDA Summary JSON ───────────────────────────────────────────────────────
eda_summary <- list(
  dataset          = "Elliptic++",
  language         = "R",
  preprocessing_v  = "v2_smart_scaling",
  num_nodes        = n_nodes,
  num_edges        = nrow(edges_out),
  num_features     = ncol(X_final),
  num_time_steps   = length(unique(time_steps)),
  label_counts     = list(
    unknown = sum(y == -1L),
    licit   = sum(y == 0L),
    illicit = sum(y == 1L)
  ),
  labeled_ratio      = round(mean(y >= 0), 4),
  fraud_ratio        = round(sum(y == 1L) / sum(y >= 0), 4),
  imbalance_ratio    = round(sum(y == 0L) / sum(y == 1L), 1),
  illicit_ratio_min  = round(min(tapply(y == 1, time_steps, function(x) mean(x[!is.na(x)]))), 4),
  illicit_ratio_max  = round(max(tapply(y == 1, time_steps, function(x) mean(x[!is.na(x)]))), 4),
  illicit_ratio_mean = round(mean(tapply(y == 1, time_steps, function(x) mean(x[!is.na(x)]))), 4),
  illicit_ratio_std  = round(sd(tapply(y == 1, time_steps, function(x) mean(x[!is.na(x)]))), 4),
  degree_mean        = round(mean(in_deg + out_deg), 1),
  degree_max         = max(in_deg + out_deg),
  degree_isolated    = sum((in_deg + out_deg) == 0),
  scaling_strategy   = list(
    local_aggregate = "no_rescale_kept_as_is_elliptic_prescaled",
    augmented_btc   = "log1p_then_minmax_01",
    graph_features_ratio = "log1p_then_minmax_01",
    graph_features_count = "winsorize_then_robust_scale_std_fallback"
  ),
  validation = list(
    nan_cells    = n_nan,
    zerocol_vars = n_zerocol,
    non_double   = n_non_double
  ),
  recommended_scale_pos_weight = round(sum(y == 0L) / sum(y == 1L)),
  recommended_metric = "PR-AUC"
)
write_json(eda_summary, file.path("outputs", "eda_summary.json"),
           pretty = TRUE, auto_unbox = TRUE)
cat("   Saved outputs/eda_summary.json\n")

cat("\n=== PREPROCESSING v2 COMPLETE ===\n")
cat(sprintf("Features: %d cols | Nodes: %s | Edges: %s\n",
            ncol(X_final),
            format(n_nodes, big.mark = ","),
            format(nrow(edges_out), big.mark = ",")))
cat("Feature groups:\n")
cat(sprintf("  Local+Aggregate : %d cols (pre-scaled, kept as-is)\n",
            length(local_cols) + length(agg_cols)))
cat(sprintf("  Augmented BTC   : %d cols (log1p + minmax [0,1])\n", length(augmented_cols)))
cat(sprintf("  Graph features  : %d cols (smart scale)\n", ncol(graph_feats)))
cat(sprintf("Validation: NaN=%d | ZeroVarCols=%d | NonDouble=%d\n",
            n_nan, n_zerocol, n_non_double))
cat("\nReady for: Rscript R/4_visualization.R && Rscript R/5_train_model.R\n")

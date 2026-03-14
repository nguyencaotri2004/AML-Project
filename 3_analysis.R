# 3_analysis.R — EDA, Data Validation & SQL Database
# ══════════════════════════════════════════════════════════════════════════════
# Bước này thực hiện 3 việc:
#   A. Tạo SQLite database (processed/elliptic_aml.db) liên kết tất cả files
#   B. Phân tích & validate data (EDA)
#   C. Chạy 5 SQL queries quan trọng nhất + giải thích
#
# Đọc từ : processed/wallet/ + processed/tx/
# Xuất ra : processed/elliptic_aml.db + outputs/eda_summary.json
# ══════════════════════════════════════════════════════════════════════════════

.libPaths(c("C:/Users/Trinc/R/win-library/4.5", .libPaths()))
library(data.table)
library(DBI)
library(RSQLite)
library(jsonlite)
set.seed(42)

PROC_W <- "processed/wallet"
PROC_T <- "processed/tx"
DB_OUT <- "processed/elliptic_aml.db"
OUT    <- "outputs"
dir.create(OUT, recursive=TRUE, showWarnings=FALSE)
dir.create("processed", recursive=TRUE, showWarnings=FALSE)

sec <- function(title) {
  cat(sprintf("\n╔══════════════════════════════════════════════════════╗\n"))
  cat(sprintf("║  %-52s║\n", title))
  cat(sprintf("╚══════════════════════════════════════════════════════╝\n"))
}

# ══════════════════════════════════════════════════════════════════════════════
# PHẦN A: TẠO SQLite DATABASE
# ══════════════════════════════════════════════════════════════════════════════
sec("PHẦN A: Tạo SQLite Database")

cat("  Đọc data...\n")
wal_nodes  <- fread(file.path(PROC_W, "wallet_nodes.csv"))
wal_feats  <- fread(file.path(PROC_W, "wallet_features_full.csv"))
feat_names <- fromJSON(file.path(PROC_W, "feature_names.json"))
tx_nodes   <- fread(file.path(PROC_T, "tx_nodes.csv"))
tx_feats   <- fread(file.path(PROC_T, "tx_features.csv"))
tx_edge    <- fread(file.path(PROC_T, "tx_edgelist.csv"))
tr_idx     <- fread(file.path(PROC_W, "train_idx.csv"))$idx
va_idx     <- fread(file.path(PROC_W, "val_idx.csv"))$idx
te_idx     <- fread(file.path(PROC_W, "test_idx.csv"))$idx
cat("  OK\n")

# Tạo DB
if (file.exists(DB_OUT)) file.remove(DB_OUT)
con <- dbConnect(RSQLite::SQLite(), DB_OUT)
dbExecute(con, "PRAGMA journal_mode=WAL")

# Import tables
cat("  Import tables:\n")
wal_feats[, row_idx := seq_len(.N) - 1L]
tx_feats[,  row_idx := seq_len(.N) - 1L]
names(tx_edge) <- c("txId1","txId2")
splits <- data.frame(
  row_idx = c(tr_idx, va_idx, te_idx),
  split   = c(rep("train",length(tr_idx)), rep("val",length(va_idx)), rep("test",length(te_idx)))
)

for (tbl_info in list(
  list(name="wallet_nodes",    df=as.data.frame(wal_nodes)),
  list(name="wallet_features", df=as.data.frame(wal_feats)),
  list(name="tx_nodes",        df=as.data.frame(tx_nodes)),
  list(name="tx_features",     df=as.data.frame(tx_feats)),
  list(name="tx_edgelist",     df=as.data.frame(tx_edge)),
  list(name="wallet_splits",   df=splits)
)) {
  dbWriteTable(con, tbl_info$name, tbl_info$df, overwrite=TRUE)
  cat(sprintf("    ✓ %-20s (%s rows)\n", tbl_info$name,
      format(nrow(tbl_info$df), big.mark=",")))
}

# Indexes
for (sql in c(
  "CREATE INDEX idx_wn_address  ON wallet_nodes(address)",
  "CREATE INDEX idx_wn_label    ON wallet_nodes(label)",
  "CREATE INDEX idx_wn_time     ON wallet_nodes(time_last)",
  "CREATE INDEX idx_wf_rowidx   ON wallet_features(row_idx)",
  "CREATE INDEX idx_tn_txid     ON tx_nodes(txId)",
  "CREATE INDEX idx_tn_label    ON tx_nodes(label)",
  "CREATE INDEX idx_tf_rowidx   ON tx_features(row_idx)",
  "CREATE INDEX idx_e_txid1     ON tx_edgelist(txId1)",
  "CREATE INDEX idx_e_txid2     ON tx_edgelist(txId2)",
  "CREATE INDEX idx_sp_rowidx   ON wallet_splits(row_idx)"
)) dbExecute(con, sql)

# Views
feat_cols_wal <- setdiff(dbListFields(con, "wallet_features"), "row_idx")
feat_sel_wal  <- paste(sprintf("wf.\"%s\"", feat_cols_wal), collapse=",\n    ")

dbExecute(con, sprintf("CREATE VIEW v_wallet_full AS
  SELECT wn.row_idx, wn.address, wn.label,
         wn.time_first, wn.time_last, wn.n_tsteps,
         COALESCE(ws.split,'unlabeled') AS split,
         %s
  FROM wallet_nodes wn
  LEFT JOIN wallet_features wf ON wn.row_idx = wf.row_idx
  LEFT JOIN wallet_splits   ws ON wn.row_idx = ws.row_idx", feat_sel_wal))

dbExecute(con, "CREATE VIEW v_illicit_wallets  AS SELECT * FROM v_wallet_full WHERE label=1")
dbExecute(con, "CREATE VIEW v_licit_wallets    AS SELECT * FROM v_wallet_full WHERE label=0")
dbExecute(con, "CREATE VIEW v_temporal_summary AS
  SELECT time_last AS time_step,
    COUNT(*) AS total_labeled,
    SUM(CASE WHEN label=1 THEN 1 ELSE 0 END) AS n_illicit,
    SUM(CASE WHEN label=0 THEN 1 ELSE 0 END) AS n_licit,
    ROUND(100.0*SUM(CASE WHEN label=1 THEN 1 ELSE 0 END)/COUNT(*),2) AS pct_illicit
  FROM wallet_nodes WHERE label>=0
  GROUP BY time_last ORDER BY time_last")
dbExecute(con, "CREATE VIEW v_split_summary AS
  SELECT ws.split,
    COUNT(*) AS total,
    SUM(CASE WHEN wn.label=1 THEN 1 ELSE 0 END) AS n_illicit,
    SUM(CASE WHEN wn.label=0 THEN 1 ELSE 0 END) AS n_licit,
    ROUND(100.0*SUM(CASE WHEN wn.label=1 THEN 1 ELSE 0 END)/COUNT(*),2) AS pct_illicit,
    MIN(wn.time_last) AS time_min, MAX(wn.time_last) AS time_max
  FROM wallet_splits ws JOIN wallet_nodes wn ON ws.row_idx=wn.row_idx
  GROUP BY ws.split")

db_mb <- round(file.info(DB_OUT)$size/1024/1024, 0)
cat(sprintf("  Database: %s (~%d MB) | %d tables | 5 views | 10 indexes ✅\n",
    DB_OUT, db_mb, length(dbListTables(con))))

# ══════════════════════════════════════════════════════════════════════════════
# PHẦN B: KIỂM TRA DATA SẠCH (Validation)
# ══════════════════════════════════════════════════════════════════════════════
sec("PHẦN B: Kiểm tra Data Sạch")

n_nan_wal <- sum(is.na(wal_feats[, feat_names, with=FALSE]))
n_inf_wal <- sum(sapply(wal_feats[, feat_names, with=FALSE], function(x) sum(is.infinite(x))))
n_nan_tx  <- sum(is.na(tx_feats[, setdiff(names(tx_feats),c("txId","row_idx")), with=FALSE]))

cat(sprintf("  Wallet features : NaN=%d | Inf=%d | %d features × %s rows %s\n",
    n_nan_wal, n_inf_wal, length(feat_names),
    format(nrow(wal_feats), big.mark=","),
    ifelse(n_nan_wal==0 && n_inf_wal==0, "✅ CLEAN", "⚠")))
cat(sprintf("  TX features     : NaN=%d | %d features × %s rows %s\n",
    n_nan_tx, ncol(tx_feats)-2,
    format(nrow(tx_feats), big.mark=","),
    ifelse(n_nan_tx==0, "✅ CLEAN", "⚠")))

n_wal_ill <- sum(wal_nodes$label==1L); n_wal_lic <- sum(wal_nodes$label==0L)
n_wal_unk <- sum(wal_nodes$label==-1L); n_labeled <- n_wal_ill + n_wal_lic
n_tx_ill  <- sum(tx_nodes$label==1L,na.rm=T); n_tx_lic <- sum(tx_nodes$label==0L,na.rm=T)
ir_wallet <- n_wal_lic / max(1, n_wal_ill)
ir_tx     <- n_tx_lic  / max(1, n_tx_ill)

cat(sprintf("  Wallet labels   : Illicit=%s | Licit=%s | Unknown=%s (IR=%.1f:1)\n",
    format(n_wal_ill,big.mark=","), format(n_wal_lic,big.mark=","),
    format(n_wal_unk,big.mark=","), ir_wallet))
cat(sprintf("  TX labels       : Illicit=%s | Licit=%s (IR=%.1f:1)\n",
    format(n_tx_ill,big.mark=","), format(n_tx_lic,big.mark=","), ir_tx))

# Data leakage check
tr_tmax <- max(wal_nodes$time_last[tr_idx+1L], na.rm=TRUE)
va_tmin <- min(wal_nodes$time_last[va_idx+1L], na.rm=TRUE)
te_tmin <- min(wal_nodes$time_last[te_idx+1L], na.rm=TRUE)
leak_ok  <- tr_tmax <= va_tmin && va_tmin <= te_tmin
cat(sprintf("  Data Leakage    : train_tmax=%d ≤ val_tmin=%d ≤ test_tmin=%d → %s\n",
    tr_tmax, va_tmin, te_tmin,
    ifelse(leak_ok, "✅ KHÔNG LEAK", "⚠ CÓ THỂ LEAK")))

# ══════════════════════════════════════════════════════════════════════════════
# PHẦN C: 5 SQL QUERIES QUAN TRỌNG NHẤT
# ══════════════════════════════════════════════════════════════════════════════
sec("PHẦN C: 5 SQL Queries Quan Trọng")

run_query <- function(title, sql, explain) {
  cat(sprintf("\n┌─────────────────────────────────────────────────────┐\n"))
  cat(sprintf("│  %s\n", title))
  cat(sprintf("└─────────────────────────────────────────────────────┘\n"))
  result <- dbGetQuery(con, sql)
  print(result)
  cat(sprintf("\n  💡 Ý nghĩa: %s\n", explain))
}

# ── QUERY 1: Phân bố illicit theo thời gian ──────────────────────────────────
run_query(
  "QUERY 1 — Tỉ lệ illicit theo thời gian (Concept Drift)",
  "SELECT time_step, n_illicit, n_licit, pct_illicit
   FROM v_temporal_summary
   ORDER BY time_step",
  paste0(
    "Phát hiện concept drift: nếu pct_illicit thay đổi nhiều qua các time step,\n",
    "  model cần được đánh giá theo temporal split (không random) để phản ánh thực tế.\n",
    "  Biến động lớn = khó dự đoán về tương lai = cần multi-window evaluation."
  )
)

# ── QUERY 2: Split train/val/test ─────────────────────────────────────────────
run_query(
  "QUERY 2 — Phân bố Train / Val / Test Split",
  "SELECT split, total, n_illicit, n_licit, pct_illicit, time_min, time_max
   FROM v_split_summary
   ORDER BY CASE split WHEN 'train' THEN 1 WHEN 'val' THEN 2 ELSE 3 END",
  paste0(
    "Kiểm tra xem mỗi tập có đủ wallets illicit để train/evaluate không.\n",
    "  pct_illicit cần tương đối nhất quán giữa train và test.\n",
    "  time_min/max xác nhận split đúng temporal order (không future leak)."
  )
)

# ── QUERY 3: Wallets illicit nguy hiểm nhất theo graph signal ─────────────────
run_query(
  "QUERY 3 — Top 10 Wallets Illicit nguy hiểm nhất (pct_illicit_neighbors)",
  "SELECT address, time_last, n_tsteps, pct_illicit_neighbors,
          total_connected_txs, total_illicit_txs
   FROM v_illicit_wallets
   ORDER BY pct_illicit_neighbors DESC
   LIMIT 10",
  paste0(
    "pct_illicit_neighbors = % giao dịch của wallet có kết nối với tx illicit.\n",
    "  Đây là KEY AML SIGNAL: wallet rửa tiền thường giao dịch qua nhiều tx bất hợp pháp.\n",
    "  Top wallets này = các đối tượng rõ ràng nhất cần điều tra trước."
  )
)

# ── QUERY 4: So sánh behavioral pattern illicit vs licit ──────────────────────
run_query(
  "QUERY 4 — So sánh Illicit vs Licit theo đặc trưng quan trọng",
  "SELECT
    CASE label WHEN 1 THEN 'ILLICIT' ELSE 'LICIT' END AS label_name,
    COUNT(*)                                   AS n_wallets,
    ROUND(AVG(n_tsteps), 1)                    AS avg_active_timesteps,
    ROUND(AVG(time_last - time_first), 1)      AS avg_lifetime_steps,
    ROUND(AVG(pct_illicit_neighbors), 4)       AS avg_pct_illicit_nbr,
    ROUND(AVG(total_connected_txs), 1)         AS avg_connected_txs,
    ROUND(AVG(total_illicit_txs), 2)           AS avg_illicit_txs
   FROM v_wallet_full
   WHERE label >= 0
   GROUP BY label
   ORDER BY label DESC",
  paste0(
    "Khác biệt rõ ràng giữa illicit và licit:\n",
    "  - pct_illicit_neighbors: illicit >> licit (KEY graph AML signal)\n",
    "  - avg_active_timesteps: illicit thường xuất hiện trong ít time steps hơn\n",
    "  - avg_lifetime_steps: vòng đời hoạt động của illicit vs licit\n",
    "  - Đây là cơ sở XGBoost học phân biệt 2 lớp — càng khác biệt càng tốt."
  )
)

# ── QUERY 5: Wallets illicit hoạt động lâu nhất (persistent actors) ──────────
run_query(
  "QUERY 5 — Persistent Illicit Actors (hoạt động lâu nhất)",
  "SELECT address,
          time_first, time_last,
          n_tsteps,
          (time_last - time_first) AS lifetime_steps,
          pct_illicit_neighbors,
          total_illicit_txs
   FROM v_illicit_wallets
   WHERE n_tsteps >= 5
   ORDER BY lifetime_steps DESC
   LIMIT 10",
  paste0(
    "Persistent actors = wallets illicit xuất hiện qua nhiều time steps.\n",
    "  Đây là đặc trưng của tổ chức rửa tiền có tổ chức (structured money laundering).\n",
    "  Khác với one-shot wallets chỉ dùng 1 lần rồi bỏ.\n",
    "  Phân tích này giúp phân loại mức độ nguy hiểm của tổ chức đứng sau."
  )
)

# ══════════════════════════════════════════════════════════════════════════════
# EXPORT eda_summary.json
# ══════════════════════════════════════════════════════════════════════════════
sec("EXPORT: eda_summary.json")

labeled_wal <- wal_nodes[label >= 0]
temporal_dt <- labeled_wal[, .(
  n_total=.N, n_illicit=sum(label==1L), n_licit=sum(label==0L)
), by=time_last][order(time_last)]
temporal_dt[, pct_illicit := round(n_illicit/n_total*100, 2)]
pct_v <- temporal_dt$pct_illicit

ill_idx <- which(wal_nodes$label==1L); lic_idx <- which(wal_nodes$label==0L)
X_ill <- as.data.frame(wal_feats[ill_idx, feat_names, with=FALSE])
X_lic <- as.data.frame(wal_feats[lic_idx, feat_names, with=FALSE])
X_lab <- rbind(X_ill, X_lic); y_lab <- c(rep(1,nrow(X_ill)), rep(0,nrow(X_lic)))
ill_means <- colMeans(X_ill, na.rm=TRUE); lic_means <- colMeans(X_lic, na.rm=TRUE)
pool_sds  <- apply(X_lab, 2, sd, na.rm=TRUE)
disc      <- abs(ill_means - lic_means) / pmax(pool_sds, 1e-8)
disc_sort <- sort(disc, decreasing=TRUE)

eda <- list(
  dataset = "Elliptic++ (Wallet-Level AML)",
  database = DB_OUT,
  wallet = list(
    total_wallets=nrow(wal_nodes), n_labeled=n_labeled,
    n_illicit=n_wal_ill, n_licit=n_wal_lic, n_unknown=n_wal_unk,
    illicit_pct=round(n_wal_ill/n_labeled*100,2),
    imbalance_ratio=round(ir_wallet,1),
    n_features=length(feat_names),
    split=list(train=length(tr_idx), val=length(va_idx), test=length(te_idx)),
    scale_pos_weight=round(ir_wallet,0)
  ),
  tx = list(
    total_txs=nrow(tx_nodes), n_illicit=n_tx_ill, n_licit=n_tx_lic,
    illicit_pct=round(n_tx_ill/nrow(tx_nodes)*100,2),
    imbalance_ratio=round(ir_tx,1), n_features=ncol(tx_feats)-2,
    n_edges=nrow(tx_edge), scale_pos_weight=round(ir_tx,0)
  ),
  data_quality = list(
    nan_wallet=n_nan_wal, inf_wallet=n_inf_wal,
    nan_tx=n_nan_tx, leakage_free=leak_ok
  ),
  temporal = list(
    concept_drift_sd=round(sd(pct_v),2),
    illicit_pct_min=round(min(pct_v),2),
    illicit_pct_max=round(max(pct_v),2),
    illicit_pct_mean=round(mean(pct_v),2),
    by_timestep=lapply(seq_len(nrow(temporal_dt)), function(i) list(
      time=temporal_dt$time_last[i], n_illicit=temporal_dt$n_illicit[i],
      n_licit=temporal_dt$n_licit[i], pct_illicit=temporal_dt$pct_illicit[i]
    ))
  ),
  top_features = list(
    top10_discriminative=names(disc_sort)[1:min(10,length(disc_sort))],
    cohens_d_top=round(disc_sort[1:min(10,length(disc_sort))],4)
  ),
  recommended = list(
    metric="PR-AUC", threshold="tune on val set",
    strategy="scale_pos_weight + temporal split"
  )
)
write_json(eda, file.path(OUT,"eda_summary.json"), pretty=TRUE, auto_unbox=TRUE)
cat(sprintf("  Saved: outputs/eda_summary.json ✅\n"))

dbDisconnect(con)

cat("\n╔══════════════════════════════════════════════════════╗\n")
cat("║  ✅ PHÂN TÍCH HOÀN THÀNH                             ║\n")
cat("╚══════════════════════════════════════════════════════╝\n")
cat(sprintf("  Database : %s\n", DB_OUT))
cat(sprintf("  EDA JSON : outputs/eda_summary.json\n"))
cat(sprintf("  Buoc tiep: Rscript R/5_train_model.R\n\n"))

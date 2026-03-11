# 3_analysis.R — Exploratory Data Analysis (EDA)
# ─────────────────────────────────────────────────────────────────
# Đọc từ: processed/elliptic_aml.db + processed/*.csv
# Xuất ra: outputs/eda_summary.json
#
# Bao gồm:
#   1. Thống kê tổng quan dataset
#   2. Label distribution
#   3. Temporal analytics (illicit ratio theo time step)
#   4. Degree distribution (graph connectivity)
#   5. Feature summary (top variance, correlation)
#   6. Class imbalance analysis
# ─────────────────────────────────────────────────────────────────

.libPaths(c("C:/Users/Trinc/R/win-library/4.5", .libPaths()))
library(data.table)
library(DBI)
library(RSQLite)
library(jsonlite)

PROC_DIR <- "processed"
OUT_DIR  <- "outputs"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

cat("=======================================================\n")
cat("  Elliptic++ EDA — Analysis Report\n")
cat("=======================================================\n\n")

# ── Kết nối SQLite ─────────────────────────────────────────────────
con <- dbConnect(RSQLite::SQLite(), file.path(PROC_DIR, "elliptic_aml.db"))

# ═══════════════════════════════════════════════════════════════════
# 1. THỐNG KÊ TỔNG QUAN
# ═══════════════════════════════════════════════════════════════════
cat("[1/6] Dataset Overview\n")

n_nodes <- dbGetQuery(con, "SELECT COUNT(*) as n FROM nodes")$n
n_edges <- dbGetQuery(con, "SELECT COUNT(*) as n FROM edges")$n
n_feats <- ncol(dbGetQuery(con, "SELECT * FROM features LIMIT 1")) - 1  # trừ node_idx

time_range <- dbGetQuery(con, "SELECT MIN(time) as t_min, MAX(time) as t_max FROM nodes")
n_labeled  <- dbGetQuery(con, "SELECT COUNT(*) as n FROM nodes WHERE label >= 0")$n
n_illicit  <- dbGetQuery(con, "SELECT COUNT(*) as n FROM nodes WHERE label = 1")$n
n_licit    <- dbGetQuery(con, "SELECT COUNT(*) as n FROM nodes WHERE label = 0")$n
n_unknown  <- dbGetQuery(con, "SELECT COUNT(*) as n FROM nodes WHERE label = -1")$n

cat(sprintf("   Transactions (nodes): %s\n",   format(n_nodes, big.mark=",")))
cat(sprintf("   Edges (tx→tx flows) : %s\n",   format(n_edges, big.mark=",")))
cat(sprintf("   Features            : %d\n",   n_feats))
cat(sprintf("   Time steps          : %d → %d (%d steps)\n",
            time_range$t_min, time_range$t_max,
            time_range$t_max - time_range$t_min + 1))
cat(sprintf("   Labeled nodes       : %s (%.1f%%)\n",
            format(n_labeled, big.mark=","), n_labeled/n_nodes*100))
cat(sprintf("   Illicit             : %s (%.1f%% of labeled)\n",
            format(n_illicit, big.mark=","), n_illicit/n_labeled*100))
cat(sprintf("   Licit               : %s (%.1f%% of labeled)\n",
            format(n_licit, big.mark=","), n_licit/n_labeled*100))
cat(sprintf("   Unknown             : %s (%.1f%% of total)\n",
            format(n_unknown, big.mark=","), n_unknown/n_nodes*100))
cat(sprintf("   Imbalance ratio     : 1 illicit per %.0f licit\n",
            n_licit / max(1, n_illicit)))

# ═══════════════════════════════════════════════════════════════════
# 2. LABEL DISTRIBUTION
# ═══════════════════════════════════════════════════════════════════
cat("\n[2/6] Label Distribution\n")
label_dist <- dbGetQuery(con, "SELECT * FROM v_label_summary ORDER BY label")
print(label_dist)

# Check class imbalance severity
ir <- n_licit / max(1, n_illicit)
if (ir > 20) {
  cat(sprintf("\n   ⚠️  Severe class imbalance: %.0f:1 (licit:illicit)\n", ir))
  cat("   Recommended: use scale_pos_weight in XGBoost or class_weight in RF\n")
} else if (ir > 5) {
  cat(sprintf("\n   ⚠️  Moderate imbalance: %.0f:1 — handle with weighting\n", ir))
} else {
  cat("\n   ✅ Class balance acceptable\n")
}

# ═══════════════════════════════════════════════════════════════════
# 3. TEMPORAL ANALYTICS
# ═══════════════════════════════════════════════════════════════════
cat("\n[3/6] Temporal Analytics\n")
temporal <- dbGetQuery(con, "SELECT * FROM v_illicit_by_time")

cat("   Illicit ratio by time step (first 10 & last 10):\n")
cat("   Time | Total  | Illicit | Licit  | Illicit%\n")
cat("   -----|--------|---------|--------|--------\n")
show_rows <- c(head(1:nrow(temporal), 10), tail(1:nrow(temporal), 10))
show_rows <- unique(show_rows)
for (i in show_rows) {
  r <- temporal[i,]
  cat(sprintf("   %4d | %6s | %7s | %6s | %6.1f%%\n",
      r$time,
      format(r$total, big.mark=","),
      format(r$n_illicit, big.mark=","),
      format(r$n_licit, big.mark=","),
      ifelse(is.na(r$illicit_ratio), 0, r$illicit_ratio*100)))
}

# Temporal stats
illicit_ratios <- temporal$illicit_ratio[!is.na(temporal$illicit_ratio)]
cat(sprintf("\n   Illicit ratio — Min  : %.1f%%  (time step %d)\n",
            min(illicit_ratios)*100,
            temporal$time[which.min(temporal$illicit_ratio)]))
cat(sprintf("   Illicit ratio — Max  : %.1f%%  (time step %d)\n",
            max(illicit_ratios)*100,
            temporal$time[which.max(temporal$illicit_ratio)]))
cat(sprintf("   Illicit ratio — Mean : %.1f%%\n", mean(illicit_ratios)*100))
cat(sprintf("   Illicit ratio — Std  : %.2f%%  (concept drift indicator)\n",
            sd(illicit_ratios)*100))

# ═══════════════════════════════════════════════════════════════════
# 4. DEGREE DISTRIBUTION (graph connectivity)
# ═══════════════════════════════════════════════════════════════════
cat("\n[4/6] Graph Degree Distribution\n")
nodes_dt <- fread(file.path(PROC_DIR, "nodes.csv"))
edges_dt  <- fread(file.path(PROC_DIR, "edges.csv"))

out_deg  <- edges_dt[, .N, by = src_idx][, .(node_idx=src_idx, out_deg=N)]
in_deg   <- edges_dt[, .N, by = dst_idx][, .(node_idx=dst_idx, in_deg=N)]
deg_dt   <- merge(nodes_dt, out_deg, by="node_idx", all.x=TRUE)
deg_dt   <- merge(deg_dt, in_deg, by="node_idx", all.x=TRUE)
deg_dt[is.na(out_deg), out_deg := 0L]
deg_dt[is.na(in_deg),  in_deg  := 0L]
deg_dt[, total_deg := out_deg + in_deg]

deg_summary <- function(col, name) {
  cat(sprintf("   %-12s — Mean: %5.1f | Median: %4d | Max: %6d | Isolated(0): %s\n",
    name,
    mean(col), as.integer(median(col)), max(col),
    format(sum(col == 0), big.mark=",")))
}
deg_summary(deg_dt$out_deg,   "Out-degree")
deg_summary(deg_dt$in_deg,    "In-degree")
deg_summary(deg_dt$total_deg, "Total-degree")

# Degree by label
cat("\n   Mean total degree by label:\n")
deg_by_label <- deg_dt[, .(mean_deg=round(mean(total_deg),2), count=.N), by=label]
for (i in seq_len(nrow(deg_by_label))) {
  lbl <- c("-1"="unknown","0"="licit","1"="illicit")[as.character(deg_by_label$label[i])]
  cat(sprintf("     label %2d (%-8s): mean degree = %.2f  (n=%s)\n",
      deg_by_label$label[i], lbl,
      deg_by_label$mean_deg[i],
      format(deg_by_label$count[i], big.mark=",")))
}

# ═══════════════════════════════════════════════════════════════════
# 5. FEATURE ANALYSIS (top variance & graph feature impact)
# ═══════════════════════════════════════════════════════════════════
cat("\n[5/6] Feature Analysis\n")
feats_std <- fread(file.path(PROC_DIR, "features_std.csv"))
feat_names <- names(feats_std)

# Variance của từng feature
variances <- sapply(feats_std, var)
top_var   <- sort(variances, decreasing=TRUE)[1:10]
cat("   Top 10 features by variance (after scaling):\n")
for (i in seq_along(top_var)) {
  cat(sprintf("     %2d. %-35s variance = %.4f\n", i, names(top_var)[i], top_var[i]))
}

# Graph features đứng ở vị trí nào?
gf_cols   <- grep("^gf_", feat_names, value=TRUE)
gf_ranks  <- rank(-variances)[gf_cols]
cat("\n   Graph feature ranks (by variance, lower=more important):\n")
for (g in gf_cols) {
  cat(sprintf("     %-35s rank = %d / %d  (var=%.4f)\n",
      g, as.integer(gf_ranks[g]), length(variances), variances[g]))
}

# ═══════════════════════════════════════════════════════════════════
# 6. CLASS IMBALANCE ANALYSIS & RECOMMENDED STRATEGY
# ═══════════════════════════════════════════════════════════════════
cat("\n[6/6] Class Imbalance & Recommended Training Strategy\n")
cat(sprintf("   Imbalance ratio (licit/illicit): %.1f : 1\n", ir))
cat("   Recommended strategies:\n")
cat(sprintf("   ✅ XGBoost  : scale_pos_weight = %.0f\n", ir))
cat(sprintf("   ✅ RF       : classwt = c('0'=1, '1'=%.0f)\n", ir))
cat("   ✅ Metric   : use PR-AUC (not ROC-AUC) for imbalanced data\n")
cat("   ✅ Threshold: tune classification threshold (default 0.5 may miss illicit)\n")

# ═══════════════════════════════════════════════════════════════════
# EXPORT eda_summary.json
# ═══════════════════════════════════════════════════════════════════
cat("\n[Export] Saving eda_summary.json ...\n")
eda_summary <- list(
  dataset          = "Elliptic++",
  language         = "R",
  # Overview
  num_nodes        = n_nodes,
  num_edges        = n_edges,
  num_features     = n_feats,
  time_min         = time_range$t_min,
  time_max         = time_range$t_max,
  num_time_steps   = time_range$t_max - time_range$t_min + 1L,
  # Labels
  label_counts     = list(unknown=n_unknown, licit=n_licit, illicit=n_illicit),
  labeled_ratio    = round(n_labeled/n_nodes, 4),
  fraud_ratio      = round(n_illicit/n_labeled, 4),
  imbalance_ratio  = round(ir, 1),
  # Temporal
  illicit_ratio_min  = round(min(illicit_ratios), 4),
  illicit_ratio_max  = round(max(illicit_ratios), 4),
  illicit_ratio_mean = round(mean(illicit_ratios), 4),
  illicit_ratio_std  = round(sd(illicit_ratios), 4),
  # Degree
  degree_mean      = round(mean(deg_dt$total_deg), 2),
  degree_max       = max(deg_dt$total_deg),
  degree_isolated  = sum(deg_dt$total_deg == 0),
  # Strategy
  recommended_scale_pos_weight = round(ir, 0),
  recommended_metric = "PR-AUC",
  # Temporal data for visualization
  temporal_by_step = lapply(seq_len(nrow(temporal)), function(i) {
    list(time         = temporal$time[i],
         total        = temporal$total[i],
         n_illicit    = temporal$n_illicit[i],
         n_licit      = temporal$n_licit[i],
         illicit_ratio = round(ifelse(is.na(temporal$illicit_ratio[i]),0,
                                      temporal$illicit_ratio[i]), 4))
  })
)

write_json(eda_summary,
           file.path(OUT_DIR, "eda_summary.json"),
           pretty = TRUE, auto_unbox = TRUE)
dbDisconnect(con)

cat(sprintf("   Saved: outputs/eda_summary.json\n"))
cat("\n=== EDA DONE ===\n")
cat("Ready for: Rscript R/4_visualization.R\n\n")

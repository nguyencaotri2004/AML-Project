# 4_visualization.R — 5 Biểu Đồ Quan Trọng (FIXED v2)
# ─────────────────────────────────────────────────────────────────
# FIXES v2:
#   Chart 3: dùng faceted histogram thay stacked (illicit bị ẩn)
#   Chart 4: clip PCA axes ở 95th percentile (loại bỏ outlier kéo trục)
#   Chart 5: violin+boxplot thay density (rõ hơn với skewed distributions)
# ─────────────────────────────────────────────────────────────────

.libPaths(c("C:/Users/Trinc/R/win-library/4.5", .libPaths()))
library(data.table)
library(ggplot2)
library(DBI)
library(RSQLite)

PROC_DIR <- "processed"
OUT_DIR  <- "outputs/charts"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

PALETTE <- c(illicit = "#c62828", licit = "#2e7d32", unknown = "#9e9e9e")
THEME <- theme_minimal(base_size = 13) +
  theme(plot.title    = element_text(face = "bold", size = 15),
        plot.subtitle = element_text(color = "grey40", size = 11),
        panel.grid.minor = element_blank(),
        plot.margin = margin(15, 15, 15, 15))

cat("=======================================================\n")
cat("  Elliptic++ Visualization v2 — 5 Key Charts (Fixed)\n")
cat("=======================================================\n\n")

con      <- dbConnect(RSQLite::SQLite(), file.path(PROC_DIR, "elliptic_aml.db"))
nodes_dt <- fread(file.path(PROC_DIR, "nodes.csv"))
feats_dt <- fread(file.path(PROC_DIR, "features_std.csv"))
edges_dt <- fread(file.path(PROC_DIR, "edges.csv"))

# ═══════════════════════════════════════════════════════════════════
# CHART 1: LABEL DISTRIBUTION
# ═══════════════════════════════════════════════════════════════════
cat("[1/5] Label Distribution\n")

label_df <- data.frame(
  label = c("Unknown\n(-1)", "Licit\n(0)", "Illicit\n(1)"),
  count = c(sum(nodes_dt$label == -1), sum(nodes_dt$label == 0), sum(nodes_dt$label == 1)),
  color = c("#9e9e9e", "#2e7d32", "#c62828")
)
label_df$label <- factor(label_df$label, levels = label_df$label)
label_df$pct   <- round(label_df$count / sum(label_df$count) * 100, 1)

p1 <- ggplot(label_df, aes(x = label, y = count, fill = label)) +
  geom_col(width = 0.6, show.legend = FALSE) +
  geom_text(aes(label = paste0(format(count, big.mark = ","), "\n(", pct, "%)")),
            vjust = -0.4, size = 4, fontface = "bold") +
  scale_fill_manual(values = setNames(label_df$color, label_df$label)) +
  scale_y_continuous(labels = scales::comma, expand = expansion(mult = c(0, 0.15))) +
  labs(title    = "Transaction Label Distribution",
       subtitle = paste0("Elliptic++ — ", format(nrow(nodes_dt), big.mark=","), " transactions"),
       x = "Label", y = "Count") +
  THEME

ggsave(file.path(OUT_DIR, "01_label_distribution.png"), p1, width = 7, height = 5, dpi = 150)
cat("   Saved: 01_label_distribution.png\n")

# ═══════════════════════════════════════════════════════════════════
# CHART 2: ILLICIT RATIO OVER TIME
# ═══════════════════════════════════════════════════════════════════
cat("[2/5] Illicit Ratio Over Time\n")

temporal <- dbGetQuery(con, "SELECT * FROM v_illicit_by_time")
temporal$illicit_ratio <- ifelse(is.na(temporal$illicit_ratio), 0, temporal$illicit_ratio)
temporal$illicit_pct   <- temporal$illicit_ratio * 100

p2 <- ggplot(temporal, aes(x = time)) +
  geom_ribbon(aes(ymin = 0, ymax = illicit_pct), fill = "#c62828", alpha = 0.25) +
  geom_line(aes(y = illicit_pct), color = "#c62828", linewidth = 1.2) +
  geom_point(aes(y = illicit_pct), color = "#c62828", size = 1.8) +
  scale_x_continuous(breaks = seq(1, max(temporal$time), 4)) +
  scale_y_continuous(labels = function(x) paste0(x, "%"),
                     limits = c(0, NA), expand = expansion(mult = c(0, 0.1))) +
  labs(title    = "Illicit Transaction Ratio Over Time",
       subtitle = "Each point = 1 time step. High variation = concept drift in fraud patterns.",
       x = "Time Step", y = "Illicit Rate (%)") +
  THEME

ggsave(file.path(OUT_DIR, "02_illicit_ratio_over_time.png"), p2, width = 10, height = 5, dpi = 150)
cat("   Saved: 02_illicit_ratio_over_time.png\n")

# ═══════════════════════════════════════════════════════════════════
# CHART 3: DEGREE DISTRIBUTION — [FIX] Faceted (not stacked)
# ═══════════════════════════════════════════════════════════════════
cat("[3/5] Degree Distribution (Fixed: faceted by label)\n")

out_deg <- edges_dt[, .N, by = src_idx][, .(node_idx = src_idx, out_deg = N)]
in_deg  <- edges_dt[, .N, by = dst_idx][, .(node_idx = dst_idx, in_deg  = N)]
deg_dt  <- merge(nodes_dt, out_deg, by = "node_idx", all.x = TRUE)
deg_dt  <- merge(deg_dt,  in_deg,  by = "node_idx", all.x = TRUE)
deg_dt[is.na(out_deg), out_deg := 0L]
deg_dt[is.na(in_deg),  in_deg  := 0L]
deg_dt[, total_deg := out_deg + in_deg]

# [FIX] Chỉ vẽ labeled nodes (licit + illicit), không vẽ unknown
deg_labeled <- deg_dt[label >= 0 & total_deg > 0,
                       .(total_deg,
                         label_cat = ifelse(label == 1L, "Illicit (1)", "Licit (0)"))]
deg_labeled$label_cat <- factor(deg_labeled$label_cat, levels = c("Licit (0)", "Illicit (1)"))

p3 <- ggplot(deg_labeled, aes(x = total_deg + 1, fill = label_cat)) +
  geom_histogram(bins = 40, alpha = 0.8, color = "white", linewidth = 0.2) +
  facet_wrap(~ label_cat, scales = "free_y", ncol = 1) +
  scale_x_log10(labels = scales::comma, breaks = c(1, 2, 3, 5, 10, 30, 100, 300)) +
  scale_y_continuous(labels = scales::comma) +
  scale_fill_manual(values = c("Illicit (1)" = "#c62828", "Licit (0)" = "#2e7d32"),
                    guide = "none") +
  labs(title    = "Transaction Degree Distribution (Labeled Nodes Only)",
       subtitle = "Illicit nodes tend toward mid-range degrees. Licit nodes more likely = low degree.",
       x = "Total Degree + 1 (log scale)", y = "Count") +
  THEME

ggsave(file.path(OUT_DIR, "03_degree_distribution.png"), p3, width = 9, height = 7, dpi = 150)
cat("   Saved: 03_degree_distribution.png\n")

# ═══════════════════════════════════════════════════════════════════
# CHART 4: PCA SCATTER — [FIX] Clip outliers before plot
# ═══════════════════════════════════════════════════════════════════
cat("[4/5] PCA Scatter (2D) — Fixed: clip outlier axes\n")

set.seed(42)
labeled_idx <- which(nodes_dt$label >= 0)
if (length(labeled_idx) > 8000) labeled_idx <- sample(labeled_idx, 8000)

X_labeled <- as.matrix(feats_dt[labeled_idx, ])
y_labeled  <- nodes_dt$label[labeled_idx]

pca_res <- prcomp(X_labeled, center = TRUE, scale. = FALSE)
pca_df  <- data.frame(
  PC1   = pca_res$x[, 1],
  PC2   = pca_res$x[, 2],
  label = ifelse(y_labeled == 1, "illicit", "licit")
)

var_explained <- round(summary(pca_res)$importance[2, 1:2] * 100, 1)

# [FIX] Clip axes ở 2.5% - 97.5% để loại bỏ outliers kéo trục
pc1_lim <- quantile(pca_df$PC1, c(0.01, 0.99))
pc2_lim <- quantile(pca_df$PC2, c(0.01, 0.99))
pca_clip <- pca_df[pca_df$PC1 >= pc1_lim[1] & pca_df$PC1 <= pc1_lim[2] &
                   pca_df$PC2 >= pc2_lim[1] & pca_df$PC2 <= pc2_lim[2], ]
cat(sprintf("   PCA: kept %d/%d points after clipping 1%%–99%%\n", nrow(pca_clip), nrow(pca_df)))

p4 <- ggplot(pca_clip, aes(x = PC1, y = PC2, color = label)) +
  geom_point(data = subset(pca_clip, label == "licit"),   alpha = 0.3, size = 1.0) +
  geom_point(data = subset(pca_clip, label == "illicit"), alpha = 0.75, size = 1.5) +
  scale_color_manual(values = c(licit = "#2e7d32", illicit = "#c62828"), name = "Label") +
  labs(title    = "PCA (2D) — Feature Space Separability",
       subtitle = paste0("PC1: ", var_explained[1], "%  |  PC2: ", var_explained[2],
                         "% variance explained.  Outliers removed (99% clip)."),
       x = paste0("PC1 (", var_explained[1], "%)"),
       y = paste0("PC2 (", var_explained[2], "%)")) +
  THEME +
  guides(color = guide_legend(override.aes = list(size = 4, alpha = 1)))

ggsave(file.path(OUT_DIR, "04_pca_scatter.png"), p4, width = 8, height = 6, dpi = 150)
cat("   Saved: 04_pca_scatter.png\n")

# ═══════════════════════════════════════════════════════════════════
# CHART 5: NEIGHBOR ILLICIT RATIO — [FIX] Violin+Boxplot
# ═══════════════════════════════════════════════════════════════════
cat("[5/5] Neighbor Illicit Ratio by Label (violin+boxplot)\n")
cat("   Computing neighbor illicit ratio from raw edges...\n")

lbl_lookup    <- nodes_dt$label
names(lbl_lookup) <- nodes_dt$node_idx
labeled_nodes <- nodes_dt[nodes_dt$label >= 0, ]

edges_src <- data.frame(node_idx = as.integer(edges_dt$src_idx),
                        nbr_idx  = as.integer(edges_dt$dst_idx))
edges_dst <- data.frame(node_idx = as.integer(edges_dt$dst_idx),
                        nbr_idx  = as.integer(edges_dt$src_idx))
all_edges  <- rbind(edges_src, edges_dst)
all_edges$nbr_label <- lbl_lookup[as.character(all_edges$nbr_idx)]
all_edges_known <- all_edges[!is.na(all_edges$nbr_label) & all_edges$nbr_label >= 0, ]
all_edges_known$is_illicit <- as.integer(all_edges_known$nbr_label == 1)
nbr_ratio_dt <- aggregate(is_illicit ~ node_idx, data = all_edges_known, FUN = mean)
names(nbr_ratio_dt)[2] <- "illicit_nbr"

nbr_merged <- merge(labeled_nodes[, c("node_idx", "label")],
                    nbr_ratio_dt, by = "node_idx", all.x = TRUE)
nbr_merged$illicit_nbr[is.na(nbr_merged$illicit_nbr)] <- 0
nbr_merged$label_cat <- ifelse(nbr_merged$label == 1, "Illicit (1)", "Licit (0)")

m_ill <- round(mean(nbr_merged$illicit_nbr[nbr_merged$label == 1]) * 100, 1)
m_lic <- round(mean(nbr_merged$illicit_nbr[nbr_merged$label == 0]) * 100, 1)
cat(sprintf("   Illicit mean: %.1f%% | Licit mean: %.1f%%\n", m_ill, m_lic))

# [FIX] Violin + Boxplot (rõ hơn density với sparse/skewed distributions)
p5 <- ggplot(nbr_merged, aes(x = label_cat, y = illicit_nbr, fill = label_cat)) +
  geom_violin(alpha = 0.6, scale = "width", trim = TRUE) +
  geom_boxplot(width = 0.12, fill = "white", outlier.shape = NA, linewidth = 0.7) +
  stat_summary(fun = mean, geom = "point", shape = 18, size = 4, color = "navy") +
  scale_fill_manual(values = c("Illicit (1)" = "#c62828", "Licit (0)" = "#2e7d32"),
                    guide = "none") +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1),
                     breaks = seq(0, 1, 0.1)) +
  labs(title    = "Neighbor Illicit Ratio by Label",
       subtitle = paste0("Illicit txs: mean ", m_ill, "% illicit neighbors vs Licit: ",
                         m_lic, "% — key AML signal.  (diamond = mean)"),
       x = "Transaction Label",
       y = "% of Neighbors that are Illicit") +
  THEME

ggsave(file.path(OUT_DIR, "05_neighbor_illicit_ratio.png"), p5, width = 8, height = 6, dpi = 150)
cat("   Saved: 05_neighbor_illicit_ratio.png\n")

dbDisconnect(con)

cat("\n=== VISUALIZATION v2 DONE ===\n")
cat(sprintf("Charts saved to: %s/\n", OUT_DIR))
for (f in list.files(OUT_DIR, pattern = "\\.png$", full.names = FALSE)) {
  cat(sprintf("  - %s\n", f))
}
cat("\nReady for: Rscript R/5_train_model.R\n")

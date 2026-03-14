# 4_visualization.R — 10 AML Analysis Charts (English)
# ══════════════════════════════════════════════════════════════════════════════
# 10 most informative visualization charts for the Wallet-Level AML Detection project:
#
#  1. Label Distribution          — illicit / licit / unknown
#  2. Concept Drift               — illicit% per time step
#  3. KEY AML Signal              — pct_illicit_neighbors density (illicit vs licit)
#  4. Discriminative Power        — Top 15 features (Cohen's d)
#  5. Connected Transactions      — ECDF + tier bar (illicit vs licit)
#  6. Wallet Lifetime             — n_tsteps survival curve (illicit vs licit)
#  7. Temporal Split Timeline     — Train / Val / Test over time
#  8. Feature Correlation Heatmap — top 18 features
#  9. Top 3 Features Density      — density facet by label
# 10. Class Imbalance             — lollipop chart
#
# Output: outputs/plots/plot_01.png ... plot_10.png
# ══════════════════════════════════════════════════════════════════════════════

.libPaths(c("C:/Users/Trinc/R/win-library/4.5", .libPaths()))
library(data.table)
library(ggplot2)
library(jsonlite)
library(scales)
set.seed(42)

PROC_W   <- "processed/wallet"
PLOT_DIR <- "outputs/plots"
dir.create(PLOT_DIR, recursive=TRUE, showWarnings=FALSE)

COLORS <- c(
  illicit  = "#E63946",
  licit    = "#2A9D8F",
  unknown  = "#ADB5BD",
  accent   = "#457B9D",
  bg       = "#1A1A2E",
  text     = "#EAEAEA",
  grid     = "#2D2D4E"
)

# ── Shared dark theme ───────────────────────────────────────────────────────────
theme_aml <- function() {
  theme_minimal(base_size=13) +
  theme(
    plot.background   = element_rect(fill="#1A1A2E", color=NA),
    panel.background  = element_rect(fill="#16213E", color=NA),
    panel.grid.major  = element_line(color="#2D2D4E", linewidth=0.4),
    panel.grid.minor  = element_blank(),
    text              = element_text(color="#EAEAEA"),
    axis.text         = element_text(color="#B0B8C1"),
    axis.title        = element_text(color="#EAEAEA", face="bold"),
    plot.title        = element_text(color="#FFFFFF", face="bold", size=15,
                                    margin=margin(b=5)),
    plot.subtitle     = element_text(color="#A0AEC0", size=11,
                                    margin=margin(b=10)),
    plot.caption      = element_text(color="#606880", size=9),
    legend.background = element_rect(fill="#1A1A2E", color=NA),
    legend.text       = element_text(color="#EAEAEA"),
    legend.title      = element_text(color="#EAEAEA", face="bold"),
    strip.text        = element_text(color="#EAEAEA", face="bold"),
    strip.background  = element_rect(fill="#2D2D4E", color=NA),
    plot.margin       = margin(15,15,10,15)
  )
}

save_plot <- function(p, filename, w=10, h=6) {
  path <- file.path(PLOT_DIR, filename)
  ggsave(path, plot=p, width=w, height=h, dpi=150, bg="#1A1A2E")
  cat(sprintf("  OK %s\n", filename))
  p
}

label_map <- c("1"="Illicit", "0"="Licit", "-1"="Unknown")
label_col <- c("Illicit"="#E63946", "Licit"="#2A9D8F", "Unknown"="#ADB5BD")

# ── Load data ──────────────────────────────────────────────────────────────────
cat("\n== 4_visualization.R — 10 AML Charts (English) ==\n\n")
cat("Loading data...\n")
wal_nodes  <- fread(file.path(PROC_W, "wallet_nodes.csv"))
wal_feats  <- fread(file.path(PROC_W, "wallet_features_full.csv"))
feat_names <- fromJSON(file.path(PROC_W, "feature_names.json"))
tr_idx  <- fread(file.path(PROC_W,"train_idx.csv"))$idx
va_idx  <- fread(file.path(PROC_W,"val_idx.csv"))$idx
te_idx  <- fread(file.path(PROC_W,"test_idx.csv"))$idx
cat("  Done\n\n")

wal_nodes[, label_name := label_map[as.character(label)]]
labeled <- wal_nodes[label >= 0]

# ══════════════════════════════════════════════════════════════════════════════
# CHART 1: Label Distribution
# ══════════════════════════════════════════════════════════════════════════════
cat("[01] Label distribution...\n")

counts <- wal_nodes[, .N, by=label_name][order(-N)]
counts[, label_name := factor(label_name, levels=c("Licit","Unknown","Illicit"))]
counts[, pct := round(N/sum(N)*100,1)]
counts[, lab := paste0(format(N,big.mark=","), "\n(", pct, "%)")]

p1 <- ggplot(counts, aes(x=label_name, y=N, fill=label_name)) +
  geom_col(width=0.6, color=NA) +
  geom_text(aes(label=lab), vjust=-0.3, color="white", size=4, fontface="bold") +
  scale_fill_manual(values=label_col, guide="none") +
  scale_y_continuous(labels=scales::label_comma(),
                     expand=expansion(mult=c(0,.15))) +
  labs(title="Chart 1: Wallet Label Distribution",
       subtitle="Severe imbalance: 17.6 licit : 1 illicit — scale_pos_weight required",
       x="Label", y="Number of Wallets",
       caption="Elliptic++ | Wallet-Level AML") +
  theme_aml()

save_plot(p1, "plot_01_label_distribution.png")

# ══════════════════════════════════════════════════════════════════════════════
# CHART 2: Concept Drift — illicit% per time step
# ══════════════════════════════════════════════════════════════════════════════
cat("[02] Concept drift over time...\n")

temporal <- labeled[, .(
  n_total=.N, n_illicit=sum(label==1L)
), by=time_last][order(time_last)]
temporal[, pct_illicit := n_illicit/n_total*100]

p2 <- ggplot(temporal, aes(x=time_last)) +
  geom_ribbon(aes(ymin=0, ymax=pct_illicit), fill=COLORS["illicit"], alpha=0.25) +
  geom_line(aes(y=pct_illicit), color=COLORS["illicit"], linewidth=1.2) +
  geom_point(aes(y=pct_illicit, size=n_total), color=COLORS["illicit"],
             alpha=0.7, shape=21, fill=COLORS["illicit"]) +
  geom_hline(yintercept=mean(temporal$pct_illicit), color="#FFD166",
             linetype="dashed", linewidth=0.8) +
  annotate("text", x=min(temporal$time_last)+1,
           y=mean(temporal$pct_illicit)+0.3,
           label=sprintf("Mean: %.1f%%", mean(temporal$pct_illicit)),
           color="#FFD166", size=3.5, hjust=0) +
  scale_size(range=c(2,7), guide="none") +
  scale_y_continuous(labels=function(x) paste0(x,"%")) +
  labs(title="Chart 2: Concept Drift -- Illicit Rate Over Time",
       subtitle="Illicit% varies across time steps -- model requires temporal split, not random split",
       x="Time Step", y="% Illicit Wallets",
       caption="High SD = strong concept drift = multi-window evaluation required") +
  theme_aml()

save_plot(p2, "plot_02_concept_drift.png")

# ══════════════════════════════════════════════════════════════════════════════
# CHART 3: KEY AML Signal — pct_illicit_neighbors density
# ══════════════════════════════════════════════════════════════════════════════
cat("[03] pct_illicit_neighbors density...\n")

if ("pct_illicit_neighbors" %in% feat_names) {
  feat_idx   <- which(feat_names == "pct_illicit_neighbors")
  pct_col    <- wal_feats[[feat_idx]]
  graph_dt   <- data.table(
    label_name = wal_nodes$label_name,
    pct_nbr    = pct_col
  )[label_name %in% c("Illicit","Licit")]

  p3 <- ggplot(graph_dt, aes(x=pct_nbr, fill=label_name, color=label_name)) +
    geom_density(alpha=0.45, linewidth=1.1) +
    scale_fill_manual(values=label_col)  +
    scale_color_manual(values=label_col) +
    scale_x_continuous(labels=scales::percent) +
    labs(title="Chart 3: KEY AML Signal -- pct_illicit_neighbors",
         subtitle="Illicit wallets transact through more illicit TXs -- strongest discriminative feature",
         x="% of connected transactions that are illicit (scaled)",
         y="Density", fill="Label", color="Label",
         caption="Source: AddrTx + TxAddr edgelist (bipartite graph)") +
    theme_aml()

  save_plot(p3, "plot_03_aml_signal_density.png")
} else {
  cat("  WARNING: pct_illicit_neighbors not found in feature names, skipping\n")
}

# ══════════════════════════════════════════════════════════════════════════════
# CHART 4: Discriminative Power — Top 15 features (Cohen's d)
# ══════════════════════════════════════════════════════════════════════════════
cat("[04] Feature discriminative power...\n")

ill_idx  <- which(wal_nodes$label==1L)
lic_idx  <- which(wal_nodes$label==0L)
X_ill    <- as.data.frame(wal_feats[ill_idx, feat_names, with=FALSE])
X_lic    <- as.data.frame(wal_feats[lic_idx, feat_names, with=FALSE])
X_all    <- rbind(X_ill, X_lic)
ill_m    <- colMeans(X_ill,na.rm=TRUE); lic_m <- colMeans(X_lic,na.rm=TRUE)
pool_sd  <- apply(X_all, 2, sd, na.rm=TRUE)
disc_all <- sort(abs(ill_m - lic_m)/pmax(pool_sd,1e-8), decreasing=TRUE)
top15    <- head(disc_all, 15)
df4      <- data.frame(feature=factor(names(top15), levels=rev(names(top15))),
                       cohens_d=as.numeric(top15),
                       group=ifelse(grepl("^n_|^total_|^pct_", names(top15)),
                                   "Graph Feature", "Behavioral Feature"))

p4 <- ggplot(df4, aes(x=cohens_d, y=feature, fill=group)) +
  geom_col(width=0.65) +
  geom_text(aes(label=round(cohens_d,3)), hjust=-0.1,
            color="white", size=3.5, fontface="bold") +
  scale_fill_manual(values=c("Graph Feature"="#E63946",
                             "Behavioral Feature"="#457B9D")) +
  scale_x_continuous(expand=expansion(mult=c(0,.2))) +
  labs(title="Chart 4: Discriminative Power -- Top 15 Features",
       subtitle="Cohen's d = |mean_illicit - mean_licit| / pooled_SD -- higher = better separation",
       x="Cohen's d", y=NULL, fill="Feature Type",
       caption="Red = Graph features (from bipartite graph) | Blue = Behavioral features") +
  theme_aml() + theme(legend.position="bottom")

save_plot(p4, "plot_04_discriminative_power.png", h=7)

# ══════════════════════════════════════════════════════════════════════════════
# CHART 5: Connected Transactions — ECDF + tier bar
# ══════════════════════════════════════════════════════════════════════════════
cat("[05] Connected TX — ECDF + tiers...\n")

if ("total_connected_txs" %in% feat_names) {
  con_col <- wal_feats[[which(feat_names=="total_connected_txs")]]
  df5 <- data.table(label_name=wal_nodes$label_name, val=con_col)[
    label_name %in% c("Illicit","Licit")]
  df5[, label_name := factor(label_name, levels=c("Illicit","Licit"))]

  means5 <- df5[, .(m=mean(val)), by=label_name]

  pA <- ggplot(df5, aes(x=val, color=label_name)) +
    stat_ecdf(linewidth=1.6, geom="step") +
    geom_vline(data=means5, aes(xintercept=m, color=label_name),
               linetype="dashed", linewidth=1.0, alpha=0.9) +
    geom_label(data=means5, aes(x=m, y=0.25,
               label=sprintf("Mean: %.3f", m), fill=label_name),
               color="white", size=3.5, fontface="bold",
               hjust=-0.05, show.legend=FALSE,
               label.padding=unit(0.25,"lines")) +
    scale_color_manual(values=label_col) +
    scale_fill_manual(values=label_col)  +
    scale_y_continuous(labels=scales::percent, breaks=seq(0,1,0.2)) +
    scale_x_continuous(breaks=seq(0,1,0.1)) +
    labs(x="total_connected_txs (scaled, Log1p+MinMax)",
         y="% Wallets <= x", color="Label",
         title="A) Cumulative Distribution (ECDF) — Illicit shifted right = more connected TXs") +
    theme_aml() + theme(legend.position="bottom",
                        plot.title=element_text(size=11, color="#B0C4DE"))

  df5[, tier := fcase(
    val == 0,     "None\n(=0)",
    val <= 0.05,  "Very Low\n(0-5%]",
    val <= 0.15,  "Low\n(5-15%]",
    val <= 0.40,  "Medium\n(15-40%]",
    default     = "High\n(>40%)"
  )]
  df5[, tier := factor(tier, levels=c("None\n(=0)","Very Low\n(0-5%]",
                                       "Low\n(5-15%]","Medium\n(15-40%]","High\n(>40%)"))]
  tier_pct <- df5[, .N, by=.(label_name, tier)]
  tier_pct[, pct := N/sum(N)*100, by=label_name]

  pB <- ggplot(tier_pct, aes(x=tier, y=pct, fill=label_name)) +
    geom_col(position=position_dodge(0.72), width=0.68) +
    geom_text(aes(label=sprintf("%.1f%%",pct)),
              position=position_dodge(0.72),
              vjust=-0.45, color="white", size=3.8, fontface="bold") +
    scale_fill_manual(values=label_col) +
    scale_y_continuous(labels=function(x) paste0(x,"%"),
                       expand=expansion(mult=c(0,.18))) +
    labs(x="TX Connection Level", y="% Wallets", fill="Label",
         title="B) Wallet distribution by TX connection level") +
    theme_aml() + theme(legend.position="bottom",
                        plot.title=element_text(size=11, color="#B0C4DE"))

  library(gridExtra)
  g5 <- gridExtra::arrangeGrob(
    pA, pB, ncol=2,
    top=grid::textGrob(
      "Chart 5: Connected TX Analysis — Illicit vs Licit",
      gp=grid::gpar(fontsize=15, fontface="bold", col="white")
    ),
    bottom=grid::textGrob(
      "Illicit wallets have more connected TXs and higher 'High' tier proportion | Source: AddrTx + TxAddr edgelist",
      gp=grid::gpar(fontsize=9, col="#606880")
    )
  )
  ggsave(file.path(PLOT_DIR, "plot_05_connected_txs.png"),
         plot=g5, width=14, height=6, dpi=150, bg="#1A1A2E")
  cat("  OK plot_05_connected_txs.png\n")
}

# ══════════════════════════════════════════════════════════════════════════════
# CHART 6: Wallet Lifetime — n_tsteps survival curve
# ══════════════════════════════════════════════════════════════════════════════
cat("[06] Wallet lifetime — bar + survival curve...\n")

df6 <- wal_nodes[label_name %in% c("Illicit","Licit"), .(label_name, n_tsteps)]
df6[, label_name := factor(label_name, levels=c("Illicit","Licit"))]
df6[, bucket := fcase(
  n_tsteps == 1L,  "1 step\n(one-shot)",
  n_tsteps == 2L,  "2 steps",
  n_tsteps == 3L,  "3 steps",
  n_tsteps <= 5L,  "4-5 steps",
  n_tsteps <= 10L, "6-10 steps",
  default         = "11+ steps\n(persistent)"
)]
df6[, bucket := factor(bucket, levels=c("1 step\n(one-shot)","2 steps","3 steps",
                                          "4-5 steps","6-10 steps","11+ steps\n(persistent)"))]
pct_df6 <- df6[, .N, by=.(label_name, bucket)]
pct_df6[, pct := N/sum(N)*100, by=label_name]
one_shot6 <- df6[, .(pct=round(mean(n_tsteps==1)*100,1)), by=label_name]

pA6 <- ggplot(pct_df6, aes(x=bucket, y=pct, fill=label_name)) +
  geom_col(position=position_dodge(0.78), width=0.72, color="#1A1A2E", linewidth=0.3) +
  geom_text(aes(label=sprintf("%.1f%%",pct)), position=position_dodge(0.78),
            vjust=-0.4, color="white", size=3.6, fontface="bold") +
  annotate("rect", xmin=0.6, xmax=1.4, ymin=0, ymax=Inf, fill="#FFD166", alpha=0.06) +
  annotate("text", x=1, y=max(pct_df6$pct)*0.92, label="One-shot\nZone",
           color="#FFD166", size=3, fontface="bold") +
  scale_fill_manual(values=label_col) +
  scale_y_continuous(labels=function(x) paste0(x,"%"), expand=expansion(mult=c(0,.18))) +
  labs(title=sprintf("A) Distribution by Time Steps (Illicit: %.1f%% one-shot | Licit: %.1f%% one-shot)",
                     one_shot6[label_name=="Illicit", pct], one_shot6[label_name=="Licit", pct]),
       x="Number of Time Steps (n_tsteps)", y="% Wallets", fill="Label") +
  theme_aml() + theme(legend.position="bottom", plot.title=element_text(size=10, color="#B0C4DE"))

max_step6 <- min(49L, max(df6$n_tsteps))
surv6 <- rbindlist(lapply(seq_len(max_step6), function(k) list(
  k=k,
  Illicit=mean(df6[label_name=="Illicit", n_tsteps] >= k)*100,
  Licit  =mean(df6[label_name=="Licit",   n_tsteps] >= k)*100
)))
surv_long6 <- melt(surv6, id.vars="k", variable.name="label_name", value.name="pct")
surv_long6[, label_name := factor(label_name, levels=c("Illicit","Licit"))]

pB6 <- ggplot(surv_long6, aes(x=k, y=pct, color=label_name, fill=label_name)) +
  geom_ribbon(aes(ymin=0, ymax=pct), alpha=0.12, color=NA) +
  geom_line(linewidth=1.6) +
  geom_point(data=surv_long6[k %in% c(1,2,3,5,10,20)], size=3, shape=21, fill="white") +
  geom_hline(yintercept=50, color="#FFD166", linetype="dashed", linewidth=0.8) +
  annotate("text", x=max_step6*0.7, y=54, label="50% still active",
           color="#FFD166", size=3.5, fontface="bold") +
  scale_color_manual(values=label_col) +
  scale_fill_manual(values=label_col)  +
  scale_y_continuous(labels=function(x) paste0(x,"%"), breaks=seq(0,100,20)) +
  scale_x_continuous(breaks=c(1,2,3,5,10,20,30,40)) +
  labs(title="B) Survival Curve — % Wallets still active after >= k steps",
       subtitle="Illicit drops faster (one-shot pattern) | Licit persists longer",
       x="k (minimum time steps)", y="% Still Active", color="Label") +
  guides(fill="none") +
  theme_aml() + theme(legend.position="bottom", plot.title=element_text(size=10, color="#B0C4DE"))

library(gridExtra)
g6 <- gridExtra::arrangeGrob(
  pA6, pB6, ncol=2,
  top=grid::textGrob(
    "Chart 6: Wallet Lifetime — Illicit (one-shot) vs Licit (persistent)",
    gp=grid::gpar(fontsize=14, fontface="bold", col="white")
  ),
  bottom=grid::textGrob(
    "Illicit wallets: ~98% active for only 1 time step | Licit: longer and more sustained activity",
    gp=grid::gpar(fontsize=9, col="#606880")
  )
)
ggsave(file.path(PLOT_DIR, "plot_06_wallet_lifetime.png"),
       plot=g6, width=14, height=6.5, dpi=150, bg="#1A1A2E")
cat("  OK plot_06_wallet_lifetime.png\n")

# ══════════════════════════════════════════════════════════════════════════════
# CHART 7: Temporal Split — Train / Val / Test over time
# ══════════════════════════════════════════════════════════════════════════════
cat("[07] Temporal split timeline...\n")

wal_nodes[, split := "Unlabeled"]
wal_nodes$split[tr_idx+1L] <- "Train"
wal_nodes$split[va_idx+1L] <- "Val"
wal_nodes$split[te_idx+1L] <- "Test"
wal_nodes[, split := factor(split, levels=c("Train","Val","Test","Unlabeled"))]

df7 <- wal_nodes[split != "Unlabeled", .N, by=.(time_last, split)][order(time_last)]

p7 <- ggplot(df7, aes(x=time_last, y=N, fill=split)) +
  geom_col(width=0.8) +
  scale_fill_manual(values=c(Train="#457B9D", Val="#FFD166",
                             Test="#E63946")) +
  geom_vline(xintercept=c(
    max(wal_nodes$time_last[wal_nodes$split=="Train"], na.rm=TRUE)+0.5,
    max(wal_nodes$time_last[wal_nodes$split=="Val"]  , na.rm=TRUE)+0.5
  ), color="white", linetype="dashed", linewidth=0.8) +
  annotate("text", x=mean(wal_nodes$time_last[wal_nodes$split=="Train"],na.rm=T),
           y=max(df7$N)*0.9, label="Train\n70%",
           color="white", size=4, fontface="bold") +
  annotate("text", x=mean(wal_nodes$time_last[wal_nodes$split=="Val"],na.rm=T),
           y=max(df7$N)*0.9, label="Val\n10%",
           color="white", size=4, fontface="bold") +
  annotate("text", x=mean(wal_nodes$time_last[wal_nodes$split=="Test"],na.rm=T),
           y=max(df7$N)*0.9, label="Test\n20%",
           color="white", size=4, fontface="bold") +
  scale_y_continuous(labels=scales::comma) +
  labs(title="Chart 7: Temporal Split -- Train / Val / Test Over Time",
       subtitle="Chronological split (not random) to prevent data leakage",
       x="Time Step", y="Number of Wallets", fill="Split",
       caption="Dashed lines = split boundaries") +
  theme_aml()

save_plot(p7, "plot_07_temporal_split.png")

# ══════════════════════════════════════════════════════════════════════════════
# CHART 8: Feature Correlation Heatmap (top 18 features)
# ══════════════════════════════════════════════════════════════════════════════
cat("[08] Correlation heatmap...\n")

top_feats18 <- names(disc_all)[1:min(18, length(disc_all))]
X_cor       <- as.data.frame(wal_feats[, top_feats18, with=FALSE])
corr_mat    <- cor(X_cor, use="pairwise.complete.obs")
corr_mat[is.na(corr_mat)] <- 0

corr_melt <- as.data.table(reshape2::melt(corr_mat))
setnames(corr_melt, c("Var1","Var2","value"))

shorten <- function(x) {
  x <- gsub("_neighbors","_nbr", x)
  x <- gsub("total_connected","total_con", x)
  x <- gsub("illicit","ill", x)
  substr(x, 1, 20)
}
corr_melt[, Var1 := shorten(as.character(Var1))]
corr_melt[, Var2 := shorten(as.character(Var2))]

p8 <- ggplot(corr_melt, aes(x=Var1, y=Var2, fill=value)) +
  geom_tile(color="#1A1A2E", linewidth=0.3) +
  scale_fill_gradient2(low="#2A9D8F", mid="#1A1A2E", high="#E63946",
                       midpoint=0, limits=c(-1,1),
                       name="Pearson r") +
  geom_text(aes(label=ifelse(abs(value)>0.5 & Var1!=Var2,
                             round(value,1), "")),
            color="white", size=2.8, fontface="bold") +
  theme_aml() +
  theme(axis.text.x=element_text(angle=45, hjust=1, size=9),
        axis.text.y=element_text(size=9)) +
  labs(title="Chart 8: Feature Correlation Heatmap",
       subtitle="Top 18 features | Red = positive correlation, Teal = negative correlation",
       x=NULL, y=NULL,
       caption="Feature pairs with |r| > 0.95 were removed by feature selection")

save_plot(p8, "plot_08_correlation_heatmap.png", w=11, h=8)

# ══════════════════════════════════════════════════════════════════════════════
# CHART 9: Top 3 Features Density (density facet)
# ══════════════════════════════════════════════════════════════════════════════
cat("[09] Top 3 features density facet...\n")

top3 <- names(disc_all)[1:3]
ill_rows <- which(wal_nodes$label == 1L)
lic_rows <- which(wal_nodes$label == 0L)

df9 <- rbindlist(lapply(top3, function(fn) {
  data.table(
    feature    = fn,
    label_name = c(rep("Illicit", length(ill_rows)), rep("Licit", length(lic_rows))),
    value      = c(as.numeric(wal_feats[ill_rows, fn, with=FALSE][[1]]),
                   as.numeric(wal_feats[lic_rows, fn, with=FALSE][[1]]))
  )
}))
df9[, feature := factor(feature, levels=top3)]

p9 <- ggplot(df9, aes(x=value, fill=label_name, color=label_name)) +
  geom_density(alpha=0.4, linewidth=1.0) +
  facet_wrap(~feature, scales="free", ncol=1,
             labeller=labeller(feature=function(x) paste("Feature:", x))) +
  scale_fill_manual(values=label_col)  +
  scale_color_manual(values=label_col) +
  labs(title="Chart 9: Top 3 Most Discriminative Features -- Density by Label",
       subtitle="Features with highest Cohen's d -- clearest separation between illicit and licit",
       x="Feature Value (scaled)", y="Density",
       fill="Label", color="Label",
       caption="Scaling: Log1p+MinMax or RobustScale depending on feature type") +
  theme_aml() + theme(legend.position="bottom")

save_plot(p9, "plot_09_top3_features_density.png", w=9, h=9)

# ══════════════════════════════════════════════════════════════════════════════
# CHART 10: Class Imbalance — Lollipop Chart
# ══════════════════════════════════════════════════════════════════════════════
cat("[10] Class imbalance lollipop...\n")

n_ill <- sum(wal_nodes$label==1L); n_lic <- sum(wal_nodes$label==0L)
n_unk <- sum(wal_nodes$label==-1L)

df10 <- data.table(
  category = c("Illicit\n(money laundering)", "Licit\n(legitimate)", "Unknown\n(unlabeled)"),
  n = c(n_ill, n_lic, n_unk),
  color_grp = c("Illicit","Licit","Unknown")
)
df10[, category := factor(category, levels=rev(category))]
df10[, pct := round(n/sum(n)*100,1)]

p10 <- ggplot(df10, aes(x=n, y=category, color=color_grp)) +
  geom_segment(aes(xend=0, yend=category), linewidth=2, alpha=0.5) +
  geom_point(aes(size=n), alpha=0.9) +
  geom_text(aes(label=paste0(format(n,big.mark=","), " (", pct, "%)")),
            hjust=-0.1, color="white", size=4.5, fontface="bold") +
  scale_color_manual(values=label_col, guide="none") +
  scale_size(range=c(4,18), guide="none") +
  scale_x_continuous(labels=scales::comma,
                     expand=expansion(mult=c(0,.5))) +
  labs(title="Chart 10: Class Imbalance -- Wallet Label Distribution",
       subtitle=sprintf("Imbalance ratio: %.0f:1 licit/illicit | XGBoost scale_pos_weight = %.0f",
                        n_lic/n_ill, n_lic/n_ill),
       x="Number of Wallets", y=NULL,
       caption="Class imbalance is the main challenge | PR-AUC is more meaningful than ROC-AUC") +
  theme_aml() +
  theme(panel.grid.major.y=element_blank())

save_plot(p10, "plot_10_class_imbalance.png")

# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
cat("\n== 10 CHARTS SAVED to outputs/plots/ ==\n\n")

plots_info <- list(
  list(file="plot_01_label_distribution.png",   desc="Label distribution: 17.6:1 imbalance"),
  list(file="plot_02_concept_drift.png",         desc="Concept drift: illicit% varies over time"),
  list(file="plot_03_aml_signal_density.png",    desc="KEY AML signal: pct_illicit_neighbors"),
  list(file="plot_04_discriminative_power.png",  desc="Top 15 features: Cohen's d"),
  list(file="plot_05_connected_txs.png",         desc="Connected TXs: illicit vs licit"),
  list(file="plot_06_wallet_lifetime.png",       desc="Wallet lifetime: n_tsteps"),
  list(file="plot_07_temporal_split.png",        desc="Train/Val/Test split timeline"),
  list(file="plot_08_correlation_heatmap.png",   desc="Feature correlation heatmap"),
  list(file="plot_09_top3_features_density.png", desc="Top 3 features: density illicit vs licit"),
  list(file="plot_10_class_imbalance.png",       desc="Class imbalance lollipop")
)
for (i in seq_along(plots_info)) {
  p <- plots_info[[i]]
  cat(sprintf("  %2d. %-38s — %s\n", i, p$file, p$desc))
}
cat(sprintf("\nFolder: %s/\n", PLOT_DIR))
cat("Next step: open the Shiny dashboard to view updated charts\n\n")

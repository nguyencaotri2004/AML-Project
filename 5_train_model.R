# 5_train_model.R — GPU-Accelerated Training
# ─────────────────────────────────────────────────────────────────
# CHANGES v3 (GPU):
#   - XGBoost LR + XGBoost Main: device='cuda' (RTX 4050)
#   - Random Forest: randomForest với num.threads max cores
#   - Temporal eval: XGBoost device='cuda'
# ─────────────────────────────────────────────────────────────────

source("d:/dsr/config.R", local = FALSE)
# config.R provides: GPU_DEVICE, N_THREADS, DEFAULT_XGB_PARAMS, make_xgb_params()
library(data.table)
library(randomForest)
library(jsonlite)

set.seed(42)

PROC_DIR  <- "processed"
OUT_DIR   <- "outputs"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

# ══════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════

# ── PR-AUC (tốt hơn ROC-AUC với imbalanced data) ─────────────────
pr_auc <- function(labels, scores) {
  ord     <- order(scores, decreasing = TRUE)
  labels  <- labels[ord]
  tp      <- cumsum(labels)
  fp      <- cumsum(1 - labels)
  n_pos   <- sum(labels)
  if (n_pos == 0) return(0)
  precision <- tp / (tp + fp)
  recall    <- tp / n_pos
  n  <- length(recall)
  dr <- diff(recall)
  mp <- (precision[-n] + precision[-1]) / 2
  sum(dr * mp)
}

roc_auc <- function(labels, scores) {
  n1 <- sum(labels); n0 <- sum(1 - labels)
  if (n1 == 0 || n0 == 0) return(0.5)
  r  <- rank(scores)
  (sum(r[labels == 1]) - n1 * (n1 + 1) / 2) / (n1 * n0)
}

# ── Threshold Search — fine-grained (step 0.005) ─────────────────
find_best_threshold <- function(labels, scores) {
  thresholds <- seq(0.02, 0.95, by = 0.005)
  best_f1 <- 0; best_th <- 0.5
  for (th in thresholds) {
    pred <- as.integer(scores >= th)
    tp <- sum(pred == 1 & labels == 1); fp <- sum(pred == 1 & labels == 0)
    fn <- sum(pred == 0 & labels == 1)
    prec <- ifelse(tp + fp == 0, 0, tp / (tp + fp))
    rec  <- ifelse(tp + fn == 0, 0, tp / (tp + fn))
    f1   <- ifelse(prec + rec == 0, 0, 2 * prec * rec / (prec + rec))
    if (f1 > best_f1) { best_f1 <- f1; best_th <- th }
  }
  best_th
}

# ── [FIX 1] Isotonic Calibration ─────────────────────────────────
# Vấn đề: score từ model trained trên time step cũ không calibrated
# cho time step mới (concept drift). Isotonic regression map score
# → calibrated probability dựa trên val set.
# Sau calibration, threshold search trên val áp vào test sẽ đúng hơn.
isotonic_calibrate <- function(val_scores, val_labels, test_scores) {
  # Fit isotonic regression: score → P(illicit)
  # isoreg() đòi monotone tăng → sort by score trước
  ord   <- order(val_scores)
  iso   <- isoreg(val_scores[ord], val_labels[ord])

  # Build step-function lookup
  x_fit <- iso$x
  y_fit <- iso$yf

  # Apply to test scores via piecewise linear interpolation
  calibrated <- approx(x_fit, y_fit, xout = test_scores,
                       method = "linear",
                       yleft  = y_fit[1],
                       yright = y_fit[length(y_fit)])$y
  calibrated[is.na(calibrated)] <- mean(val_labels)
  pmin(pmax(calibrated, 0), 1)
}

# ── Eval metrics với isotonic calibration ─────────────────────────
eval_metrics <- function(labels, scores,
                         val_labels = NULL, val_scores = NULL,
                         calibrate = FALSE) {
  if (!is.null(val_labels) && !is.null(val_scores)) {
    if (calibrate) {
      # Calibrate test scores dùng isotonic regression từ val
      scores_for_th <- isotonic_calibrate(val_scores, val_labels, val_scores)
      scores_eval   <- isotonic_calibrate(val_scores, val_labels, scores)
      threshold     <- find_best_threshold(val_labels, scores_for_th)
    } else {
      scores_eval <- scores
      threshold   <- find_best_threshold(val_labels, val_scores)
    }
  } else {
    scores_eval <- scores
    threshold   <- find_best_threshold(labels, scores)
  }

  pred <- as.integer(scores_eval >= threshold)
  tp <- sum(pred == 1 & labels == 1); fp <- sum(pred == 1 & labels == 0)
  fn <- sum(pred == 0 & labels == 1); tn <- sum(pred == 0 & labels == 0)
  precision <- ifelse(tp + fp == 0, 0, tp / (tp + fp))
  recall    <- ifelse(tp + fn == 0, 0, tp / (tp + fn))
  f1        <- ifelse(precision + recall == 0, 0,
                      2 * precision * recall / (precision + recall))
  list(precision  = round(precision, 4),
       recall     = round(recall,    4),
       f1         = round(f1,        4),
       pr_auc     = round(pr_auc(labels, scores), 4),
       roc_auc    = round(roc_auc(labels, scores), 4),
       threshold  = round(threshold, 4),
       calibrated = calibrate,
       tp = tp, fp = fp, fn = fn, tn = tn)
}


cat("=======================================================\n")
cat("  Elliptic++ — Model Training v2 (Fixed)\n")
cat("=======================================================\n\n")

# ── Load data ──────────────────────────────────────────────────────
cat("[Load] Reading processed data...\n")
nodes_dt  <- fread(file.path(PROC_DIR, "nodes.csv"))
feats_dt  <- fread(file.path(PROC_DIR, "features_std.csv"))
train_idx <- fread(file.path(PROC_DIR, "train_idx.csv"))$idx + 1L
val_idx   <- fread(file.path(PROC_DIR, "val_idx.csv"))$idx   + 1L
test_idx  <- fread(file.path(PROC_DIR, "test_idx.csv"))$idx  + 1L

X <- as.matrix(feats_dt)
y <- nodes_dt$label

X_tr <- X[train_idx, ]; y_tr <- y[train_idx]
X_va <- X[val_idx,   ]; y_va <- y[val_idx]
X_te <- X[test_idx,  ]; y_te <- y[test_idx]

ir <- sum(y_tr == 0) / max(1, sum(y_tr == 1))
cat(sprintf("   Train: %s | Val: %s | Test: %s\n",
    format(length(train_idx), big.mark=","),
    format(length(val_idx),   big.mark=","),
    format(length(test_idx),  big.mark=",")))
cat(sprintf("   Imbalance ratio: %.1f:1 → scale_pos_weight = %.0f\n", ir, ir))
cat(sprintf("   Features: %d\n\n", ncol(X)))

all_metrics <- list()

# ══════════════════════════════════════════════════════════════════
# MODEL 1: LOGISTIC REGRESSION (baseline — gblinear XGBoost)
# ══════════════════════════════════════════════════════════════════
cat("[1/3] Logistic Regression (baseline)...\n")
lr_params <- list(
  booster     = "gblinear",
  objective   = "binary:logistic",
  eval_metric = "aucpr",
  alpha  = 0.01,
  lambda = 1.0,
  device = GPU_DEVICE,
  nthread = N_THREADS
)
dtrain_lr <- xgb.DMatrix(X_tr, label = y_tr)
dtest_lr  <- xgb.DMatrix(X_te, label = y_te)
dval_lr   <- xgb.DMatrix(X_va, label = y_va)

m_lr <- xgb.train(lr_params, dtrain_lr, nrounds = 1,
                  verbose = 0, scale_pos_weight = ir)
pred_lr     <- predict(m_lr, dtest_lr)
pred_lr_val <- predict(m_lr, dval_lr)

# Isotonic calibration cho LR cũng vậy
m_lr_metrics <- eval_metrics(y_te, pred_lr, y_va, pred_lr_val, calibrate = TRUE)
cat(sprintf("   LR  — F1: %.3f | PR-AUC: %.3f | ROC-AUC: %.3f | threshold: %.3f\n",
    m_lr_metrics$f1, m_lr_metrics$pr_auc, m_lr_metrics$roc_auc, m_lr_metrics$threshold))
all_metrics[["logistic_regression"]] <- m_lr_metrics
# Save LR model
xgb.save(m_lr, file.path(OUT_DIR, "lr_model.bin"))
cat(sprintf("   Saved: lr_model.bin (threshold=%.3f)\n", m_lr_metrics$threshold))

# ══════════════════════════════════════════════════════════════════
# MODEL 2: RANDOM FOREST
# [FIX 3] Tìm threshold tốt hơn trên val + calibration
# ══════════════════════════════════════════════════════════════════
cat("[2/3] Random Forest (parallel CPU — all cores)...\n")
class_weights <- c("0" = 1, "1" = round(ir))
m_rf <- randomForest(
  x        = X_tr,
  y        = factor(y_tr, levels = c(0, 1)),
  ntree    = 500,
  mtry     = floor(sqrt(ncol(X_tr))),
  classwt  = class_weights,
  importance = TRUE,
  num.threads = N_THREADS   # sử dụng tất cả CPU cores
)
pred_rf_prob <- predict(m_rf, X_te, type = "prob")[, "1"]
pred_rf_val  <- predict(m_rf, X_va, type = "prob")[, "1"]

# RF prob có thể không calibrated → dùng isotonic
m_rf_metrics <- eval_metrics(y_te, pred_rf_prob, y_va, pred_rf_val, calibrate = TRUE)
cat(sprintf("   RF  — F1: %.3f | PR-AUC: %.3f | ROC-AUC: %.3f | threshold: %.3f\n",
    m_rf_metrics$f1, m_rf_metrics$pr_auc, m_rf_metrics$roc_auc, m_rf_metrics$threshold))
all_metrics[["random_forest"]] <- m_rf_metrics
# Save RF model
saveRDS(m_rf, file.path(OUT_DIR, "rf_model.rds"))
cat(sprintf("   Saved: rf_model.rds (threshold=%.3f)\n", m_rf_metrics$threshold))

# RF feature importance (top 15)
rf_imp    <- importance(m_rf, type = 1)
rf_imp_df <- data.frame(feature    = rownames(rf_imp),
                         importance = rf_imp[, 1])
rf_imp_df <- rf_imp_df[order(-rf_imp_df$importance), ]

# ══════════════════════════════════════════════════════════════════
# MODEL 3: XGBOOST (main model)
# [FIX 2] Hyperparams tuned: eta nhỏ hơn, gamma, nrounds 500
# ══════════════════════════════════════════════════════════════════
cat(sprintf("[3/3] XGBoost (GPU: %s — tuned v3)...\n", GPU_DEVICE))

xgb_params <- list(
  objective        = "binary:logistic",
  eval_metric      = "aucpr",
  tree_method      = "hist",       # hist là default cho GPU
  device           = GPU_DEVICE,   # 'cuda' nếu GPU available
  max_depth        = 5,
  eta              = 0.03,
  subsample        = 0.8,
  colsample_bytree = 0.8,
  min_child_weight = 10,
  gamma            = 0.1,
  scale_pos_weight = ir,
  nthread          = N_THREADS
)

dtrain <- xgb.DMatrix(X_tr, label = y_tr)
dval   <- xgb.DMatrix(X_va, label = y_va)
dtest  <- xgb.DMatrix(X_te, label = y_te)

m_xgb <- xgb.train(
  params    = xgb_params,
  data      = dtrain,
  nrounds   = 500,
  evals     = list(val = dval),
  early_stopping_rounds = 30,
  verbose   = 1,
  print_every_n = 50
)

pred_xgb     <- predict(m_xgb, dtest)
pred_xgb_val <- predict(m_xgb, dval)

# [FIX 1] Isotonic calibration — fix concept drift threshold shift
m_xgb_metrics <- eval_metrics(y_te, pred_xgb, y_va, pred_xgb_val, calibrate = TRUE)
cat(sprintf("\n   XGB — F1: %.3f | PR-AUC: %.3f | ROC-AUC: %.3f | threshold: %.3f\n",
    m_xgb_metrics$f1, m_xgb_metrics$pr_auc, m_xgb_metrics$roc_auc, m_xgb_metrics$threshold))
all_metrics[["xgboost"]] <- m_xgb_metrics

# XGBoost feature importance
xgb_imp    <- xgb.importance(model = m_xgb)
xgb_imp_df <- as.data.frame(xgb_imp)[1:min(20, nrow(xgb_imp)), ]

cat("\n   Top 15 XGBoost features:\n")
for (i in seq_len(min(15, nrow(xgb_imp_df)))) {
  cat(sprintf("     %2d. %-40s  Gain=%.4f\n",
      i, xgb_imp_df$Feature[i], xgb_imp_df$Gain[i]))
}

# Save XGBoost model
xgb.save(m_xgb, file.path(OUT_DIR, "xgboost_model.bin"))
cat(sprintf("   Saved: xgboost_model.bin (threshold=%.3f)\n", m_xgb_metrics$threshold))

# Save thresholds for all models (used by Shiny app real-time prediction)
thresholds_out <- list(
  logistic_regression = list(
    threshold = m_lr_metrics$threshold,
    f1 = m_lr_metrics$f1,
    pr_auc = m_lr_metrics$pr_auc
  ),
  random_forest = list(
    threshold = m_rf_metrics$threshold,
    f1 = m_rf_metrics$f1,
    pr_auc = m_rf_metrics$pr_auc
  ),
  xgboost = list(
    threshold = m_xgb_metrics$threshold,
    f1 = m_xgb_metrics$f1,
    pr_auc = m_xgb_metrics$pr_auc
  )
)
write_json(thresholds_out, file.path(OUT_DIR, "model_thresholds.json"),
           pretty = TRUE, auto_unbox = TRUE)
cat("   Saved: model_thresholds.json\n")

# ══════════════════════════════════════════════════════════════════
# ROLLING TEMPORAL EVALUATION
# ══════════════════════════════════════════════════════════════════
cat("\n[Temporal] Rolling evaluation over windows...\n")
splits_dir  <- file.path(PROC_DIR, "splits")
window_dirs <- sort(list.dirs(splits_dir, recursive = FALSE, full.names = TRUE))

temporal_results <- list()

for (wdir in window_dirs) {
  win_id <- as.integer(gsub(".*window_", "", basename(wdir)))
  meta   <- fromJSON(file.path(wdir, "meta.json"))

  w_tr <- fread(file.path(wdir, "train_idx.csv"))$idx + 1L
  w_va <- fread(file.path(wdir, "val_idx.csv"))$idx   + 1L
  w_te <- fread(file.path(wdir, "test_idx.csv"))$idx  + 1L

  Xw_tr <- X[w_tr, ]; yw_tr <- y[w_tr]
  Xw_va <- X[w_va, ]; yw_va <- y[w_va]
  Xw_te <- X[w_te, ]; yw_te <- y[w_te]

  if (sum(yw_te == 1) == 0 || sum(yw_tr == 1) == 0) {
    cat(sprintf("   Window %02d: skipped (no illicit in train/test)\n", win_id))
    next
  }

  ir_w  <- sum(yw_tr == 0) / max(1, sum(yw_tr == 1))
  dw_tr <- xgb.DMatrix(Xw_tr, label = yw_tr)
  dw_va <- xgb.DMatrix(Xw_va, label = yw_va)
  dw_te <- xgb.DMatrix(Xw_te, label = yw_te)

  m_w <- xgb.train(
    params    = modifyList(xgb_params, list(scale_pos_weight = ir_w)),
    data      = dw_tr,
    nrounds   = 200,
    evals     = list(val = dw_va),
    early_stopping_rounds = 20,
    verbose   = 0
  )

  pw   <- predict(m_w, dw_te)
  pv   <- predict(m_w, dw_va)
  mets <- eval_metrics(yw_te, pw, yw_va, pv, calibrate = TRUE)

  temporal_results[[win_id]] <- list(
    window_id      = win_id,
    train_time_max = meta$train_time_max,
    test_times     = paste(unlist(meta$test_times), collapse="-"),
    n_train        = length(w_tr),
    n_test         = length(w_te),
    n_illicit_test = sum(yw_te == 1),
    f1             = mets$f1,
    pr_auc         = mets$pr_auc,
    roc_auc        = mets$roc_auc,
    precision      = mets$precision,
    recall         = mets$recall
  )

  cat(sprintf("   Window %02d (test t%s): F1=%.3f | PR-AUC=%.3f | Recall=%.3f\n",
      win_id, temporal_results[[win_id]]$test_times,
      mets$f1, mets$pr_auc, mets$recall))
}

temporal_dt <- rbindlist(temporal_results)
fwrite(temporal_dt, file.path(OUT_DIR, "temporal_results.csv"))

cat(sprintf("\n   Temporal PR-AUC — Mean: %.3f | Std: %.3f | Min: %.3f | Max: %.3f\n",
    mean(temporal_dt$pr_auc), sd(temporal_dt$pr_auc),
    min(temporal_dt$pr_auc), max(temporal_dt$pr_auc)))
cat(sprintf("   Temporal F1     — Mean: %.3f | Std: %.3f\n",
    mean(temporal_dt$f1), sd(temporal_dt$f1)))

# ══════════════════════════════════════════════════════════════════
# SAVE METRICS
# ══════════════════════════════════════════════════════════════════
cat("\n[Save] Saving metrics...\n")

metrics_out <- list(
  dataset  = "Elliptic++",
  language = "R",
  version  = "v2_isotonic_calibration",
  models   = list(
    logistic_regression = modifyList(all_metrics$logistic_regression,
                                     list(model = "Logistic Regression")),
    random_forest       = modifyList(all_metrics$random_forest,
                                     list(model = "Random Forest", ntree = 500)),
    xgboost             = modifyList(all_metrics$xgboost,
                                     list(model    = "XGBoost v2",
                                          best_iteration   = m_xgb$best_iteration,
                                          scale_pos_weight = round(ir),
                                          params    = xgb_params))
  ),
  temporal_summary = list(
    n_windows   = nrow(temporal_dt),
    pr_auc_mean = round(mean(temporal_dt$pr_auc), 4),
    pr_auc_std  = round(sd(temporal_dt$pr_auc),   4),
    pr_auc_min  = round(min(temporal_dt$pr_auc),  4),
    pr_auc_max  = round(max(temporal_dt$pr_auc),  4),
    f1_mean     = round(mean(temporal_dt$f1),      4),
    f1_std      = round(sd(temporal_dt$f1),        4)
  ),
  top_features_xgb = as.list(xgb_imp_df$Feature[1:min(15, nrow(xgb_imp_df))]),
  top_features_rf  = as.list(rf_imp_df$feature[1:min(15, nrow(rf_imp_df))])
)

write_json(metrics_out, file.path(OUT_DIR, "metrics.json"),
           pretty = TRUE, auto_unbox = TRUE)

cat("\n=== MODEL COMPARISON ===\n")
cat(sprintf("%-22s | %-6s | %-7s | %-7s | %-6s | %-9s\n",
    "Model", "F1", "PR-AUC", "ROC-AUC", "Recall", "Threshold"))
cat(strrep("-", 72), "\n")
for (nm in names(all_metrics)) {
  m <- all_metrics[[nm]]
  cat(sprintf("%-22s | %.3f  | %.4f  | %.4f  | %.3f  | %.4f\n",
      nm, m$f1, m$pr_auc, m$roc_auc, m$recall, m$threshold))
}

cat("\n=== DONE ===\n")
cat("outputs/metrics.json\n")
cat("outputs/temporal_results.csv\n")
cat("outputs/xgboost_model.bin\n")
cat("\nReady for: Rscript R/app/app.R (Shiny)\n")

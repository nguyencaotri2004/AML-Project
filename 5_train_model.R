# 5_train_model.R — Wallet-Level AML: Train + Evaluate
# ══════════════════════════════════════════════════════════════════════════════
# Input : processed/wallet/wallet_nodes.csv
#         processed/wallet/wallet_features_full.csv  (40 features, scaled)
#         processed/wallet/train_idx.csv / val_idx.csv / test_idx.csv
#         processed/wallet/windows/win_01/ ... win_36/  (36-window splits)
#
# PHAN 1 — GLOBAL TRAIN/TEST:
#   Train 3 models (LR, RF, XGBoost) tren global split
#   So sanh + chon best model
#
# PHAN 2 — 36-WINDOW TRAIN/TEST:
#   Moi window: XGBoost train tren win_XX/train_idx.csv
#               Eval   tren win_XX/test_idx.csv
#   Bao cao F1/PR-AUC tren toan bo 36 windows
#
# Output: outputs/wallet_models/best_model.*
#         outputs/wallet_metrics.json
#         outputs/wallet_predictions.csv
#         outputs/multiwindow_36_results.csv
# ══════════════════════════════════════════════════════════════════════════════

source("d:/dsr/config.R", local=FALSE)
library(data.table)
library(randomForest)
library(jsonlite)
set.seed(42)

PROC_W  <- "processed/wallet"
OUT_DIR <- "outputs"
MDL_DIR <- file.path(OUT_DIR, "wallet_models")
WIN_DIR <- file.path(PROC_W, "windows")
dir.create(MDL_DIR, showWarnings=FALSE, recursive=TRUE)

# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

pr_auc <- function(labels, scores) {
  ord  <- order(scores, decreasing=TRUE); labels <- labels[ord]
  tp   <- cumsum(labels); fp <- cumsum(1-labels); n_pos <- sum(labels)
  if (n_pos == 0) return(0)
  prec <- tp/(tp+fp); rec <- tp/n_pos; n <- length(rec)
  sum(diff(rec) * (prec[-n]+prec[-1])/2)
}

roc_auc <- function(labels, scores) {
  n1 <- sum(labels); n0 <- sum(1-labels)
  if (n1==0 || n0==0) return(0.5)
  (sum(rank(scores)[labels==1]) - n1*(n1+1)/2) / (n1*n0)
}

find_best_threshold <- function(labels, scores) {
  best_f1 <- 0; best_th <- 0.5
  for (th in seq(0.01, 0.99, by=0.005)) {
    pred <- as.integer(scores >= th)
    tp <- sum(pred==1 & labels==1); fp <- sum(pred==1 & labels==0)
    fn <- sum(pred==0 & labels==1)
    prec <- ifelse(tp+fp==0, 0, tp/(tp+fp))
    rec  <- ifelse(tp+fn==0, 0, tp/(tp+fn))
    f1   <- ifelse(prec+rec==0, 0, 2*prec*rec/(prec+rec))
    if (f1 > best_f1) { best_f1 <- f1; best_th <- th }
  }
  best_th
}

isotonic_calibrate <- function(val_scores, val_labels, test_scores) {
  ord <- order(val_scores)
  iso <- isoreg(val_scores[ord], val_labels[ord])
  cal <- approx(iso$x, iso$yf, xout=test_scores, method="linear",
                yleft=iso$yf[1], yright=iso$yf[length(iso$yf)])$y
  cal[is.na(cal)] <- mean(val_labels)
  pmin(pmax(cal, 0), 1)
}

eval_model <- function(y_te, scores_te, y_va, scores_va) {
  cal_va <- isotonic_calibrate(scores_va, y_va, scores_va)
  cal_te <- isotonic_calibrate(scores_va, y_va, scores_te)
  th     <- find_best_threshold(y_va, cal_va)
  pred   <- as.integer(cal_te >= th)
  tp <- sum(pred==1 & y_te==1); fp <- sum(pred==1 & y_te==0)
  fn <- sum(pred==0 & y_te==1); tn <- sum(pred==0 & y_te==0)
  prec <- ifelse(tp+fp==0, 0, tp/(tp+fp))
  rec  <- ifelse(tp+fn==0, 0, tp/(tp+fn))
  f1   <- ifelse(prec+rec==0, 0, 2*prec*rec/(prec+rec))
  list(f1=round(f1,4), precision=round(prec,4), recall=round(rec,4),
       pr_auc=round(pr_auc(y_te, cal_te),4),
       roc_auc=round(roc_auc(y_te, cal_te),4),
       threshold=round(th,4), tp=tp, fp=fp, fn=fn, tn=tn,
       scores_te=cal_te)
}

# ══════════════════════════════════════════════════════════════════════════════
# LOAD DATA
# ══════════════════════════════════════════════════════════════════════════════
cat("╔══════════════════════════════════════════════════════╗\n")
cat("║  5_train_model.R — Wallet AML Training              ║\n")
cat("╚══════════════════════════════════════════════════════╝\n\n")

cat("[Load] Doc data...\n")
nodes_dt  <- fread(file.path(PROC_W, "wallet_nodes.csv"))
feats_dt  <- fread(file.path(PROC_W, "wallet_features_full.csv"))
train_idx <- fread(file.path(PROC_W, "train_idx.csv"))$idx + 1L
val_idx   <- fread(file.path(PROC_W, "val_idx.csv"))$idx   + 1L
test_idx  <- fread(file.path(PROC_W, "test_idx.csv"))$idx  + 1L

X <- as.matrix(feats_dt)
y <- nodes_dt$label

X_tr <- X[train_idx,]; y_tr <- y[train_idx]
X_va <- X[val_idx,  ]; y_va <- y[val_idx  ]
X_te <- X[test_idx, ]; y_te <- y[test_idx ]

keep_tr <- which(y_tr %in% c(0L,1L))
keep_va <- which(y_va %in% c(0L,1L))
keep_te <- which(y_te %in% c(0L,1L))
X_tr <- X_tr[keep_tr,]; y_tr <- y_tr[keep_tr]
X_va <- X_va[keep_va,]; y_va <- y_va[keep_va]
X_te <- X_te[keep_te,]; y_te <- y_te[keep_te]

ir <- sum(y_tr==0) / max(1, sum(y_tr==1))

cat(sprintf("  Train:  %s wallets (%d illicit)\n",
    format(length(y_tr),big.mark=","), sum(y_tr==1)))
cat(sprintf("  Val:    %s wallets (%d illicit)\n",
    format(length(y_va),big.mark=","), sum(y_va==1)))
cat(sprintf("  Test:   %s wallets (%d illicit)\n",
    format(length(y_te),big.mark=","), sum(y_te==1)))
cat(sprintf("  Imbalance: %.1f:1 | Features: %d\n\n", ir, ncol(X)))

all_metrics <- list()
all_scores  <- list()

# ══════════════════════════════════════════════════════════════════════════════
# PHAN 1: GLOBAL TRAIN/TEST — 3 MODELS
# ══════════════════════════════════════════════════════════════════════════════
cat("┌─────────────────────────────────────────────────────┐\n")
cat("│  PHAN 1: GLOBAL TRAIN/TEST (1 split toan bo)        │\n")
cat("└─────────────────────────────────────────────────────┘\n\n")

# --- Model 1: Logistic Regression (baseline) ---
cat("[1/3] Logistic Regression (XGBoost gblinear)...\n")
lr_params <- list(booster="gblinear", objective="binary:logistic",
                  eval_metric="aucpr", alpha=0.01, lambda=1.0,
                  device=GPU_DEVICE, nthread=N_THREADS)
dtrain_lr <- xgb.DMatrix(X_tr, label=y_tr)
m_lr      <- xgb.train(lr_params, dtrain_lr, nrounds=1,
                       verbose=0, scale_pos_weight=ir)
m_lr_met  <- eval_model(y_te, predict(m_lr, xgb.DMatrix(X_te)),
                        y_va, predict(m_lr, xgb.DMatrix(X_va)))
all_metrics[["logistic_regression"]] <- m_lr_met
all_scores[["logistic_regression"]]  <- m_lr_met$scores_te
cat(sprintf("  F1=%.4f | PR-AUC=%.4f | Recall=%.4f | th=%.3f\n",
    m_lr_met$f1, m_lr_met$pr_auc, m_lr_met$recall, m_lr_met$threshold))
cat(sprintf("  TP=%d FP=%d FN=%d\n\n",
    m_lr_met$tp, m_lr_met$fp, m_lr_met$fn))

# --- Model 2: Random Forest ---
cat("[2/3] Random Forest (500 cay, parallel CPU)...\n")
cat("  (Co the mat 2-5 phut...)\n")
m_rf <- randomForest(x=X_tr, y=factor(y_tr, levels=c(0,1)),
                     ntree=500, mtry=floor(sqrt(ncol(X_tr))),
                     classwt=c("0"=1, "1"=round(ir)),
                     importance=TRUE, num.threads=N_THREADS)
pred_rf_te <- predict(m_rf, X_te, type="prob")[,"1"]
pred_rf_va <- predict(m_rf, X_va, type="prob")[,"1"]
m_rf_met   <- eval_model(y_te, pred_rf_te, y_va, pred_rf_va)
all_metrics[["random_forest"]] <- m_rf_met
all_scores[["random_forest"]]  <- m_rf_met$scores_te
cat(sprintf("  F1=%.4f | PR-AUC=%.4f | Recall=%.4f | th=%.3f\n",
    m_rf_met$f1, m_rf_met$pr_auc, m_rf_met$recall, m_rf_met$threshold))
cat(sprintf("  TP=%d FP=%d FN=%d\n\n", m_rf_met$tp, m_rf_met$fp, m_rf_met$fn))

rf_imp    <- importance(m_rf, type=1)
rf_imp_df <- data.frame(feature=rownames(rf_imp), importance=rf_imp[,1])
rf_imp_df <- rf_imp_df[order(-rf_imp_df$importance),]

# --- Model 3: XGBoost GPU ---
cat(sprintf("[3/3] XGBoost GPU (%s)...\n", GPU_DEVICE))
xgb_params <- list(
  objective="binary:logistic", eval_metric="aucpr",
  tree_method="hist", device=GPU_DEVICE,
  max_depth=6, eta=0.03, subsample=0.8,
  colsample_bytree=0.8, min_child_weight=10,
  gamma=0.1, scale_pos_weight=ir, nthread=N_THREADS
)
dtrain <- xgb.DMatrix(X_tr, label=y_tr)
dval   <- xgb.DMatrix(X_va, label=y_va)
dtest  <- xgb.DMatrix(X_te, label=y_te)
m_xgb  <- xgb.train(params=xgb_params, data=dtrain, nrounds=1000,
                     evals=list(val=dval), early_stopping_rounds=50,
                     verbose=1, print_every_n=100)
m_xgb_met <- eval_model(y_te, predict(m_xgb, dtest),
                        y_va, predict(m_xgb, dval))
all_metrics[["xgboost"]] <- m_xgb_met
all_scores[["xgboost"]]  <- m_xgb_met$scores_te
cat(sprintf("\n  F1=%.4f | PR-AUC=%.4f | Recall=%.4f | th=%.3f\n",
    m_xgb_met$f1, m_xgb_met$pr_auc, m_xgb_met$recall, m_xgb_met$threshold))
cat(sprintf("  TP=%d FP=%d FN=%d | Best round: %d\n\n",
    m_xgb_met$tp, m_xgb_met$fp, m_xgb_met$fn, m_xgb$best_iteration))

xgb_imp    <- xgb.importance(model=m_xgb)
xgb_imp_df <- as.data.frame(xgb_imp)[1:min(15,nrow(xgb_imp)),]

# --- So sanh va chon best ---
cat("┌─────────────────────────────────────────────────────┐\n")
cat("│  KET QUA SO SANH — PHAN 1                          │\n")
cat("└─────────────────────────────────────────────────────┘\n")
cat(sprintf("  %-24s | %6s | %7s | %7s | %6s\n",
    "Model","F1","PR-AUC","ROC-AUC","Recall"))
cat(strrep("-",66), "\n")
for (nm in names(all_metrics)) {
  m <- all_metrics[[nm]]
  cat(sprintf("  %-24s | %.4f | %.4f  | %.4f  | %.4f\n",
      nm, m$f1, m$pr_auc, m$roc_auc, m$recall))
}
cat(sprintf("  %-24s | 0.8300 (paper reference)\n", "Elmougy 2023 RF"))

# Chon best theo PR-AUC
best_name   <- names(which.max(sapply(all_metrics, function(m) m$pr_auc)))
best_met    <- all_metrics[[best_name]]
best_scores <- all_scores[[best_name]]

cat(sprintf("\n  => BEST MODEL: [%s] PR-AUC=%.4f F1=%.4f\n\n",
    toupper(best_name), best_met$pr_auc, best_met$f1))

# --- Luu models ---
cat("[Save] Luu models...\n")
xgb.save(m_lr,  file.path(MDL_DIR, "lr_model.ubj"))
saveRDS(m_rf,   file.path(MDL_DIR, "rf_model.rds"))
xgb.save(m_xgb, file.path(MDL_DIR, "xgboost_wallet.ubj"))

if (best_name == "xgboost") {
  xgb.save(m_xgb, file.path(MDL_DIR, "best_model.ubj"))
  info_json <- jsonlite::toJSON(list(type="xgboost",
    threshold=best_met$threshold, file="wallet_models/best_model.ubj"),
    auto_unbox=TRUE)
  writeLines(as.character(info_json), file.path(OUT_DIR, "best_model_info.json"))
} else if (best_name == "random_forest") {
  saveRDS(m_rf, file.path(MDL_DIR, "best_model.rds"))
  info_json <- jsonlite::toJSON(list(type="random_forest",
    threshold=best_met$threshold, file="wallet_models/best_model.rds"),
    auto_unbox=TRUE)
  writeLines(as.character(info_json), file.path(OUT_DIR, "best_model_info.json"))
} else {
  xgb.save(m_lr, file.path(MDL_DIR, "best_model.ubj"))
  info_json <- jsonlite::toJSON(list(type="logistic_regression",
    threshold=best_met$threshold, file="wallet_models/best_model.ubj"),
    auto_unbox=TRUE)
  writeLines(as.character(info_json), file.path(OUT_DIR, "best_model_info.json"))
}

# Predictions tren test set (best model)
test_nodes     <- nodes_dt[test_idx[keep_te],]
predictions_dt <- data.table(
  wallet_id  = test_nodes$wallet_id,
  true_label = y_te,
  score      = round(best_scores, 6),
  predicted  = as.integer(best_scores >= best_met$threshold),
  model      = best_name
)
fwrite(predictions_dt, file.path(OUT_DIR, "wallet_predictions.csv"))

# Metrics JSON
metrics_p1 <- list(
  dataset="Elliptic++ (Wallet-Level AML)", part="global_split",
  n_features=ncol(X), imbalance_ratio=round(ir,2),
  split=list(train=length(y_tr), val=length(y_va), test=length(y_te)),
  best_model=best_name,
  models=list(
    logistic_regression=c(list(model="Logistic Regression"),
      all_metrics$logistic_regression[c("f1","pr_auc","roc_auc","precision","recall","threshold","tp","fp","fn","tn")]),
    random_forest=c(list(model="Random Forest", ntree=500),
      all_metrics$random_forest[c("f1","pr_auc","roc_auc","precision","recall","threshold","tp","fp","fn","tn")]),
    xgboost=c(list(model="XGBoost GPU", best_round=m_xgb$best_iteration),
      all_metrics$xgboost[c("f1","pr_auc","roc_auc","precision","recall","threshold","tp","fp","fn","tn")])
  ),
  top_features_xgb=as.list(xgb_imp_df$Feature[1:min(15,nrow(xgb_imp_df))]),
  top_features_rf =as.list(rf_imp_df$feature[1:min(15,nrow(rf_imp_df))]),
  reference=list(elmougy_2023_rf_f1=0.83)
)
write_json(metrics_p1, file.path(OUT_DIR, "wallet_metrics.json"),
           pretty=TRUE, auto_unbox=TRUE)

cat(sprintf("  Saved: best_model=%s (th=%.3f)\n", best_name, best_met$threshold))
cat(sprintf("  Saved: wallet_predictions.csv (%d rows)\n", nrow(predictions_dt)))
cat(sprintf("  Saved: wallet_metrics.json\n\n"))

# ══════════════════════════════════════════════════════════════════════════════
# PHAN 2: 36-WINDOW TRAIN/TEST — MOI WINDOW LA 1 THU MUC RIENG
# ══════════════════════════════════════════════════════════════════════════════
cat("┌─────────────────────────────────────────────────────┐\n")
cat("│  PHAN 2: 36-WINDOW TRAIN/TEST (folder-based)        │\n")
cat("└─────────────────────────────────────────────────────┘\n\n")

win_dirs <- sort(list.dirs(WIN_DIR, recursive=FALSE, full.names=TRUE))

if (length(win_dirs) == 0) {
  cat("  [SKIP] Chua co window folders.\n")
  cat("  Chay lai: Rscript R/2_tien_xu_ly_du_lieu.R truoc.\n\n")
} else {
  cat(sprintf("  Tim thay %d window folders trong: %s\n\n", length(win_dirs), WIN_DIR))

  xgb_win <- list(
    objective="binary:logistic", eval_metric="aucpr",
    tree_method="hist", device=GPU_DEVICE,
    max_depth=6, eta=0.05, subsample=0.8,
    colsample_bytree=0.8, min_child_weight=5,
    gamma=0.05, nthread=N_THREADS
  )

  cat(sprintf("  %-5s | %-8s | %-7s | %-7s | %-7s | %-7s | %s\n",
      "Win","TrainMax","n_train","n_test","F1","PR-AUC","FP"))
  cat(strrep("-", 62), "\n")

  win_results <- list()

  for (wdir in win_dirs) {
    meta  <- fromJSON(file.path(wdir, "meta.json"))
    wid   <- meta$window_id
    tr_i  <- fread(file.path(wdir, "train_idx.csv"))$idx + 1L
    va_i  <- fread(file.path(wdir, "val_idx.csv"))$idx   + 1L
    te_i  <- fread(file.path(wdir, "test_idx.csv"))$idx  + 1L
    y_tr_w <- y[tr_i]; y_va_w <- y[va_i]; y_te_w <- y[te_i]

    if (sum(y_te_w==1L) < 1L || sum(y_tr_w==1L) < 10L || length(va_i) < 5L) {
      cat(sprintf("  %3d  | t01-t%02d | SKIP (ill_tr=%d ill_te=%d)\n",
          wid, meta$train_max, sum(y_tr_w==1), sum(y_te_w==1)))
      next
    }

    ir_w  <- sum(y_tr_w==0L) / max(1L, sum(y_tr_w==1L))
    dm_tr <- xgb.DMatrix(X[tr_i,], label=y_tr_w)
    dm_va <- xgb.DMatrix(X[va_i,], label=y_va_w)
    dm_te <- xgb.DMatrix(X[te_i,], label=y_te_w)

    m_w  <- xgb.train(
      modifyList(xgb_win, list(scale_pos_weight=ir_w)),
      dm_tr, nrounds=200,
      evals=list(val=dm_va), early_stopping_rounds=15,
      verbose=0
    )

    sc_va  <- predict(m_w, dm_va); sc_te <- predict(m_w, dm_te)
    cal_va <- isotonic_calibrate(sc_va, y_va_w, sc_va)
    cal_te <- isotonic_calibrate(sc_va, y_va_w, sc_te)
    th_w   <- find_best_threshold(y_va_w, cal_va)

    pred_w <- as.integer(cal_te >= th_w)
    tp_w <- sum(pred_w==1 & y_te_w==1); fp_w <- sum(pred_w==1 & y_te_w==0)
    fn_w <- sum(pred_w==0 & y_te_w==1)
    pr_w <- ifelse(tp_w+fp_w==0, 0, tp_w/(tp_w+fp_w))
    re_w <- ifelse(tp_w+fn_w==0, 0, tp_w/(tp_w+fn_w))
    f1_w <- ifelse(pr_w+re_w==0, 0, 2*pr_w*re_w/(pr_w+re_w))
    pa_w <- pr_auc(y_te_w, cal_te)

    cat(sprintf("  %3d  | t01-t%02d | %6d | %6d | %.4f | %.4f | %d\n",
        wid, meta$train_max, length(tr_i), length(te_i), f1_w, pa_w, fp_w))

    win_results[[length(win_results)+1]] <- list(
      window=wid, train_max=meta$train_max,
      n_train=length(tr_i), n_test=length(te_i),
      n_illicit_test=sum(y_te_w==1),
      f1=round(f1_w,4), pr_auc=round(pa_w,4),
      recall=round(re_w,4), precision=round(pr_w,4),
      fp=fp_w, fn=fn_w, threshold=round(th_w,4)
    )
  }

  if (length(win_results) > 0) {
    dt_win <- rbindlist(win_results)
    fwrite(dt_win, file.path(OUT_DIR, "multiwindow_36_results.csv"))

    cat(strrep("-", 62), "\n")
    cat(sprintf("  MEAN | n=%d | F1=%.4f | PR-AUC=%.4f | Recall=%.4f\n",
        nrow(dt_win), mean(dt_win$f1), mean(dt_win$pr_auc,na.rm=TRUE),
        mean(dt_win$recall)))
    cat(sprintf("  STD  |       | F1=%.4f | PR-AUC=%.4f\n",
        sd(dt_win$f1), sd(dt_win$pr_auc,na.rm=TRUE)))
    cat(sprintf("  MIN  |       | F1=%.4f | PR-AUC=%.4f\n",
        min(dt_win$f1), min(dt_win$pr_auc,na.rm=TRUE)))

    worst <- dt_win[which.min(dt_win$f1),]
    cat(sprintf("\n  Window kho nhat: Win%d (t01-t%02d) F1=%.4f ill_te=%d\n",
        worst$window, worst$train_max, worst$f1, worst$n_illicit_test))

    # Update wallet_metrics.json voi ket qua 36-window
    m_json <- fromJSON(file.path(OUT_DIR,"wallet_metrics.json"))
    m_json$multiwindow_36 <- list(
      n_windows=nrow(dt_win), method="folder-based",
      f1_mean=round(mean(dt_win$f1),4), f1_std=round(sd(dt_win$f1),4),
      f1_min=round(min(dt_win$f1),4), f1_max=round(max(dt_win$f1),4),
      pr_auc_mean=round(mean(dt_win$pr_auc,na.rm=TRUE),4)
    )
    write_json(m_json, file.path(OUT_DIR,"wallet_metrics.json"),
               pretty=TRUE, auto_unbox=TRUE)

    cat(sprintf("\n  Saved: outputs/multiwindow_36_results.csv\n"))
    cat(sprintf("  wallet_metrics.json updated voi section multiwindow_36\n\n"))
    if (mean(dt_win$f1) > 0.95) {
      cat(sprintf("  => MODEL ROBUST: F1 mean=%.4f qua %d windows!\n",
          mean(dt_win$f1), nrow(dt_win)))
    }
  }
}

# ══════════════════════════════════════════════════════════════════════════════
# TONG KET
# ══════════════════════════════════════════════════════════════════════════════
cat("\n╔══════════════════════════════════════════════════════╗\n")
cat("║  HOAN THANH — TOM TAT                               ║\n")
cat("╚══════════════════════════════════════════════════════╝\n\n")
cat(sprintf("  PHAN 1 — Global best model : %s\n", best_name))
cat(sprintf("           F1=%.4f | PR-AUC=%.4f | Recall=%.4f\n",
    best_met$f1, best_met$pr_auc, best_met$recall))
cat(sprintf("           False Alarms: %d FP / %d total predictions\n\n",
    best_met$fp, best_met$tp+best_met$fp))
cat("  PHAN 2 — 36-window XGBoost: xem ket qua o tren\n\n")
cat("  Output files:\n")
cat("    outputs/wallet_metrics.json        — metrics ca 2 phan\n")
cat("    outputs/wallet_predictions.csv     — predictions test set\n")
cat("    outputs/wallet_models/best_model.* — model tot nhat\n")
cat("    outputs/multiwindow_36_results.csv — 36-window results\n\n")
cat("Next: Rscript R/6_evaluate.R\n")

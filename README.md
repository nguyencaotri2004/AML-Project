# 🕵️ AML Detective — Anti-Money Laundering Detection on Elliptic++

[![R](https://img.shields.io/badge/R-%3E%3D4.1-blue)](https://www.r-project.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![Dataset](https://img.shields.io/badge/Dataset-Elliptic++-orange)](https://www.kaggle.com/datasets/ellipticco/elliptic-data-set)

Một hệ thống phát hiện rửa tiền (Anti-Money Laundering) trên mạng Bitcoin, sử dụng dataset **Elliptic++** với mô hình **Random Forest**, **XGBoost**, và kỹ thuật trích xuất đặc trưng từ đồ thị bipartite. Bao gồm một dashboard **Shiny** trực quan để khám phá dữ liệu, so sánh mô hình, và kiểm tra ví Bitcoin cụ thể.

---

## 📊 Kết quả chính

| Mô hình | F1-Score | PR-AUC | Precision | Recall | Threshold |
|---|---|---|---|---|---|
| Logistic Regression | 0.9031 | 0.8954 | 0.9348 | 0.8735 | 0.085 |
| **Random Forest** ⭐ | **1.0000** | **0.9996** | **1.0000** | **1.0000** | 0.010 |
| XGBoost (GPU) | **1.0000** | **0.9996** | **1.0000** | **1.0000** | 0.010 |

**36-Window Walk-Forward Temporal Evaluation:**
- Mean F1 = **0.9999** (±0.0003) — Min: 0.9984 | Max: 1.0000
- Mean PR-AUC = **0.9975**

---

## 🗂️ Cấu trúc project

```
dsr/
├── config.R                   # Global config: auto-detect paths, GPU, packages
├── run_app.R                  # One-click app launcher
│
├── R/
│   ├── 1_download_data.R      # Tải dataset từ Kaggle
│   ├── 2_tien_xu_ly_du_lieu.R # Tiền xử lý: cleaning, graph features, scaling, split
│   ├── 3_analysis.R           # EDA & tổng hợp kết quả → JSON
│   ├── 4_visualization.R      # Sinh 10 biểu đồ phân tích (PNG)
│   ├── 5_train_model.R        # Huấn luyện LR / RF / XGBoost + 36-window eval
│   └── app/
│       └── app.R              # Shiny dashboard (5 tabs)
│
├── Dataset/                   # 8 files gốc từ Elliptic++
├── processed/                 # Features đã xử lý (wallet & tx)
├── outputs/                   # Metrics JSON, plots, trained models
└── report/
    └── aml_ieee_paper.tex     # Báo cáo IEEE conference (LaTeX)
```

---

## 🚀 Hướng dẫn chạy

### Yêu cầu
- **R** ≥ 4.1 ([tải tại đây](https://www.r-project.org/))
- **Dataset Elliptic++** (xem mục Dataset bên dưới)
- GPU NVIDIA (tùy chọn, cho XGBoost nhanh hơn)

### Bước 1 — Clone và chuẩn bị dataset

```bash
git clone https://github.com/<your-username>/AML-Project.git
cd AML-Project
```

Tải dataset Elliptic++ từ Kaggle và đặt vào thư mục `Dataset/`:

```
Dataset/
├── wallets_features_classes_combined.csv
├── wallets_classes.csv
├── wallets_features.csv
├── txs_features.csv
├── txs_classes.csv
├── txs_edgelist.csv
├── AddrTx_edgelist.csv
└── TxAddr_edgelist.csv
```

### Bước 2 — Chạy pipeline xử lý dữ liệu & huấn luyện mô hình

```bash
# Tiền xử lý (tạo features, split)
Rscript R/2_tien_xu_ly_du_lieu.R

# EDA và analysis
Rscript R/3_analysis.R

# Sinh biểu đồ
Rscript R/4_visualization.R

# Huấn luyện mô hình
Rscript R/5_train_model.R
```

### Bước 3 — Khởi động Dashboard

```bash
Rscript run_app.R
```

Các tùy chọn:
```bash
Rscript run_app.R --port 8080       # đổi port (mặc định: 7777)
Rscript run_app.R --no-browser      # không mở trình duyệt tự động
Rscript run_app.R --host 0.0.0.0    # cho phép truy cập từ mạng nội bộ
```

> **Lưu ý:** `config.R` tự động cài các package còn thiếu khi chạy lần đầu.

---

## 🖥️ Dashboard — 5 Tabs

| Tab | Nội dung |
|---|---|
| **Data Analysis** | Thống kê trước/sau xử lý, missing values, feature engineering |
| **Visualization** | 10 biểu đồ phân tích: phân bố nhãn, concept drift, Cohen's d, ... |
| **Model Comparison** | So sánh LR / RF / XGBoost: F1, PR-AUC, confusion matrix |
| **Temporal Robustness** | Kết quả 36-window walk-forward evaluation (F1 & PR-AUC) |
| **Wallet Inspector** | Nhập địa chỉ ví Bitcoin → tra dataset → predict → so sánh Ground Truth |

### Wallet Inspector
Nhập bất kỳ địa chỉ ví Bitcoin có trong Elliptic++:

```
Ví dụ: 1117wASFaYgJJP6MiY8cPD5DMdQda8gDZ
```

Dashboard sẽ hiển thị:
- **Xác suất illicit** (%) từ mô hình RF
- **Risk badge**: Licit / Illicit
- **Ground Truth** từ dataset + kết quả so khớp
- **Top feature contributions** (biểu đồ thanh) giải thích tại sao model đưa ra quyết định đó
- **Interpretation** tự động

---

## ⚙️ Chi tiết kỹ thuật

### Dataset
- **822,942** ví Bitcoin | 49 time steps (2019)
- **14,266** illicit (1.7%) | **251,088** licit (30.5%) | **557,588** unknown
- Imbalance ratio: **16.62:1**

### Feature Engineering
40 features = **30 behavioral** + **10 graph-derived**:

| Nhóm | Features |
|---|---|
| Behavioral | BTC amounts, fees, timing, wallet lifetime, tx counts |
| Graph (mới tạo) | `n_illicit_in_txs`, `pct_illicit_neighbors`, `total_illicit_txs`, ... |

`pct_illicit_neighbors` là feature **quan trọng nhất** (Cohen's d = 4.16).

### Xử lý mất cân bằng
- Random Forest: `classwt = c("0"=1, "1"=16.62)`
- XGBoost: `scale_pos_weight = 16.62`

### Temporal Split (no leakage)
```
Train (70%): t ≤ t₇₀  |  Val (10%): t₇₀ < t ≤ t₈₀  |  Test (20%): t > t₈₀
```

---

## 📦 Packages sử dụng

```r
shiny, data.table, plotly, jsonlite,
randomForest, xgboost, ggplot2, scales,
gridExtra, reshape2, DBI, RSQLite
```

Được tự động cài khi chạy `source("config.R")` hoặc `Rscript run_app.R`.

---

## 📄 Báo cáo

File LaTeX (IEEE Conference format): [`report/aml_ieee_paper.tex`](report/aml_ieee_paper.tex)

Compile:
```bash
pdflatex report/aml_ieee_paper.tex
```
Hoặc upload lên [Overleaf](https://www.overleaf.com/).

---

## 📚 References

- Elmougy et al. (2023). *Demystifying Fraudulent Transactions and Illicit Nodes in the Bitcoin Network*. KDD 2023.
- Weber et al. (2019). *Anti-Money Laundering in Bitcoin: Experimenting with Graph Convolutional Networks*. KDD Workshop.
- Chainalysis (2023). *Crypto Crime Report*.

---

## 👤 Tác giả

**Nguyễn Cao Trị** — Trí Tuệ Nhân Tạo

GitHub: [nguyencaotri2004/AML-Project](https://github.com/nguyencaotri2004/AML-Project)

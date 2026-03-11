# 🔍 AML Detective — Anti-Money Laundering Detection

Phát hiện giao dịch rửa tiền (AML) trên dataset **Elliptic++** sử dụng Machine Learning và Graph Analysis.

## 📊 Dataset

- **Elliptic++ Bitcoin Transaction Graph**
- 203,769 nodes (giao dịch) — 46,564 có nhãn, 157,205 unlabeled
- 234,355 edges (kết nối giữa các giao dịch)
- 191 features (local + aggregate + graph features)

## 🤖 Models

| Model | F1 | PR-AUC | ROC-AUC |
|-------|-----|--------|---------|
| XGBoost *(GPU)* | **0.334** | **0.337** | **0.651** |
| Random Forest | 0.266 | 0.240 | ~0.65 |
| Logistic Regression | 0.280 | 0.207 | 0.607 |

## 🗂️ Project Structure

```
dsr/
├── R/
│   ├── 1_download_data.R     # Download Elliptic++ dataset
│   ├── 2_preprocessing.R     # Data cleaning & feature engineering
│   ├── 3_analysis.R          # EDA & statistical analysis
│   ├── 4_visualization.R     # Generate charts
│   ├── 5_train_model.R       # Train LR + Random Forest
│   └── app/
│       └── app.R             # Shiny dashboard (4 tabs)
├── train_gpu.py              # GPU-accelerated XGBoost (RTX 4050)
├── config.R                  # Global GPU/CPU config
├── run_app.R                 # Launch dashboard
└── outputs/
    ├── metrics.json          # Model performance results
    ├── model_thresholds.json # Optimal thresholds per model
    └── charts/               # Visualization charts
```

## 🚀 Quick Start

### 1. Install R Dependencies
```r
install.packages(c("shiny", "bslib", "plotly", "DT", "xgboost",
                   "randomForest", "data.table", "jsonlite",
                   "shinyjs", "DBI", "RSQLite", "scales"))
```

### 2. Install Python Dependencies
```bash
pip install xgboost numpy pandas scikit-learn
```

### 3. Download & Preprocess Data
```r
Rscript R/1_download_data.R
Rscript R/2_preprocessing.R
Rscript R/3_analysis.R
Rscript R/4_visualization.R
```

### 4. Train Models
```bash
# XGBoost with GPU (RTX 4050 — 0.5s)
python train_gpu.py

# Logistic Regression + Random Forest (CPU)
Rscript R/5_train_model.R
```

### 5. Launch Dashboard
```r
Rscript run_app.R
# → http://127.0.0.1:7777
```

## 📱 Dashboard Features

| Tab | Nội dung |
|-----|---------|
| 📁 Data Analysis | Dataset overview, feature dictionary, preprocessing report |
| 📊 Visualization | 5 interactive Plotly charts với insight analysis |
| 🤖 Model Comparison | Metrics comparison, feature importance, temporal robustness |
| 🔍 Node Inspector | Real-time prediction + SHAP explainability cho từng transaction |

## ⚙️ GPU Training

XGBoost được train bằng **Python với CUDA** (XGBoost R không có CUDA binary cho Windows):

```python
# train_gpu.py
PRODUCTION_MODE = False  # True = train trên toàn bộ 46k labeled nodes
```

| Mode | Data | Mục đích |
|------|------|---------|
| `False` | 70/10/20 split | Đánh giá metric |
| `True` | 100% labeled | Production model |

## 📈 Key Findings

- **Temporal concept drift**: Tỉ lệ illicit thay đổi từ 1.7% → 24% qua các time step
- **Top features**: `gf_neighbor_illicit_ratio`, `gf_2hop_illicit_ratio`, `gf_pagerank`
- **Imbalanced challenge**: Illicit/Licit ratio = 1:9 → optimized threshold ≠ 0.5

## 🛠️ Tech Stack

- **R**: Shiny, bslib, plotly, DT, xgboost, randomForest, data.table
- **Python**: xgboost (GPU), pandas, scikit-learn
- **GPU**: NVIDIA RTX 4050 Laptop (CUDA 8.9)
- **Database**: SQLite (via DBI/RSQLite)

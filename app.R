# AML Detective Dashboard v2 — 4 Tabs
.libPaths(c("C:/Users/Trinc/R/win-library/4.5", .libPaths()))
library(shiny)
library(bslib)
library(jsonlite)
library(data.table)
library(DBI)
library(RSQLite)
library(xgboost)
library(plotly)
library(DT)   # load after plotly to avoid DT masking issues
library(shinyjs) # load last

ROOT_DIR   <- "d:/dsr"
PROC_DIR   <- file.path(ROOT_DIR, "processed")
OUT_DIR    <- file.path(ROOT_DIR, "outputs")
CHARTS_DIR <- file.path(ROOT_DIR, "outputs/charts")
DB_PATH    <- file.path(PROC_DIR, "elliptic_aml.db")

# Load data once
eda     <- fromJSON(file.path(OUT_DIR, "eda_summary.json"))
metrics <- tryCatch(fromJSON(file.path(OUT_DIR, "metrics.json")), error=function(e) NULL)
temporal_csv <- tryCatch(fread(file.path(OUT_DIR, "temporal_results.csv")), error=function(e) NULL)

con <- dbConnect(RSQLite::SQLite(), DB_PATH)
nodes_dt    <- fread(file.path(PROC_DIR, "nodes.csv"))
feats_dt    <- fread(file.path(PROC_DIR, "features_std.csv"))
edges_dt    <- fread(file.path(PROC_DIR, "edges.csv"))
time_series <- dbGetQuery(con, "SELECT * FROM v_illicit_by_time")
dbDisconnect(con)

xgb_model  <- tryCatch(xgb.load(file.path(OUT_DIR, "xgboost_model.bin")), error=function(e) NULL)
lr_model   <- tryCatch(xgb.load(file.path(OUT_DIR, "lr_model.bin")),       error=function(e) NULL)
rf_model   <- tryCatch(readRDS(file.path(OUT_DIR, "rf_model.rds")),         error=function(e) NULL)
thresholds <- tryCatch(fromJSON(file.path(OUT_DIR, "model_thresholds.json")), error=function(e)
  list(logistic_regression=list(threshold=0.5), random_forest=list(threshold=0.5), xgboost=list(threshold=0.45)))

# Pre-computed scores (instant lookup, avoids blocking predict in Shiny)
all_scores <- tryCatch(
  fread(file.path(OUT_DIR, "all_scores.csv")),
  error=function(e) NULL
)
if (!is.null(all_scores)) {
  setkey(all_scores, node_idx)
  cat("[App] all_scores.csv loaded — Node Inspector will use instant lookup\n")
} else {
  cat("[App] all_scores.csv not found — will compute on-the-fly (slower)\n")
}

feat_names <- names(feats_dt)
n_nodes  <- nrow(nodes_dt)
n_edges  <- nrow(edges_dt)
n_ill    <- sum(nodes_dt$label == 1)
n_lic    <- sum(nodes_dt$label == 0)
n_unk    <- sum(nodes_dt$label == -1)

# ── Theme ──────────────────────────────────────────────────────────────────
THEME <- bs_theme(
  version = 5,
  bg = "#0f1117", fg = "#e8eaf6",
  primary = "#5c6bc0", secondary = "#283593",
  success = "#2e7d32", danger = "#c62828",
  base_font = font_google("Inter"),
  heading_font = font_google("Inter")
)

# ── CSS ───────────────────────────────────────────────────────────────────
CSS <- "
body { background:#0f1117; color:#e8eaf6; }
.card { background:#1a1d2e !important; border:1px solid #2a2d3e !important; border-radius:12px !important; }
.metric-card { background:linear-gradient(135deg,#1a1d2e,#252840); border-left:4px solid #5c6bc0;
  padding:18px 20px; border-radius:10px; margin-bottom:14px; }
.metric-val { font-size:2rem; font-weight:700; color:#7986cb; }
.metric-lbl { font-size:.75rem; color:#9fa8da; text-transform:uppercase; letter-spacing:.08em; }
.metric-sub { font-size:.8rem; color:#7986cb; margin-top:2px; }
.nav-tabs .nav-link { color:#9fa8da !important; font-weight:600; border-radius:8px 8px 0 0; }
.nav-tabs .nav-link.active { background:#1a1d2e !important; color:#fff !important; border-color:#5c6bc0 #5c6bc0 #1a1d2e !important; }
.insight-box { background:#12162b; border-left:3px solid #5c6bc0; padding:12px 16px;
  border-radius:6px; margin:10px 0; font-size:.88rem; color:#c5cae9; line-height:1.6; }
.flag-illicit { background:linear-gradient(135deg,#b71c1c22,#c6282822); border:2px solid #c62828;
  border-radius:12px; padding:20px; text-align:center; }
.flag-licit { background:linear-gradient(135deg,#1b5e2022,#2e7d3222); border:2px solid #2e7d32;
  border-radius:12px; padding:20px; text-align:center; }
.flag-title { font-size:1.8rem; font-weight:800; }
.section-title { font-size:1.1rem; font-weight:700; color:#9fa8da;
  border-bottom:1px solid #2a2d3e; padding-bottom:8px; margin:16px 0 12px; }
.shap-label { font-size:.8rem; color:#9fa8da; }
table.dataTable { background:#1a1d2e !important; color:#e8eaf6 !important; }
table.dataTable thead th { background:#252840 !important; color:#9fa8da !important; }
table.dataTable tbody tr:hover { background:#252840 !important; }
.btn-primary { background:#5c6bc0; border-color:#5c6bc0; }
.form-control, .form-select { background:#252840; border-color:#3949ab; color:#e8eaf6; }
.form-control:focus { background:#252840; border-color:#5c6bc0; color:#fff; box-shadow:0 0 0 .2rem rgba(92,107,192,.25); }
"

# ── Feature Dictionary ─────────────────────────────────────────────────────
feat_dict <- rbind(
  data.frame(Feature="Local_feature_1..93", Group="Local (Pre-scaled)", Count=93,
    Description="Các features local của transaction: lịch sử giao dịch trong 1 time step. Đã được chuẩn hóa bởi Elliptic++."),
  data.frame(Feature="Aggregate_feature_1..72", Group="Aggregate (Pre-scaled)", Count=72,
    Description="Thống kê tổng hợp của neighbor transactions (mean, std, min, max). Biểu diễn context mạng lưới giao dịch."),
  data.frame(Feature="in_txs_degree / out_txs_degree", Group="Augmented BTC", Count=2,
    Description="Số lượng giao dịch đầu vào/đầu ra. [0,1] sau log1p+minmax."),
  data.frame(Feature="total_BTC, fees, size", Group="Augmented BTC", Count=3,
    Description="Tổng BTC giao dịch, phí giao dịch, kích thước byte. Indicator của quy mô giao dịch."),
  data.frame(Feature="num_input/output_addresses", Group="Augmented BTC", Count=2,
    Description="Số địa chỉ ví đầu vào/đầu ra. Nhiều địa chỉ = có thể là mixing service."),
  data.frame(Feature="in/out_BTC_min/max/mean/median/total", Group="Augmented BTC", Count=10,
    Description="Thống kê phân phối BTC đầu vào/đầu ra. Heavy-tail → log1p scaled."),
  data.frame(Feature="gf_in_degree / gf_out_degree / gf_total_degree", Group="Graph Feature", Count=3,
    Description="Bậc kết nối trong transaction graph. High degree = hub node (exchange, mixer)."),
  data.frame(Feature="gf_pagerank", Group="Graph Feature", Count=1,
    Description="PageRank score: tầm quan trọng của node trong mạng. Node illicit thường có PageRank cao do kết nối nhiều."),
  data.frame(Feature="gf_neighbor_illicit_ratio", Group="Graph Feature ⭐", Count=1,
    Description="Tỷ lệ % neighbors là illicit. ⭐ Feature quan trọng nhất! Illicit: 21.5% vs Licit: 3.1%"),
  data.frame(Feature="gf_neighbor_licit_ratio", Group="Graph Feature", Count=1,
    Description="Tỷ lệ % neighbors là licit. Bổ sung cho neighbor_illicit_ratio."),
  data.frame(Feature="gf_2hop_illicit_ratio", Group="Graph Feature", Count=1,
    Description="Tỷ lệ illicit ở 2-hop radius. Illicit thường nằm trong cluster nhiều tầng (mixing attacks)."),
  data.frame(Feature="gf_clustering_coef", Group="Graph Feature", Count=1,
    Description="Clustering coefficient: xác suất 2 neighbors kết nối với nhau. Illicit hay hoạt động trong cluster kín."),
  data.frame(Feature="gf_triangle_count", Group="Graph Feature", Count=1,
    Description="Số tam giác trong graph chứa node. Cao = node trung tâm của cluster giao dịch tuần hoàn.")
)

# ── UI ─────────────────────────────────────────────────────────────────────
ui <- page_navbar(
  title = span(
    tags$img(src="https://img.icons8.com/color/32/fraud-detection.png", height="28px",
             style="margin-right:8px;vertical-align:middle;"),
    "AML Detective"
  ),
  theme = THEME,
  useShinyjs(),
  tags$head(tags$style(HTML(CSS))),
  fillable = FALSE,
  header = NULL,

  # ── TAB 1: DATA ANALYSIS ──────────────────────────────────────────────
  nav_panel("📁 Data Analysis",
    div(style="padding:20px;",
      # Header
      div(class="card", style="padding:24px;margin-bottom:20px;",
        h2(style="color:#7986cb;margin:0;", "Elliptic++ Dataset Overview"),
        p(style="color:#9fa8da;margin:4px 0 0;",
          "Anti-Money Laundering detection on Bitcoin transaction graph")
      ),
      # Metric cards row
      fluidRow(
        column(2, div(class="metric-card",
          div(class="metric-lbl","Total Transactions"),
          div(class="metric-val", prettyNum(n_nodes, big.mark=",")),
          div(class="metric-sub","nodes in graph"))),
        column(2, div(class="metric-card",
          div(class="metric-lbl","Total Edges"),
          div(class="metric-val", prettyNum(n_edges, big.mark=",")),
          div(class="metric-sub","transaction links"))),
        column(2, div(class="metric-card", style="border-left-color:#c62828;",
          div(class="metric-lbl","Illicit Txs"),
          div(class="metric-val", style="color:#ef5350;", prettyNum(n_ill, big.mark=",")),
          div(class="metric-sub", paste0(round(n_ill/(n_ill+n_lic)*100,1),"% of labeled")))),
        column(2, div(class="metric-card", style="border-left-color:#2e7d32;",
          div(class="metric-lbl","Licit Txs"),
          div(class="metric-val", style="color:#66bb6a;", prettyNum(n_lic, big.mark=",")),
          div(class="metric-sub", paste0(round(n_lic/(n_ill+n_lic)*100,1),"% of labeled")))),
        column(2, div(class="metric-card", style="border-left-color:#f57c00;",
          div(class="metric-lbl","Unknown Txs"),
          div(class="metric-val", style="color:#ffa726;", prettyNum(n_unk, big.mark=",")),
          div(class="metric-sub","unlabeled nodes"))),
        column(2, div(class="metric-card", style="border-left-color:#ab47bc;",
          div(class="metric-lbl","Total Features"),
          div(class="metric-val", style="color:#ba68c8;", length(feat_names)),
          div(class="metric-sub","after engineering")))
      ),
      fluidRow(
        # Dataset Files
        column(5,
          div(class="card", style="padding:18px;",
            div(class="section-title", "📂 Dataset Files"),
            DT::dataTableOutput("tbl_files")
          )
        ),
        # Feature groups
        column(7,
          div(class="card", style="padding:18px;",
            div(class="section-title", "🔍 Feature Dictionary"),
            textInput("feat_search","Search features:", placeholder="e.g. pagerank, BTC, neighbor..."),
            DT::dataTableOutput("tbl_features")
          )
        )
      ),
      # Data quality
      div(class="card", style="padding:18px;margin-top:16px;",
        div(class="section-title","📊 Data Quality & Preprocessing Report"),
        fluidRow(
          column(4,
            div(class="insight-box",
              strong("🔧 Preprocessing Pipeline (v2)"), br(),
              "1. NaN fill → column median (975 cells in 17 augmented cols)", br(),
              "2. Winsorize BTC features (1%–99%)", br(),
              "3. Local + Aggregate: KEPT AS-IS (pre-scaled by Elliptic++)", br(),
              "4. Augmented BTC: Log1p → Min-Max [0,1]", br(),
              "5. Graph features: Robust scale (IQR>0) or Std fallback (IQR=0)"
            )
          ),
          column(4,
            div(class="insight-box",
              strong("✅ Validation Results"), br(),
              "NaN cells after preprocessing: 0", br(),
              "Zero-variance columns: 0  (was 58!)", br(),
              "Non-double columns: 0", br(),
              "Total features: 191", br(),
              "Total labeled: 46,564 (22.9% of all)"
            )
          ),
          column(4,
            div(class="insight-box",
              strong("⚖️ Class Imbalance"), br(),
              paste0("Illicit : Licit ratio = 1 : ", round(n_lic/n_ill,1)), br(),
              "Strategy: scale_pos_weight in XGBoost", br(),
              "Recommended metric: PR-AUC", br(),
              "(ROC-AUC overly optimistic on imbalanced data)", br(),
              paste0("scale_pos_weight = ", round(n_lic/n_ill))
            )
          )
        )
      )
    )
  ),

  # ── TAB 2: VISUALIZATION ─────────────────────────────────────────────────
  nav_panel("📊 Visualization",
    div(style="padding:20px;",
      fluidRow(
        column(6,
          div(class="card", style="padding:16px;margin-bottom:16px;",
            plotlyOutput("viz_labels", height="320px"),
            div(class="insight-box","⚡ 77.1% giao dịch không có nhãn (unknown). Mô hình chỉ train trên 22.9% còn lại. Đây là challenge lớn của semi-supervised AML detection.")
          )
        ),
        column(6,
          div(class="card", style="padding:16px;margin-bottom:16px;",
            plotlyOutput("viz_time", height="320px"),
            div(class="insight-box","📈 Illicit ratio dao động mạnh từ 1.7% đến 24% theo thời gian — concept drift thật sự của Bitcoin AML. Mô hình cần robust với temporal shift.")
          )
        )
      ),
      fluidRow(
        column(12,
          div(class="card", style="padding:16px;margin-bottom:16px;",
            plotlyOutput("viz_degree", height="320px"),
            div(class="insight-box","🔗 Degree distribution: Illicit nodes có xu hướng tập trung ở mid-range degree (3–10), trong khi licit nodes chủ yếu low-degree (1–2). High-degree nodes thường là exchange hubs.")
          )
        )
      ),
      fluidRow(
        column(6,
          div(class="card", style="padding:16px;margin-bottom:16px;",
            plotlyOutput("viz_pca", height="380px"),
            div(class="insight-box","🔵 PCA chỉ giải thích 11%+7.4% = 18.5% variance. Illicit và licit overlap nặng trong không gian linear → cần tree-based model (XGBoost) thay vì linear classifier.")
          )
        ),
        column(6,
          div(class="card", style="padding:16px;margin-bottom:16px;",
            plotlyOutput("viz_neighbor", height="380px"),
            div(class="insight-box","⭐ Feature quan trọng nhất! Illicit nodes có trung bình 21.5% neighbors là illicit, so với chỉ 3.1% với licit nodes. Đây là 7x gap — AML signal mạnh nhất từ graph structure.")
          )
        )
      ),
      fluidRow(
        column(12,
          div(class="card", style="padding:16px;",
            div(class="section-title","📷 Pre-generated Static Charts"),
            fluidRow(lapply(list.files(CHARTS_DIR, "\\.png$"), function(f) {
              column(4, div(style="margin-bottom:12px;",
                tags$img(src=file.path("charts", f), style="width:100%;border-radius:8px;"),
                p(style="font-size:.8rem;color:#9fa8da;text-align:center;", f)
              ))
            }))
          )
        )
      )
    )
  ),

  # ── TAB 3: MODEL COMPARISON ───────────────────────────────────────────────
  nav_panel("🤖 Model Comparison",
    div(style="padding:20px;",
      # Metric explanations
      div(class="card", style="padding:18px;margin-bottom:16px;",
        div(class="section-title","📖 Giải thích các chỉ số đánh giá"),
        fluidRow(
          column(4, div(class="insight-box",
            strong("Precision (Độ chính xác)"), br(),
            "Trong số các transaction bị model FLAG là illicit, bao nhiêu % thật sự là illicit?",br(),br(),
            em("Precision cao → ít false alarm → quan trọng khi cost của việc nhầm lẫn cao"))),
          column(4, div(class="insight-box",
            strong("Recall (Độ nhạy / Sensitivity)"),br(),
            "Trong số tất cả giao dịch illicit thật sự, model phát hiện được bao nhiêu %?",br(),br(),
            em("Recall cao → ít miss illicit → quan trọng trong AML (bỏ sót 1 rửa tiền = nguy hiểm)"))),
          column(4, div(class="insight-box",
            strong("F1 Score"),br(),
            "Trung bình điều hòa của Precision và Recall. Cân bằng 2 chỉ số trên.",br(),br(),
            em("F1 = 2 × (P×R)/(P+R). Metric quan trọng nhất với imbalanced data")))
        ),
        fluidRow(
          column(4, div(class="insight-box",
            strong("PR-AUC (Precision-Recall Area Under Curve)"),br(),
            "Diện tích dưới đường cong Precision-Recall. Không bị ảnh hưởng bởi class imbalance.",br(),br(),
            em("⭐ Metric được khuyến nghị cho bài toán AML. Cao hơn random baseline (2.2%)"))),
          column(4, div(class="insight-box",
            strong("ROC-AUC"),br(),
            "Xác suất model xếp hạng đúng illicit cao hơn licit. Nhưng bị inflate với imbalanced data.",br(),br(),
            em("ROC-AUC > 0.5 = tốt hơn random. Nhưng PR-AUC tin cậy hơn cho AML"))),
          column(4, div(class="insight-box",
            strong("Threshold"),br(),
            "Ngưỡng phân loại: nếu score ≥ threshold → ILLICIT. Được tối ưu trên validation set.",br(),br(),
            em("Threshold thấp = recall cao nhưng precision thấp. Cần cân bằng theo use case")))
        )
      ),
      # Model comparison table
      fluidRow(
        column(7,
          div(class="card", style="padding:18px;",
            div(class="section-title","📊 Kết quả so sánh Models"),
            uiOutput("model_table"),
            br(),
            div(class="insight-box",
              strong("Phương pháp:"), br(),
              "• Logistic Regression: gblinear XGBoost proxy — baseline linear model", br(),
              "• Random Forest: 500 trees, classwt imbalance, isotonic calibration", br(),
              "• XGBoost: Gradient Boosted Trees, eta=0.03, max_depth=5, scale_pos_weight, early stopping"
            )
          )
        ),
        column(5,
          div(class="card", style="padding:18px;",
            div(class="section-title","📈 PR-AUC Comparison"),
            plotlyOutput("viz_model_compare", height="260px"),
            br(),
            div(class="insight-box",
              strong("Tại sao XGBoost tốt nhất?"), br(),
              "1. Xử lý non-linear boundaries tốt", br(),
              "2. scale_pos_weight tự động handle imbalance", br(),
              "3. Feature interaction qua tree splits", br(),
              "4. Early stopping: tránh overfit trên test"
            )
          )
        )
      ),
      # Feature importance
      fluidRow(
        column(6,
          div(class="card", style="padding:18px;margin-top:16px;",
            div(class="section-title","⭐ XGBoost Feature Importance (Top 20)"),
            plotlyOutput("viz_feat_imp", height="400px")
          )
        ),
        column(6,
          div(class="card", style="padding:18px;margin-top:16px;",
            div(class="section-title","📅 Temporal Robustness (36 Rolling Windows)"),
            plotlyOutput("viz_temporal", height="400px"),
            div(class="insight-box","Mỗi window = train trên t<T2, test trên T+1,T+2. Variance cao = concept drift. XGBoost với isotonic calibration ổn định hơn qua các time steps.")
          )
        )
      )
    )
  ),

  # ── TAB 4: NODE INSPECTOR ─────────────────────────────────────────────────
  nav_panel("🔍 Node Inspector",
    div(style="padding:20px;",
      fluidRow(
        # Input panel
        column(3,
          div(class="card", style="padding:18px;",
            div(class="section-title","🎯 Inspect Transaction"),
            selectInput("inspector_model","Model:",
              choices=c("XGBoost"="xgb","Logistic Regression"="lr","Random Forest"="rf"),
              selected="xgb"),
            hr(style="border-color:#2a2d3e;"),
            p(style="color:#9fa8da;font-size:.85rem;","Nhập Transaction ID (txId) hoặc chọn từ danh sách:"),
            textInput("txid_input","Transaction ID:", placeholder="e.g. 230425980"),
            selectInput("txid_preset","Hoặc chọn ví dụ:",
              choices=c("-- Chọn --"="",
                        "Illicit example"="illicit",
                        "Licit example"="licit",
                        "Random"="random")),
            actionButton("btn_inspect","🔍 Inspect", class="btn-primary w-100"),
            br(),br(),
            div(class="insight-box", style="font-size:.8rem;",
              "💡 SHAP values được tính real-time từ XGBoost contrib predictions.", br(),
              "Xanh lá = feature kéo về licit", br(),
              "Đỏ = feature đẩy về illicit")
          )
        ),
        # Results panel
        column(9,
          uiOutput("inspector_verdict"),
          br(),
          fluidRow(
            column(6, div(class="card", style="padding:18px;",
              div(class="section-title","🧮 Multi-Model Comparison"),
              uiOutput("multi_model_scores")
            )),
            column(6, div(class="card", style="padding:18px;",
              div(class="section-title","📋 Node Context"),
              uiOutput("node_context")
            ))
          ),
          br(),
          div(class="card", style="padding:18px;",
            div(class="section-title","🔬 SHAP Feature Explanation (XGBoost)"),
            p(style="color:#9fa8da;font-size:.85rem;",
              "Waterfall chart: mỗi bar cho thấy ảnh hưởng của feature đến quyết định. Đỏ = tăng nguy cơ illicit, Xanh = giảm nguy cơ."),
            plotlyOutput("viz_shap", height="420px")
          )
        )
      )
    )
  )
)

# ── SERVER ────────────────────────────────────────────────────────────────
server <- function(input, output, session) {

  # Serve static charts
  addResourcePath("charts", CHARTS_DIR)

  # ── TAB 1 ────────────────────────────────────────────────────────────────
  files_df <- data.frame(
    File=c("txs_features.csv","txs_classes.csv","txs_edgelist.csv",
           "wallets_features.csv","AddrTx_edgelist.csv","TxAddr_edgelist.csv"),
    `Size (MB)`=c(663,2.2,4.3,578,20.2,35.0),
    Rows=c("203,769","203,769","234,355","~2.6M","~570K","~570K"),
    Description=c(
      "Feature matrix chính: 182 features cho mỗi transaction Bitcoin",
      "Ground truth labels: illicit (1), licit (2), unknown (3)",
      "Cạnh graph: txId1 → txId2 (giao dịch gửi tiền cho giao dịch)",
      "Features của ví Bitcoin (wallet-level, optional)",
      "Địa chỉ → Transaction links (bipartite graph)",
      "Transaction → Địa chỉ links"
    ),
    Used=c("✅ Chính","✅ Chính","✅ Chính","⏸️ Optional","⏸️ Optional","⏸️ Optional"),
    check.names=FALSE
  )
  output$tbl_files <- DT::renderDataTable(
    files_df, rownames=FALSE, options=list(pageLength=6, dom="t"),
    style="bootstrap5",
    class="table-dark table-sm"
  )

  feat_df_filtered <- reactive({
    s <- trimws(tolower(input$feat_search))
    if (nchar(s) == 0) return(feat_dict)
    feat_dict[grepl(s, tolower(feat_dict$Feature)) |
              grepl(s, tolower(feat_dict$Description)) |
              grepl(s, tolower(feat_dict$Group)), ]
  })
  output$tbl_features <- DT::renderDataTable(
    feat_df_filtered(), rownames=FALSE,
    options=list(pageLength=8, dom="t", scrollX=TRUE),
    style="bootstrap5", class="table-dark table-sm",
    escape=FALSE
  )

  # ── TAB 2 ────────────────────────────────────────────────────────────────
  dark_layout <- function(p, ...) {
    layout(p, ...,
      paper_bgcolor="#1a1d2e", plot_bgcolor="#1a1d2e",
      font=list(color="#e8eaf6"),
      xaxis=list(gridcolor="#2a2d3e", zerolinecolor="#3a3d5e"),
      yaxis=list(gridcolor="#2a2d3e", zerolinecolor="#3a3d5e")
    )
  }

  output$viz_labels <- renderPlotly({
    df <- data.frame(
      Label=c("Unknown (-1)","Licit (0)","Illicit (1)"),
      Count=c(n_unk, n_lic, n_ill),
      Color=c("#9e9e9e","#2e7d32","#c62828")
    )
    df$Pct <- round(df$Count/sum(df$Count)*100,1)
    p <- plot_ly(df, x=~Label, y=~Count, type="bar",
      marker=list(color=df$Color, line=list(color="#0f1117",width=1)),
      text=~paste0(prettyNum(Count,big.mark=","),"<br>(",Pct,"%)"),
      textposition="outside", hovertemplate="%{x}: %{y:,}<extra></extra>") |>
      layout(title=list(text="Transaction Label Distribution",font=list(size=14)),
             paper_bgcolor="#1a1d2e", plot_bgcolor="#1a1d2e",
             font=list(color="#e8eaf6"), showlegend=FALSE,
             yaxis=list(gridcolor="#2a2d3e",title="Count"),
             xaxis=list(title=""))
    p
  })

  output$viz_time <- renderPlotly({
    plot_ly(time_series, x=~time, y=~round(illicit_ratio*100,1),
      type="scatter", mode="lines+markers",
      line=list(color="#ef5350",width=2),
      marker=list(color="#ef5350",size=5),
      fill="tozeroy", fillcolor="rgba(198,40,40,0.15)",
      hovertemplate="Step %{x}: %{y:.1f}%<extra></extra>") |>
      layout(title=list(text="Illicit Ratio Over Time",font=list(size=14)),
             paper_bgcolor="#1a1d2e", plot_bgcolor="#1a1d2e",
             font=list(color="#e8eaf6"), showlegend=FALSE,
             xaxis=list(gridcolor="#2a2d3e",title="Time Step"),
             yaxis=list(gridcolor="#2a2d3e",title="Illicit Rate (%)"))
  })

  output$viz_degree <- renderPlotly({
    out_d <- edges_dt[, .N, by=src_idx][, .(node_idx=src_idx, od=N)]
    in_d  <- edges_dt[, .N, by=dst_idx][, .(node_idx=dst_idx, id=N)]
    deg   <- merge(nodes_dt[label>=0], out_d, by="node_idx", all.x=TRUE)
    deg   <- merge(deg, in_d, by="node_idx", all.x=TRUE)
    deg[is.na(od), od:=0L]; deg[is.na(id), id:=0L]
    deg[, tot:=od+id+1L]
    ill_d <- log10(deg$tot[deg$label==1])
    lic_d <- log10(deg$tot[deg$label==0])
    plot_ly() |>
      add_trace(x=ill_d, type="histogram", name="Illicit",
        marker=list(color="#c62828",opacity=0.75), nbinsx=40) |>
      add_trace(x=lic_d, type="histogram", name="Licit",
        marker=list(color="#2e7d32",opacity=0.75), nbinsx=40) |>
      layout(barmode="overlay",
             title=list(text="Degree Distribution (log10)",font=list(size=14)),
             paper_bgcolor="#1a1d2e", plot_bgcolor="#1a1d2e",
             font=list(color="#e8eaf6"),
             xaxis=list(gridcolor="#2a2d3e",title="log10(Total Degree + 1)"),
             yaxis=list(gridcolor="#2a2d3e",title="Count"))
  })

  output$viz_pca <- renderPlotly({
    set.seed(42)
    idx <- which(nodes_dt$label >= 0)
    if(length(idx)>5000) idx <- sample(idx, 5000)
    pc <- prcomp(as.matrix(feats_dt[idx,]), center=TRUE, scale.=FALSE)
    pca_df <- data.frame(PC1=pc$x[,1], PC2=pc$x[,2], lbl=nodes_dt$label[idx])
    q1 <- quantile(pca_df$PC1,c(.01,.99)); q2 <- quantile(pca_df$PC2,c(.01,.99))
    pca_df <- pca_df[pca_df$PC1>q1[1]&pca_df$PC1<q1[2]&pca_df$PC2>q2[1]&pca_df$PC2<q2[2],]
    ve <- round(summary(pc)$importance[2,1:2]*100,1)
    plot_ly() |>
      add_trace(data=pca_df[pca_df$lbl==0,], x=~PC1, y=~PC2, type="scatter",
        mode="markers", name="Licit",
        marker=list(color="#2e7d32",size=4,opacity=0.3)) |>
      add_trace(data=pca_df[pca_df$lbl==1,], x=~PC1, y=~PC2, type="scatter",
        mode="markers", name="Illicit",
        marker=list(color="#ef5350",size=5,opacity=0.8)) |>
      layout(title=list(text=paste0("PCA 2D — PC1:",ve[1],"% / PC2:",ve[2],"%"),font=list(size=14)),
             paper_bgcolor="#1a1d2e", plot_bgcolor="#1a1d2e",
             font=list(color="#e8eaf6"),
             xaxis=list(gridcolor="#2a2d3e",title=paste0("PC1 (",ve[1],"%)")),
             yaxis=list(gridcolor="#2a2d3e",title=paste0("PC2 (",ve[2],"%)")))
  })

  output$viz_neighbor <- renderPlotly({
    lbl_lkp <- nodes_dt$label; names(lbl_lkp) <- nodes_dt$node_idx
    lab_nd  <- nodes_dt[nodes_dt$label>=0,]
    esrc <- data.frame(node_idx=as.integer(edges_dt$src_idx), nbr=as.integer(edges_dt$dst_idx))
    edst <- data.frame(node_idx=as.integer(edges_dt$dst_idx), nbr=as.integer(edges_dt$src_idx))
    ae   <- rbind(esrc, edst)
    ae$nl <- lbl_lkp[as.character(ae$nbr)]
    ae   <- ae[!is.na(ae$nl) & ae$nl>=0,]
    ae$is_ill <- as.integer(ae$nl==1)
    nr   <- aggregate(is_ill~node_idx, ae, mean)
    names(nr)[2] <- "ratio"
    mg   <- merge(lab_nd[,c("node_idx","label")], nr, by="node_idx", all.x=TRUE)
    mg$ratio[is.na(mg$ratio)] <- 0
    ill_r <- mg$ratio[mg$label==1]
    lic_r <- mg$ratio[mg$label==0]
    plot_ly() |>
      add_trace(x=~ill_r, type="box", name="Illicit",
        marker=list(color="#ef5350"), fillcolor="rgba(198,40,40,0.4)",
        line=list(color="#ef5350")) |>
      add_trace(x=~lic_r, type="box", name="Licit",
        marker=list(color="#66bb6a"), fillcolor="rgba(46,125,50,0.4)",
        line=list(color="#2e7d32")) |>
      layout(title=list(text="Neighbor Illicit Ratio Distribution",font=list(size=14)),
             paper_bgcolor="#1a1d2e", plot_bgcolor="#1a1d2e",
             font=list(color="#e8eaf6"), boxmode="group",
             xaxis=list(gridcolor="#2a2d3e",title="% Illicit Neighbors",
                        tickformat=".0%"),
             yaxis=list(gridcolor="#2a2d3e"))
  })

  # ── TAB 3 ────────────────────────────────────────────────────────────────
  output$model_table <- renderUI({
    if(is.null(metrics)) return(div("metrics.json not found"))
    rows <- lapply(names(metrics$models), function(nm) {
      m <- metrics$models[[nm]]
      badge_col <- if(nm=="xgboost") "#5c6bc0" else "#37474f"
      tags$tr(
        tags$td(span(style=paste0("background:",badge_col,";padding:2px 8px;border-radius:4px;font-size:.8rem;"), nm)),
        tags$td(style="text-align:center;", round(m$f1,3)),
        tags$td(style="text-align:center;", round(m$pr_auc,4)),
        tags$td(style="text-align:center;", round(m$roc_auc,4)),
        tags$td(style="text-align:center;", round(m$precision,3)),
        tags$td(style="text-align:center;", round(m$recall,3)),
        tags$td(style="text-align:center;", round(m$threshold,3))
      )
    })
    tags$table(class="table table-dark table-hover table-sm",
      tags$thead(tags$tr(
        lapply(c("Model","F1","PR-AUC","ROC-AUC","Precision","Recall","Threshold"),
               function(h) tags$th(style="color:#9fa8da;", h))
      )),
      tags$tbody(rows)
    )
  })

  output$viz_model_compare <- renderPlotly({
    if(is.null(metrics)) return(plotly_empty())
    nms <- names(metrics$models)
    prauc_vals <- sapply(nms, function(n) metrics$models[[n]]$pr_auc)
    f1_vals    <- sapply(nms, function(n) metrics$models[[n]]$f1)
    plot_ly() |>
      add_trace(x=nms, y=prauc_vals, type="bar", name="PR-AUC",
        marker=list(color="#5c6bc0")) |>
      add_trace(x=nms, y=f1_vals, type="bar", name="F1",
        marker=list(color="#ef5350")) |>
      layout(barmode="group",
             paper_bgcolor="#1a1d2e", plot_bgcolor="#1a1d2e",
             font=list(color="#e8eaf6"),
             xaxis=list(gridcolor="#2a2d3e"),
             yaxis=list(gridcolor="#2a2d3e",title="Score"),
             legend=list(orientation="h",y=1.1))
  })

  output$viz_feat_imp <- renderPlotly({
    if(is.null(metrics)) return(plotly_empty())
    top_feats <- unlist(metrics$top_features_xgb)
    if(length(top_feats)==0) return(plotly_empty())
    n <- length(top_feats)
    colors <- ifelse(grepl("^gf_", top_feats), "#ef5350",
               ifelse(grepl("^Local|^Aggregate", top_feats), "#5c6bc0", "#ffa726"))
    plot_ly(x=seq(n,1,-1), y=rev(top_feats), type="bar", orientation="h",
      marker=list(color=rev(colors)),
      hovertemplate="%{y}<extra></extra>") |>
      layout(paper_bgcolor="#1a1d2e", plot_bgcolor="#1a1d2e",
             font=list(color="#e8eaf6",size=10),
             xaxis=list(gridcolor="#2a2d3e",title="Importance Rank"),
             yaxis=list(gridcolor="#2a2d3e",title=""),
             showlegend=FALSE)
  })

  output$viz_temporal <- renderPlotly({
    if(is.null(temporal_csv)) return(plotly_empty())
    tc <- temporal_csv
    plot_ly(tc, x=~window_id) |>
      add_trace(y=~pr_auc, type="scatter", mode="lines+markers",
        name="PR-AUC", line=list(color="#5c6bc0",width=2),
        marker=list(color="#5c6bc0",size=5)) |>
      add_trace(y=~f1, type="scatter", mode="lines+markers",
        name="F1", line=list(color="#ef5350",width=2),
        marker=list(color="#ef5350",size=5)) |>
      layout(paper_bgcolor="#1a1d2e", plot_bgcolor="#1a1d2e",
             font=list(color="#e8eaf6"),
             xaxis=list(gridcolor="#2a2d3e",title="Window"),
             yaxis=list(gridcolor="#2a2d3e",title="Score"),
             legend=list(orientation="h",y=1.1))
  })

  # ── TAB 4: NODE INSPECTOR ────────────────────────────────────────────────
  insp_result <- reactiveVal(NULL)

  # Preset txId
  observeEvent(input$txid_preset, {
    req(input$txid_preset != "")
    sel <- input$txid_preset
    if(sel == "illicit") {
      ill_ids <- nodes_dt$txId[nodes_dt$label==1]
      updateTextInput(session, "txid_input", value=as.character(ill_ids[1]))
    } else if(sel == "licit") {
      lic_ids <- nodes_dt$txId[nodes_dt$label==0]
      updateTextInput(session, "txid_input", value=as.character(lic_ids[1]))
    } else if(sel == "random") {
      lbl_ids <- nodes_dt$txId[nodes_dt$label>=0]
      updateTextInput(session, "txid_input", value=as.character(sample(lbl_ids,1)))
    }
  })

  observeEvent(input$btn_inspect, {
    req(input$txid_input)
    txid_str <- trimws(input$txid_input)

    # Find node
    node_row <- nodes_dt[as.character(nodes_dt$txId) == txid_str,]
    if(nrow(node_row) == 0) {
      showNotification(paste("txId", txid_str, "không tìm thấy!"), type="error")
      return()
    }

    node_idx_1based <- node_row$node_idx + 1L
    feat_vec <- as.matrix(feats_dt[node_idx_1based, ])

    result <- list(
      txId      = txid_str,
      node_idx  = node_row$node_idx,
      true_label= node_row$label,
      time      = node_row$time,
      xgb_score = NA_real_,
      lr_score  = NA_real_,
      rf_score  = NA_real_,
      shap_vals = NULL
    )

    dm <- xgb.DMatrix(feat_vec)

    # ── Scores: use pre-computed CSV if available (instant), else compute ──
    if (!is.null(all_scores) && node_row$node_idx %in% all_scores$node_idx) {
      sc <- all_scores[.(node_row$node_idx)]
      result$xgb_score <- sc$xgb_score
      result$lr_score  <- sc$lr_score
      result$rf_score  <- sc$rf_score
    } else {
      # Fallback: compute on the fly (only for nodes not in precomputed table)
      if (!is.null(xgb_model)) result$xgb_score <- predict(xgb_model, dm)
      if (!is.null(lr_model))  result$lr_score  <- predict(lr_model,  dm)
      # RF is slow — skip if not in cache to avoid UI freeze
    }

    # ── SHAP: always computed real-time from XGBoost (fast) ──────────────
    if (!is.null(xgb_model) && !is.null(xgb_model)) {
      contrib <- predict(xgb_model, dm, predcontrib=TRUE)
      shap_df <- data.frame(
        feature=c(feat_names, "BIAS"),
        shap=as.numeric(contrib[1,])
      )
      shap_df <- shap_df[order(abs(shap_df$shap), decreasing=TRUE),]
      shap_df <- shap_df[shap_df$feature != "BIAS",]
      result$shap_vals <- head(shap_df, 20)
      top_feats <- result$shap_vals$feature
      result$shap_vals$feat_val <- as.numeric(feat_vec[1, match(top_feats, feat_names)])
    }

    insp_result(result)
  })

  output$inspector_verdict <- renderUI({
    r <- insp_result()
    if(is.null(r)) return(div(class="card",style="padding:40px;text-align:center;color:#9fa8da;",
      h4("👈 Nhập Transaction ID và nhấn Inspect")))

    score <- if(!is.na(r$xgb_score)) r$xgb_score else 0.5
    # Use a simple threshold of 0.45
    is_flagged <- score >= 0.45
    true_lbl_txt <- switch(as.character(r$true_label),
      "1"="✅ Đúng (True Positive)", "0"="✅ Correct (True Negative)",
      "-1"="❓ Unknown (no ground truth)")

    fluidRow(
      column(5,
        div(class=if(is_flagged)"flag-illicit" else "flag-licit",
          div(class="flag-title", style=paste0("color:",if(is_flagged)"#ef5350" else "#66bb6a",";"),
            if(is_flagged) "⚠️ FLAGGED: ILLICIT" else "✅ CLEAR: LICIT"),
          br(),
          h4(style="color:#fff;margin:0;", paste0("XGBoost Score: ", round(score*100,1), "%")),
          p(style="color:#9fa8da;margin:4px 0;", paste0("Threshold: 45% | txId: ", r$txId)),
          p(style="color:#ffd54f;", paste0("Ground Truth: ", true_lbl_txt))
        )
      ),
      column(7,
        div(class="card", style="padding:14px;",
          plotlyOutput("viz_score_gauge", height="160px")
        )
      )
    )
  })

  output$viz_score_gauge <- renderPlotly({
    r <- insp_result()
    if(is.null(r) || is.na(r$xgb_score)) return(plotly_empty())
    score_pct <- round(r$xgb_score * 100, 1)
    plot_ly(type="indicator", mode="gauge+number+delta",
      value=score_pct,
      number=list(suffix="%", font=list(size=28, color="#fff")),
      gauge=list(
        axis=list(range=list(0,100), tickfont=list(color="#9fa8da")),
        bar=list(color=if(score_pct>=45)"#ef5350" else "#66bb6a"),
        bgcolor="#252840",
        steps=list(
          list(range=c(0,45), color="#1b5e20"),
          list(range=c(45,100), color="#b71c1c")
        ),
        threshold=list(line=list(color="#ffd54f",width=3), thickness=0.8, value=45)
      ),
      domain=list(row=0,column=0)) |>
      layout(paper_bgcolor="#1a1d2e", font=list(color="#e8eaf6"),
             margin=list(t=20,b=10,l=30,r=30))
  })

  output$multi_model_scores <- renderUI({
    r <- insp_result()
    if(is.null(r)) return(p("No data", style="color:#9fa8da;"))
    xgb_th <- thresholds$xgboost$threshold
    lr_th  <- thresholds$logistic_regression$threshold
    rf_th  <- thresholds$random_forest$threshold
    rows <- list(
      list(model="XGBoost",       score=r$xgb_score, threshold=xgb_th, color="#5c6bc0",
           pr_auc=thresholds$xgboost$pr_auc),
      list(model="Logistic Reg.", score=r$lr_score,  threshold=lr_th,  color="#f57c00",
           pr_auc=thresholds$logistic_regression$pr_auc),
      list(model="Random Forest", score=r$rf_score,  threshold=rf_th,  color="#26a69a",
           pr_auc=thresholds$random_forest$pr_auc)
    )
    lapply(rows, function(row) {
      s  <- row$score
      th <- row$threshold
      verdict <- if(is.na(s)) span("Model not loaded", style="color:#9fa8da;font-size:.8rem;")
                 else if(s >= th) span("⚠️ ILLICIT", style="color:#ef5350;font-weight:700;")
                 else             span("✅ LICIT",   style="color:#66bb6a;font-weight:700;")
      score_pct <- if(is.na(s)) "N/A" else paste0(round(s*100,1), "%")
      bar_w <- if(is.na(s)) 0 else round(s*100)
      bar_col <- if(is.na(s)) "#37474f" else if(s >= th) "#ef5350" else "#2e7d32"
      div(style=paste0("background:#252840;border-left:3px solid ",row$color,
        ";padding:10px 14px;border-radius:6px;margin-bottom:8px;"),
        fluidRow(
          column(4, div(
            strong(row$model, style="color:#e8eaf6;"), br(),
            span(paste0("PR-AUC: ", round(row$pr_auc,3), " | Thresh: ", round(th*100,0), "%"),
                 style="color:#9fa8da;font-size:.75rem;")
          )),
          column(4, div(style="padding-top:6px;",
            if(!is.na(s)) div(style="background:#1a1d2e;border-radius:4px;padding:2px;",
              div(style=paste0("width:",bar_w,"%;background:",bar_col,
                ";height:10px;border-radius:4px;transition:width .5s;"))),
            div(style="color:#e8eaf6;font-size:.85rem;margin-top:2px;", score_pct)
          )),
          column(4, div(style="padding-top:8px;", verdict))
        )
      )
    })
  })

  output$node_context <- renderUI({
    r <- insp_result()
    if(is.null(r)) return(p("No data", style="color:#9fa8da;"))
    nd <- nodes_dt[nodes_dt$node_idx == r$node_idx,]
    if(nrow(nd)==0) return(p("Node not found"))

    feat_row <- feats_dt[r$node_idx+1,]

    ctx_rows <- list(
      c("Time Step", as.character(nd$time)),
      c("Graph In-Degree", round(feat_row$gf_in_degree, 4)),
      c("Graph Out-Degree", round(feat_row$gf_out_degree, 4)),
      c("Neighbor Illicit %", paste0(round(feat_row$gf_neighbor_illicit_ratio*100, 1),"%")),
      c("2-hop Illicit %", paste0(round(feat_row$gf_2hop_illicit_ratio*100, 1),"%")),
      c("PageRank", formatC(feat_row$gf_pagerank, format="e", digits=2)),
      c("Clustering Coef.", round(feat_row$gf_clustering_coef, 4))
    )
    tagList(lapply(ctx_rows, function(row) {
      div(style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #2a2d3e;",
        span(style="color:#9fa8da;font-size:.85rem;", row[1]),
        span(style="color:#e8eaf6;font-weight:600;", row[2])
      )
    }))
  })

  output$viz_shap <- renderPlotly({
    r <- insp_result()
    if(is.null(r) || is.null(r$shap_vals)) {
      return(plot_ly() |> layout(paper_bgcolor="#1a1d2e",
        annotations=list(list(text="Run Inspect to see SHAP values",
          x=0.5,y=0.5,xref="paper",yref="paper",showarrow=FALSE,
          font=list(color="#9fa8da",size=16)))))
    }
    sv  <- r$shap_vals
    sv  <- head(sv, 15)
    sv  <- sv[order(sv$shap),]
    cols <- ifelse(sv$shap > 0, "#ef5350", "#66bb6a")
    hover_txt <- paste0(sv$feature,"<br>SHAP: ",round(sv$shap,4),
                        "<br>Value: ",round(sv$feat_val,4))
    plot_ly(sv, x=~shap, y=~factor(feature, levels=feature),
      type="bar", orientation="h",
      marker=list(color=cols, line=list(color="#0f1117",width=0.5)),
      text=hover_txt, hoverinfo="text") |>
      layout(paper_bgcolor="#1a1d2e", plot_bgcolor="#1a1d2e",
             font=list(color="#e8eaf6",size=11),
             xaxis=list(gridcolor="#2a2d3e",title="SHAP Value (impact on log-odds)",
                        zeroline=TRUE, zerolinecolor="#9fa8da", zerolinewidth=2),
             yaxis=list(gridcolor="#2a2d3e",title="",automargin=TRUE),
             showlegend=FALSE,
             margin=list(l=200))
  })

}

shinyApp(ui, server, options=list(port=7777, host="127.0.0.1", launch.browser=FALSE))

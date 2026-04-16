"""
dashboard.py — Afro Automation: Cost Analysis Dashboard
=========================================================
Run with:
    streamlit run dashboard.py

Pipeline shown to the user:
    [1] Credentials  →  [2] Scrape / Load CSVs  →  [3] Clean  →  [4] Analyse
"""

import sys
from pathlib import Path
from datetime import date

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))
from cleaner import clean_cost_data

# Scraper requires Selenium + Firefox — not available on Streamlit Cloud.
# Dashboard still works in CSV-upload mode without it.
try:
    from scraper import scrape_cost_data, _load_csvs_to_dataframe
    SCRAPER_AVAILABLE = True
except ImportError:
    SCRAPER_AVAILABLE = False

    def _load_csvs_to_dataframe(folder: Path) -> pd.DataFrame:
        """Fallback CSV loader (no selenium dependency)."""
        frames = []
        for csv_file in sorted(folder.glob("*.csv")):
            try:
                df = pd.read_csv(csv_file, encoding="utf-8-sig")
                df["Source.Name"] = csv_file.name
                frames.append(df)
            except Exception:
                pass
        if not frames:
            return pd.DataFrame()
        combined = pd.concat(frames, ignore_index=True)
        dedup_keys = ["JE No.", "Project", "JE Date", "Debit", "Credit"]
        existing = [c for c in dedup_keys if c in combined.columns]
        if existing:
            combined = combined.drop_duplicates(subset=existing, keep="first")
        return combined

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Afro Group — Cost Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Brand colours ─────────────────────────────────────────────────────────────
ACCOUNT_COLORS = {
    "WE":                  "#0070C0",
    "Vodafone":            "#E60000",
    "Orange":              "#FF6600",
    "Raya":                "#7030A0",
    "ASIS":                "#00B050",
    "Nokia":               "#124191",
    "New Account & Sales": "#00B0C1",
    "Etisalat":            "#00A99D",
    "NEC":                 "#C00000",
    "Enterprise":          "#595959",
    "Other":               "#A6A6A6",
}
CATEGORY_COLORS = px.colors.qualitative.Safe
MONTH_ORDER = [
    "January","February","March","April","May","June",
    "July","August","September","October","November","December",
]

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .section-title {
        font-size: 13px; font-weight: 700; color: #999;
        text-transform: uppercase; letter-spacing: 1.2px;
        border-bottom: 1px solid #e0e0e0; padding-bottom: 6px;
        margin: 20px 0 10px 0;
    }
    .step-badge {
        display: inline-block; background: #0070C0; color: white;
        border-radius: 50%; width: 22px; height: 22px; line-height: 22px;
        text-align: center; font-size: 12px; font-weight: 700;
        margin-right: 6px;
    }
    .step-label {
        font-weight: 600; font-size: 14px;
    }
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────
def fmt_egp(v: float) -> str:
    if abs(v) >= 1_000_000:
        return f"EGP {v/1_000_000:.2f}M"
    if abs(v) >= 1_000:
        return f"EGP {v/1_000:.1f}K"
    return f"EGP {v:,.0f}"


def _do_clean(raw: pd.DataFrame, status_placeholder, raw_before_dedup: int = None) -> pd.DataFrame:
    """Run the cleaner with live status updates shown in the sidebar."""
    status_placeholder.info("🧹 Step 3 — Cleaning & mapping data …")
    cleaned = clean_cost_data(raw)
    unknown_cats = (cleaned["Category"] == "Unknown").sum()
    null_accs    = cleaned["Account"].isna().sum()
    msg = f"✅ Cleaned — **{len(cleaned):,} rows**, {len(cleaned.columns)} columns"
    if raw_before_dedup and raw_before_dedup > len(raw):
        dupes = raw_before_dedup - len(raw)
        msg += f"  \n🔁 {dupes:,} duplicate rows removed"
    if unknown_cats:
        msg += f"  \n⚠️ {unknown_cats} unmapped categories"
    if null_accs:
        msg += f"  \n⚠️ {null_accs} unmapped accounts"
    status_placeholder.success(msg)
    return cleaned


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/combo-chart.png", width=56)
    st.title("Afro Cost Dashboard")
    st.divider()

    # ── STEP 1 : Credentials ──────────────────────────────────────────────────
    st.markdown(
        '<p class="step-label"><span class="step-badge">1</span>Credentials</p>',
        unsafe_allow_html=True,
    )
    username    = st.text_input("Username",    value="motaha",        key="cred_user")
    password    = st.text_input("Password",    value="",              key="cred_pass",
                                type="password", placeholder="Enter ERP password")
    access_code = st.text_input("Access Code", value="",              key="cred_access",
                                type="password", placeholder="6-digit access code")

    st.divider()

    # ── STEP 2 : Data Source ──────────────────────────────────────────────────
    st.markdown(
        '<p class="step-label"><span class="step-badge">2</span>Data Source</p>',
        unsafe_allow_html=True,
    )
    _source_options = ["Upload CSV files"]
    if SCRAPER_AVAILABLE:
        _source_options.append("CSV folder path (local)")
        _source_options.append("Run scraper (live ERP)")
    data_source = st.radio(
        "Load data from",
        _source_options,
        horizontal=False,
    )
    if not SCRAPER_AVAILABLE:
        st.caption("ℹ️ Live scraping unavailable on this deployment — upload your CSV files.")

    status_box = st.empty()   # placeholder for step-3 cleaning status

    # ── 2a  Upload CSV files (works on cloud & local) ─────────────────────────
    if data_source == "Upload CSV files":
        uploaded_files = st.file_uploader(
            "Upload ERP CSV files",
            type="csv",
            accept_multiple_files=True,
            help="Upload the CSV files exported from the ERP scraper.",
        )

        if st.button("📂 Load & Clean", type="primary", use_container_width=True,
                     disabled=not uploaded_files):
            if not uploaded_files:
                st.error("Please upload at least one CSV file.")
            else:
                with st.spinner("Step 2 — Loading CSVs …"):
                    import io
                    frames = []
                    for f in uploaded_files:
                        try:
                            df_f = pd.read_csv(f, encoding="utf-8-sig")
                            df_f["Source.Name"] = f.name
                            frames.append(df_f)
                        except Exception:
                            pass
                    if not frames:
                        raw = pd.DataFrame()
                    else:
                        raw = pd.concat(frames, ignore_index=True)
                        dedup_keys = ["JE No.", "Project", "JE Date", "Debit", "Credit"]
                        existing = [c for c in dedup_keys if c in raw.columns]
                        if existing:
                            raw = raw.drop_duplicates(subset=existing, keep="first")

                if raw.empty:
                    st.error("No valid CSV files found in the uploaded files.")
                else:
                    st.success(f"📄 Loaded **{len(raw):,} unique rows** from {len(uploaded_files)} files")
                    cleaned = _do_clean(raw, status_box)
                    st.session_state["df"] = cleaned

    # ── 2b  Existing folder (local only) ──────────────────────────────────────
    elif data_source == "CSV folder path (local)":
        default_folder = str(Path.home() / "Downloads" / "Cost_01-2026")
        folder_path = st.text_input("CSV folder path", value=default_folder)

        if st.button("📂 Load & Clean", type="primary", use_container_width=True):
            if not Path(folder_path).exists():
                st.error(f"Folder not found:\n{folder_path}")
            else:
                with st.spinner("Step 2 — Loading CSVs …"):
                    raw = _load_csvs_to_dataframe(Path(folder_path))

                if raw.empty:
                    st.error("No valid CSV files found in that folder.")
                else:
                    n_files = len(list(Path(folder_path).glob("*.csv")))
                    st.success(f"📄 Loaded **{len(raw):,} unique rows** from {n_files} files")
                    cleaned = _do_clean(raw, status_box)
                    st.session_state["df"] = cleaned

    # ── 2b  Run scraper ───────────────────────────────────────────────────────
    else:
        if not password:
            st.warning("Enter your password in Step 1 before scraping.")

        c1, c2 = st.columns(2)
        with c1:
            d_from = st.date_input("From", value=date(2026, 1, 1))
        with c2:
            d_to   = st.date_input("To",   value=date(2026, 2, 28))

        out_dir = st.text_input(
            "Save CSVs to",
            value=str(Path.home() / "Downloads" / "Cost_scraped"),
        )
        clear_first = st.checkbox(
            "🗑️ Clear output folder before scraping",
            value=True,
            help="Recommended — prevents duplicate rows from previous scrape runs.",
        )

        if st.button("🚀 Scrape & Clean", type="primary",
                     use_container_width=True, disabled=not password):
            if not username or not password or not access_code:
                st.error("Fill in all three credential fields first.")
            else:
                # Optionally wipe the output folder to avoid accumulation
                if clear_first and Path(out_dir).exists():
                    import shutil as _shutil
                    _shutil.rmtree(out_dir)
                    Path(out_dir).mkdir(parents=True, exist_ok=True)
                    st.info(f"🗑️ Cleared existing files in {out_dir}")

                with st.spinner("Step 2 — Scraping ERP (~10 min) …"):
                    try:
                        raw = scrape_cost_data(
                            date_from   = d_from.strftime("%m/%d/%Y"),
                            date_to     = d_to.strftime("%m/%d/%Y"),
                            output_dir  = out_dir,
                            username    = username,
                            password    = password,
                            access_code = access_code,
                        )
                    except Exception as e:
                        st.error(f"Scraper error: {e}")
                        raw = pd.DataFrame()

                if raw.empty:
                    st.error("Scraper returned no data — check credentials and ERP availability.")
                else:
                    st.success(f"📄 Scraped **{len(raw):,} unique rows**")
                    cleaned = _do_clean(raw, status_box)
                    st.session_state["df"] = cleaned

    # ── Filters (shown only when data is loaded) ──────────────────────────────
    df_full: pd.DataFrame = st.session_state.get("df", pd.DataFrame())

    if not df_full.empty:
        st.divider()
        st.markdown(
            '<p class="step-label"><span class="step-badge">4</span>Filters</p>',
            unsafe_allow_html=True,
        )
        accounts   = sorted(df_full["Account"].dropna().unique())
        categories = sorted(df_full["Category"].dropna().unique())
        months     = [m for m in MONTH_ORDER if m in df_full["month"].unique()]
        items      = sorted(df_full["item"].dropna().unique())
        projects   = sorted(df_full["Project"].dropna().unique())

        sel_acc  = st.multiselect("Account",  accounts,   default=accounts)
        sel_cat  = st.multiselect("Category", categories, default=categories)
        sel_mon  = st.multiselect("Month",    months,     default=months)
        sel_item = st.multiselect("Item",     items,      default=items,
                                  placeholder="All items…")
        sel_proj = st.multiselect("Project",  projects,   default=projects,
                                  placeholder="All projects…")

        mask = (
            df_full["Account"].isin(sel_acc) &
            df_full["Category"].isin(sel_cat) &
            df_full["month"].isin(sel_mon) &
            df_full["item"].isin(sel_item) &
            df_full["Project"].isin(sel_proj)
        )
        df = df_full[mask].copy()
        st.caption(f"Showing **{len(df):,}** / **{len(df_full):,}** rows")
    else:
        df = pd.DataFrame()


# ── Main content ──────────────────────────────────────────────────────────────
if df_full.empty if "df_full" in dir() else True:
    st.markdown("## 👈 Complete Steps 1 & 2 in the sidebar to load data")
    c1, c2, c3 = st.columns(3)
    c1.info("**Step 1** — Enter your ERP username, password and access code")
    c2.info("**Step 2** — Choose an existing CSV folder or scrape live data")
    c3.info("**Step 3** — Cleaner runs automatically after loading")
    st.stop()

if "df" not in st.session_state or st.session_state["df"].empty:
    st.markdown("## 👈 Complete Steps 1 & 2 in the sidebar to load data")
    st.stop()

if df.empty:
    st.warning("No data matches the current filters — try widening your selection.")
    st.stop()


# ── KPI Row ───────────────────────────────────────────────────────────────────
total_cost      = df["Cost amount"].sum()
total_txn       = len(df)
top_account     = df.groupby("Account")["Cost amount"].sum().idxmax()
top_account_val = df.groupby("Account")["Cost amount"].sum().max()
top_category    = df.groupby("Category")["Cost amount"].sum().idxmax()
top_cat_val     = df.groupby("Category")["Cost amount"].sum().max()

k1, k2, k3, k4 = st.columns(4)
k1.metric("💰 Total Cost",    fmt_egp(total_cost))
k2.metric("📄 Transactions",  f"{total_txn:,}")
k3.metric("🏆 Top Account",   top_account,  delta=fmt_egp(top_account_val))
k4.metric("📦 Top Category",  top_category, delta=fmt_egp(top_cat_val))

st.divider()

# ── Cost Breakdown ────────────────────────────────────────────────────────────
st.markdown('<p class="section-title">Cost Breakdown</p>', unsafe_allow_html=True)
col_a, col_b = st.columns(2)

with col_a:
    acc_df = (
        df.groupby("Account")["Cost amount"].sum()
        .reset_index().sort_values("Cost amount", ascending=True)
    )
    acc_df["color"] = acc_df["Account"].map(ACCOUNT_COLORS).fillna("#888")
    fig = go.Figure(go.Bar(
        x=acc_df["Cost amount"], y=acc_df["Account"], orientation="h",
        marker_color=acc_df["color"],
        text=acc_df["Cost amount"].apply(fmt_egp), textposition="outside",
        hovertemplate="<b>%{y}</b><br>EGP %{x:,.0f}<extra></extra>",
    ))
    fig.update_layout(
        title="Cost by Account", xaxis_title="EGP",
        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="#ccc",
        height=370, margin=dict(l=10, r=90, t=40, b=10),
        xaxis=dict(gridcolor="#333"),
    )
    st.plotly_chart(fig, use_container_width=True)

with col_b:
    cat_df = (
        df.groupby("Category")["Cost amount"].sum()
        .reset_index().sort_values("Cost amount", ascending=False)
    )
    fig = px.pie(
        cat_df, names="Category", values="Cost amount",
        color_discrete_sequence=CATEGORY_COLORS, hole=0.45,
        title="Cost by Category",
    )
    fig.update_traces(
        textposition="inside", textinfo="percent+label",
        hovertemplate="<b>%{label}</b><br>EGP %{value:,.0f} (%{percent})<extra></extra>",
    )
    fig.update_layout(
        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="#ccc",
        height=370, margin=dict(l=10, r=10, t=40, b=10), showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)


# ── Trends & Distribution ─────────────────────────────────────────────────────
st.markdown('<p class="section-title">Trends & Distribution</p>', unsafe_allow_html=True)
col_c, col_d = st.columns(2)

with col_c:
    months_present = [m for m in MONTH_ORDER if m in df["month"].unique()]
    trend_df = (
        df.groupby(["month", "Account"])["Cost amount"].sum().reset_index()
    )
    trend_df["month"] = pd.Categorical(trend_df["month"], categories=months_present, ordered=True)
    trend_df = trend_df.sort_values("month")
    fig = px.bar(
        trend_df, x="month", y="Cost amount", color="Account",
        color_discrete_map=ACCOUNT_COLORS, barmode="stack",
        title="Monthly Cost by Account", labels={"Cost amount": "EGP", "month": ""},
    )
    fig.update_traces(hovertemplate="<b>%{x} — %{fullData.name}</b><br>EGP %{y:,.0f}<extra></extra>")
    fig.update_layout(
        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="#ccc",
        height=370, margin=dict(l=10, r=10, t=40, b=10),
        yaxis=dict(gridcolor="#333"),
        legend=dict(orientation="h", yanchor="bottom", y=-0.4, font_size=11),
    )
    st.plotly_chart(fig, use_container_width=True)

with col_d:
    heat_df = (
        df.groupby(["Account", "Category"])["Cost amount"]
        .sum().unstack(fill_value=0)
    )
    heat_df = heat_df.loc[:, heat_df.sum() > 0]
    heat_df = heat_df.loc[heat_df.sum(axis=1).sort_values(ascending=False).index]
    fig = px.imshow(
        heat_df, color_continuous_scale="Blues", aspect="auto",
        title="Account × Category Heatmap", labels=dict(color="EGP"),
    )
    fig.update_traces(hovertemplate="<b>%{y} | %{x}</b><br>EGP %{z:,.0f}<extra></extra>")
    fig.update_layout(
        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="#ccc",
        height=370, margin=dict(l=10, r=10, t=40, b=10),
        coloraxis_showscale=False,
        xaxis=dict(tickangle=-35, tickfont_size=10),
    )
    st.plotly_chart(fig, use_container_width=True)


# ── Category & Item Detail ────────────────────────────────────────────────────
st.markdown('<p class="section-title">Category & Item Detail</p>', unsafe_allow_html=True)
col_e, col_f = st.columns([3, 2])

with col_e:
    cat_bar = (
        df.groupby(["Category", "Account"])["Cost amount"]
        .sum().reset_index().sort_values("Cost amount", ascending=False)
    )
    fig = px.bar(
        cat_bar, x="Category", y="Cost amount", color="Account",
        color_discrete_map=ACCOUNT_COLORS, barmode="stack",
        title="Category Cost by Account",
        labels={"Cost amount": "EGP", "Category": ""},
    )
    fig.update_layout(
        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="#ccc",
        height=370, margin=dict(l=10, r=10, t=40, b=10),
        xaxis=dict(tickangle=-35), yaxis=dict(gridcolor="#333"),
        legend=dict(orientation="h", yanchor="bottom", y=-0.5, font_size=10),
    )
    st.plotly_chart(fig, use_container_width=True)

with col_f:
    st.markdown("**Top 15 Expense Items**")
    top_items = (
        df.groupby(["item", "Category"])["Cost amount"]
        .sum().reset_index()
        .sort_values("Cost amount", ascending=False).head(15)
    )
    top_items["Cost amount"] = top_items["Cost amount"].apply(lambda x: f"EGP {x:,.0f}")
    top_items.columns = ["Item", "Category", "Cost"]
    st.dataframe(top_items, use_container_width=True, hide_index=True, height=350)


# ── Account Share ─────────────────────────────────────────────────────────────
st.markdown('<p class="section-title">Account Share Analysis</p>', unsafe_allow_html=True)

acc_share = (
    df.groupby("Account")["Cost amount"].sum()
    .reset_index().sort_values("Cost amount", ascending=False)
)
acc_share["pct"] = acc_share["Cost amount"] / acc_share["Cost amount"].sum() * 100

fig = go.Figure()
for _, row in acc_share.iterrows():
    fig.add_trace(go.Bar(
        name=row["Account"], x=[row["Account"]], y=[row["Cost amount"]],
        marker_color=ACCOUNT_COLORS.get(row["Account"], "#888"),
        text=f"{row['pct']:.1f}%", textposition="outside",
        hovertemplate=f"<b>{row['Account']}</b><br>EGP {row['Cost amount']:,.0f} | {row['pct']:.1f}%<extra></extra>",
    ))
fig.update_layout(
    title="Total Cost & % Share by Account",
    plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="#ccc",
    height=340, margin=dict(l=10, r=10, t=40, b=10), showlegend=False,
    yaxis=dict(title="EGP", gridcolor="#333"), barmode="group",
)
st.plotly_chart(fig, use_container_width=True)


# ── Project Drilldown ─────────────────────────────────────────────────────────
st.markdown('<p class="section-title">Project Drilldown</p>', unsafe_allow_html=True)

proj_df = (
    df.groupby(["Project", "Account", "Category"])["Cost amount"]
    .sum().reset_index().sort_values("Cost amount", ascending=False)
)
selected_account = st.selectbox(
    "Filter by Account", ["All"] + sorted(df["Account"].dropna().unique()),
)
if selected_account != "All":
    proj_df = proj_df[proj_df["Account"] == selected_account]

fig = px.bar(
    proj_df.head(30), x="Cost amount", y="Project", color="Category",
    orientation="h", color_discrete_sequence=CATEGORY_COLORS,
    title=f"Top Projects — {selected_account if selected_account != 'All' else 'All Accounts'}",
    labels={"Cost amount": "EGP", "Project": ""},
)
fig.update_layout(
    plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="#ccc",
    height=500, margin=dict(l=10, r=10, t=40, b=10),
    xaxis=dict(gridcolor="#333"),
    legend=dict(orientation="h", yanchor="bottom", y=-0.3, font_size=10),
)
st.plotly_chart(fig, use_container_width=True)


# ── Raw Data ──────────────────────────────────────────────────────────────────
st.markdown('<p class="section-title">Raw Data</p>', unsafe_allow_html=True)

with st.expander("📋 View & Export Cleaned Data", expanded=False):
    disp_cols = [
        "JE Date", "JE No.", "Project", "Account",
        "item", "Category", "Debit", "Credit", "Cost amount",
        "month", "Quarter", "%",
    ]
    disp_df = df[disp_cols].copy()
    disp_df["JE Date"] = disp_df["JE Date"].dt.strftime("%Y-%m-%d")
    st.dataframe(disp_df, use_container_width=True, height=400, hide_index=True)
    st.download_button(
        "⬇️ Download CSV",
        data=disp_df.to_csv(index=False).encode("utf-8-sig"),
        file_name="afro_cost_cleaned.csv",
        mime="text/csv",
        use_container_width=True,
    )


# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption("Afro Group — Cost Analysis Dashboard  |  Afro ERP")

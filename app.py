import streamlit as st
import pandas as pd
import numpy as np
import io
import time
from datetime import datetime

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="Order Processor - Final",
    page_icon="📦",
    layout="wide"
)

st.title("📦 Order Processor - Final Version")
st.markdown("Upload file Shopee & Tokopedia, langsung proses & download!")

# =========================================================
# SESSION STATE
# =========================================================
if 'processed' not in st.session_state:
    st.session_state.processed = False
if 'results' not in st.session_state:
    st.session_state.results = {}

# =========================================================
# SKU CLEANING LOGIC (DIPERTAHANKAN)
# =========================================================
def clean_sku_for_lookup(sku):
    """
    FG- & CS- => TIDAK dibersihkan
    SKU lain  => ambil bagian setelah hyphen pertama (untuk varian)
    """
    if pd.isna(sku):
        return ""

    sku = str(sku).strip().upper()

    if sku.startswith(("FG-", "CS-")):
        return sku

    if "-" in sku:
        return sku.split("-", 1)[1].split("-")[0].strip()

    return sku


def get_original_sku_for_display(sku):
    return "" if pd.isna(sku) else str(sku).strip()


# =========================================================
# SKU MASTER LOOKUP
# =========================================================
def create_sku_mapping(df_sku):
    """
    Sheet: SKU Master
    Kolom: Product_Sku | Product_Name
    """
    mapping = {}

    required = {"Product_Sku", "Product_Name"}
    if not required.issubset(df_sku.columns):
        raise ValueError("Sheet SKU Master wajib punya kolom Product_Sku & Product_Name")

    for _, row in df_sku.iterrows():
        sku = clean_sku_for_lookup(row["Product_Sku"])
        name = str(row["Product_Name"]).strip()

        if sku and name and sku not in mapping:
            mapping[sku] = name

    return mapping


# =========================================================
# BUNDLE MASTER LOOKUP (FINAL)
# =========================================================
def create_bundle_mapping(df_bundle):
    """
    Sheet: Bundle Master
    Kolom:
    - Kit_Sku
    - Component_Sku
    - Component_Qty
    """
    bundle_mapping = {}

    required_cols = ["Kit_Sku", "Component_Sku", "Component_Qty"]
    for col in required_cols:
        if col not in df_bundle.columns:
            raise ValueError(f"Kolom '{col}' wajib ada di Bundle Master")

    for _, row in df_bundle.iterrows():
        kit_sku = clean_sku_for_lookup(row["Kit_Sku"])
        component_sku = str(row["Component_Sku"]).strip()
        qty = row["Component_Qty"] if pd.notna(row["Component_Qty"]) else 1

        if not kit_sku or not component_sku:
            continue

        bundle_mapping.setdefault(kit_sku, []).append(
            (component_sku, float(qty))
        )

    return bundle_mapping


# =========================================================
# READ KAMUS FILE
# =========================================================
def read_kamus_file(kamus_file):
    try:
        excel = pd.ExcelFile(kamus_file, engine="openpyxl")
        sheets = excel.sheet_names

        if len(sheets) < 3:
            raise ValueError("File kamus wajib punya minimal 3 sheet")

        return {
            "kurir": pd.read_excel(excel, sheet_name=0),
            "bundle": pd.read_excel(excel, sheet_name=1),
            "sku": pd.read_excel(excel, sheet_name=2)
        }
    except Exception as e:
        st.error(f"Error baca file kamus: {e}")
        return None


# =========================================================
# PROCESS SHOPEE
# =========================================================
def process_shopee(df, bundle_mapping, sku_mapping):
    expanded = []

    df.columns = [str(c).strip() for c in df.columns]

    col_order = next((c for c in df.columns if "pesanan" in c.lower()), df.columns[0])
    col_status = next((c for c in df.columns if "status" in c.lower()), None)
    col_managed = next((c for c in df.columns if "kelola" in c.lower()), None)
    col_sku = next((c for c in df.columns if "sku" in c.lower()), df.columns[1])
    col_qty = next((c for c in df.columns if "jumlah" in c.lower() or "qty" in c.lower()), df.columns[2])
    col_resi = next((c for c in df.columns if "resi" in c.lower()), None)
    col_ship = next((c for c in df.columns if "pengiriman" in c.lower()), None)

    # FILTERS
    if col_status:
        df = df[df[col_status].astype(str).str.contains("PERLU", case=False)]
    if col_managed:
        df = df[df[col_managed].astype(str).str.contains("NO", case=False)]
    if col_resi:
        df = df[df[col_resi].isna() | (df[col_resi].astype(str).str.strip() == "")]

    for _, r in df.iterrows():
        sku_original = get_original_sku_for_display(r[col_sku])
        sku_clean = clean_sku_for_lookup(sku_original)
        qty = float(r[col_qty]) if pd.notna(r[col_qty]) else 1
        order_id = str(r[col_order])

        if sku_clean in bundle_mapping:
            for comp_sku, comp_qty in bundle_mapping[sku_clean]:
                expanded.append({
                    "Marketplace": "Shopee",
                    "Order ID": order_id,
                    "Original SKU": sku_original,
                    "Cleaned SKU": sku_clean,
                    "Product Name": sku_mapping.get(sku_clean, ""),
                    "Quantity": qty,
                    "Bundle Y/N": "Y",
                    "Component SKU": comp_sku,
                    "Quantity Final": qty * comp_qty
                })
        else:
            expanded.append({
                "Marketplace": "Shopee",
                "Order ID": order_id,
                "Original SKU": sku_original,
                "Cleaned SKU": sku_clean,
                "Product Name": sku_mapping.get(sku_clean, ""),
                "Quantity": qty,
                "Bundle Y/N": "N",
                "Component SKU": sku_clean,
                "Quantity Final": qty
            })

    return expanded


# =========================================================
# PROCESS TOKOPEDIA / TIKTOK (NO FILTER)
# =========================================================
def process_tokped(df, bundle_mapping, sku_mapping):
    expanded = []

    df.columns = [str(c).strip() for c in df.columns]

    col_order = df.columns[0]
    col_sku = next((c for c in df.columns if "sku" in c.lower()), df.columns[1])
    col_qty = next((c for c in df.columns if "qty" in c.lower()), df.columns[2])

    for i, r in df.iterrows():
        sku_original = get_original_sku_for_display(r[col_sku])
        sku_clean = clean_sku_for_lookup(sku_original)
        qty = float(r[col_qty]) if pd.notna(r[col_qty]) else 1
        order_id = str(r[col_order]) if pd.notna(r[col_order]) else f"TOKPED_{i+1}"

        if sku_clean in bundle_mapping:
            for comp_sku, comp_qty in bundle_mapping[sku_clean]:
                expanded.append({
                    "Marketplace": "Tokopedia/TikTok",
                    "Order ID": order_id,
                    "Original SKU": sku_original,
                    "Cleaned SKU": sku_clean,
                    "Product Name": sku_mapping.get(sku_clean, ""),
                    "Quantity": qty,
                    "Bundle Y/N": "Y",
                    "Component SKU": comp_sku,
                    "Quantity Final": qty * comp_qty
                })
        else:
            expanded.append({
                "Marketplace": "Tokopedia/TikTok",
                "Order ID": order_id,
                "Original SKU": sku_original,
                "Cleaned SKU": sku_clean,
                "Product Name": sku_mapping.get(sku_clean, ""),
                "Quantity": qty,
                "Bundle Y/N": "N",
                "Component SKU": sku_clean,
                "Quantity Final": qty
            })

    return expanded


# =========================================================
# SIDEBAR UPLOAD
# =========================================================
with st.sidebar:
    st.header("📁 Upload Files")

    shopee_file = st.file_uploader("File Shopee", type=["csv", "xlsx", "xls"])
    tokped_file = st.file_uploader("File Tokopedia / TikTok", type=["csv", "xlsx", "xls"])
    kamus_file = st.file_uploader("File Kamus (Excel)", type=["xlsx", "xls"])

    if kamus_file and (shopee_file or tokped_file):
        if st.button("🚀 PROCESS ALL DATA", type="primary", use_container_width=True):
            st.session_state.files = (shopee_file, tokped_file, kamus_file)
            st.rerun()


# =========================================================
# MAIN PROCESS
# =========================================================
if "files" in st.session_state:
    shopee_file, tokped_file, kamus_file = st.session_state.files

    with st.spinner("Processing data..."):
        kamus = read_kamus_file(kamus_file)
        bundle_mapping = create_bundle_mapping(kamus["bundle"])
        sku_mapping = create_sku_mapping(kamus["sku"])

        rows = []

        if shopee_file:
            df = pd.read_csv(shopee_file) if shopee_file.name.endswith(".csv") else pd.read_excel(shopee_file)
            rows += process_shopee(df, bundle_mapping, sku_mapping)

        if tokped_file:
            df = pd.read_csv(tokped_file) if tokped_file.name.endswith(".csv") else pd.read_excel(tokped_file)
            rows += process_tokped(df, bundle_mapping, sku_mapping)

        df_detail = pd.DataFrame(rows)
        st.session_state.results = df_detail
        st.session_state.processed = True
        del st.session_state.files
        st.rerun()


# =========================================================
# DISPLAY
# =========================================================
if st.session_state.processed:
    df = st.session_state.results

    st.subheader("📋 Summary Detail Order")
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.subheader("💾 Download")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv = df.to_csv(index=False, encoding="utf-8-sig")

    st.download_button(
        "📥 Download CSV",
        csv,
        f"order_detail_{ts}.csv",
        "text/csv",
        use_container_width=True
    )

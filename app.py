import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(page_title="Instant & Sameday Kurir", layout="wide")

# ======================================================
# HELPER FUNCTIONS
# ======================================================
def normalize_columns(df):
    df.columns = df.columns.str.strip()
    return df

def clean_sku_for_lookup(sku):
    if pd.isna(sku):
        return sku
    sku = str(sku).strip()
    if sku.startswith(("FG-", "CS-")):
        return sku
    return sku.split("-")[0]

def explode_bundle(df, sku_col, qty_col, bundle_df):
    df = df.copy()
    df["Clean_SKU"] = df[sku_col].apply(clean_sku_for_lookup)

    bundle_map = bundle_df.rename(columns={"Kit_Sku": "Clean_SKU"})

    merged = df.merge(bundle_map, on="Clean_SKU", how="left")

    is_bundle = merged["Component_Sku"].notna()

    exploded = merged[is_bundle].copy()
    exploded["Final SKU"] = exploded["Component_Sku"]
    exploded["Qty"] = exploded[qty_col] * exploded["Component_Qty"]

    single = merged[~is_bundle].copy()
    single["Final SKU"] = single[sku_col]
    single["Qty"] = single[qty_col]

    return pd.concat([exploded, single], ignore_index=True)

def safe_filter(df, column, value):
    if column in df.columns:
        return df[df[column] == value]
    return df

# ======================================================
# SIDEBAR UPLOAD
# ======================================================
st.sidebar.header("Upload File")

kamus_file = st.sidebar.file_uploader(
    "Upload Kamus Dashboard.xlsx",
    type=["xlsx"]
)

shopee_file = st.sidebar.file_uploader(
    "Upload Shopee Order Report",
    type=["xlsx"]
)

tokped_file = st.sidebar.file_uploader(
    "Upload Tokopedia Order Report",
    type=["xlsx"]
)

# ======================================================
# LOAD KAMUS
# ======================================================
@st.cache_data
def load_kamus(file):
    xls = pd.ExcelFile(file)
    bundle = normalize_columns(pd.read_excel(xls, "Bundle Master"))
    sku = normalize_columns(pd.read_excel(xls, "SKU Master"))
    return bundle, sku

if kamus_file:
    bundle_master, sku_master = load_kamus(kamus_file)
    sku_lookup = dict(
        zip(
            sku_master["Product_Sku"].astype(str),
            sku_master["Product_Name"]
        )
    )
else:
    st.warning("Upload Kamus Dashboard terlebih dahulu")
    st.stop()

# ======================================================
# PROCESS SHOPEE
# ======================================================
def process_shopee(file):
    df = normalize_columns(pd.read_excel(file))

    df = safe_filter(df, "Pesanan yang Dikelola Shopee", "No")

    if "No Resi" in df.columns:
        df = df[df["No Resi"].isna()]

    df = explode_bundle(
        df,
        sku_col="SKU Produk",
        qty_col="Jumlah",
        bundle_df=bundle_master
    )

    df["Product Name"] = (
        df["Final SKU"]
        .apply(clean_sku_for_lookup)
        .map(sku_lookup)
    )

    return df

# ======================================================
# PROCESS TOKOPEDIA
# ======================================================
def process_tokped(file):
    df = normalize_columns(pd.read_excel(file))

    df = safe_filter(df, "Order Status", "Perlu dikirim")

    df = explode_bundle(
        df,
        sku_col="Seller SKU",
        qty_col="Quantity",
        bundle_df=bundle_master
    )

    df["Product Name"] = (
        df["Final SKU"]
        .apply(clean_sku_for_lookup)
        .map(sku_lookup)
    )

    return df

# ======================================================
# DASHBOARD
# ======================================================
tabs = st.tabs(["Shopee", "Tokopedia", "Rekap Final"])

with tabs[0]:
    if shopee_file:
        shopee_df = process_shopee(shopee_file)
        report = (
            shopee_df
            .groupby(["Final SKU", "Product Name"], as_index=False)["Qty"]
            .sum()
            .sort_values("Qty", ascending=False)
        )
        st.dataframe(report, use_container_width=True)

with tabs[1]:
    if tokped_file:
        tokped_df = process_tokped(tokped_file)
        report = (
            tokped_df
            .groupby(["Final SKU", "Product Name"], as_index=False)["Qty"]
            .sum()
            .sort_values("Qty", ascending=False)
        )
        st.dataframe(report, use_container_width=True)

with tabs[2]:
    if shopee_file and tokped_file:
        final = pd.concat([shopee_df, tokped_df])
        report = (
            final
            .groupby(["Final SKU", "Product Name"], as_index=False)["Qty"]
            .sum()
            .sort_values("Qty", ascending=False)
        )
        st.dataframe(report, use_container_width=True)

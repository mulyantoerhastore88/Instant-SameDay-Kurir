import streamlit as st
import pandas as pd
from io import BytesIO

st.set_page_config(
    page_title="Instant & Same Day – Bundle & Picking",
    layout="wide"
)

# =====================================================
# UTILITIES
# =====================================================

def clean_sku_for_lookup(sku: str) -> str:
    if pd.isna(sku):
        return sku
    sku = str(sku).strip()
    if sku.startswith("FG-") or sku.startswith("CS-"):
        return sku
    if "-" in sku:
        return sku.split("-", 1)[1]
    return sku


# =====================================================
# SIDEBAR
# =====================================================

st.sidebar.header("Upload File")

order_file = st.sidebar.file_uploader(
    "Upload Order Report (Shopee)",
    type=["xlsx", "xls", "csv"]
)

kamus_file = st.sidebar.file_uploader(
    "Upload Kamus Dashboard.xlsx",
    type=["xlsx"]
)

# =====================================================
# MAIN
# =====================================================

if order_file and kamus_file:

    # -----------------------------
    # LOAD ORDER REPORT
    # -----------------------------
    if order_file.name.endswith(".csv"):
        order_df = pd.read_csv(order_file)
    else:
        order_df = pd.read_excel(order_file)

    # -----------------------------
    # LOAD KAMUS
    # -----------------------------
    bundle_master = pd.read_excel(kamus_file, sheet_name="Bundle Master")
    sku_master = pd.read_excel(kamus_file, sheet_name="SKU Master")

    # =================================================
    # FILTER SHOPEE (FINAL)
    # =================================================
    order_df = order_df[
        (order_df["Status Pesanan"] == "Perlu Dikirim") &
        (order_df["No. Resi"].isna()) &
        (order_df["Opsi Pengiriman"].isin(["Instant", "Same Day"]))
    ]

    # =================================================
    # PREPARE SKU MASTER
    # =================================================
    sku_master["Product_Sku_Clean"] = sku_master["Product_Sku"].apply(clean_sku_for_lookup)
    sku_name_map = dict(
        zip(sku_master["Product_Sku_Clean"], sku_master["Product_Name"])
    )

    # =================================================
    # PREPARE BUNDLE MASTER
    # =================================================
    bundle_master["Kit_Sku_Clean"] = bundle_master["Kit_Sku"].apply(clean_sku_for_lookup)
    bundle_master["Component_Sku_Clean"] = bundle_master["Component_Sku"].apply(clean_sku_for_lookup)

    bundle_mapping = {}
    for _, r in bundle_master.iterrows():
        bundle_mapping.setdefault(r["Kit_Sku_Clean"], []).append({
            "component_sku": r["Component_Sku_Clean"],
            "component_name": r["Component_Product_Name"],
            "qty": r["Component_Qty"]
        })

    # =================================================
    # CLEAN ORDER SKU
    # =================================================
    order_df["SKU_Clean"] = order_df["SKU Induk"].apply(clean_sku_for_lookup)

    # =================================================
    # EXPLODE ORDER → DETAIL
    # =================================================
    exploded_rows = []

    for _, row in order_df.iterrows():
        order_id = row["No. Pesanan"]
        sku_clean = row["SKU_Clean"]
        qty_order = row["Jumlah Produk Dibeli"]

        if sku_clean in bundle_mapping:
            for comp in bundle_mapping[sku_clean]:
                exploded_rows.append({
                    "Order ID": order_id,
                    "Original SKU": row["SKU Induk"],
                    "Final SKU": comp["component_sku"],
                    "Product Name": sku_name_map.get(
                        comp["component_sku"], comp["component_name"]
                    ),
                    "Qty": qty_order * comp["qty"]
                })
        else:
            exploded_rows.append({
                "Order ID": order_id,
                "Original SKU": row["SKU Induk"],
                "Final SKU": sku_clean,
                "Product Name": sku_name_map.get(sku_clean, ""),
                "Qty": qty_order
            })

    DETAIL_COLUMNS = [     "Order ID",     "Original SKU",     "Final SKU",     "Product Name",     "Qty" ]  detail_df = pd.DataFrame(exploded_rows, columns=DETAIL_COLUMNS)

    # =================================================
    # PICKING LIST
    # =================================================
    picking_df = (
        detail_df
        .groupby(["Final SKU", "Product Name"], as_index=False)["Qty"]
        .sum()
        .sort_values("Final SKU")
    )

    # =================================================
    # TABS
    # =================================================
    tab1, tab2, tab3 = st.tabs([
        "📦 Detail Order",
        "🧾 Picking List",
        "⬇️ Download"
    ])

    with tab1:
        st.subheader("Detail Order (Bundle Expanded)")
        st.dataframe(detail_df, use_container_width=True)

    with tab2:
        st.subheader("Picking List")
        st.dataframe(picking_df, use_container_width=True)

    with tab3:
        st.subheader("Download Report")

        csv = detail_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download Detail CSV",
            csv,
            "detail_order.csv",
            "text/csv"
        )

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            detail_df.to_excel(writer, sheet_name="Detail Order", index=False)
            picking_df.to_excel(writer, sheet_name="Picking List", index=False)

        st.download_button(
            "Download Excel",
            output.getvalue(),
            "order_report.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

else:
    st.info("Upload Order Report dan Kamus Dashboard untuk mulai.")

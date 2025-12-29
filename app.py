import streamlit as st
import pandas as pd
import numpy as np
import io
import time
from datetime import datetime
import plotly.express as px

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Universal Order Processor",
    page_icon="🛒",
    layout="wide"
)

# --- STYLE ---
st.markdown("""
    <style>
    .main { background-color: #f5f7f9; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    </style>
    """, unsafe_allow_html=True)

st.title("🛒 Universal Marketplace Order Processor")
st.markdown("Proses order Shopee & Tokopedia/TikTok dengan logic Instant/Same Day & Bundle Expansion.")

# --- FUNGSI CLEANING SKU (LOGIC KITA) ---
def clean_sku(sku):
    """Konversi ke string, strip, dan ambil bagian kanan hyphen."""
    sku = str(sku).strip()
    if '-' in sku:
        return sku.split('-', 1)[-1].strip()
    return sku

# --- FUNGSI STANDARDIZE SHOPEE (LOGIC KITA) ---
def standardize_shopee_data(df):
    column_mapping = {
        'No. Pesanan': 'order_id',
        'Status Pesanan': 'status',
        'Pesanan yang Dikelola Shopee': 'managed_by_platform',
        'Opsi Pengiriman': 'shipping_option',
        'No. Resi': 'tracking_id',
        'Nomor Referensi SKU': 'sku_reference',
        'Jumlah': 'quantity',
        'Pesanan Harus Dikirimkan Sebelum (Menghindari keterlambatan)': 'deadline'
    }
    df_std = df.rename(columns=column_mapping)
    df_std['marketplace'] = 'shopee'
    return df_std

# --- FUNGSI STANDARDIZE TOKPED/TIKTOK ---
def standardize_tokped_data(df):
    column_mapping = {
        'Order ID': 'order_id',
        'Order Status': 'status',
        'Seller SKU': 'sku_reference',
        'Quantity': 'quantity',
        'No. Resi': 'tracking_id',
        'Kurir': 'shipping_option'
    }
    df_std = df.rename(columns=column_mapping)
    df_std['marketplace'] = 'tokped_tiktok'
    df_std['managed_by_platform'] = 'No' # Default No untuk Tokped
    return df_std

# --- FUNGSI PROCESSING UTAMA (REVISI LOGIC) ---
def process_universal_data(df_orders_list, kamus_data, marketplace_types):
    start_time = time.time()
    all_expanded_rows = []
    
    # 1. Prepare Kamus Data
    df_bundle = kamus_data['bundle'].copy()
    df_sku = kamus_data['sku'].copy()
    df_kurir = kamus_data['kurir'].copy()

    # Pre-clean Kamus Keys
    df_bundle['SKU Bundle'] = df_bundle['SKU Bundle'].apply(clean_sku)
    df_sku['SKU Component'] = df_sku.iloc[:, 0].apply(clean_sku) # Asumsi Kolom 1 adalah Material
    
    # 2. Get Instant/Same Day Options (Kriteria Kita)
    instant_same_day_options = []
    if 'Instant/Same Day' in df_kurir.columns:
        instant_same_day_options = df_kurir[
            df_kurir['Instant/Same Day'].astype(str).str.upper().str.strip().isin(['YES', 'YA', 'TRUE'])
        ].iloc[:, 0].astype(str).str.strip().unique()

    # 3. Processing Marketplaces
    for df_raw, mtype in zip(df_orders_list, marketplace_types):
        # Standardize
        if mtype == 'shopee':
            df_std = standardize_shopee_data(df_raw)
            # FILTER SHOPEE (Logic Kita)
            df_filtered = df_std[
                (df_std['status'].str.strip() == 'Perlu Dikirim') &
                (df_std['managed_by_platform'].str.strip().str.upper() == 'NO') &
                (df_std['shipping_option'].str.strip().isin(instant_same_day_options)) &
                (df_std['tracking_id'].astype(str).str.strip().isin(['', 'nan']))
            ].copy()
        else:
            df_std = standardize_tokped_data(df_raw)
            # FILTER TOKPED (Biasanya status 'Ready to Ship' atau 'Pesanan Baru')
            # Kita fokuskan ke Resi Blank juga agar seragam
            df_filtered = df_std[
                (df_std['tracking_id'].astype(str).str.strip().isin(['', 'nan']))
            ].copy()

        if df_filtered.empty: continue

        # 4. Expansion & Bundle Logic
        df_filtered['sku_clean'] = df_filtered['sku_reference'].apply(clean_sku)
        
        for _, row in df_filtered.iterrows():
            sku_key = row['sku_clean']
            
            # Cek Bundle
            is_bundle = sku_key in df_bundle['SKU Bundle'].values
            
            if is_bundle:
                components = df_bundle[df_bundle['SKU Bundle'] == sku_key]
                for _, comp in components.iterrows():
                    all_expanded_rows.append({
                        'Marketplace': row['marketplace'],
                        'Order ID': row['order_id'],
                        'Status': row['status'],
                        'Shipping': row['shipping_option'],
                        'SKU Original': row['sku_reference'],
                        'Is Bundle?': 'Yes',
                        'SKU Component': clean_sku(comp['SKU Component']),
                        'Qty': row['quantity'] * comp['Component Quantity']
                    })
            else:
                all_expanded_rows.append({
                    'Marketplace': row['marketplace'],
                    'Order ID': row['order_id'],
                    'Status': row['status'],
                    'Shipping': row['shipping_option'],
                    'SKU Original': row['sku_reference'],
                    'Is Bundle?': 'No',
                    'SKU Component': sku_key,
                    'Qty': row['quantity']
                })

    if not all_expanded_rows:
        return {"error": "❌ Tidak ada order yang memenuhi kriteria (Perlu Dikirim & Resi Kosong)."}

    df_final = pd.DataFrame(all_expanded_rows)
    
    # 5. Lookup Product Name (Delayed Lookup seperti permintaanmu)
    df_sku_lookup = df_sku.iloc[:, [0, 1]] # Material & Description
    df_sku_lookup.columns = ['SKU Component', 'Product Name Master']
    df_sku_lookup['SKU Component'] = df_sku_lookup['SKU Component'].apply(clean_sku)
    
    df_final = pd.merge(df_final, df_sku_lookup, on='SKU Component', how='left')
    df_final['Product Name'] = df_final['Product Name Master'].fillna(df_final['SKU Component'])

    # 6. Aggregation (Output 2)
    df_summary = df_final.groupby(['Marketplace', 'SKU Component', 'Product Name']).agg({'Qty': 'sum'}).reset_index()

    return {
        'detail': df_final,
        'summary': df_summary,
        'time': time.time() - start_time
    }

# --- SIDEBAR UPLOAD ---
st.sidebar.header("📁 Data Source")
marketplace_options = st.sidebar.multiselect("Marketplace", ["Shopee", "Tokopedia/TikTok"], default=["Shopee"])

uploaded_orders = []
m_types = []

for m in marketplace_options:
    f = st.sidebar.file_uploader(f"Upload Order {m}", type=['xlsx', 'csv'])
    if f:
        df_f = pd.read_excel(f) if f.name.endswith('xlsx') else pd.read_csv(f)
        uploaded_orders.append(df_f)
        m_types.append('shopee' if m == "Shopee" else "tokped_tiktok")

kamus_file = st.sidebar.file_uploader("Upload Kamus Master", type=['xlsx'])

# --- EXECUTION ---
if st.sidebar.button("🚀 PROSES SEKARANG", type="primary", use_container_width=True):
    if not uploaded_orders or not kamus_file:
        st.error("Lengkapi file order dan kamus dulu, Bro!")
    else:
        with st.spinner("Sedang mengolah data..."):
            # Load Kamus
            excel_kamus = pd.ExcelFile(kamus_file)
            kamus_data = {
                'kurir': pd.read_excel(kamus_file, sheet_name='Kurir-Shopee'),
                'bundle': pd.read_excel(kamus_file, sheet_name='Bundle Master'),
                'sku': pd.read_excel(kamus_file, sheet_name='SKU Master')
            }
            
            results = process_universal_data(uploaded_orders, kamus_data, m_types)
            
            if "error" in results:
                st.warning(results["error"])
            else:
                st.balloons()
                st.session_state.res = results

# --- DISPLAY ---
if 'res' in st.session_state:
    res = st.session_state.res
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Order Filtered", res['detail']['Order ID'].nunique())
    col2.metric("Total Item Expand", len(res['detail']))
    col3.metric("Processing Time", f"{res['time']:.2f}s")

    tab1, tab2, tab3 = st.tabs(["📋 Detail Order", "📦 Ringkasan SKU", "📥 Download"])
    
    with tab1:
        st.dataframe(res['detail'], use_container_width=True)
    
    with tab2:
        st.dataframe(res['summary'], use_container_width=True)
        
    with tab3:
        # Excel Download logic
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            res['detail'].to_excel(writer, sheet_name='Detail', index=False)
            res['summary'].to_excel(writer, sheet_name='Summary', index=False)
        
        st.download_button(
            label="📥 Download Hasil (Excel)",
            data=output.getvalue(),
            file_name=f"Processed_Orders_{datetime.now().strftime('%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

import streamlit as st
import pandas as pd
import numpy as np
import io
from datetime import datetime
import plotly.express as px

# Page config
st.set_page_config(
    page_title="Instant & SameDay Processor",
    page_icon="🚚",
    layout="wide"
)

# Title
st.title("🚚 Instant & SameDay Order Processor")
st.markdown("Upload file order dan kamus master, proses langsung di sini!")

# Initialize session state
if 'processed' not in st.session_state:
    st.session_state.processed = False
if 'results' not in st.session_state:
    st.session_state.results = {}

# --- FUNGSI CLEANING ---
def clean_sku(sku):
    """Konversi ke string, strip, dan ambil bagian kanan hyphen."""
    if pd.isna(sku):
        return ""
    sku = str(sku).strip()
    if '-' in sku:
        return sku.split('-', 1)[-1].strip()
    return sku

def clean_df_strings(df):
    """Membersihkan semua kolom string dari spasi/karakter tak terlihat."""
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
    return df

# --- FUNGSI LOOKUP PRODUCT NAME ---
def lookup_product_name(df_sku, sku_code):
    """Lookup product name dari SKU Master berdasarkan SKU Code"""
    try:
        # Cari di kolom SKU Code atau Material
        if 'SKU Code' in df_sku.columns:
            match = df_sku[df_sku['SKU Code'].astype(str).str.strip() == str(sku_code).strip()]
        elif 'Material' in df_sku.columns:
            match = df_sku[df_sku['Material'].astype(str).str.strip() == str(sku_code).strip()]
        else:
            return ""
        
        if not match.empty and 'Material description' in match.columns:
            return match.iloc[0]['Material description']
        elif not match.empty and 'Material description' not in match.columns:
            # Coba kolom lain yang mungkin berisi nama produk
            for col in ['Description', 'Product Name', 'Nama Produk']:
                if col in match.columns:
                    return match.iloc[0][col]
        
        return ""
    except:
        return ""

# --- FUNGSI MEMBACA FILE KAMUS ---
def read_kamus_file(kamus_file):
    """Membaca file kamus Excel yang memiliki 3 sheet"""
    try:
        if kamus_file.name.endswith('.xlsx') or kamus_file.name.endswith('.xls'):
            # Baca semua sheet
            excel_file = pd.ExcelFile(kamus_file, engine='openpyxl')
            sheet_names = excel_file.sheet_names
            
            # Cari nama sheet yang sesuai
            sheets = {}
            
            # Cari sheet Kurir-Shopee
            kurir_sheets = [s for s in sheet_names if 'kurir' in s.lower() or 'shopee' in s.lower()]
            if kurir_sheets:
                sheets['kurir'] = pd.read_excel(kamus_file, sheet_name=kurir_sheets[0], engine='openpyxl')
            else:
                # Coba sheet pertama jika tidak ditemukan
                sheets['kurir'] = pd.read_excel(kamus_file, sheet_name=0, engine='openpyxl')
            
            # Cari sheet Bundle Master
            bundle_sheets = [s for s in sheet_names if 'bundle' in s.lower()]
            if bundle_sheets:
                sheets['bundle'] = pd.read_excel(kamus_file, sheet_name=bundle_sheets[0], engine='openpyxl')
            else:
                # Coba sheet kedua jika tidak ditemukan
                if len(sheet_names) > 1:
                    sheets['bundle'] = pd.read_excel(kamus_file, sheet_name=1, engine='openpyxl')
                else:
                    st.error("Sheet Bundle Master tidak ditemukan")
                    return None
            
            # Cari sheet SKU Master
            sku_sheets = [s for s in sheet_names if 'sku' in s.lower() and 'master' in s.lower()]
            if sku_sheets:
                sheets['sku'] = pd.read_excel(kamus_file, sheet_name=sku_sheets[0], engine='openpyxl')
            else:
                # Coba sheet ketiga jika tidak ditemukan
                if len(sheet_names) > 2:
                    sheets['sku'] = pd.read_excel(kamus_file, sheet_name=2, engine='openpyxl')
                else:
                    st.error("Sheet SKU Master tidak ditemukan")
                    return None
            
            # Tampilkan info sheet yang terbaca
            st.sidebar.success(f"✅ Sheet terbaca: {', '.join(sheets.keys())}")
            return sheets
            
        else:
            st.error("File kamus harus dalam format Excel (.xlsx atau .xls)")
            return None
            
    except Exception as e:
        st.error(f"Error membaca file kamus: {str(e)}")
        return None

# --- FUNGSI PROCESSING UTAMA ---
def process_data(df_orders, kamus_data):
    """Logika processing utama"""
    
    # Extract data dari kamus
    df_kurir = kamus_data['kurir']
    df_bundle = kamus_data['bundle']
    df_sku = kamus_data['sku']
    
    # Cleaning data master
    df_bundle = clean_df_strings(df_bundle)
    df_sku = clean_df_strings(df_sku)
    
    # Clean SKU columns
    if 'SKU Bundle' in df_bundle.columns:
        df_bundle['SKU Bundle'] = df_bundle['SKU Bundle'].apply(clean_sku)
    
    # Standardize SKU Master column names
    sku_col_mapping = {}
    if 'Material' in df_sku.columns:
        sku_col_mapping['Material'] = 'SKU Code'
    if 'SKU Code' in df_sku.columns:
        sku_col_mapping['SKU Code'] = 'SKU Code'
    if 'Material description' in df_sku.columns:
        sku_col_mapping['Material description'] = 'Product Name'
    
    df_sku.rename(columns=sku_col_mapping, inplace=True)
    
    # Clean orders data
    df_orders.columns = df_orders.columns.astype(str).str.strip()
    
    # Pastikan kolom yang diperlukan ada
    required_cols = ['Status Pesanan', 'Pesanan yang Dikelola Shopee', 'Opsi Pengiriman', 'No. Resi', 'Jumlah']
    for col in required_cols:
        if col not in df_orders.columns:
            st.warning(f"⚠️ Kolom '{col}' tidak ditemukan di file order")
            if col == 'Jumlah':
                df_orders[col] = 1  # Default quantity
            else:
                df_orders[col] = ""
    
    df_orders['Status Pesanan'] = df_orders['Status Pesanan'].astype(str).str.strip()
    df_orders['Pesanan yang Dikelola Shopee'] = df_orders['Pesanan yang Dikelola Shopee'].astype(str).str.strip()
    df_orders['Opsi Pengiriman'] = df_orders['Opsi Pengiriman'].astype(str).str.strip()
    df_orders['No. Resi'] = df_orders['No. Resi'].astype(str).str.strip()
    df_orders['Is Resi Blank'] = (df_orders['No. Resi'] == '') | (df_orders['No. Resi'].isna()) | (df_orders['No. Resi'].str.lower() == 'nan')
    
    # Clean kurir data
    if 'Opsi Pengiriman' not in df_kurir.columns:
        # Coba cari kolom dengan nama berbeda
        for col in df_kurir.columns:
            if 'pengiriman' in col.lower() or 'opsi' in col.lower():
                df_kurir.rename(columns={col: 'Opsi Pengiriman'}, inplace=True)
                break
    
    if 'Instant/Same Day' not in df_kurir.columns:
        # Coba cari kolom dengan nama berbeda
        for col in df_kurir.columns:
            if 'instant' in col.lower() or 'same' in col.lower():
                df_kurir.rename(columns={col: 'Instant/Same Day'}, inplace=True)
                break
    
    if 'Opsi Pengiriman' in df_kurir.columns:
        df_kurir['Opsi Pengiriman'] = df_kurir['Opsi Pengiriman'].astype(str).str.strip()
    
    if 'Instant/Same Day' in df_kurir.columns:
        instant_same_day_options = df_kurir[
            df_kurir['Instant/Same Day'].astype(str).str.strip().str.upper().isin(['YES', 'YA', '1', 'TRUE', 'Y'])
        ]['Opsi Pengiriman'].unique()
    else:
        st.error("Kolom 'Instant/Same Day' tidak ditemukan di file Kamus")
        return None
    
    # Filter utama
    df_filtered = df_orders[
        (df_orders['Status Pesanan'].str.upper() == 'PERLU DIKIRIM') &
        (df_orders['Pesanan yang Dikelola Shopee'].str.upper().isin(['NO', 'TIDAK', 'FALSE'])) &
        (df_orders['Opsi Pengiriman'].isin(instant_same_day_options)) &
        (df_orders['Is Resi Blank'] == True)
    ].copy()
    
    if df_filtered.empty:
        return {"error": "❌ Tidak ada data yang memenuhi kriteria filter.\n\nCek:\n1. Status = 'Perlu Dikirim'\n2. Dikelola Shopee = 'No'\n3. Opsi Instant/Same Day\n4. No. Resi kosong"}
    
    # Output 3: Order Summarize
    df_output3 = df_filtered.groupby('Opsi Pengiriman').agg(
        {'No. Pesanan': 'nunique'}
    ).reset_index()
    df_output3.rename(columns={'No. Pesanan': 'Total Order'}, inplace=True)
    
    # Clean SKU Awal
    sku_cols = ['Nomor Referensi SKU', 'SKU Induk', 'Nama Produk']
    available_cols = [col for col in sku_cols if col in df_filtered.columns]
    
    if available_cols:
        df_filtered['SKU Awal'] = df_filtered[available_cols[0]]
        for col in available_cols[1:]:
            df_filtered['SKU Awal'] = df_filtered['SKU Awal'].fillna(df_filtered[col])
    else:
        # Coba cari kolom yang mengandung 'SKU'
        sku_columns = [col for col in df_filtered.columns if 'sku' in col.lower()]
        if sku_columns:
            df_filtered['SKU Awal'] = df_filtered[sku_columns[0]]
        else:
            df_filtered['SKU Awal'] = ""
    
    df_filtered['SKU Awal'] = df_filtered['SKU Awal'].apply(clean_sku)
    
    # Bundle Expansion
    sku_bundle_list = df_bundle['SKU Bundle'].unique() if 'SKU Bundle' in df_bundle.columns else []
    expanded_rows = []
    
    for _, row in df_filtered.iterrows():
        sku_awal_cleaned = row['SKU Awal']
        original_sku_raw = row.get('Nomor Referensi SKU', '')
        
        if sku_awal_cleaned and sku_awal_cleaned in sku_bundle_list:
            # Bundle ditemukan
            bundle_components = df_bundle[df_bundle['SKU Bundle'] == sku_awal_cleaned]
            for _, comp_row in bundle_components.iterrows():
                component_sku = clean_sku(comp_row.get('SKU Component', comp_row.get('Component', '')))
                expanded_rows.append({
                    'No. Pesanan': row.get('No. Pesanan', ''),
                    'Status Pesanan': row.get('Status Pesanan', ''),
                    'Opsi Pengiriman': row.get('Opsi Pengiriman', ''),
                    'Nomor Referensi SKU Original': original_sku_raw,
                    'Is Bundle?': 'Yes',
                    'SKU Component': component_sku,
                    'Product Name': lookup_product_name(df_sku, component_sku),
                    'Jumlah Final': row.get('Jumlah', 1) * comp_row.get('Component Quantity', 1),
                })
        else:
            # Item satuan
            expanded_rows.append({
                'No. Pesanan': row.get('No. Pesanan', ''),
                'Status Pesanan': row.get('Status Pesanan', ''),
                'Opsi Pengiriman': row.get('Opsi Pengiriman', ''),
                'Nomor Referensi SKU Original': original_sku_raw,
                'Is Bundle?': 'No',
                'SKU Component': sku_awal_cleaned,
                'Product Name': lookup_product_name(df_sku, sku_awal_cleaned),
                'Jumlah Final': row.get('Jumlah', 1) * 1,
            })
    
    df_bundle_expanded = pd.DataFrame(expanded_rows)
    
    # Output 1: Detail Order (dengan Product Name)
    df_output1 = df_bundle_expanded.copy()
    df_output1 = df_output1[[
        'No. Pesanan',
        'Status Pesanan',
        'Opsi Pengiriman',
        'Nomor Referensi SKU Original',
        'Is Bundle?',
        'SKU Component',
        'Product Name',
        'Jumlah Final'
    ]]
    
    df_output1 = df_output1.rename(columns={
        'SKU Component': 'Nomor Referensi SKU (Component/Cleaned)',
        'Jumlah Final': 'Jumlah dikalikan Component Quantity (Grand Total)'
    })
    
    # Output 2: Grand Total per SKU (dengan Product Name)
    if not df_bundle_expanded.empty:
        # Group by SKU Component dan Product Name
        df_output2 = df_bundle_expanded.groupby(['SKU Component', 'Product Name']).agg(
            {'Jumlah Final': 'sum'}
        ).reset_index()
        
        # Reorder columns untuk konsistensi
        df_output2 = df_output2.rename(columns={
            'SKU Component': 'Nomor Referensi SKU (Cleaned)',
            'Jumlah Final': 'Jumlah (Grand total by SKU)'
        })
        
        # Urutkan dari yang terbesar
        df_output2 = df_output2.sort_values('Jumlah (Grand total by SKU)', ascending=False)
    else:
        df_output2 = pd.DataFrame(columns=['Nomor Referensi SKU (Cleaned)', 'Product Name', 'Jumlah (Grand total by SKU)'])
    
    # Tampilkan preview kamus yang terbaca
    with st.sidebar.expander("🔍 Preview Kamus"):
        st.write("**Kurir:**", df_kurir.shape, "rows")
        st.write("**Bundle:**", df_bundle.shape, "rows")
        st.write("**SKU:**", df_sku.shape, "rows")
        
        # Tampilkan contoh lookup
        if 'SKU Code' in df_sku.columns and not df_sku.empty:
            sample_sku = df_sku.iloc[0]['SKU Code'] if 'SKU Code' in df_sku.columns else ""
            sample_name = df_sku.iloc[0]['Product Name'] if 'Product Name' in df_sku.columns else ""
            st.write(f"Sample lookup: {sample_sku} → {sample_name}")
    
    return {
        'output1': df_output1,
        'output2': df_output2,
        'output3': df_output3,
        'filtered_count': len(df_filtered),
        'expanded_count': len(df_bundle_expanded),
        'original_count': len(df_orders),
        'sku_master': df_sku  # Simpan untuk reference
    }

# --- SIDEBAR UPLOAD ---
with st.sidebar:
    st.header("📁 Upload Files")
    
    st.subheader("1. File Order")
    order_file = st.file_uploader(
        "Upload file order (CSV/Excel)",
        type=['csv', 'xlsx', 'xls'],
        key="order"
    )
    
    st.subheader("2. File Kamus Master")
    kamus_file = st.file_uploader(
        "Upload file kamus (Excel dengan 3 sheet)",
        type=['xlsx', 'xls'],
        key="kamus",
        help="File Excel harus berisi sheet: Kurir-Shopee, Bundle Master, SKU Master"
    )
    
    st.divider()
    
    # Advanced options
    with st.expander("⚙️ Advanced Options"):
        auto_process = st.checkbox("Auto-process after upload", value=True)
        show_debug = st.checkbox("Show debug info", value=False)
    
    # Process button
    process_btn = st.button("🚀 Process Data", type="primary", use_container_width=True)
    
    if process_btn or (auto_process and order_file and kamus_file):
        if order_file and kamus_file:
            with st.spinner("Memproses data..."):
                try:
                    # Read order file
                    if order_file.name.endswith('.csv'):
                        df_orders = pd.read_csv(order_file)
                    else:
                        df_orders = pd.read_excel(order_file, engine='openpyxl')
                    
                    # Read kamus file
                    kamus_data = read_kamus_file(kamus_file)
                    
                    if kamus_data:
                        # Process data
                        results = process_data(df_orders, kamus_data)
                        
                        if results and "error" not in results:
                            st.session_state.results = results
                            st.session_state.processed = True
                            st.success("✅ Data berhasil diproses!")
                            
                            # Debug info
                            if show_debug:
                                st.write("**Debug Info:**")
                                st.write(f"- Orders: {results['original_count']} original, {results['filtered_count']} filtered")
                                st.write(f"- SKU Master entries: {len(results['sku_master'])}")
                                
                        elif results and "error" in results:
                            st.error(results["error"])
                        else:
                            st.error("❌ Gagal memproses data")
                    
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
                    st.error("Pastikan format file sesuai!")
        else:
            st.warning("⚠️ Silakan upload kedua file terlebih dahulu!")
    
    st.divider()
    st.caption("Version 3.0 | Dengan Product Name Lookup")

# --- MAIN CONTENT ---
if st.session_state.processed:
    results = st.session_state.results
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Orders Original", results['original_count'])
    with col2:
        st.metric("Orders Filtered", results['filtered_count'])
    with col3:
        st.metric("Items Expanded", results['expanded_count'])
    with col4:
        st.metric("SKU dengan Product Name", 
                 results['output2']['Product Name'].notna().sum())
    
    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📋 Detail Order", 
        "📦 SKU Summary", 
        "📊 Statistics", 
        "💾 Download"
    ])
    
    with tab1:
        st.subheader("Detail Order (Expanded dengan Product Name)")
        if not results['output1'].empty:
            # Show statistics
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                bundle_count = results['output1']['Is Bundle?'].value_counts().get('Yes', 0)
                st.metric("Bundle Items", bundle_count)
            with col_b:
                single_count = results['output1']['Is Bundle?'].value_counts().get('No', 0)
                st.metric("Single Items", single_count)
            with col_c:
                with_name = results['output1']['Product Name'].notna().sum()
                st.metric("Dengan Product Name", with_name)
            
            # Tampilkan dataframe
            st.dataframe(
                results['output1'],
                use_container_width=True,
                hide_index=True,
                height=400,
                column_config={
                    "Product Name": st.column_config.TextColumn(
                        "Product Name",
                        width="large"
                    )
                }
            )
            
            # Show missing product names
            missing_names = results['output1'][results['output1']['Product Name'] == ""]
            if not missing_names.empty:
                st.warning(f"⚠️ {len(missing_names)} item tidak memiliki Product Name (tidak ditemukan di SKU Master)")
                
                with st.expander("Lihat item tanpa Product Name"):
                    st.dataframe(missing_names[['Nomor Referensi SKU (Component/Cleaned)', 'Product Name']], 
                               use_container_width=True)
        else:
            st.info("Tidak ada data detail order")
    
    with tab2:
        st.subheader("Grand Total by SKU (dengan Product Name)")
        if not results['output2'].empty:
            # Tampilkan SKU dengan dan tanpa product name
            with_names = results['output2'][results['output2']['Product Name'].notna()]
            without_names = results['output2'][results['output2']['Product Name'].isna()]
            
            col_x, col_y = st.columns(2)
            with col_x:
                st.metric("SKU dengan Product Name", len(with_names))
            with col_y:
                st.metric("SKU tanpa Product Name", len(without_names))
            
            # Tabs untuk dengan dan tanpa product name
            tab_a, tab_b = st.tabs(["✅ Dengan Product Name", "⚠️ Tanpa Product Name"])
            
            with tab_a:
                if not with_names.empty:
                    st.dataframe(
                        with_names,
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    # Bar chart untuk SKU dengan product name
                    fig = px.bar(
                        with_names.head(20),
                        x='Product Name',
                        y='Jumlah (Grand total by SKU)',
                        title="Top 20 Products by Quantity",
                        hover_data=['Nomor Referensi SKU (Cleaned)']
                    )
                    fig.update_xaxes(tickangle=45)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Tidak ada SKU dengan Product Name")
            
            with tab_b:
                if not without_names.empty:
                    st.dataframe(
                        without_names[['Nomor Referensi SKU (Cleaned)', 'Jumlah (Grand total by SKU)']],
                        use_container_width=True,
                        hide_index=True
                    )
                    st.info("SKU ini tidak ditemukan di SKU Master. Periksa file kamus.")
                else:
                    st.success("🎉 Semua SKU memiliki Product Name!")
        else:
            st.info("Tidak ada data SKU summary")
    
    with tab3:
        st.subheader("Order Summary by Courier")
        if not results['output3'].empty:
            col_i, col_ii = st.columns([2, 1])
            
            with col_i:
                st.dataframe(
                    results['output3'],
                    use_container_width=True,
                    hide_index=True
                )
            
            with col_ii:
                total = results['output3']['Total Order'].sum()
                st.metric("Total Orders", total)
                
                # Pie chart
                fig = px.pie(
                    results['output3'],
                    values='Total Order',
                    names='Opsi Pengiriman',
                    title="Distribution by Courier Service",
                    hole=0.3
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Tidak ada data courier summary")
    
    with tab4:
        st.subheader("📥 Download Results")
        
        # Generate timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if not results['output1'].empty:
            st.info("Pilih format download:")
            
            # Format selection
            col_format1, col_format2 = st.columns(2)
            with col_format1:
                include_product_names = st.checkbox("Include Product Names", value=True)
            with col_format2:
                download_format = st.radio("Format", ["CSV", "Excel"], horizontal=True)
            
            st.divider()
            
            # Prepare data berdasarkan pilihan
            if include_product_names:
                download_df1 = results['output1']
                download_df2 = results['output2']
            else:
                # Hilangkan kolom Product Name
                download_df1 = results['output1'].drop(columns=['Product Name'], errors='ignore')
                download_df2 = results['output2'].drop(columns=['Product Name'], errors='ignore')
            
            col1, col2, col3 = st.columns(3)
            
            if download_format == "CSV":
                with col1:
                    # Download Output 1
                    csv1 = download_df1.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📥 Detail Order",
                        data=csv1,
                        file_name=f"detail_order_{timestamp}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                
                with col2:
                    # Download Output 2
                    csv2 = download_df2.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📥 SKU Summary",
                        data=csv2,
                        file_name=f"sku_summary_{timestamp}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                
                with col3:
                    # Download Output 3
                    csv3 = results['output3'].to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📥 Courier Summary",
                        data=csv3,
                        file_name=f"courier_summary_{timestamp}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
            else:  # Excel format
                # Download All as Excel
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    download_df1.to_excel(writer, sheet_name='Detail Order', index=False)
                    download_df2.to_excel(writer, sheet_name='SKU Summary', index=False)
                    results['output3'].to_excel(writer, sheet_name='Courier Summary', index=False)
                    # Tambahkan sheet SKU Master sebagai reference
                    results['sku_master'].to_excel(writer, sheet_name='SKU Master Ref', index=False)
                
                st.download_button(
                    label="📊 Download All (Excel)",
                    data=output.getvalue(),
                    file_name=f"instant_sameday_report_{timestamp}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            
            # Quick download untuk yang sering dipakai
            st.divider()
            st.subheader("🚀 Quick Downloads")
            
            col_q1, col_q2 = st.columns(2)
            with col_q1:
                # Download untuk packing list (tanpa product name)
                df_packing = results['output1'][['Nomor Referensi SKU (Component/Cleaned)', 'Jumlah dikalikan Component Quantity (Grand Total)']]
                df_packing = df_packing.groupby('Nomor Referensi SKU (Component/Cleaned)').sum().reset_index()
                csv_packing = df_packing.to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📦 Packing List (Qty per SKU)",
                    data=csv_packing,
                    file_name=f"packing_list_{timestamp}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            with col_q2:
                # Download untuk tim kurir
                df_courier = results['output1'][['No. Pesanan', 'Opsi Pengiriman', 'Nomor Referensi SKU Original']]
                csv_courier = df_courier.drop_duplicates().to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="🚚 Courier List (Per Order)",
                    data=csv_courier,
                    file_name=f"courier_list_{timestamp}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        else:
            st.info("Tidak ada data untuk didownload")

else:
    # Show instructions if no data processed
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.info("""
        ## 📋 Instruksi Penggunaan:
        
        1. **Upload 2 file** di sidebar kiri:
           - **File Order** (CSV/Excel dari Shopee/Marketplace)
           - **File Kamus Master** (Excel dengan 3 sheet)
        
        2. Klik tombol **"Process Data"**
        
        3. Lihat hasil di dashboard
        
        4. Download hasil dalam format CSV atau Excel
        """)
        
        st.success("**✨ Fitur Baru:** Product Name otomatis di-lookup dari SKU Master!")
    
    with col2:
        st.image("https://cdn-icons-png.flaticon.com/512/3208/3208720.png", width=150)
    
    # Sample data structure
    with st.expander("📝 Contoh Struktur File Kamus"):
        col_a, col_b = st.columns(2)
        
        with col_a:
            st.write("""
            **Sheet: Kurir-Shopee**
            | Opsi Pengiriman | Instant/Same Day |
            |----------------|------------------|
            | Instant        | Yes              |
            | Same Day       | Yes              |
            | Regular        | No               |
            """)
        
        with col_b:
            st.write("""
            **Sheet: SKU Master**
            | Material | Material description |
            |----------|---------------------|
            | SKU001   | Product A           |
            | SKU002   | Product B           |
            | SKU003   | Product C           |
            """)

# Footer
st.divider()
st.caption("💡 **Product Name Lookup:** SKU akan di-lookup ke SKU Master untuk mendapatkan nama produk. Pastikan SKU Master sudah lengkap!")

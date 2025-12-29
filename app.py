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
            st.sidebar.info(f"✅ Sheet terbaca: {', '.join(sheets.keys())}")
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
    
    if 'Material' in df_sku.columns:
        df_sku.rename(columns={'Material': 'SKU Component'}, inplace=True)
    elif 'SKU Code' in df_sku.columns:
        df_sku.rename(columns={'SKU Code': 'SKU Component'}, inplace=True)
    
    if 'SKU Component' in df_sku.columns:
        df_sku['SKU Component'] = df_sku['SKU Component'].apply(clean_sku)
    else:
        st.error("Kolom SKU Component tidak ditemukan di SKU Master")
        return None
    
    # Clean orders data
    df_orders.columns = df_orders.columns.astype(str).str.strip()
    
    # Pastikan kolom yang diperlukan ada
    required_cols = ['Status Pesanan', 'Pesanan yang Dikelola Shopee', 'Opsi Pengiriman', 'No. Resi']
    for col in required_cols:
        if col not in df_orders.columns:
            st.warning(f"⚠️ Kolom '{col}' tidak ditemukan di file order")
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
            df_kurir['Instant/Same Day'].astype(str).str.strip().str.upper().isin(['YES', 'YA', '1', 'TRUE'])
        ]['Opsi Pengiriman'].unique()
    else:
        st.error("Kolom 'Instant/Same Day' tidak ditemukan di file Kamus")
        return None
    
    # Filter utama
    df_filtered = df_orders[
        (df_orders['Status Pesanan'].str.upper() == 'PERLU DIKIRIM') &
        (df_orders['Pesanan yang Dikelola Shopee'].str.upper().isin(['NO', 'TIDAK'])) &
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
                expanded_rows.append({
                    'No. Pesanan': row.get('No. Pesanan', ''),
                    'Status Pesanan': row.get('Status Pesanan', ''),
                    'Opsi Pengiriman': row.get('Opsi Pengiriman', ''),
                    'Nomor Referensi SKU Original': original_sku_raw,
                    'Is Bundle?': 'Yes',
                    'SKU Component': clean_sku(comp_row.get('SKU Component', comp_row.get('Component', ''))),
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
                'Jumlah Final': row.get('Jumlah', 1) * 1,
            })
    
    df_bundle_expanded = pd.DataFrame(expanded_rows)
    
    # Output 1: Detail Order
    df_output1 = df_bundle_expanded.rename(columns={
        'SKU Component': 'Nomor Referensi SKU (Component/Cleaned)',
        'Jumlah Final': 'Jumlah dikalikan Component Quantity (Grand Total)'
    })
    
    # Output 2: Grand Total per SKU
    if not df_bundle_expanded.empty:
        df_output2 = df_bundle_expanded.groupby('SKU Component').agg(
            {'Jumlah Final': 'sum'}
        ).reset_index().rename(columns={
            'SKU Component': 'Nomor Referensi SKU (Cleaned)',
            'Jumlah Final': 'Jumlah (Grand total by SKU)'
        })
    else:
        df_output2 = pd.DataFrame(columns=['Nomor Referensi SKU (Cleaned)', 'Jumlah (Grand total by SKU)'])
    
    # Tampilkan preview kamus yang terbaca
    with st.sidebar.expander("🔍 Preview Kamus"):
        st.write("**Kurir:**", df_kurir.shape, "rows")
        st.write("**Bundle:**", df_bundle.shape, "rows")
        st.write("**SKU:**", df_sku.shape, "rows")
    
    return {
        'output1': df_output1,
        'output2': df_output2,
        'output3': df_output3,
        'filtered_count': len(df_filtered),
        'expanded_count': len(df_bundle_expanded),
        'original_count': len(df_orders)
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
    
    # Process button
    process_btn = st.button("🚀 Process Data", type="primary", use_container_width=True)
    
    if process_btn:
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
    st.caption("Version 2.0 | Upload 2 File Saja")

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
        total_orders = results['output3']['Total Order'].sum()
        st.metric("Unique SKUs", len(results['output2']))
    
    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📋 Detail Order", 
        "📦 SKU Summary", 
        "📊 Statistics", 
        "💾 Download"
    ])
    
    with tab1:
        st.subheader("Detail Order (Expanded)")
        if not results['output1'].empty:
            # Show statistics
            col_a, col_b = st.columns(2)
            with col_a:
                bundle_count = results['output1']['Is Bundle?'].value_counts().get('Yes', 0)
                st.metric("Bundle Items", bundle_count)
            with col_b:
                single_count = results['output1']['Is Bundle?'].value_counts().get('No', 0)
                st.metric("Single Items", single_count)
            
            st.dataframe(
                results['output1'],
                use_container_width=True,
                hide_index=True,
                height=400
            )
        else:
            st.info("Tidak ada data detail order")
    
    with tab2:
        st.subheader("Grand Total by SKU")
        if not results['output2'].empty:
            st.dataframe(
                results['output2'].sort_values('Jumlah (Grand total by SKU)', ascending=False),
                use_container_width=True,
                hide_index=True
            )
            
            # Bar chart
            fig = px.bar(
                results['output2'].sort_values('Jumlah (Grand total by SKU)', ascending=False).head(20),
                x='Nomor Referensi SKU (Cleaned)',
                y='Jumlah (Grand total by SKU)',
                title="Top 20 SKUs by Quantity"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Tidak ada data SKU summary")
    
    with tab3:
        st.subheader("Order Summary by Courier")
        if not results['output3'].empty:
            st.dataframe(
                results['output3'],
                use_container_width=True,
                hide_index=True
            )
            
            # Pie chart
            fig = px.pie(
                results['output3'],
                values='Total Order',
                names='Opsi Pengiriman',
                title="Distribution by Courier Service"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Total orders
            total = results['output3']['Total Order'].sum()
            st.metric("Total Orders", total)
        else:
            st.info("Tidak ada data courier summary")
    
    with tab4:
        st.subheader("Download Results")
        
        # Generate timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if not results['output1'].empty:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Download Output 1
                csv1 = results['output1'].to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📥 Detail Order (CSV)",
                    data=csv1,
                    file_name=f"detail_order_{timestamp}.csv",
                    mime="text/csv",
                    use_container_width=True,
                    help="Download detail order yang sudah di-expand"
                )
            
            with col2:
                # Download Output 2
                csv2 = results['output2'].to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📥 SKU Summary (CSV)",
                    data=csv2,
                    file_name=f"sku_summary_{timestamp}.csv",
                    mime="text/csv",
                    use_container_width=True,
                    help="Download grand total per SKU"
                )
            
            with col3:
                # Download Output 3
                csv3 = results['output3'].to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📥 Courier Summary (CSV)",
                    data=csv3,
                    file_name=f"courier_summary_{timestamp}.csv",
                    mime="text/csv",
                    use_container_width=True,
                    help="Download summary per kurir"
                )
            
            # Download All as Excel
            st.divider()
            st.subheader("📊 Download All in One Excel File")
            
            # Create Excel file with multiple sheets
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                results['output1'].to_excel(writer, sheet_name='Detail Order', index=False)
                results['output2'].to_excel(writer, sheet_name='SKU Summary', index=False)
                results['output3'].to_excel(writer, sheet_name='Courier Summary', index=False)
            
            st.download_button(
                label="📥 Download All (Excel)",
                data=output.getvalue(),
                file_name=f"instant_sameday_report_{timestamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                help="Download semua hasil dalam 1 file Excel"
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
    
    with col2:
        st.image("https://cdn-icons-png.flaticon.com/512/3208/3208720.png", width=150)
    
    # Sample data structure
    with st.expander("📝 Contoh Struktur File"):
        col_a, col_b = st.columns(2)
        
        with col_a:
            st.write("""
            **File Order harus memiliki kolom:**
            - No. Pesanan
            - Status Pesanan
            - Pesanan yang Dikelola Shopee
            - Opsi Pengiriman
            - No. Resi
            - Nomor Referensi SKU
            - SKU Induk
            - Nama Produk
            - Jumlah
            """)
        
        with col_b:
            st.write("""
            **File Kamus Master (Excel) harus berisi 3 sheet:**
            1. **Kurir-Shopee**
               - Opsi Pengiriman
               - Instant/Same Day
            
            2. **Bundle Master**
               - SKU Bundle
               - SKU Component
               - Component Quantity
            
            3. **SKU Master**
               - Material (atau SKU Code)
               - Material description
            """)

# Footer
st.divider()
st.caption("💡 Tips: File Kamus harus dalam format Excel (.xlsx) dengan 3 sheet seperti contoh di atas.")

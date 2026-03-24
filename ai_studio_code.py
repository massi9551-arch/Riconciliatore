import streamlit as st
import pandas as pd
import io
from datetime import datetime
import re

st.set_page_config(page_title="Riconciliatore Bancario", page_icon="🔄", layout="wide")

def parse_amount(val):
    if pd.isna(val) or val == '': return 0.0
    if isinstance(val, (int, float)): return float(val)
    s = str(val).strip()
    if len(s) > 15: return 0.0
    s = re.sub(r'[^0-9,.-]', '', s)
    if not s: return 0.0
    if ',' in s and '.' in s:
        if s.rfind('.') > s.rfind(','): s = s.replace(',', '')
        else: s = s.replace('.', '').replace(',', '.')
    elif ',' in s: s = s.replace(',', '.')
    try:
        res = float(s)
        if res > 500000 and '.' not in s and ',' not in s: return 0.0
        return res
    except:
        return 0.0

def get_row_amount(row, desc_col_name=None, date_col_name=None):
    dare_keys = ['dare', 'debit', 'uscita', 'pagamento', 'addebito']
    avere_keys = ['avere', 'credit', 'entrata', 'versamento', 'accredito']
    for col in row.index:
        l_col = str(col).lower()
        if l_col == desc_col_name or l_col == date_col_name: continue
        if any(k in l_col for k in dare_keys):
            v = parse_amount(row[col])
            if v != 0: return round(v, 2)
        if any(k in l_col for k in avere_keys):
            v = parse_amount(row[col])
            if v != 0: return round(-abs(v), 2)
    for col in row.index:
        if col == desc_col_name or col == date_col_name: continue
        if any(k in str(col).lower() for k in ['importo', 'valore', 'netto', 'amount']):
            v = parse_amount(row[col])
            if v != 0: return round(v, 2)
    for col in row.index:
        if col == desc_col_name or col == date_col_name: continue
        p = parse_amount(row[col])
        if 0.01 <= abs(p) < 500000: return round(p, 2)
    return 0.0

def process_file(file):
    df = pd.read_excel(file)
    if df.columns.str.contains('Unnamed').any() or len(df.columns) < 2:
        for i in range(min(len(df), 40)):
            row_str = ' '.join([str(v).lower() for v in df.iloc[i].values if not pd.isna(v)])
            if any(k in row_str for k in ['data', 'importo', 'descrizione', 'causale']):
                df.columns = df.iloc[i]
                df = df.iloc[i+1:].reset_index(drop=True)
                break
    date_col = next((c for c in df.columns if any(k in str(c).lower() for k in ['data', 'date'])), df.columns[0])
    desc_col = next((c for c in df.columns if any(k in str(c).lower() for k in ['descrizione', 'causale', 'note', 'operazione']) and c != date_col), None)
    rows = []
    for _, row in df.iterrows():
        try:
            d = pd.to_datetime(row[date_col], errors='coerce')
            if pd.isna(d): continue
            amt = get_row_amount(row, desc_col_name=str(desc_col).lower() if desc_col else None, date_col_name=str(date_col).lower())
            if amt == 0: continue
            desc = str(row[desc_col]) if desc_col and not pd.isna(row[desc_col]) else ""
            rows.append({'date': d, 'amount': amt, 'description': desc})
        except: continue
    return pd.DataFrame(rows)

def run_reconciliation(off_df, tar_df, start, end):
    off = off_df[(off_df['date'] >= pd.Timestamp(start)) & (off_df['date'] <= pd.Timestamp(end))].copy()
    tar = tar_df[(tar_df['date'] >= pd.Timestamp(start)) & (tar_df['date'] <= pd.Timestamp(end))].copy()
    matched_off_idx, matched_tar_idx = [], []

    for t_idx, t_row in tar.iterrows():
        possible = off[~off.index.isin(matched_off_idx) & (off['date'] == t_row['date'])]
        match = possible[abs(abs(possible['amount']) - abs(t_row['amount'])) < 0.01]
        if not match.empty:
            matched_off_idx.append(match.index[0]); matched_tar_idx.append(t_idx)
        else:
            possible_any_date = off[~off.index.isin(matched_off_idx)]
            date_diff = (possible_any_date['date'] - t_row['date']).dt.days.abs()
            match_any = possible_any_date[(date_diff <= 4) & (abs(abs(possible_any_date['amount']) - abs(t_row['amount'])) < 0.01)]
            if not match_any.empty:
                matched_off_idx.append(match_any.index[0]); matched_tar_idx.append(t_idx)

    discrepancies = []
    for o_idx, o_row in off.iterrows():
        if o_idx not in matched_off_idx:
            discrepancies.append({
                'Data': o_row['date'].strftime('%d/%m/%Y'),
                'Fonte': 'Ufficiale (Banca)',
                'Descrizione': o_row['description'],
                'Importo': -abs(o_row['amount'])
            })
    for t_idx, t_row in tar.iterrows():
        if t_idx not in matched_tar_idx:
            discrepancies.append({
                'Data': t_row['date'].strftime('%d/%m/%Y'),
                'Fonte': 'Da Riconciliare (Gestionale)',
                'Descrizione': t_row['description'],
                'Importo': t_row['amount']
            })
            
    return pd.DataFrame(discrepancies)

st.title("🔄 Riconciliatore Bancario")
st.markdown("Confronto tra Estratto Conto e Gestionale")

c1, c2 = st.columns(2)
with c1: off_file = st.file_uploader("Estratto Conto (Ufficiale)", type=['xlsx'])
with c2: tar_file = st.file_uploader("Gestionale (Da Riconciliare)", type=['xlsx'])

d1, d2 = st.columns(2)
with d1: start = st.date_input("Inizio", datetime(2025, 1, 1))
with d2: end = st.date_input("Fine", datetime(2025, 1, 31))

if st.button("🚀 Avvia Analisi", use_container_width=True):
    if off_file and tar_file:
        with st.spinner('Confronto in corso...'):
            off_df = process_file(off_file)
            tar_df = process_file(tar_file)
            results = run_reconciliation(off_df, tar_df, start, end)
            
            if not results.empty:
                st.subheader(f"📊 Risultato: {len(results)} discrepanze trovate")
                
                # Ordinamento per data
                results = results.sort_values(['Data', 'Fonte'])
                
                # Visualizzazione con formattazione Euro e 2 decimali
                st.dataframe(
                    results,
                    column_config={
                        "Importo": st.column_config.NumberColumn(
                            "Importo",
                            format="€ %.2f",
                        )
                    },
                    use_container_width=True
                )
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    results.to_excel(writer, index=False)
                st.download_button("📥 Scarica Report Excel", output.getvalue(), "discrepanze_riconciliazione.xlsx")
            else:
                st.success("✅ Riconciliazione perfetta! Tutti i movimenti coincidono.")
    else:
        st.error("Carica entrambi i file per procedere.")

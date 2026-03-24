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
    s = re.sub(r'[^0-9,.-]', '', s)
    if not s: return 0.0
    if ',' in s and '.' in s:
        if s.rfind('.') > s.rfind(','): s = s.replace(',', '')
        else: s = s.replace('.', '').replace(',', '.')
    elif ',' in s: s = s.replace(',', '.')
    try: return float(s)
    except: return 0.0

def get_row_amount(row):
    """Estrae l'importo netto indipendentemente dalla colonna."""
    # 1. Cerca colonne Dare/Avere
    dare_keys = ['dare', 'debit', 'uscita', 'pagamento', 'addebito']
    avere_keys = ['avere', 'credit', 'entrata', 'versamento', 'accredito']
    dare, avere, found_split = 0.0, 0.0, False
    
    for col in row.index:
        l_col = str(col).lower()
        if any(k in l_col for k in dare_keys) and 'data' not in l_col:
            v = parse_amount(row[col])
            if v != 0: dare = v; found_split = True
        if any(k in l_col for k in avere_keys) and 'data' not in l_col:
            v = parse_amount(row[col])
            if v != 0: avere = v; found_split = True
    
    if found_split: return round(dare - avere, 2)

    # 2. Cerca colonna Importo
    for col in row.index:
        if any(k in str(col).lower() for k in ['importo', 'valore', 'netto', 'amount']):
            v = parse_amount(row[col])
            if v != 0: return round(v, 2)

    # 3. Fallback numerico
    for val in row:
        p = parse_amount(val)
        if 0.01 <= abs(p) < 10000000: return round(p, 2)
    return 0.0

def process_file(file):
    df = pd.read_excel(file)
    # Rilevamento intestazione
    if df.columns.str.contains('Unnamed').any() or len(df.columns) < 2:
        for i in range(min(len(df), 40)):
            row_str = ' '.join([str(v).lower() for v in df.iloc[i].values if not pd.isna(v)])
            if any(k in row_str for k in ['data', 'importo', 'descrizione', 'causale']):
                df.columns = df.iloc[i]
                df = df.iloc[i+1:].reset_index(drop=True)
                break
    
    # Identificazione colonne
    date_col = next((c for c in df.columns if any(k in str(c).lower() for k in ['data', 'date'])), df.columns[0])
    desc_col = next((c for c in df.columns if any(k in str(c).lower() for k in ['descrizione', 'causale', 'note', 'operazione']) and c != date_col), None)

    rows = []
    for _, row in df.iterrows():
        try:
            d = pd.to_datetime(row[date_col], errors='coerce')
            if pd.isna(d): continue
            amt = get_row_amount(row)
            if amt == 0: continue
            desc = str(row[desc_col]) if desc_col and not pd.isna(row[desc_col]) else ""
            rows.append({'date': d, 'amount': amt, 'description': desc})
        except: continue
    return pd.DataFrame(rows)

def run_reconciliation(off_df, tar_df, start, end):
    # Filtro periodo
    off = off_df[(off_df['date'] >= pd.Timestamp(start)) & (off_df['date'] <= pd.Timestamp(end))].copy()
    tar = tar_df[(tar_df['date'] >= pd.Timestamp(start)) & (tar_df['date'] <= pd.Timestamp(end))].copy()

    matched_off_idx = []
    matched_tar_idx = []

    # Matching basato su Valore Assoluto (ignora i segni invertiti Banca/Gestionale)
    for t_idx, t_row in tar.iterrows():
        # Cerca nella stessa data
        possible = off[~off.index.isin(matched_off_idx) & (off['date'] == t_row['date'])]
        match = possible[abs(abs(possible['amount']) - abs(t_row['amount'])) < 0.01]
        
        if not match.empty:
            matched_off_idx.append(match.index[0])
            matched_tar_idx.append(t_idx)
        else:
            # Cerca in date diverse (entro 4 giorni)
            possible_any_date = off[~off.index.isin(matched_off_idx)]
            date_diff = (possible_any_date['date'] - t_row['date']).dt.days.abs()
            match_any = possible_any_date[(date_diff <= 4) & (abs(abs(possible_any_date['amount']) - abs(t_row['amount'])) < 0.01)]
            if not match_any.empty:
                matched_off_idx.append(match_any.index[0])
                matched_tar_idx.append(t_idx)

    # Generazione lista finale (solo le 29 righe mancanti dall'estratto conto)
    discrepancies = []
    for o_idx, o_row in off.iterrows():
        if o_idx not in matched_off_idx:
            # Mostriamo l'importo come negativo come richiesto dall'utente
            discrepancies.append({
                'Data': o_row['date'].strftime('%d/%m/%Y'),
                'Descrizione': o_row['description'],
                'Importo non trovato': -abs(o_row['amount'])
            })

    return pd.DataFrame(discrepancies)

st.title("🔄 Riconciliatore Bancario")

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
                st.dataframe(results.sort_values('Data', ascending=True), use_container_width=True)
                
                # Export Excel
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    results.to_excel(writer, index=False)
                st.download_button("📥 Scarica Report Excel", output.getvalue(), "discrepanze_gennaio.xlsx")
            else:
                st.success("✅ Riconciliazione perfetta! Tutti i movimenti coincidono.")
    else:
        st.error("Carica entrambi i file per procedere.")

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
    if '.' in s and ',' in s:
        if s.rfind('.') > s.rfind(','): s = s.replace(',', '')
        else: s = s.replace('.', '').replace(',', '.')
    elif ',' in s: s = s.replace(',', '.')
    try: return float(s)
    except: return 0.0

def normalize_row_amount(row):
    dare_keys = ['dare', 'debit', 'uscita', 'uscite', 'pagamento', 'addebito']
    avere_keys = ['avere', 'credit', 'entrata', 'entrate', 'versamento', 'accredito']
    amount_keys = ['importo', 'amount', 'valore', 'netto', 'totale']
    keys = row.index
    dare, avere, found_split = None, None, False
    def is_anomalous(k):
        l = str(k).lower().strip()
        return l.isdigit() or 'saldo' in l
    for key in keys:
        if is_anomalous(key): continue
        l_key = str(key).lower().strip()
        if any(k in l_key for k in dare_keys) and 'data' not in l_key:
            dare = parse_amount(row[key])
            if not pd.isna(row[key]) and row[key] != '': found_split = True
        elif any(k in l_key for k in avere_keys) and 'data' not in l_key:
            avere = parse_amount(row[key])
            if not pd.isna(row[key]) and row[key] != '': found_split = True
    if found_split:
        d = dare if dare is not None else 0.0
        a = avere if avere is not None else 0.0
        return round(d - a, 2)
    for key in keys:
        if is_anomalous(key): continue
        l_key = str(key).lower().strip()
        if any(k in l_key for k in amount_keys):
            val = parse_amount(row[key])
            if not pd.isna(row[key]) and row[key] != '': return round(val, 2)
    return None

def find_date_column(df, preferred_keywords):
    for kw in preferred_keywords:
        for col in df.columns:
            if kw.lower() in str(col).lower(): return col
    for col in df.columns:
        if 'data' in str(col).lower() or 'date' in str(col).lower(): return col
    return df.columns[0]

def find_desc_column(df):
    keywords = ['descrizione', 'causale', 'note', 'dettagli', 'description', 'operazione']
    for kw in keywords:
        for col in df.columns:
            if kw in str(col).lower(): return col
    return None

def process_file(file, date_keywords):
    df = pd.read_excel(file)
    if df.columns.str.contains('Unnamed').any() or len(df.columns) < 3:
        for i in range(min(len(df), 20)):
            row_vals = [str(v).lower() for v in df.iloc[i].values if v is not None]
            if any(k in ' '.join(row_vals) for k in ['data', 'importo', 'descrizione']):
                df.columns = df.iloc[i]
                df = df.iloc[i+1:].reset_index(drop=True)
                break
    date_col = find_date_column(df, date_keywords)
    desc_col = find_desc_column(df)
    rows = []
    for _, row in df.iterrows():
        try:
            date_val = pd.to_datetime(row[date_col], errors='coerce')
            if pd.isna(date_val): continue
            amount = normalize_row_amount(row)
            if amount is None: continue
            desc = str(row[desc_col]) if desc_col and not pd.isna(row[desc_col]) else ""
            rows.append({'date': date_val, 'amount': amount, 'description': desc})
        except: continue
    return pd.DataFrame(rows)

def reconcile(official_df, target_df, start_date, end_date):
    off = official_df[(official_df['date'] >= pd.Timestamp(start_date)) & (official_df['date'] <= pd.Timestamp(end_date))].copy()
    tar = target_df[(target_df['date'] >= pd.Timestamp(start_date)) & (target_df['date'] <= pd.Timestamp(end_date))].copy()
    matches_same, matches_opp = 0, 0
    for _, r in tar.head(50).iterrows():
        if any((off['date'] == r['date']) & (abs(off['amount'] - r['amount']) < 0.01)): matches_same += 1
        if any((off['date'] == r['date']) & (abs(off['amount'] + r['amount']) < 0.01)): matches_opp += 1
    if matches_opp > matches_same and matches_opp > 0: off['amount'] = -off['amount']
    matched_off_idx, matched_tar_idx, near_matches, date_mismatches = [], [], [], []
    for t_idx, t_row in tar.iterrows():
        possible = off[~off.index.isin(matched_off_idx) & (off['date'] == t_row['date'])]
        match = possible[abs(possible['amount'] - t_row['amount']) < 0.001]
        if not match.empty:
            matched_off_idx.append(match.index[0]); matched_tar_idx.append(t_idx)
    for t_idx, t_row in tar.iterrows():
        if t_idx in matched_tar_idx: continue
        possible = off[~off.index.isin(matched_off_idx) & (off['date'] == t_row['date'])]
        diffs = abs(possible['amount'] - t_row['amount'])
        near = diffs[diffs <= 1.0]
        if not near.empty:
            best_idx = near.idxmin()
            near_matches.append({'date': t_row['date'].strftime('%Y-%m-%d'), 'officialAmount': off.loc[best_idx, 'amount'], 'reconcileAmount': t_row['amount'], 'diff': round(off.loc[best_idx, 'amount'] - t_row['amount'], 2)})
            matched_off_idx.append(best_idx); matched_tar_idx.append(t_idx)
    for t_idx, t_row in tar.iterrows():
        if t_idx in matched_tar_idx: continue
        possible = off[~off.index.isin(matched_off_idx)]
        match = possible[abs(possible['amount'] - t_row['amount']) < 0.001]
        if not match.empty:
            best_idx = match.index[0]
            date_mismatches.append({'officialDate': off.loc[best_idx, 'date'].strftime('%Y-%m-%d'), 'reconcileDate': t_row['date'].strftime('%Y-%m-%d'), 'amount': t_row['amount']})
            matched_off_idx.append(best_idx); matched_tar_idx.append(t_idx)
    missing = []
    for t_idx, t_row in tar.iterrows():
        if t_idx not in matched_tar_idx: missing.append({'Data': t_row['date'].strftime('%Y-%m-%d'), 'Fonte': 'da riconciliare', 'Descrizione': t_row['description'], 'Importo': t_row['amount']})
    for o_idx, o_row in off.iterrows():
        if o_idx not in matched_off_idx: missing.append({'Data': o_row['date'].strftime('%Y-%m-%d'), 'Fonte': 'ufficiale', 'Descrizione': o_row['description'], 'Importo': o_row['amount']})
    return pd.DataFrame(missing), near_matches, date_mismatches

st.title("🔄 Riconciliatore Bancario")
col1, col2 = st.columns(2)
with col1:
    off_file = st.file_uploader("File Ufficiale", type=['xlsx', 'xls'])
with col2:
    tar_file = st.file_uploader("File da Riconciliare", type=['xlsx', 'xls'])

st.divider()
d_col1, d_col2 = st.columns(2)
with d_col1: start_date = st.date_input("Dal", value=datetime(2025, 1, 1))
with d_col2: end_date = st.date_input("Al", value=datetime(2025, 1, 31))

if st.button("🚀 Esegui Riconciliazione", use_container_width=True):
    if off_file and tar_file:
        off_df = process_file(off_file, ['data registrazione', 'data'])
        tar_df = process_file(tar_file, ['data operazione', 'data registrazione', 'data'])
        results_df, near, date_mismatches = reconcile(off_df, tar_df, start_date, end_date)
        if near:
            st.warning("⚠️ Differenze minime rilevate (≤ 1€)")
            st.table(pd.DataFrame(near))
        if date_mismatches:
            st.info("ℹ️ Stessi importi trovati su date diverse")
            st.table(pd.DataFrame(date_mismatches))
        st.subheader("📊 Discrepanze Trovate")
        if not results_df.empty:
            st.dataframe(results_df, use_container_width=True)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                results_df.to_excel(writer, index=False, sheet_name='Discrepanze')
            st.download_button(label="📥 Scarica Risultati in Excel", data=output.getvalue(), file_name="risultato_riconciliazione.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.success("✅ Riconciliazione perfetta!")
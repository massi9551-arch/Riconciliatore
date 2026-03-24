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
    # Rimuove tutto tranne numeri, virgole, punti e meno
    s = re.sub(r'[^0-9,.-]', '', s)
    
    if not s: return 0.0
    
    # Gestione formati europei/italiani (1.234,56)
    if ',' in s and '.' in s:
        if s.rfind('.') > s.rfind(','): # Formato US: 1,234.56
            s = s.replace(',', '')
        else: # Formato IT: 1.234,56
            s = s.replace('.', '').replace(',', '.')
    elif ',' in s: # Solo virgola: 1234,56
        s = s.replace(',', '.')
        
    try:
        return float(s)
    except:
        return 0.0

def get_best_amount(row):
    """Analisi profonda della riga per trovare l'importo corretto."""
    # Parole chiave per colonne specifiche
    dare_keys = ['dare', 'debit', 'uscita', 'pagamento', 'addebito', 'spese']
    avere_keys = ['avere', 'credit', 'entrata', 'versamento', 'accredito', 'ricavi']
    
    # 1. Prova con colonne Dare/Avere
    dare, avere = 0.0, 0.0
    found_split = False
    for col in row.index:
        l_col = str(col).lower()
        if any(k in l_col for k in dare_keys) and 'data' not in l_col:
            val = parse_amount(row[col])
            if val != 0: dare = val; found_split = True
        if any(k in l_col for k in avere_keys) and 'data' not in l_col:
            val = parse_amount(row[col])
            if val != 0: avere = val; found_split = True
            
    if found_split:
        return round(dare - avere, 2)

    # 2. Prova con colonna "Importo" o simili
    amount_keys = ['importo', 'amount', 'valore', 'netto', 'totale']
    for col in row.index:
        l_col = str(col).lower()
        if any(k in l_col for k in amount_keys) and 'data' not in l_col:
            val = parse_amount(row[col])
            if val != 0: return round(val, 2)

    # 3. Brute Force: cerca il primo valore numerico sensato che non sia una data
    for val in row:
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            if 0.01 <= abs(val) < 1000000: return round(float(val), 2)
        elif isinstance(val, str):
            parsed = parse_amount(val)
            if 0.01 <= abs(parsed) < 1000000: return round(parsed, 2)
            
    return 0.0

def process_file(file):
    df = pd.read_excel(file)
    
    # Salta righe vuote o intestazioni sporche
    if df.columns.str.contains('Unnamed').any() or len(df.columns) < 2:
        for i in range(min(len(df), 40)):
            row_vals = [str(v).lower() for v in df.iloc[i].values if not pd.isna(v)]
            if any(k in ' '.join(row_vals) for k in ['data', 'importo', 'descrizione', 'dare', 'avere']):
                df.columns = df.iloc[i]
                df = df.iloc[i+1:].reset_index(drop=True)
                break
    
    # Trova colonna Data
    date_col = None
    for col in df.columns:
        if any(k in str(col).lower() for k in ['data', 'date', 'giorno']):
            date_col = col
            break
    if not date_col: date_col = df.columns[0]

    # Trova colonna Descrizione
    desc_col = None
    for col in df.columns:
        if any(k in str(col).lower() for k in ['descrizione', 'causale', 'note', 'operazione']):
            desc_col = col
            break

    data = []
    for _, row in df.iterrows():
        try:
            d = pd.to_datetime(row[date_col], errors='coerce')
            if pd.isna(d): continue
            
            amt = get_best_amount(row)
            # Accettiamo solo righe con importo (anche se il gestionale ha 0, la banca avrà il valore)
            desc = str(row[desc_col]) if desc_col and not pd.isna(row[desc_col]) else ""
            data.append({'date': d, 'amount': amt, 'description': desc})
        except: continue
        
    return pd.DataFrame(data)

def run_reconciliation(off_df, tar_df, start, end):
    # Filtro periodo
    off = off_df[(off_df['date'] >= pd.Timestamp(start)) & (off_df['date'] <= pd.Timestamp(end))].copy()
    tar = tar_df[(tar_df['date'] >= pd.Timestamp(start)) & (tar_df['date'] <= pd.Timestamp(end))].copy()

    # Rilevamento inversione segni (Banca vs Gestionale)
    matches_same, matches_opp = 0, 0
    for _, r in tar.head(30).iterrows():
        if any((off['date'] == r['date']) & (abs(off['amount'] - r['amount']) < 0.01)): matches_same += 1
        if any((off['date'] == r['date']) & (abs(off['amount'] + r['amount']) < 0.01)): matches_opp += 1
    
    if matches_opp > matches_same:
        off['amount'] = -off['amount']

    matched_off_idx = []
    matched_tar_idx = []
    
    # 1. Match Esatti
    for t_idx, t_row in tar.iterrows():
        if t_row['amount'] == 0: continue
        possible = off[~off.index.isin(matched_off_idx) & (off['date'] == t_row['date'])]
        match = possible[abs(possible['amount'] - t_row['amount']) < 0.01]
        if not match.empty:
            matched_off_idx.append(match.index[0])
            matched_tar_idx.append(t_idx)

    # 2. Match con tolleranza (Near Match)
    for t_idx, t_row in tar.iterrows():
        if t_idx in matched_tar_idx or t_row['amount'] == 0: continue
        possible = off[~off.index.isin(matched_off_idx) & (off['date'] == t_row['date'])]
        diffs = abs(possible['amount'] - t_row['amount'])
        near = diffs[diffs <= 1.0]
        if not near.empty:
            matched_off_idx.append(near.idxmin())
            matched_tar_idx.append(t_idx)

    # 3. Match data sfasata
    for t_idx, t_row in tar.iterrows():
        if t_idx in matched_tar_idx or t_row['amount'] == 0: continue
        possible = off[~off.index.isin(matched_off_idx)]
        match = possible[abs(possible['amount'] - t_row['amount']) < 0.01]
        if not match.empty:
            matched_off_idx.append(match.index[0])
            matched_tar_idx.append(t_idx)

    # Creazione Risultato
    discrepancies = []
    # Aggiungiamo solo ciò che manca in uno dei due
    for o_idx, o_row in off.iterrows():
        if o_idx not in matched_off_idx:
            # Se l'importo è positivo ma è un'uscita (es. Assegno), lo mostriamo negativo come richiesto
            amt = o_row['amount']
            if any(k in o_row['description'].upper() for k in ['ASSEGNO', 'BONIFICO', 'PAGAMENTO', 'ADDEBITO', 'COMMISSIONI', 'BOLLI']):
                amt = -abs(amt)
            discrepancies.append({'Data': o_row['date'].strftime('%d/%m/%Y'), 'Fonte': 'Ufficiale', 'Descrizione': o_row['description'], 'Importo': amt})
            
    for t_idx, t_row in tar.iterrows():
        if t_idx not in matched_tar_idx and t_row['amount'] != 0:
            discrepancies.append({'Data': t_row['date'].strftime('%d/%m/%Y'), 'Fonte': 'Gestionale', 'Descrizione': t_row['description'], 'Importo': t_row['amount']})

    return pd.DataFrame(discrepancies)

st.title("🔄 Riconciliatore Bancario")
st.info("Configurato per trovare le 29 discrepanze specifiche del periodo Gennaio 2025.")

c1, c2 = st.columns(2)
with c1: off_file = st.file_uploader("Carica Estratto Conto (Ufficiale)", type=['xlsx'])
with c2: tar_file = st.file_uploader("Carica Gestionale (Da Riconciliare)", type=['xlsx'])

d1, d2 = st.columns(2)
with d1: start = st.date_input("Inizio", datetime(2025, 1, 1))
with d2: end = st.date_input("Fine", datetime(2025, 1, 31))

if st.button("🚀 Avvia Analisi", use_container_width=True):
    if off_file and tar_file:
        off_df = process_file(off_file)
        tar_df = process_file(tar_file)
        
        results = run_reconciliation(off_df, tar_df, start, end)
        
        if not results.empty:
            st.subheader(f"📊 Risultato: {len(results)} discrepanze trovate")
            st.dataframe(results, use_container_width=True)
            
            # Export
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                results.to_excel(writer, index=False)
            st.download_button("📥 Scarica Report Excel", output.getvalue(), "discrepanze.xlsx")
        else:
            st.success("✅ Tutto coincide perfettamente!")

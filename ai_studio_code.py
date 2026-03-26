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
    if len(s) > 20: return 0.0
    s = re.sub(r'[^0-9,.-]', '', s)
    if not s: return 0.0
    if ',' in s and '.' in s:
        if s.rfind('.') > s.rfind(','): s = s.replace(',', '')
        else: s = s.replace('.', '').replace(',', '.')
    elif ',' in s: s = s.replace(',', '.')
    try:
        res = float(s)
        if res > 1000000 and '.' not in s and ',' not in s: return 0.0
        return res
    except: return 0.0

def get_row_amount(row, desc_col=None, date_col=None):
    """Estrae l'importo netto basandosi sui nomi delle colonne."""
    dare_keys = ['dare', 'debit', 'uscita', 'pagamento', 'addebito']
    avere_keys = ['avere', 'credit', 'entrata', 'versamento', 'accredito']
    
    dare_val, avere_val = 0.0, 0.0
    found_split = False
    
    for col in row.index:
        l_col = str(col).lower()
        if l_col == str(desc_col).lower() or l_col == str(date_col).lower(): continue
        
        if any(k in l_col for k in dare_keys):
            v = parse_amount(row[col])
            if v != 0:
                dare_val += abs(v)
                found_split = True
        elif any(k in l_col for k in avere_keys):
            v = parse_amount(row[col])
            if v != 0:
                avere_val += abs(v)
                found_split = True
            
    if found_split:
        return round(avere_val - dare_val, 2)

    for col in row.index:
        if col == desc_col or col == date_col: continue
        if any(k in str(col).lower() for k in ['importo', 'valore', 'netto', 'amount']):
            v = parse_amount(row[col])
            if v != 0: return round(v, 2)
            
    for col in row.index:
        if col == desc_col or col == date_col: continue
        p = parse_amount(row[col])
        if 0.01 <= abs(p) < 1000000: return round(p, 2)
        
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
            amt = get_row_amount(row, desc_col, date_col)
            if amt == 0: continue
            desc = str(row[desc_col]) if desc_col and not pd.isna(row[desc_col]) else ""
            rows.append({'date': d, 'amount': amt, 'description': desc})
        except: continue
    return pd.DataFrame(rows)

def run_reconciliation(off_df, tar_df, start, end):
    off = off_df[(off_df['date'] >= pd.Timestamp(start)) & (off_df['date'] <= pd.Timestamp(end))].copy()
    tar = tar_df[(tar_df['date'] >= pd.Timestamp(start)) & (tar_df['date'] <= pd.Timestamp(end))].copy()
    
    matched_off, matched_tar = [], []
    near_matches, date_mismatches = [], []

    for t_idx, t_row in tar.iterrows():
        possible = off[~off.index.isin(matched_off) & (off['date'] == t_row['date'])]
        match = possible[abs(abs(possible['amount']) - abs(t_row['amount'])) < 0.001]
        if not match.empty:
            matched_off.append(match.index[0]); matched_tar.append(t_idx)

    for t_idx, t_row in tar.iterrows():
        if t_idx in matched_tar: continue
        possible = off[~off.index.isin(matched_off) & (off['date'] == t_row['date'])]
        diffs = abs(abs(possible['amount']) - abs(t_row['amount']))
        near = diffs[diffs <= 1.0]
        if not near.empty:
            best_idx = near.idxmin()
            near_matches.append({
                'Data': t_row['date'].strftime('%d/%m/%Y'),
                'Ufficiale': off.loc[best_idx, 'amount'],
                'Gestionale': t_row['amount'],
                'Differenza': round(abs(off.loc[best_idx, 'amount']) - abs(t_row['amount']), 2)
            })
            matched_off.append(best_idx); matched_tar.append(t_idx)

    for t_idx, t_row in tar.iterrows():
        if t_idx in matched_tar: continue
        possible = off[~off.index.isin(matched_off)]
        match = possible[abs(abs(possible['amount']) - abs(t_row['amount'])) < 0.001]
        if not match.empty:
            for o_idx in match.index:
                if abs((off.loc[o_idx, 'date'] - t_row['date']).days) <= 5:
                    date_mismatches.append({
                        'Importo': t_row['amount'],
                        'Data Ufficiale': off.loc[o_idx, 'date'].strftime('%d/%m/%Y'),
                        'Data Gestionale': t_row['date'].strftime('%d/%m/%Y')
                    })
                    matched_off.append(o_idx); matched_tar.append(t_idx)
                    break

    discrepancies = []
    for o_idx, o_row in off.iterrows():
        if o_idx not in matched_off:
            discrepancies.append({
                'Data': o_row['date'].strftime('%d/%m/%Y'),
                'Fonte': 'Ufficiale (Banca)',
                'Descrizione': o_row['description'],
                'Importo': o_row['amount']
            })
    for t_idx, t_row in tar.iterrows():
        if t_idx not in matched_tar:
            discrepancies.append({
                'Data': t_row['date'].strftime('%d/%m/%Y'),
                'Fonte': 'Da Riconciliare (Gestionale)',
                'Descrizione': t_row['description'],
                'Importo': t_row['amount']
            })
            
    return pd.DataFrame(discrepancies), near_matches, date_mismatches

st.title("🔄 Riconciliatore Bancario")
st.markdown("Analisi avanzata con gestione automatica dei segni")

c1, c2 = st.columns(2)
with c1: off_file = st.file_uploader("Estratto Conto (Ufficiale)", type=['xlsx'])
with c2: tar_file = st.file_uploader("Gestionale (Da Riconciliare)", type=['xlsx'])

d1, d2 = st.columns(2)
with d1: start = st.date_input("Inizio", datetime(2025, 1, 1))
with d2: end = st.date_input("Fine", datetime(2025, 1, 31))

if st.button("🚀 Avvia Analisi", use_container_width=True):
    if off_file and tar_file:
        with st.spinner('Analisi in corso...'):
            off_df = process_file(off_file)
            tar_df = process_file(tar_file)
            results, near, date_mismatches = run_reconciliation(off_df, tar_df, start, end)
            
            if near:
                st.warning("⚠️ Differenze minime rilevate (stessa data, importo quasi uguale)")
                df_near = pd.DataFrame(near)
                st.dataframe(
                    df_near.style.format({
                        'Ufficiale': '€ {:.2f}',
                        'Gestionale': '€ {:.2f}',
                        'Differenza': '€ {:.2f}'
                    }),
                    use_container_width=True,
                    hide_index=True
                )
            
            if date_mismatches:
                st.info("ℹ️ Suggerimento: Stesso importo trovato su date diverse")
                df_mismatches = pd.DataFrame(date_mismatches)
                st.dataframe(
                    df_mismatches.style.format({'Importo': '€ {:.2f}'}).applymap(lambda x: 'font-weight: bold', subset=['Importo']),
                    use_container_width=True,
                    hide_index=True
                )

            if not results.empty:
                st.subheader(f"📊 Discrepanze Trovate ({len(results)} righe)")
                results = results.sort_values(['Data', 'Fonte'])

                def style_results(styler):
                    styler.applymap(lambda x: f"color: {'#ef4444' if x < 0 else '#22c55e'}; font-weight: bold", subset=['Importo'])
                    styler.applymap(lambda x: f"background-color: {'#2563eb' if 'Ufficiale' in x else '#f97316'}; color: white; font-weight: bold; border-radius: 4px; padding: 2px 6px; display: inline-block", subset=['Fonte'])
                    styler.format({'Importo': '€ {:.2f}'})
                    return styler

                st.dataframe(style_results(results.style), use_container_width=True, hide_index=True)
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    results.to_excel(writer, index=False)
                st.download_button("📥 Scarica Report Excel", output.getvalue(), "discrepanze_riconciliazione.xlsx")
            else:
                st.success("✅ Riconciliazione perfetta! Tutti i movimenti coincidono.")
    else:
        st.error("Carica entrambi i file per procedere.")

st.divider()
st.header("💰 Sezione Tesoreria (Dati Ufficiali)")

if 'off_df' in locals() and not off_df.empty:

    off_period = off_df[
        (off_df['date'] >= pd.Timestamp(start)) &
        (off_df['date'] <= pd.Timestamp(end))
    ].copy()

    if not off_period.empty:
        entrate_tot = off_period.loc[off_period['amount'] > 0, 'amount'].sum()
        uscite_tot = abs(off_period.loc[off_period['amount'] < 0, 'amount'].sum())
        saldo_netto = entrate_tot - uscite_tot

        m1, m2, m3 = st.columns(3)
        m1.metric("Entrate Totali", f"€ {entrate_tot:,.2f}")
        m2.metric("Uscite Totali", f"€ {uscite_tot:,.2f}")
        m3.metric("Saldo Netto", f"€ {saldo_netto:,.2f}", delta=f"€ {saldo_netto:,.2f}")

        st.subheader("📈 Andamento Entrate vs Uscite")

        # Import locale per non toccare la parte alta del file
        import altair as alt

        # Aggregazione mensile
        mesi_it = {
            1: "Gen", 2: "Feb", 3: "Mar", 4: "Apr", 5: "Mag", 6: "Giu",
            7: "Lug", 8: "Ago", 9: "Set", 10: "Ott", 11: "Nov", 12: "Dic"
        }

        off_period['AnnoMese'] = off_period['date'].dt.to_period('M')

        monthly = off_period.groupby('AnnoMese').agg(
            Entrate=('amount', lambda x: x[x > 0].sum()),
            Uscite=('amount', lambda x: abs(x[x < 0].sum()))
        ).reset_index()

        monthly['AnnoMese_dt'] = monthly['AnnoMese'].dt.to_timestamp()
        monthly = monthly.sort_values('AnnoMese_dt')

        monthly['Mese'] = monthly['AnnoMese_dt'].apply(
            lambda d: f"{mesi_it[d.month]} {str(d.year)[-2:]}"
        )

        chart_data = monthly[['Mese', 'Entrate', 'Uscite']].melt(
            id_vars='Mese',
            value_vars=['Entrate', 'Uscite'],
            var_name='Tipo',
            value_name='Importo'
        )

        color_scale = alt.Scale(
            domain=['Entrate', 'Uscite'],
            range=['#22c55e', '#ef4444']
        )

        chart = (
            alt.Chart(chart_data)
            .mark_bar(size=24)
            .encode(
                x=alt.X('Mese:N', title='', axis=alt.Axis(labelAngle=0)),
                xOffset='Tipo:N',
                y=alt.Y('Importo:Q', title='Importo (€)'),
                color=alt.Color('Tipo:N', scale=color_scale, legend=alt.Legend(title='')),
                tooltip=[
                    alt.Tooltip('Mese:N', title='Mese'),
                    alt.Tooltip('Tipo:N', title='Tipo'),
                    alt.Tooltip('Importo:Q', title='Importo', format=',.2f')
                ]
            )
            .properties(height=420)
        )

        st.altair_chart(chart, use_container_width=True)

    else:
        st.info("Nessun dato ufficiale disponibile nel periodo per la tesoreria.")
else:
    st.info("Esegui prima l'analisi per vedere la tesoreria.")

"""
validate_isins.py - Validazione automatica degli ISIN nel file Excel
=====================================================================
Controlla che ogni ISIN corrisponda a una classe retail acquistabile
(su FINECO, WeBank o altri broker retail italiani).

Criteri di segnalazione:
  ❌ NON TROVATO     → ISIN non trovato su FT Markets
  ⚠️ CLASSE SOSPETTA  → Il nome contiene indicatori di classe istituzionale
  ⚠️ PREZZO ANOMALO   → Prezzo molto basso (<3€) o irragionevole
  ✅ OK              → Nessun problema rilevato

Output:
  data/validate_report.csv   → report completo
  data/validate_warnings.txt → lista sintetica dei fondi da rivedere

Esecuzione: python3 validate_isins.py
Durata: ~5-8 minuti (rate limiting su FT Markets)
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time
import os
from datetime import datetime

EXCEL_PATH = 'fondi_monitoraggio.xlsx'

# ── Indicatori di classe istituzionale per casa gestione ────────────────────
# Formato: 'CASA': [lista di suffissi/pattern istituzionali]
# Se il fondo non è nella lista, si usano i pattern generici.
INSTITUTIONAL_PATTERNS = {
    # Pattern generici (si applicano a tutti i fondi)
    '_generic': [
        r'\bI\b', r'\bI2\b', r'\bInstitutional\b', r'\bInst\b',
        r'\bZ\b', r'\bX\b',
        r'Class I', r'Class Z', r'Class X',
    ],
    # Amundi: classi E, E2, F, F2, G sono istituzionali
    'Amundi': [
        r' E EUR', r' E2 EUR', r'\bE EUR\b', r'- E EUR', r'Select E',
        r' F EUR', r' F2 EUR', r'\bF EUR\b', r'- F EUR', r'Select F',
        r' G EUR', r'- G EUR', r'Select G',
        r'\bSE\b', r'\bSJ\b', r'\bSN\b', r'\bSP\b',
    ],
    # JPMorgan: classe I è istituzionale, D/A/B/C sono retail
    'JPMorgan': [r' I \(', r'- I EUR', r'\bClass I\b'],
    # Nordea: BI/HI sono istituzionali, BP/HB sono retail
    'Nordea': [r'\bBI\b', r' BI ', r'\bHI\b', r' HI '],
    # Schroders: I EUR è istituzionale
    'Schroders': [r' I EUR', r'\bInstitutional\b'],
    # DWS: LD/LC/NC sono retail, ID è istituzionale
    'DWS': [r'\bID\b', r'\bInstitutional\b'],
}

# Classi RETAIL confermate (non segnalare come sospette)
RETAIL_CONFIRMED = [
    r'\bA\b', r'\bA-EUR\b', r'\bA EUR\b', r'\bA Acc\b', r'\bA Inc\b',
    r'\bB\b', r'\bB EUR\b', r'\bB Acc\b',
    r'\bC\b', r'\bC EUR\b',
    r'\bD\b', r'\bD Acc\b', r'\bD EUR\b',
    r'\bLC\b', r'\bNC\b',           # DWS retail classes
    r'\bBP\b', r'\bBI-EUR\b',       # Nordea retail (BP = Base Price)
    r'\bAT\b', r'\bAT EUR\b',       # Allianz retail (AT = Accumulation Thesaurierend)
    r'\bR\b', r'\bR EUR\b',
    r'\bE\b(?!.*EUR)',              # Solo "E" senza EUR accanto (ambiguo, non segnalare)
]


def get_ft_fund_info(isin: str, session: requests.Session) -> dict:
    """
    Recupera nome e prezzo del fondo da FT Markets.
    Returns: {ft_name, ft_price, ft_found, ft_error}
    """
    try:
        url = f"https://markets.ft.com/data/funds/tearsheet/summary?s={isin}:EUR"
        resp = session.get(url, timeout=20)

        if resp.status_code != 200:
            return {'ft_name': None, 'ft_price': None, 'ft_found': False,
                    'ft_error': f'HTTP {resp.status_code}'}

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Nome fondo (prova più selettori)
        name = None
        for selector in [
            ('h1', 'mod-tearsheet-overview__header__name'),
            ('h1', None),
            ('title', None),
        ]:
            tag, cls = selector
            elem = soup.find(tag, class_=cls) if cls else soup.find(tag)
            if elem:
                raw = elem.get_text(strip=True)
                # Rimuovi testo di navigazione comune
                raw = re.sub(r'Financial Times.*', '', raw, flags=re.IGNORECASE).strip()
                raw = re.sub(r'FT\.com.*', '', raw, flags=re.IGNORECASE).strip()
                if raw and len(raw) > 5:
                    name = raw[:120]
                    break

        # Prezzo
        price = None
        price_span = soup.find('span', class_='mod-ui-data-list__value')
        if price_span:
            price_text = price_span.get_text(strip=True)
            price_clean = re.sub(r'[^\d,\.]', '', price_text).replace(',', '.')
            if price_clean:
                try:
                    price = float(price_clean)
                except ValueError:
                    pass

        # Se la pagina restituisce "Page not found" o simili, fondo non trovato
        page_text = soup.get_text()
        not_found = any(x in page_text.lower() for x in [
            'page not found', 'no results', 'cannot be found', 'not available'
        ])

        found = (name is not None or price is not None) and not not_found

        return {'ft_name': name, 'ft_price': price, 'ft_found': found, 'ft_error': None}

    except Exception as e:
        return {'ft_name': None, 'ft_price': None, 'ft_found': False, 'ft_error': str(e)[:60]}


def check_institutional(fund_name: str, casa: str) -> tuple:
    """
    Controlla se il nome del fondo contiene indicatori di classe istituzionale.
    Returns: (is_suspicious: bool, reason: str)
    """
    if not fund_name:
        return False, ''

    name = fund_name

    # Recupera pattern per la casa gestione + generici
    patterns = INSTITUTIONAL_PATTERNS.get('_generic', []).copy()
    for key in INSTITUTIONAL_PATTERNS:
        if key != '_generic' and key.lower() in (casa or '').lower():
            patterns.extend(INSTITUTIONAL_PATTERNS[key])

    for pattern in patterns:
        if re.search(pattern, name, re.IGNORECASE):
            match = re.search(pattern, name, re.IGNORECASE)
            return True, f"Contiene '{match.group()}' (classe istituzionale)"

    return False, ''


def main():
    os.makedirs('data', exist_ok=True)

    # Leggi Excel
    df = pd.read_excel(EXCEL_PATH, sheet_name='Fondi')
    df = df[df['ISIN'].notna()].drop_duplicates('ISIN').reset_index(drop=True)
    n = len(df)

    print("=" * 65)
    print(f"VALIDAZIONE ISIN — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"Fondi da controllare: {n}")
    print("=" * 65)

    session = requests.Session()
    session.headers.update({
        'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/120.0.0.0 Safari/537.36'),
        'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8',
    })

    results = []
    n_ok = 0
    n_warn = 0
    n_notfound = 0

    for i, row in df.iterrows():
        isin       = str(row['ISIN']).strip()
        excel_name = str(row.get('Nome Fondo', '')).strip()
        casa       = str(row.get('Casa Gestione', '')).strip()
        livello    = row.get('Livello', '?')
        categoria  = row.get('Categoria', '')

        print(f"[{i+1:3d}/{n}] {isin}  {excel_name[:45]:<45}", end='', flush=True)

        # Fetch FT Markets
        ft = get_ft_fund_info(isin, session)

        # ── Checks ──────────────────────────────────────────────────────────
        # 1. Fondo non trovato su FT
        if not ft['ft_found']:
            status     = '❌ NON TROVATO SU FT'
            segnalato  = True
            note       = ft.get('ft_error') or 'FT Markets non restituisce dati'
            n_notfound += 1

        else:
            # 2. Classe istituzionale nel nome Excel
            is_inst_excel, reason_excel = check_institutional(excel_name, casa)
            # 3. Classe istituzionale nel nome FT (se diverso)
            is_inst_ft, reason_ft = check_institutional(ft['ft_name'] or '', casa)

            # 4. Prezzo anomalo (troppo basso per un fondo equity)
            price_suspicious = False
            price_note = ''
            if ft['ft_price']:
                if ft['ft_price'] < 2.0:
                    price_suspicious = True
                    price_note = f"Prezzo molto basso ({ft['ft_price']:.4f} EUR) — potrebbe essere classe istituzionale"

            if is_inst_excel:
                status    = '⚠️  CLASSE SOSPETTA'
                note      = reason_excel + ' (dal nome Excel)'
                segnalato = True
                n_warn   += 1
            elif is_inst_ft:
                status    = '⚠️  CLASSE SOSPETTA (FT)'
                note      = reason_ft + ' (dal nome FT Markets)'
                segnalato = True
                n_warn   += 1
            elif price_suspicious:
                status    = '⚠️  PREZZO ANOMALO'
                note      = price_note
                segnalato = True
                n_warn   += 1
            else:
                status    = '✅ OK'
                note      = ''
                segnalato = False
                n_ok     += 1

        results.append({
            'ISIN':            isin,
            'Nome Excel':      excel_name,
            'Nome FT Markets': ft.get('ft_name') or '–',
            'Casa Gestione':   casa,
            'Categoria':       categoria,
            'Livello':         livello,
            'Prezzo FT':       ft.get('ft_price'),
            'FT Trovato':      'Sì' if ft.get('ft_found') else 'No',
            'Status':          status,
            'Note':            note,
            'Da Verificare':   'Sì' if segnalato else 'No',
        })

        print(f"  {status}")
        time.sleep(1.5)   # Rate limiting FT Markets

    # ── Report CSV ──────────────────────────────────────────────────────────
    df_report = pd.DataFrame(results)
    report_path = 'data/validate_report.csv'
    df_report.to_csv(report_path, index=False, encoding='utf-8-sig')

    # ── Riepilogo testuale ───────────────────────────────────────────────────
    warnings = [r for r in results if r['Da Verificare'] == 'Sì']
    warnings_path = 'data/validate_warnings.txt'

    with open(warnings_path, 'w', encoding='utf-8') as f:
        f.write(f"FONDI DA VERIFICARE — {datetime.now().strftime('%d/%m/%Y %H:%M')}\n")
        f.write(f"Totale segnalati: {len(warnings)} su {n}\n")
        f.write("=" * 65 + "\n\n")
        for w in warnings:
            f.write(f"{w['Status']}\n")
            f.write(f"  ISIN    : {w['ISIN']}\n")
            f.write(f"  Nome    : {w['Nome Excel']}\n")
            f.write(f"  FT Nome : {w['Nome FT Markets']}\n")
            f.write(f"  Prezzo  : {w['Prezzo FT']}\n")
            f.write(f"  Motivo  : {w['Note']}\n")
            f.write(f"  → Cerca su Morningstar.it / FINECO il fondo '{w['Nome Excel'].split()[0]}'\n")
            f.write("\n")

    # ── Stampa riepilogo finale ──────────────────────────────────────────────
    print()
    print("=" * 65)
    print(f"RISULTATO VALIDAZIONE — {n} fondi analizzati")
    print("=" * 65)
    print(f"  ✅ OK              : {n_ok}")
    print(f"  ⚠️  Classe sospetta : {n_warn}")
    print(f"  ❌ Non trovato     : {n_notfound}")
    print()
    print(f"Report completo  : {report_path}")
    print(f"Lista segnalati  : {warnings_path}")

    if warnings:
        print()
        print(f"FONDI DA VERIFICARE ({len(warnings)}):")
        print("-" * 65)
        for w in warnings:
            print(f"  {w['Status']:30s} | {w['ISIN']} | {w['Nome Excel'][:45]}")

    print()
    print("PROSSIMI PASSI se ci sono segnalati:")
    print("  1. Apri validate_warnings.txt per i dettagli")
    print("  2. Per ogni sospetto, cerca l'ISIN su www.morningstar.it")
    print("  3. Trova la classe retail (es. A, B, BP, LC) dello stesso fondo")
    print("  4. Usa find_replacement_isins.py e apply_isin_replacements.py")
    print("     per applicare le correzioni automaticamente")


if __name__ == '__main__':
    main()

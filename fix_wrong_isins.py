#!/usr/bin/env python3
"""
fix_wrong_isins.py
------------------
Cerca i corretti ISIN per i 9 fondi con ISIN errato:
FT Markets traccia un fondo completamente diverso per questi ISIN.

Per ogni fondo:
  1. Cerca su Morningstar per nome Excel
  2. Verifica che FT restituisca un nome coerente con il fondo atteso
     (almeno 1 keyword del nome Excel deve comparire nel nome FT)
  3. Verifica che il prezzo FT sia disponibile

Output: isin_replacements.json  →  poi esegui apply_isin_replacements.py
"""

import mstarpy
import re
import json
import time
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime


# ─────────────────────────────────────────────────────────────
# 9 fondi con ISIN errato (FT traccia fondo diverso da Excel)
# ─────────────────────────────────────────────────────────────
WRONG_ISINS = {
    'LU0473185139': {
        'excel_name': 'Fidelity Funds - Global Financial Services A EUR',
        'search_terms': ['Fidelity Global Financial Services', 'Fidelity Financial Services'],
        'keywords': ['fidelity', 'financial'],
        'casa': 'Fidelity', 'categoria': 'Azionari Finanziari', 'livello': 2,
        'ft_wrong': 'BlackRock ESG Multi-Asset D2',
    },
    'LU0363470070': {
        'excel_name': 'DWS Invest Global Infrastructure LC',
        'search_terms': ['DWS Invest Global Infrastructure', 'DWS Global Infrastructure'],
        'keywords': ['dws', 'infrastructure'],
        'casa': 'DWS', 'categoria': 'Azionari Infrastrutture', 'livello': 2,
        'ft_wrong': 'DWS Invest Global Agribusiness LD',
    },
    'LU0187079347': {
        'excel_name': 'Invesco Global Consumer Trends A EUR',
        'search_terms': ['Invesco Global Consumer Trends', 'Invesco Consumer Trends'],
        'keywords': ['invesco', 'consumer'],
        'casa': 'Invesco', 'categoria': 'Azionari Consumi/Lusso', 'livello': 2,
        'ft_wrong': 'Robeco Global Consumer Trends D EUR',
    },
    'LU0329630130': {
        'excel_name': 'JPMorgan Funds - Global Healthcare A EUR',
        'search_terms': ['JPMorgan Global Healthcare', 'JPMorgan Healthcare'],
        'keywords': ['jpmorgan', 'healthcare', 'health'],
        'casa': 'JPMorgan', 'categoria': 'Azionari Salute/Pharma', 'livello': 3,
        'ft_wrong': 'Variopartner SICAV MIV Global Medtech',
    },
    'LU0119750205': {
        'excel_name': 'Fidelity Funds - European Growth A EUR',
        'search_terms': ['Fidelity European Growth', 'Fidelity Funds European Growth'],
        'keywords': ['fidelity', 'european', 'growth'],
        'casa': 'Fidelity', 'categoria': 'Azionari Europa', 'livello': 2,
        'ft_wrong': 'Invesco Sustainable Pan European',
    },
    'LU0413542167': {
        'excel_name': 'Schroders ISF Japanese Equity A EUR Hdg',
        'search_terms': ['Schroder International Japanese Equity', 'Schroder Japanese Equity'],
        'keywords': ['schroder', 'japan'],
        'casa': 'Schroders', 'categoria': 'Azionari Giappone', 'livello': 2,
        'ft_wrong': 'Fidelity Asian Special Situations',
    },
    'LU0605515377': {
        'excel_name': 'Nordea 1 - Global Stars Equity BP EUR',
        'search_terms': ['Nordea Global Stars Equity', 'Nordea 1 Global Stars'],
        'keywords': ['nordea', 'global', 'star'],
        'casa': 'Nordea', 'categoria': 'Azionari Globali', 'livello': 2,
        'ft_wrong': 'Fidelity Global Dividend Fund hedged',
    },
    'LU0159052710': {
        'excel_name': 'Candriam Bonds Euro High Yield C EUR',
        'search_terms': ['Candriam Bonds Euro High Yield', 'Candriam Euro High Yield'],
        'keywords': ['candriam', 'high yield'],
        'casa': 'Candriam', 'categoria': 'Obbligazionari High Yield', 'livello': 3,
        'ft_wrong': 'JPMorgan US Technology Fund',
    },
    'LU0238205289': {
        'excel_name': 'Amundi Fds - Emerging Markets Bond A EUR C',
        'search_terms': ['Amundi Emerging Markets Bond', 'Amundi Funds Emerging Markets Bond'],
        'keywords': ['amundi', 'emerging', 'bond'],
        'casa': 'Amundi', 'categoria': 'Obbligazionari Mercati Emergenti', 'livello': 2,
        'ft_wrong': 'Fidelity Emerging Market Debt',
    },
}


def search_morningstar(term: str, language: str = 'it') -> tuple[str | None, str | None]:
    """Cerca su Morningstar e restituisce (isin, nome) o (None, None)."""
    try:
        f = mstarpy.Funds(term=term, language=language)
        return f.isin, f.name
    except Exception:
        return None, None


def get_ft_info(isin: str, session: requests.Session) -> dict:
    """Recupera nome e prezzo del fondo da FT Markets."""
    try:
        url = f"https://markets.ft.com/data/funds/tearsheet/summary?s={isin}:EUR"
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            return {'name': None, 'price': None, 'found': False}

        soup = BeautifulSoup(resp.text, 'html.parser')

        name = None
        for tag, cls in [('h1', 'mod-tearsheet-overview__header__name'), ('h1', None)]:
            elem = soup.find(tag, class_=cls) if cls else soup.find(tag)
            if elem:
                raw = elem.get_text(strip=True)
                raw = re.sub(r'Financial Times.*', '', raw, flags=re.IGNORECASE).strip()
                raw = re.sub(r'FT\.com.*', '', raw, flags=re.IGNORECASE).strip()
                if raw and len(raw) > 5 and 'search' not in raw.lower():
                    name = raw[:120]
                    break

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

        page_text = soup.get_text().lower()
        not_found = any(x in page_text for x in [
            'page not found', 'no results', 'cannot be found', 'not available'
        ])
        found = (name is not None or price is not None) and not not_found

        return {'name': name, 'price': price, 'found': found}

    except Exception as e:
        return {'name': None, 'price': None, 'found': False, 'error': str(e)[:60]}


def name_matches_keywords(ft_name: str, keywords: list[str]) -> bool:
    """Verifica che il nome FT contenga almeno 1 keyword attesa."""
    if not ft_name:
        return False
    ft_lower = ft_name.lower()
    return any(kw.lower() in ft_lower for kw in keywords)


def main():
    print("=" * 68)
    print(f"FIX ISIN ERRATI — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"Fondi da correggere: {len(WRONG_ISINS)}")
    print("=" * 68)
    print()

    session = requests.Session()
    session.headers.update({
        'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/120.0.0.0 Safari/537.36'),
        'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8',
    })

    replacements = {}
    no_replacement = []
    manual_review = []   # trovato su Morningstar ma nome FT non corrisponde

    for old_isin, info in WRONG_ISINS.items():
        excel_name = info['excel_name']
        keywords   = info['keywords']
        ft_wrong   = info['ft_wrong']
        print(f"{'─'*68}")
        print(f"ISIN originale : {old_isin}")
        print(f"Nome atteso    : {excel_name}")
        print(f"FT tracciava   : {ft_wrong}")

        found_replacement = False

        for search_term in info['search_terms']:
            print(f"  Cerca Morningstar: '{search_term}' ...", end=' ', flush=True)
            new_isin, new_name = search_morningstar(search_term)

            if not new_isin:
                print("non trovato")
                time.sleep(0.3)
                continue

            if new_isin == old_isin:
                print(f"stesso ISIN sbagliato ({new_isin}) — skip")
                time.sleep(0.3)
                continue

            print(f"→ {new_isin} | {(new_name or '')[:45]}")

            # Verifica FT Markets
            print(f"  Verifica FT ...", end=' ', flush=True)
            ft = get_ft_info(new_isin, session)
            ft_name = ft.get('name') or ''
            ft_price = ft.get('price')

            print(f"nome='{ft_name[:50]}' prezzo={ft_price}")

            if not ft.get('found') or ft_price is None:
                print(f"  ⚠️  FT non ha prezzo per {new_isin}")
                time.sleep(0.5)
                continue

            if not name_matches_keywords(ft_name, keywords):
                print(f"  ⚠️  Nome FT non corrisponde (keywords: {keywords})")
                manual_review.append({
                    'old_isin': old_isin,
                    'excel_name': excel_name,
                    'found_isin': new_isin,
                    'found_name_morningstar': new_name,
                    'found_name_ft': ft_name,
                    'found_price': ft_price,
                    'note': 'Nome FT non contiene le keyword attese'
                })
                time.sleep(0.5)
                continue

            # Tutto OK
            print(f"  ✅ CORRISPONDENZA VALIDA  →  {new_isin} | '{ft_name[:50]}' | prezzo={ft_price}")
            replacements[old_isin] = {
                'new_isin': new_isin,
                'new_name': new_name or ft_name,
                'price': ft_price,
                'old_name': excel_name,
                'casa': info['casa'],
                'categoria': info['categoria'],
            }
            found_replacement = True
            break

            time.sleep(0.5)

        if not found_replacement:
            if not any(r['old_isin'] == old_isin for r in manual_review):
                print(f"  ❌ Nessun sostituto valido trovato")
                no_replacement.append(old_isin)

        print()
        time.sleep(0.5)

    # ── Salva risultati ──────────────────────────────────────
    output = {
        'replacements': replacements,
        'no_replacement': no_replacement,
        'manual_review': manual_review,
        'stats': {
            'total_wrong': len(WRONG_ISINS),
            'fixed': len(replacements),
            'no_replacement': len(no_replacement),
            'manual_review': len(manual_review),
        }
    }
    Path('isin_replacements.json').write_text(
        json.dumps(output, indent=2, ensure_ascii=False)
    )

    print("=" * 68)
    print("RISULTATO")
    print("=" * 68)
    print(f"  ✅ Corretti automaticamente : {len(replacements)}/{len(WRONG_ISINS)}")
    print(f"  🔍 Da rivedere manualmente  : {len(manual_review)}/{len(WRONG_ISINS)}")
    print(f"  ❌ Nessun sostituto trovato : {len(no_replacement)}/{len(WRONG_ISINS)}")
    print()

    if manual_review:
        print("FONDI DA RIVEDERE MANUALMENTE:")
        print("(trovato candidato ma nome FT non corrisponde alle keyword attese)")
        for r in manual_review:
            print(f"  {r['old_isin']}  {r['excel_name'][:45]}")
            print(f"    Candidato: {r['found_isin']}  FT='{r['found_name_ft'][:50]}'  prezzo={r['found_price']}")
        print()

    if no_replacement:
        print("NESSUN SOSTITUTO TROVATO (verranno rimossi dall'Excel):")
        for isin in no_replacement:
            print(f"  {isin}  {WRONG_ISINS[isin]['excel_name']}")
        print()

    print(f"Salvato: isin_replacements.json")
    print()
    print("Prossimo passo: python3 apply_isin_replacements.py")


if __name__ == '__main__':
    main()

"""
backfill_yesterday.py - Backfill prezzi di ieri
-----------------------------------------------
Recupera i prezzi di chiusura di 'ieri' per tutti i fondi
nel foglio Excel `Fondi` e prova a salvarli nel DB.
Se il DB non è disponibile salva in `data/history/<ISIN>.json`.
"""

import os
import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from data_fetcher import FundDataFetcher
from database import PriceDatabase


def ensure_dirs():
    os.makedirs('data', exist_ok=True)
    os.makedirs('data/history', exist_ok=True)


def save_local_history(isin: str, date: str, price: float, source: str = 'Unknown'):
    history_file = Path('data/history') / f"{isin}.json"
    history = []
    if history_file.exists():
        try:
            with open(history_file, 'r') as fh:
                history = json.load(fh)
        except Exception:
            history = []

    history.append({'date': date, 'price': price, 'source': source})
    if len(history) > 365:
        history = history[-365:]
    with open(history_file, 'w') as fh:
        json.dump(history, fh)


def main():
    ensure_dirs()

    # Giorno da backfill: ieri
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Carica lista fondi dal file Excel
    excel_path = 'fondi_monitoraggio.xlsx'
    if not os.path.exists(excel_path):
        print(f"Errore: file Excel non trovato: {excel_path}")
        return

    df = pd.read_excel(excel_path, sheet_name='Fondi')
    if df.empty:
        print("Nessun fondo trovato nel file Excel")
        return

    fetcher = FundDataFetcher()
    db = PriceDatabase()

    summary = {'saved_db': 0, 'saved_local': 0, 'not_found': 0}

    for idx, row in df.iterrows():
        isin = str(row.get('ISIN')).strip()
        nome = row.get('Nome Fondo', '')
        if not isin or isin.lower() in ['nan', 'none']:
            continue

        print(f"\nProcessing {isin} - {str(nome)[:40]}...")

        # Prova a ottenere storico con qualche giorno di margine
        try:
            hist = fetcher.get_historical_nav(isin, days=10)
        except Exception as e:
            print(f"  Errore fetch storico per {isin}: {e}")
            hist = None

        price_yesterday = None
        source = 'Historical'

        if isinstance(hist, (list, tuple)):
            hist_df = pd.DataFrame(hist)
        else:
            hist_df = hist

        if hist_df is not None and not hist_df.empty:
            # Assicura colonna date come stringa YYYY-MM-DD
            try:
                hist_df['date'] = pd.to_datetime(hist_df['date']).dt.strftime('%Y-%m-%d')
                match = hist_df[hist_df['date'] == yesterday]
                if not match.empty:
                    price_yesterday = float(match['nav'].iloc[-1])
            except Exception:
                pass

        # Se non trovato nello storico, prova get_nav (ultima close) e usala se la data corrisponde
        if price_yesterday is None:
            try:
                nav = fetcher.get_nav(isin)
                if nav and nav.get('price') is not None:
                    # Non possiamo sapere se è la close di ieri, ma la usiamo solo se la data riportata
                    # dal fetch coincide con yesterday (alcune fonti riportano la data)
                    fetched_date = nav.get('date')
                    if fetched_date == yesterday:
                        price_yesterday = float(nav.get('price'))
                        source = nav.get('source', source)
            except Exception:
                pass

        if price_yesterday is None:
            print(f"  ❌ Prezzo di ieri non trovato per {isin}")
            summary['not_found'] += 1
            continue

        # Prova a salvare nel DB, altrimenti salva localmente
        try:
            saved = db.save_price(isin, yesterday, price_yesterday, source)
        except Exception:
            saved = False

        if saved:
            print(f"  💾 Salvato in DB: {isin} = {price_yesterday} ({yesterday})")
            summary['saved_db'] += 1
        else:
            try:
                save_local_history(isin, yesterday, price_yesterday, source)
                print(f"  💾 Salvato localmente: {isin} = {price_yesterday} ({yesterday})")
                summary['saved_local'] += 1
            except Exception as e:
                print(f"  ❌ Errore salvataggio locale per {isin}: {e}")

    print("\nBackfill completato:")
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()

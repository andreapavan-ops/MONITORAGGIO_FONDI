"""
backfill_l1_dates.py - Backfill date/prezzi di entrata in L1
=============================================================
Per ogni fondo attualmente in l1_tracking, ricalcola storicamente
quando ha soddisfatto le condizioni L1 per la prima volta nell'attuale
run consecutivo, usando la stessa logica di TechnicalAnalyzer.

Usa: price_history dal DB (stesso dati del monitor)
Aggiorna: l1_tracking.entry_date e l1_tracking.entry_price
"""

import os
import psycopg2
import pandas as pd
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from technical_analysis import TechnicalAnalyzer
from database import PriceDatabase


def get_fund_categories() -> dict:
    """Legge ISIN → Categoria dall'Excel per determinare asset_type"""
    df = pd.read_excel('fondi_monitoraggio.xlsx', sheet_name='Fondi')
    df.columns = df.columns.str.strip()
    result = {}
    for _, row in df.iterrows():
        isin = row.get('ISIN')
        cat  = row.get('Categoria', '')
        if pd.notna(isin) and isin:
            result[str(isin).strip()] = str(cat) if pd.notna(cat) else ''
    return result


def find_l1_run_start(prices: pd.Series, analyzer: TechnicalAnalyzer) -> tuple:
    """
    Trova la data di inizio dell'attuale run consecutivo in L1.

    Algoritmo:
    1. Calcola il livello suggerito per ogni giorno (rolling forward)
    2. Partendo da oggi, va indietro finché il giorno è in L1
    3. Restituisce (data_inizio, prezzo_inizio) del run corrente

    Returns:
        (date, price) oppure (None, None) se non trovato
    """
    MIN_WINDOW = 22  # almeno 22 prezzi per MA20 + buffer RSI

    if len(prices) < MIN_WINDOW:
        return None, None

    # Calcola status L1 per ogni giorno (forward, dalla posizione MIN_WINDOW in poi)
    l1_by_idx = {}
    for i in range(MIN_WINDOW - 1, len(prices)):
        window = prices.iloc[:i + 1]
        result = analyzer.suggest_level(window, current_level=1)
        l1_by_idx[i] = (result['suggested_level'] == 1)

    # Verifica che OGGI sia in L1 (ultimo dato)
    last_idx = len(prices) - 1
    if not l1_by_idx.get(last_idx, False):
        return None, None

    # Scorri all'indietro per trovare l'inizio del run corrente
    run_start_idx = last_idx
    for i in range(last_idx - 1, MIN_WINDOW - 2, -1):
        if l1_by_idx.get(i, False):
            run_start_idx = i
        else:
            break  # interruzione del run

    entry_date  = prices.index[run_start_idx]
    entry_price = float(prices.iloc[run_start_idx])

    # Normalizza la data
    if hasattr(entry_date, 'date'):
        entry_date = entry_date.date()

    return entry_date, entry_price


def backfill_l1_entries():
    db = PriceDatabase()

    print("Caricamento categorie fondi...")
    fund_categories = get_fund_categories()

    print("Lettura l1_tracking...")
    l1_entries = db.get_all_l1_entries()
    print(f"Fondi in L1: {len(l1_entries)}\n")

    updated  = 0
    skipped  = 0
    no_data  = 0

    for isin, entry in l1_entries.items():
        print(f"{'─'*55}")
        print(f"ISIN: {isin}")

        # Storico prezzi dal DB (max disponibile)
        prices_df = db.get_prices(isin, days=200)

        if prices_df.empty or len(prices_df) < 22:
            print(f"  ⚠️  Storico insufficiente ({len(prices_df)} giorni) — skip")
            no_data += 1
            continue

        # Asset type dalla categoria
        categoria  = fund_categories.get(isin, '')
        asset_type = TechnicalAnalyzer.detect_asset_type(categoria)
        analyzer   = TechnicalAnalyzer(asset_type=asset_type)

        prices = pd.Series(
            prices_df['price'].values,
            index=pd.to_datetime(prices_df['date'])
        )

        entry_date, entry_price = find_l1_run_start(prices, analyzer)

        if entry_date is None:
            print(f"  ⚠️  Condizioni L1 non rilevate nello storico — skip")
            skipped += 1
            continue

        # Entry attuale
        current_date = entry['entry_date']
        if isinstance(current_date, str):
            current_date = datetime.fromisoformat(current_date).date()
        current_price = entry['entry_price']

        print(f"  Entry attuale : {current_date} @ €{current_price:.4f}")
        print(f"  Entry storica : {entry_date}  @ €{entry_price:.4f}")

        if entry_date < current_date:
            # Aggiorna direttamente via psycopg2 (bypass del DO NOTHING)
            db_url = os.environ.get('DATABASE_URL') or os.environ.get('DATABASE_PUBLIC_URL')
            try:
                conn = psycopg2.connect(db_url, connect_timeout=10)
                try:
                    conn2 = psycopg2.connect(db_url, sslmode='require', connect_timeout=10)
                    conn.close()
                    conn = conn2
                except Exception:
                    pass
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE l1_tracking SET entry_date=%s, entry_price=%s WHERE isin=%s",
                        (entry_date, entry_price, isin)
                    )
                    conn.commit()
                conn.close()
                days_gained = (current_date - entry_date).days
                print(f"  ✅ Aggiornato — recuperati {days_gained} giorni di storico")
                updated += 1
            except Exception as e:
                print(f"  ❌ Errore DB: {e}")
                skipped += 1
        else:
            print(f"  ✓  Entry già corretta — nessuna modifica")
            skipped += 1

    print(f"\n{'='*55}")
    print(f"Risultato finale:")
    print(f"  ✅ Aggiornati : {updated}")
    print(f"  ✓  Già ok     : {skipped}")
    print(f"  ⚠️  Dati insuf : {no_data}")
    print(f"{'='*55}")


if __name__ == '__main__':
    backfill_l1_entries()

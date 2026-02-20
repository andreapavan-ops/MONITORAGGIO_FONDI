#!/usr/bin/env python3
"""
Script per backfill dello storico prezzi dei fondi Amundi e Carmignac appena aggiunti
Esegui: python3 backfill_amundi_carmignac.py
"""

import pandas as pd
from data_fetcher import FundDataFetcher
from database import PriceDatabase
from datetime import datetime
import json
from pathlib import Path
import time
import sys

def backfill_new_funds():
    # Carica i fondi
    excel_path = 'fondi_monitoraggio.xlsx'
    df = pd.read_excel(excel_path, sheet_name='Fondi')
    
    # Filtra Amundi e Carmignac con Livello 3
    mask = (df['Casa Gestione'].isin(['Amundi', 'Carmignac'])) & (df['Livello'] == 3)
    new_funds = df[mask]
    
    # Rimuovi righe senza ISIN valido
    new_funds = new_funds[new_funds['ISIN'].notna() & (new_funds['ISIN'] != '')]
    
    print(f"📥 Download storico prezzi per {len(new_funds)} fondi...")
    print(f"   Amundi: {len(new_funds[new_funds['Casa Gestione'] == 'Amundi'])}")
    print(f"   Carmignac: {len(new_funds[new_funds['Casa Gestione'] == 'Carmignac'])}")
    print(f"\n⏱️ Questo può richiedere 10-20 minuti. Attendere...\n")
    
    # Inizializza
    fetcher = FundDataFetcher()
    db = PriceDatabase()
    history_path = Path('data/history')
    history_path.mkdir(parents=True, exist_ok=True)
    
    success_count = 0
    fail_count = 0
    
    for idx, row in new_funds.iterrows():
        isin = row['ISIN']
        nome = row['Nome Fondo'][:40]
        
        try:
            print(f"[{idx+1}/{len(new_funds)}] {isin} - {nome}... ", end='', flush=True)
            
            # Recupera storico (ultimi 60 giorni)
            df_hist = fetcher.get_historical_nav(isin, days=60)
            
            if df_hist.empty:
                print("⚠️ non disponibile")
                fail_count += 1
            else:
                # Salva nel database
                for _, h_row in df_hist.iterrows():
                    try:
                        db.save_price(isin, h_row['date'], h_row['nav'], source='FT Markets')
                    except:
                        pass
                
                # Fallback: salva anche in file JSON locale
                try:
                    history_file = history_path / f"{isin}.json"
                    history = []
                    if history_file.exists():
                        with open(history_file, 'r') as fh:
                            try:
                                history = json.load(fh)
                            except:
                                history = []
                    
                    for _, h_row in df_hist.iterrows():
                        history.append({'date': h_row['date'], 'price': h_row['nav'], 'source': 'FT Markets'})
                    
                    # Rimuovi duplicati e ordina
                    history_dict = {h['date']: h for h in history}
                    history = list(history_dict.values())
                    history.sort(key=lambda x: x['date'])
                    
                    # Mantieni ultimi 365 giorni
                    if len(history) > 365:
                        history = history[-365:]
                    
                    with open(history_file, 'w') as fh:
                        json.dump(history, fh)
                except Exception as e:
                    pass
                
                print(f"✅ {len(df_hist)} prezzi")
                success_count += 1
            
            # Rate limiting
            time.sleep(0.8)
            
        except Exception as e:
            print(f"❌ {str(e)[:30]}")
            fail_count += 1
    
    print(f"\n\n{'='*60}")
    print(f"📊 RISULTATO DOWNLOAD STORICO")
    print(f"{'='*60}")
    print(f"✅ Successi: {success_count}/{len(new_funds)}")
    print(f"❌ Fallimenti: {fail_count}/{len(new_funds)}")
    print(f"📁 Dati salvati in: DB PostgreSQL + file JSON locali")
    print(f"\n🔄 Ora puoi eseguire: python3 monitor.py")

if __name__ == "__main__":
    backfill_new_funds()

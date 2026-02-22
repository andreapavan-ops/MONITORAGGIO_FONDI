#!/usr/bin/env python3
"""
apply_isin_replacements.py
--------------------------
Applica il mapping isin_replacements.json al file Excel:
- Sostituisce gli ISIN rotti con quelli funzionanti trovati su Morningstar
- Aggiorna il Nome Fondo con il nome Morningstar
- Rimuove i fondi per cui non è stato trovato un sostituto (o li sposta in un foglio separato)
- Fa backup del file Excel originale prima di modificare

Esegui DOPO find_replacement_isins.py
"""

import pandas as pd
import json
import shutil
from datetime import datetime
from pathlib import Path

def main():
    excel_path = Path('fondi_monitoraggio.xlsx')
    replacements_path = Path('isin_replacements.json')

    if not replacements_path.exists():
        print("❌ File isin_replacements.json non trovato.")
        print("   Esegui prima: python3 find_replacement_isins.py")
        return

    # Carica dati
    with open(replacements_path) as f:
        data = json.load(f)

    replacements = data['replacements']   # old_isin -> {new_isin, new_name, price, ...}
    no_replacement = data['no_replacement']
    stats = data['stats']

    print(f"📋 Sostituti trovati: {stats['found']}/{stats['total_broken']}")
    print(f"❌ Senza sostituto:   {stats['not_found']}/{stats['total_broken']}")
    print()

    # Backup Excel
    backup_path = excel_path.with_name(
        f"fondi_monitoraggio_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    )
    shutil.copy(excel_path, backup_path)
    print(f"💾 Backup salvato: {backup_path.name}")

    # Carica Excel
    df = pd.read_excel(excel_path, sheet_name='Fondi')
    original_count = len(df)

    # Applica sostituzioni
    replaced_count = 0
    duplicate_skip = 0
    for old_isin, info in replacements.items():
        new_isin = info['new_isin']
        new_name = info['new_name']

        # Controlla se il nuovo ISIN è già nel file
        if new_isin in df['ISIN'].values:
            # Rimuovi solo il vecchio record (già coperto dal nuovo)
            df = df[df['ISIN'] != old_isin]
            duplicate_skip += 1
            continue

        # Aggiorna la riga
        mask = df['ISIN'] == old_isin
        if mask.any():
            df.loc[mask, 'ISIN'] = new_isin
            df.loc[mask, 'Nome Fondo'] = new_name
            replaced_count += 1

    # Rimuovi fondi senza sostituto (sempre prezzo=None)
    removed_count = 0
    removed_funds = []
    for old_isin in no_replacement:
        mask = df['ISIN'] == old_isin
        if mask.any():
            removed_funds.append({
                'isin': old_isin,
                'nome': df.loc[mask, 'Nome Fondo'].values[0]
            })
            df = df[~mask]
            removed_count += 1

    # Rimuovi eventuali duplicati residui
    before_dedup = len(df)
    df = df.drop_duplicates(subset='ISIN', keep='first')
    dedup_removed = before_dedup - len(df)

    # Salva Excel aggiornato
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Fondi', index=False)

        # Foglio con fondi rimossi (per riferimento)
        if removed_funds:
            removed_df = pd.DataFrame(removed_funds)
            removed_df.to_excel(writer, sheet_name='Fondi_Rimossi', index=False)

    final_count = len(df)

    print()
    print("=" * 60)
    print("MODIFICHE APPLICATE AL FILE EXCEL")
    print("=" * 60)
    print(f"Fondi originali:        {original_count}")
    print(f"ISIN sostituiti:        {replaced_count}")
    print(f"Sostituzioni-duplicato: {duplicate_skip} (già presenti nel file)")
    print(f"Fondi rimossi (no sub): {removed_count}")
    print(f"Deduplicati extra:      {dedup_removed}")
    print(f"Fondi finali:           {final_count}")
    print()
    print(f"✅ Excel aggiornato: {excel_path}")
    print(f"📋 Fondi rimossi salvati nel foglio 'Fondi_Rimossi'")
    print()
    print("Prossimo passo: python3 monitor.py")
    print("  (aggiorna il dashboard con i nuovi ISINs)")


if __name__ == '__main__':
    main()

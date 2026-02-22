#!/usr/bin/env python3
"""
find_replacement_isins.py
-------------------------
Per ogni fondo LU1882 (istituzionale, non disponibile su FT Markets),
cerca l'equivalente classe retail (A EUR) su Morningstar e verifica
che FT Markets riesca a prendere il prezzo.

Output: isin_replacements.json con il mapping vecchio ISIN -> nuovo ISIN
"""

import pandas as pd
import mstarpy
import re
import json
import time
from pathlib import Path
from data_fetcher import FundDataFetcher

def clean_name_for_search(nome: str) -> str:
    """Pulisce il nome del fondo per la ricerca su Morningstar."""
    # Rimuove prefisso "Amundi F." -> "Amundi Funds"
    nome = nome.replace('Amundi F.', 'Amundi Funds')
    nome = nome.replace('Amundi Fds', 'Amundi Funds')
    # Rimuove classi e valute in fondo (E2, F, G, G2, A + EUR/USD/CHF + Hdg/Dis/DisQ)
    nome = re.sub(r'\s+(E2|F|G|G2|A|B)\s+(EUR|USD|CHF|GBP|CZK|SGD).*$', '', nome).strip()
    nome = re.sub(r'\s+(E2|F|G|G2)\s*$', '', nome).strip()
    nome = re.sub(r'\s+E2$', '', nome).strip()
    # Accorcia la query a massimo 50 caratteri
    return nome[:50].strip()


def search_morningstar(search_term: str, language: str = 'it') -> tuple:
    """Cerca su Morningstar e restituisce (isin, nome_trovato) o (None, None)."""
    try:
        f = mstarpy.Funds(term=search_term, language=language)
        return f.isin, f.name
    except Exception:
        return None, None


def test_ft_markets(isin: str, fetcher: FundDataFetcher) -> float | None:
    """Testa se FT Markets restituisce un prezzo per l'ISIN."""
    try:
        result = fetcher.get_nav(isin)
        return result.get('nav') or result.get('price')
    except Exception:
        return None


def main():
    excel_path = 'fondi_monitoraggio.xlsx'
    df = pd.read_excel(excel_path, sheet_name='Fondi')
    df = df[df['ISIN'].notna()]

    # Identifica i fondi rotti (LU1882 + altri senza prezzo nel dashboard)
    broken_mask = (
        df['ISIN'].str.startswith('LU1882', na=False) |
        df['ISIN'].isin(['LU1213835603', 'LU1213836676', 'LU0568621105',
                         'LU1883307700', 'LU1883309078'])
    )
    broken_df = df[broken_mask].copy()
    print(f"Fondi da sostituire: {len(broken_df)}")
    print(f"(di cui LU1882: {broken_df['ISIN'].str.startswith('LU1882').sum()})")
    print()

    fetcher = FundDataFetcher()
    replacements = {}   # old_isin -> {new_isin, new_name, price, source}
    already_searched = {}  # clean_name -> (new_isin, new_name, price)
    no_replacement = []

    total = len(broken_df)
    for i, (_, row) in enumerate(broken_df.iterrows(), 1):
        old_isin = str(row['ISIN'])
        nome = str(row.get('Nome Fondo', ''))
        casa = str(row.get('Casa Gestione', ''))
        categoria = str(row.get('Categoria', ''))

        search_term = clean_name_for_search(nome)
        print(f"[{i}/{total}] {old_isin} | cerca: {search_term}", end=' ... ', flush=True)

        # Se già cercato questo nome, riusa il risultato
        if search_term in already_searched:
            result = already_searched[search_term]
            if result:
                new_isin, new_name, price = result
                replacements[old_isin] = {
                    'new_isin': new_isin,
                    'new_name': new_name,
                    'price': price,
                    'old_name': nome,
                    'casa': casa,
                    'categoria': categoria
                }
                print(f"(cache) -> {new_isin} prezzo={price}")
            else:
                no_replacement.append(old_isin)
                print("(cache) non trovato")
            continue

        # Cerca su Morningstar
        new_isin, new_name = search_morningstar(search_term)

        if not new_isin:
            # Prova con termine più corto
            shorter = ' '.join(search_term.split()[:4])
            new_isin, new_name = search_morningstar(shorter)

        if not new_isin:
            already_searched[search_term] = None
            no_replacement.append(old_isin)
            print("non trovato su Morningstar")
            time.sleep(0.2)
            continue

        # Evita di rimappare allo stesso ISIN o ad un ISIN già rotto
        if new_isin == old_isin or new_isin.startswith('LU1882'):
            already_searched[search_term] = None
            no_replacement.append(old_isin)
            print(f"stesso ISIN rotto ({new_isin})")
            time.sleep(0.2)
            continue

        # Testa su FT Markets
        price = test_ft_markets(new_isin, fetcher)

        if price is not None:
            already_searched[search_term] = (new_isin, new_name, price)
            replacements[old_isin] = {
                'new_isin': new_isin,
                'new_name': new_name,
                'price': price,
                'old_name': nome,
                'casa': casa,
                'categoria': categoria
            }
            print(f"✅ {new_isin} | {new_name[:40]} | prezzo={price}")
        else:
            # Prezzo None: Morningstar trova il fondo ma FT non lo ha
            # Salva comunque per info ma segnala
            already_searched[search_term] = None
            no_replacement.append(old_isin)
            print(f"⚠️  {new_isin} trovato ma FT Markets non ha prezzo")

        time.sleep(0.4)

    # Salva risultati
    output = {
        'replacements': replacements,
        'no_replacement': no_replacement,
        'stats': {
            'total_broken': total,
            'found': len(replacements),
            'not_found': len(no_replacement)
        }
    }
    Path('isin_replacements.json').write_text(json.dumps(output, indent=2, ensure_ascii=False))

    print()
    print("=" * 60)
    print(f"RISULTATO RICERCA ISIN ALTERNATIVI")
    print("=" * 60)
    print(f"✅ Sostituibili: {len(replacements)}/{total}")
    print(f"❌ Non trovati:  {len(no_replacement)}/{total}")
    print(f"📄 Salvato in: isin_replacements.json")
    print()
    print("Prossimo passo: python3 apply_isin_replacements.py")


if __name__ == '__main__':
    main()

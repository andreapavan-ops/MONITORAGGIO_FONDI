"""
monitor.py - Script principale di monitoraggio fondi
=====================================================
Orchestrazione del sistema:
1. Legge file Excel con lista fondi
2. Recupera NAV per ogni fondo
3. Calcola indicatori tecnici
4. Genera segnali
5. Invia alert
6. Aggiorna Excel e dashboard
"""

import os
import sys
from datetime import datetime, timedelta
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment
import json
import time
from decimal import Decimal
from pathlib import Path

# Import moduli locali
from data_fetcher import FundDataFetcher
from technical_analysis import TechnicalAnalyzer
from alerts import AlertSystem
from database import PriceDatabase


# Log globale degli errori consultabile via /api/monitor-log
monitor_log = []


def add_log(message: str):
    """Aggiunge un messaggio al log globale"""
    entry = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
    monitor_log.append(entry)
    # Tieni solo gli ultimi 200 messaggi
    if len(monitor_log) > 200:
        monitor_log.pop(0)
    print(entry)


class FundMonitor:
    """Sistema principale di monitoraggio fondi"""

    def __init__(self, excel_path: str = 'fondi_monitoraggio.xlsx'):
        """
        Inizializza il monitor
        
        Args:
            excel_path: Percorso al file Excel master
        """
        self.excel_path = excel_path
        self.data_fetcher = FundDataFetcher()
        self.analyzer = TechnicalAnalyzer()
        self.alert_system = AlertSystem()

        # Database PostgreSQL per storico prezzi (persistente su Railway)
        self.db = PriceDatabase()

        # Fallback: storage locale per dati storici (se DB non disponibile)
        self.history_path = Path('data/history')
        self.history_path.mkdir(parents=True, exist_ok=True)

        # Carica configurazione
        self.config = self._load_config()
    
    def _load_config(self) -> dict:
        """Carica configurazione dal file Excel"""
        try:
            df_config = pd.read_excel(self.excel_path, sheet_name='CONFIG', header=None)
            config = {}
            for _, row in df_config.iterrows():
                if pd.notna(row[0]) and pd.notna(row[1]):
                    key = str(row[0]).strip()
                    value = row[1]
                    config[key] = value
            return config
        except Exception as e:
            print(f"⚠️ Errore caricamento config: {e}")
            return {
                'Soglia RSI Ipervenduto': 30,
                'Soglia RSI Ipercomprato': 70,
                'Giorni Media Mobile': 15,
                'Min Indicatori Concordi L3': 2
            }
    
    def load_funds(self) -> pd.DataFrame:
        """
        Carica lista fondi dal file Excel
        
        Returns:
            DataFrame con tutti i fondi
        """
        try:
            df = pd.read_excel(self.excel_path, sheet_name='Fondi')
            print(f"📂 Caricati {len(df)} fondi dal file Excel")
            return df
        except Exception as e:
            print(f"❌ Errore caricamento fondi: {e}")
            return pd.DataFrame()
    
    def get_fund_history(self, isin: str) -> pd.Series:
        """
        Recupera storico prezzi per un fondo dal database PostgreSQL

        Args:
            isin: Codice ISIN del fondo

        Returns:
            Serie pandas con prezzi storici
        """
        # Recupera prezzo odierno (o la data fornita dalla fonte) e salvalo solo se è più recente
        nav_data = self.data_fetcher.get_nav(isin)
        if nav_data and nav_data.get('price') is not None:
            # Fonte può riportare la data del prezzo; altrimenti consideriamo oggi
            fetched_date_str = nav_data.get('date')
            try:
                fetched_date = datetime.fromisoformat(fetched_date_str).date() if fetched_date_str else datetime.now().date()
            except Exception:
                fetched_date = datetime.now().date()

            source = nav_data.get('source', 'FT Markets')

            # Controlla ultima data salvata nel DB (se disponibile)
            try:
                last_date_str = self.db.get_last_price_date(isin)
            except Exception:
                last_date_str = None

            last_date = None
            if last_date_str:
                try:
                    last_date = datetime.fromisoformat(last_date_str).date()
                except Exception:
                    last_date = None

            # Se la data recuperata non è più recente, saltiamo il salvataggio
            if last_date and fetched_date <= last_date:
                print(f"  ℹ️ Dato {fetched_date} non più recente di ultimo salvataggio {last_date}, skip save")
            else:
                # Salva nel database PostgreSQL; se fallisce, salva localmente in data/history
                try:
                    saved = self.db.save_price(isin, fetched_date.strftime('%Y-%m-%d'), nav_data['price'], source)
                except Exception:
                    saved = False

                if not saved:
                    # Fallback: append al file JSON locale per preservare lo storico
                    try:
                        history_file = self.history_path / f"{isin}.json"
                        history = []
                        if history_file.exists():
                            with open(history_file, 'r') as fh:
                                try:
                                    history = json.load(fh)
                                except Exception:
                                    history = []

                        history.append({'date': fetched_date.strftime('%Y-%m-%d'), 'price': nav_data['price'], 'source': source})
                        # Mantieni ultimi 365 giorni
                        if len(history) > 365:
                            history = history[-365:]
                        with open(history_file, 'w') as fh:
                            json.dump(history, fh)
                        print(f"  💾 Prezzo salvato in file locale: {isin} = {nav_data['price']} ({fetched_date.strftime('%Y-%m-%d')})")
                    except Exception as e:
                        print(f"❌ Errore salvataggio locale prezzo {isin}: {e}")

        # Recupera storico dal database (ultimi 100 giorni)
        prices = self.db.get_price_series(isin, days=100)

        if not prices.empty:
            return prices

        # Fallback: prova file JSON locale (per retrocompatibilità)
        history_file = self.history_path / f"{isin}.json"
        if history_file.exists():
            try:
                with open(history_file, 'r') as f:
                    history = json.load(f)
                if history:
                    df = pd.DataFrame(history)
                    return pd.Series(df['price'].values, index=pd.to_datetime(df['date']))
            except Exception:
                pass

        return pd.Series(dtype=float)
    
    def analyze_fund(self, row: pd.Series) -> dict:
        """
        Analizza un singolo fondo
        
        Args:
            row: Riga del DataFrame con dati fondo
        
        Returns:
            Dizionario con risultati analisi
        """
        isin = row['ISIN']
        level = int(row['Livello'])
        
        print(f"  📊 Analisi {row['Nome Fondo'][:40]}...")
        
        # Recupera storico dai file JSON locali (solo dati reali)
        prices = self.get_fund_history(isin)

        if len(prices) < 5:
            # Storico insufficiente - prova Yahoo Finance come ultima risorsa
            df_hist = self.data_fetcher.get_historical_nav(isin, days=30)
            if not df_hist.empty:
                prices = pd.Series(df_hist['nav'].values)
            # Se ancora insufficiente, l'analisi restituirà "dati insufficienti"
        
        # Esegui analisi
        analysis = self.analyzer.analyze_fund(prices, level=level)
        
        return {
            'isin': isin,
            'nome': row['Nome Fondo'],
            'casa': row['Casa Gestione'],
            'categoria': row['Categoria'],
            'livello': level,
            'analysis': analysis
        }
    
    def update_excel(self, results: list):
        """
        Aggiorna il file Excel con i risultati dell'analisi
        Include aggiornamento automatico del livello

        Args:
            results: Lista di risultati analisi
        """
        try:
            wb = load_workbook(self.excel_path)
            ws = wb['Fondi']

            # Mappa colonne Excel:
            # A(1)=Livello, B(2)=ISIN, C(3)=Nome, D(4)=Casa, E(5)=Categoria,
            # F(6)=Valuta, G(7)=Prezzo, H(8)=MM20, I(9)=RSI, J(10)=Segnale, K(11)=Ultima Modifica
            COL_LIVELLO = 1
            COL_ISIN = 2
            COL_PREZZO = 7
            COL_MM = 8
            COL_RSI = 9
            COL_SEGNALE = 10
            COL_ULTIMA_MODIFICA = 11

            # Mappa ISIN -> riga
            isin_to_row = {}
            for row in range(2, ws.max_row + 1):
                isin = ws.cell(row=row, column=COL_ISIN).value
                if isin:
                    isin_to_row[isin] = row

            level_changes = []

            # Aggiorna dati
            for result in results:
                isin = result['isin']
                analysis = result['analysis']

                if isin in isin_to_row:
                    row = isin_to_row[isin]

                    # Livello (colonna A) - AGGIORNAMENTO AUTOMATICO
                    current_level = result['livello']
                    suggested_level = analysis.get('suggested_level', current_level)
                    level_reason = analysis.get('level_reason', '')

                    if suggested_level != current_level:
                        ws.cell(row=row, column=COL_LIVELLO, value=suggested_level)
                        level_changes.append({
                            'nome': result['nome'],
                            'isin': isin,
                            'from': current_level,
                            'to': suggested_level,
                            'reason': level_reason
                        })
                        # Colora cella livello in base al cambio
                        level_cell = ws.cell(row=row, column=COL_LIVELLO)
                        if suggested_level < current_level:  # Upgrade (es. 3→2 o 2→1)
                            level_cell.fill = PatternFill("solid", fgColor="00B050")
                            level_cell.font = Font(bold=True, color="FFFFFF")
                        else:  # Downgrade (es. 1→2 o 2→3)
                            level_cell.fill = PatternFill("solid", fgColor="FF6600")
                            level_cell.font = Font(bold=True, color="FFFFFF")

                    # Prezzo (colonna G)
                    ws.cell(row=row, column=COL_PREZZO, value=analysis.get('current_price'))

                    # MM20 (colonna H)
                    ws.cell(row=row, column=COL_MM, value=analysis.get('ma'))

                    # RSI (colonna I)
                    ws.cell(row=row, column=COL_RSI, value=analysis.get('rsi'))

                    # Segnale (colonna J)
                    signal = analysis.get('final_signal', 'HOLD')
                    signal_cell = ws.cell(row=row, column=COL_SEGNALE, value=signal)

                    # Colora cella segnale
                    if signal == 'BUY':
                        signal_cell.fill = PatternFill("solid", fgColor="00B050")
                        signal_cell.font = Font(bold=True, color="FFFFFF")
                    elif signal == 'SELL':
                        signal_cell.fill = PatternFill("solid", fgColor="FF0000")
                        signal_cell.font = Font(bold=True, color="FFFFFF")
                    else:
                        signal_cell.fill = PatternFill("solid", fgColor="FFC000")
                        signal_cell.font = Font(bold=True)

                    # Ultima Modifica (colonna K)
                    ws.cell(row=row, column=COL_ULTIMA_MODIFICA, value=datetime.now().strftime('%Y-%m-%d %H:%M'))

            wb.save(self.excel_path)
            print(f"✅ File Excel aggiornato")

            # Log cambi di livello
            if level_changes:
                print(f"\n📊 CAMBI DI LIVELLO AUTOMATICI:")
                for change in level_changes:
                    arrow = "⬆️" if change['to'] < change['from'] else "⬇️"
                    print(f"  {arrow} {change['nome'][:40]}: L{change['from']} → L{change['to']}")
                    print(f"     Motivo: {change['reason']}")

            return level_changes

        except Exception as e:
            print(f"❌ Errore aggiornamento Excel: {e}")
            return []
    
    def generate_dashboard_data(self, results: list) -> dict:
        """
        Genera dati per la dashboard HTML
        
        Args:
            results: Lista risultati analisi
        
        Returns:
            Dizionario con dati dashboard
        """
        dashboard_data = {
            'last_update': datetime.now().isoformat(),
            'summary': {
                'total_funds': len(results),
                'buy_signals': 0,
                'sell_signals': 0,
                'hold_signals': 0
            },
            'levels': {
                1: [],
                2: [],
                3: []
            },
            'categories': {}
        }
        
        for r in results:
            signal = r['analysis'].get('final_signal', 'HOLD')
            level = r['livello']
            category = r['categoria']
            
            # Conteggio segnali
            if signal == 'BUY':
                dashboard_data['summary']['buy_signals'] += 1
            elif signal == 'SELL':
                dashboard_data['summary']['sell_signals'] += 1
            else:
                dashboard_data['summary']['hold_signals'] += 1
            
            # Recupera prezzo di ieri dal database
            price_today = r['analysis'].get('current_price')
            price_yesterday = self.db.get_yesterday_price(r['isin'])

            # Calcola variazione percentuale (converti Decimal in float)
            change_pct = None
            if price_today and price_yesterday:
                change_pct = round(((float(price_today) - float(price_yesterday)) / float(price_yesterday)) * 100, 2)

            # Dati per livello
            fund_data = {
                'isin': r['isin'],
                'nome': r['nome'],
                'casa': r['casa'],
                'categoria': category,
                'price': float(price_today) if price_today else None,
                'price_yesterday': float(price_yesterday) if price_yesterday else None,
                'change_pct': change_pct,
                'ma': float(r['analysis'].get('ma')) if r['analysis'].get('ma') is not None else None,
                'rsi': float(r['analysis'].get('rsi')) if r['analysis'].get('rsi') is not None else None,
                'pct_1d': float(r['analysis'].get('pct_change_1d')) if r['analysis'].get('pct_change_1d') is not None else None,
                'pct_1w': float(r['analysis'].get('pct_change_1w')) if r['analysis'].get('pct_change_1w') is not None else None,
                'pct_1m': float(r['analysis'].get('pct_change_1m')) if r['analysis'].get('pct_change_1m') is not None else None,
                'buy_count': int(r['analysis'].get('buy_count', 0))
            }
            dashboard_data['levels'][level].append(fund_data)
            
            # Dati per categoria
            if category not in dashboard_data['categories']:
                dashboard_data['categories'][category] = []
            dashboard_data['categories'][category].append(fund_data)
        
        # Salva JSON per dashboard (converte Decimal e numpy types)
        class SafeEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, Decimal):
                    return float(obj)
                # Gestione tipi numpy
                import numpy as np
                if isinstance(obj, (np.integer,)):
                    return int(obj)
                if isinstance(obj, (np.floating,)):
                    return float(obj)
                if isinstance(obj, (np.bool_,)):
                    return bool(obj)
                if isinstance(obj, np.ndarray):
                    return obj.tolist()
                return super().default(obj)

        with open('data/dashboard_data.json', 'w') as f:
            json.dump(dashboard_data, f, indent=2, cls=SafeEncoder)
        
        return dashboard_data
    
    def send_alerts(self, results: list):
        """
        Invia alert per segnali significativi
        
        Args:
            results: Lista risultati analisi
        """
        min_indicators_l3 = int(self.config.get('Min Indicatori Concordi L3', 2))
        
        for r in results:
            signal = r['analysis'].get('final_signal', 'HOLD')
            strength = r['analysis'].get('signal_strength', 0)
            level = r['livello']
            
            fund_info = {
                'isin': r['isin'],
                'nome': r['nome'],
                'casa': r['casa'],
                'categoria': r['categoria'],
                'livello': level
            }
            
            # Livello 1: Alert sempre su SELL
            if level == 1 and signal == 'SELL':
                print(f"  🔴 Alert SELL per L1: {r['nome'][:40]}")
                self.alert_system.send_sell_alert(fund_info, r['analysis'])
            
            # Livello 2: Alert su BUY forte o SELL
            elif level == 2:
                if signal == 'BUY' and strength >= 2:
                    print(f"  🟢 Alert BUY per L2: {r['nome'][:40]}")
                    self.alert_system.send_buy_alert(fund_info, r['analysis'])
                elif signal == 'SELL':
                    print(f"  🔴 Alert SELL per L2: {r['nome'][:40]}")
                    self.alert_system.send_sell_alert(fund_info, r['analysis'])
            
            # Livello 3: Alert solo se indicatori concordi >= soglia
            elif level == 3 and strength >= min_indicators_l3:
                if signal == 'BUY':
                    print(f"  🟢 Alert BUY per L3: {r['nome'][:40]}")
                    self.alert_system.send_buy_alert(fund_info, r['analysis'])
                elif signal == 'SELL':
                    print(f"  🔴 Alert SELL per L3: {r['nome'][:40]}")
                    self.alert_system.send_sell_alert(fund_info, r['analysis'])
    
    def run(self, send_daily_report: bool = True):
        """
        Esegue ciclo completo di monitoraggio

        Args:
            send_daily_report: Se True, invia report giornaliero
        """
        import traceback
        add_log("="*50)
        add_log(f"FUND MONITOR - Avvio monitoraggio {datetime.now().strftime('%Y-%m-%d %H:%M')}")

        # 1. Carica fondi
        df_funds = self.load_funds()
        if df_funds.empty:
            add_log("ERRORE: Nessun fondo da monitorare (DataFrame vuoto)")
            return

        add_log(f"Caricati {len(df_funds)} fondi dal file Excel")

        # 2. Analizza ogni fondo
        add_log(f"Inizio analisi di {len(df_funds)} fondi...")
        results = []
        errors = []

        for idx, row in df_funds.iterrows():
            try:
                result = self.analyze_fund(row)
                results.append(result)
                add_log(f"  OK {row['ISIN']} - {row['Nome Fondo'][:30]}")
                time.sleep(0.5)  # Rate limiting
            except Exception as e:
                error_detail = traceback.format_exc()
                errors.append({'isin': row['ISIN'], 'error': str(e), 'traceback': error_detail})
                add_log(f"  ERRORE {row['ISIN']}: {e}")
                add_log(f"  TRACEBACK: {error_detail}")

        add_log(f"Analisi completata: {len(results)} OK, {len(errors)} errori")

        # 3. Aggiorna Excel
        try:
            add_log("Step 3: Aggiornamento file Excel...")
            self.update_excel(results)
            add_log("Step 3: Excel aggiornato OK")
        except Exception as e:
            add_log(f"Step 3 ERRORE Excel: {e}")
            add_log(traceback.format_exc())

        # 4. Genera dati dashboard
        try:
            add_log(f"Step 4: Generazione dashboard con {len(results)} risultati...")
            os.makedirs('data', exist_ok=True)
            dashboard_data = self.generate_dashboard_data(results)
            total = dashboard_data.get('summary', {}).get('total_funds', '?')
            add_log(f"Step 4: Dashboard generata OK - {total} fondi")
        except Exception as e:
            add_log(f"Step 4 ERRORE Dashboard: {e}")
            add_log(traceback.format_exc())
            # Genera dashboard minima per non bloccare tutto
            dashboard_data = {
                'last_update': datetime.now().isoformat(),
                'summary': {'total_funds': len(results), 'buy_signals': 0, 'sell_signals': 0, 'hold_signals': 0},
                'levels': {1: [], 2: [], 3: []},
                'categories': {}
            }
            # Popola con dati minimi
            for r in results:
                try:
                    fund_data = {
                        'isin': r['isin'], 'nome': r['nome'], 'casa': r['casa'],
                        'categoria': r['categoria'], 'price': float(r['analysis'].get('current_price')) if r['analysis'].get('current_price') else None,
                        'price_yesterday': None, 'change_pct': None,
                        'ma': float(r['analysis'].get('ma')) if r['analysis'].get('ma') else None,
                        'rsi': float(r['analysis'].get('rsi')) if r['analysis'].get('rsi') else None,
                        'pct_1d': float(r['analysis'].get('pct_change_1d')) if r['analysis'].get('pct_change_1d') is not None else None,
                        'pct_1w': float(r['analysis'].get('pct_change_1w')) if r['analysis'].get('pct_change_1w') is not None else None,
                        'pct_1m': float(r['analysis'].get('pct_change_1m')) if r['analysis'].get('pct_change_1m') is not None else None,
                        'buy_count': int(r['analysis'].get('buy_count', 0))
                    }
                    dashboard_data['levels'][r['livello']].append(fund_data)
                except:
                    pass
            with open('data/dashboard_data.json', 'w') as f:
                json.dump(dashboard_data, f, indent=2)
            add_log(f"Step 4: Dashboard fallback salvata con {len(results)} fondi")

        # 5. Invia alert
        try:
            add_log("Step 5: Invio alert...")
            self.send_alerts(results)
            add_log("Step 5: Alert OK")
        except Exception as e:
            add_log(f"Step 5 ERRORE Alert: {e}")

        # 6. Report giornaliero
        if send_daily_report:
            try:
                add_log("Step 6: Invio report giornaliero...")
                summary = {
                    'buy_signals': dashboard_data['summary']['buy_signals'],
                    'sell_signals': dashboard_data['summary']['sell_signals'],
                    'hold_signals': dashboard_data['summary']['hold_signals'],
                    'level_1': dashboard_data['levels'].get(1, []),
                    'level_2': dashboard_data['levels'].get(2, []),
                    'level_3': dashboard_data['levels'].get(3, [])[:10]
                }
                self.alert_system.send_daily_report(summary)
                add_log("Step 6: Report OK")
            except Exception as e:
                add_log(f"Step 6 ERRORE Report: {e}")

        add_log(f"Monitoraggio completato - {datetime.now().strftime('%H:%M')}")
        add_log("="*50)


def main():
    """Entry point"""
    monitor = FundMonitor()
    monitor.run()


if __name__ == "__main__":
    main()

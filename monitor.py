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
from pathlib import Path

# Import moduli locali
from data_fetcher import FundDataFetcher
from technical_analysis import TechnicalAnalyzer
from alerts import AlertSystem


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
        
        # Storage per dati storici (in produzione usare database)
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
        Recupera storico prezzi per un fondo
        
        Args:
            isin: Codice ISIN del fondo
        
        Returns:
            Serie pandas con prezzi storici
        """
        history_file = self.history_path / f"{isin}.json"
        
        # Carica storico esistente
        history = []
        if history_file.exists():
            with open(history_file, 'r') as f:
                history = json.load(f)
        
        # Aggiungi prezzo odierno
        nav_data = self.data_fetcher.get_nav(isin)
        if nav_data and nav_data.get('price'):
            today = datetime.now().strftime('%Y-%m-%d')
            
            # Evita duplicati
            if not history or history[-1]['date'] != today:
                history.append({
                    'date': today,
                    'price': nav_data['price']
                })
                
                # Mantieni solo ultimi 100 giorni
                history = history[-100:]
                
                # Salva
                with open(history_file, 'w') as f:
                    json.dump(history, f)
        
        # Converti in Serie
        if history:
            df = pd.DataFrame(history)
            return pd.Series(df['price'].values, index=pd.to_datetime(df['date']))
        else:
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
        
        # Recupera storico
        prices = self.get_fund_history(isin)
        
        if len(prices) < 5:
            # Se poco storico, usa dati simulati per iniziare
            df_hist = self.data_fetcher.get_historical_nav(isin, days=30)
            prices = pd.Series(df_hist['nav'].values)
        
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
        
        Args:
            results: Lista di risultati analisi
        """
        try:
            wb = load_workbook(self.excel_path)
            ws = wb['Fondi']
            
            # Mappa ISIN -> riga
            isin_to_row = {}
            for row in range(2, ws.max_row + 1):
                isin = ws.cell(row=row, column=2).value
                if isin:
                    isin_to_row[isin] = row
            
            # Aggiorna dati
            for result in results:
                isin = result['isin']
                analysis = result['analysis']
                
                if isin in isin_to_row:
                    row = isin_to_row[isin]
                    
                    # Prezzo (colonna G)
                    ws.cell(row=row, column=7, value=analysis.get('current_price'))
                    
                    # MM15 (colonna H)
                    ws.cell(row=row, column=8, value=analysis.get('ma'))
                    
                    # RSI (colonna I)
                    ws.cell(row=row, column=9, value=analysis.get('rsi'))
                    
                    # Segnale (colonna J)
                    signal = analysis.get('final_signal', 'HOLD')
                    signal_cell = ws.cell(row=row, column=10, value=signal)
                    
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
                    ws.cell(row=row, column=11, value=datetime.now().strftime('%Y-%m-%d %H:%M'))
            
            wb.save(self.excel_path)
            print(f"✅ File Excel aggiornato")
            
        except Exception as e:
            print(f"❌ Errore aggiornamento Excel: {e}")
    
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
            
            # Dati per livello
            fund_data = {
                'isin': r['isin'],
                'nome': r['nome'],
                'casa': r['casa'],
                'categoria': category,
                'price': r['analysis'].get('current_price'),
                'ma': r['analysis'].get('ma'),
                'rsi': r['analysis'].get('rsi'),
                'signal': signal,
                'signal_strength': r['analysis'].get('signal_strength', 0)
            }
            dashboard_data['levels'][level].append(fund_data)
            
            # Dati per categoria
            if category not in dashboard_data['categories']:
                dashboard_data['categories'][category] = []
            dashboard_data['categories'][category].append(fund_data)
        
        # Salva JSON per dashboard
        with open('data/dashboard_data.json', 'w') as f:
            json.dump(dashboard_data, f, indent=2)
        
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
        print("\n" + "="*60)
        print(f"🚀 FUND MONITOR - Avvio monitoraggio {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("="*60)
        
        # 1. Carica fondi
        df_funds = self.load_funds()
        if df_funds.empty:
            print("❌ Nessun fondo da monitorare")
            return
        
        # 2. Analizza ogni fondo
        print(f"\n📊 Analisi di {len(df_funds)} fondi...")
        results = []
        
        for idx, row in df_funds.iterrows():
            try:
                result = self.analyze_fund(row)
                results.append(result)
                time.sleep(0.5)  # Rate limiting
            except Exception as e:
                print(f"  ⚠️ Errore analisi {row['ISIN']}: {e}")
        
        print(f"\n✅ Analisi completata: {len(results)} fondi processati")
        
        # 3. Aggiorna Excel
        print("\n📝 Aggiornamento file Excel...")
        self.update_excel(results)
        
        # 4. Genera dati dashboard
        print("\n📈 Generazione dati dashboard...")
        os.makedirs('data', exist_ok=True)
        dashboard_data = self.generate_dashboard_data(results)
        
        # 5. Invia alert
        print("\n📧 Verifica e invio alert...")
        self.send_alerts(results)
        
        # 6. Report giornaliero
        if send_daily_report:
            print("\n📋 Invio report giornaliero...")
            summary = {
                'buy_signals': dashboard_data['summary']['buy_signals'],
                'sell_signals': dashboard_data['summary']['sell_signals'],
                'hold_signals': dashboard_data['summary']['hold_signals'],
                'level_1': dashboard_data['levels'][1],
                'level_2': dashboard_data['levels'][2],
                'level_3': dashboard_data['levels'][3][:10]  # Solo primi 10 per L3
            }
            self.alert_system.send_daily_report(summary)
        
        print("\n" + "="*60)
        print(f"✅ Monitoraggio completato - {datetime.now().strftime('%H:%M')}")
        print("="*60 + "\n")


def main():
    """Entry point"""
    monitor = FundMonitor()
    monitor.run()


if __name__ == "__main__":
    main()

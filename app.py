"""
app.py - Web server per la dashboard
=====================================
Serve la dashboard HTML e i dati JSON.
Legge dati direttamente da PostgreSQL (persistente).
Include auto-recovery: se il monitoraggio non ha girato oggi, lo lancia automaticamente.
"""

from flask import Flask, send_file, send_from_directory, jsonify, request
import os
import json
import threading
from datetime import datetime, date

from database import PriceDatabase
import monitor_lock

app = Flask(__name__)

# Database instance globale
db = PriceDatabase()


@app.route('/')
def index():
    """Serve la dashboard principale"""
    return send_file('dashboard.html')

@app.route('/data/<path:filename>')
def serve_data(filename):
    """Serve i file dati JSON"""
    return send_from_directory('data', filename)

def _get_last_update():
    """Legge la data dell'ultimo aggiornamento dal file JSON"""
    try:
        with open('data/dashboard_data.json', 'r') as f:
            data = json.load(f)
        return data.get('last_update')
    except:
        return None


def _should_run_today():
    """Controlla se il monitoraggio deve girare oggi.
    Ritorna True se:
    - Non ha mai girato, oppure
    - L'ultimo aggiornamento non e' di oggi E siamo dopo l'orario programmato
    - Il file esiste ma ha 0 fondi (file iniziale vuoto creato al deploy)
    """
    now = datetime.now()
    monitor_hour = int(os.environ.get('MONITOR_HOUR', 18))

    try:
        with open('data/dashboard_data.json', 'r') as f:
            data = json.load(f)
        last_update = data.get('last_update')
        total_funds = data.get('summary', {}).get('total_funds', 0)

        # Se ha 0 fondi, il file e' vuoto (creato al deploy) → deve girare
        if total_funds == 0:
            return True

        if not last_update:
            return True

        last_dt = datetime.fromisoformat(last_update)
        # Se l'ultimo aggiornamento non e' di oggi E siamo dopo l'ora programmata
        if last_dt.date() < now.date() and now.hour >= monitor_hour:
            return True
    except:
        return True

    return False


def _trigger_auto_monitor():
    """Lancia il monitoraggio in background se non sta gia' girando"""
    if not monitor_lock.try_acquire():
        return False

    def run():
        try:
            print(f"\n🔄 AUTO-RECOVERY: Monitoraggio automatico avviato - {datetime.now()}")
            from monitor import FundMonitor
            monitor = FundMonitor()
            monitor.run(send_daily_report=True)
            print(f"✅ AUTO-RECOVERY: Monitoraggio completato - {datetime.now()}")
        except Exception as e:
            print(f"❌ AUTO-RECOVERY: Errore monitoraggio: {e}")
        finally:
            monitor_lock.release()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return True


@app.route('/api/status')
def status():
    """Endpoint per verificare lo stato del sistema (+ auto-recovery)"""
    # Prova prima il file JSON (generato dal monitor)
    json_status = None
    try:
        with open('data/dashboard_data.json', 'r') as f:
            data = json.load(f)
        json_status = {
            'last_update': data.get('last_update'),
            'total_funds': data.get('summary', {}).get('total_funds', 0)
        }
    except:
        pass

    # Controlla anche lo stato del database
    db_stats = db.get_stats()

    # AUTO-RECOVERY: se il monitoraggio non ha girato oggi, lancialo
    auto_recovery_triggered = False
    if _should_run_today() and not monitor_lock.is_running():
        auto_recovery_triggered = _trigger_auto_monitor()

    return jsonify({
        'status': 'ok',
        'json_data': json_status,
        'database': db_stats,
        'database_url_set': bool(os.environ.get('DATABASE_URL')),
        'monitor_running': monitor_lock.is_running(),
        'auto_recovery_triggered': auto_recovery_triggered
    })

@app.route('/api/funds')
def get_funds():
    """API per ottenere tutti i fondi - legge da JSON generato dal monitor"""
    try:
        with open('data/dashboard_data.json', 'r') as f:
            return jsonify(json.load(f))
    except:
        return jsonify({'error': 'Data not available'}), 404

@app.route('/api/db-status')
def db_status_endpoint():
    """Diagnostica connessione database PostgreSQL"""
    raw_url = os.environ.get('DATABASE_URL', '')

    # Mostra URL mascherato (senza password)
    safe_url = 'NOT SET'
    if raw_url:
        import re
        safe_url = re.sub(r'://([^:]+):([^@]+)@', r'://\1:***@', raw_url)

    result = {
        'database_url_safe': safe_url,
        'database_url_length': len(raw_url),
        'database_url_starts_with': raw_url[:20] if raw_url else 'NOT SET',
        'db_url_resolved': bool(db.database_url),
        'timestamp': datetime.now().isoformat()
    }

    # Test connessione FRESCA (non usa cache)
    try:
        import psycopg2
        # Prova connessione diretta
        try:
            conn = psycopg2.connect(raw_url, sslmode='require', connect_timeout=5)
            conn.close()
            result['connection'] = 'OK (SSL)'
        except Exception as ssl_err:
            result['ssl_error'] = str(ssl_err)
            try:
                conn = psycopg2.connect(raw_url, connect_timeout=5)
                conn.close()
                result['connection'] = 'OK (no SSL)'
            except Exception as no_ssl_err:
                result['connection'] = 'ERRORE'
                result['no_ssl_error'] = str(no_ssl_err)
    except Exception as e:
        result['connection'] = 'ERRORE'
        result['error'] = str(e)

    return jsonify(result)

@app.route('/api/prices')
def get_prices():
    """API per ottenere prezzi dal database PostgreSQL"""
    isin = request.args.get('isin')
    days = int(request.args.get('days', 30))

    if isin:
        # Prezzi per un singolo fondo
        df = db.get_prices(isin, days)
        if not df.empty:
            prices = []
            for _, row in df.iterrows():
                prices.append({
                    'date': str(row['date']),
                    'price': float(row['price'])
                })
            return jsonify({'isin': isin, 'prices': prices, 'count': len(prices)})
        return jsonify({'isin': isin, 'prices': [], 'count': 0})
    else:
        # Tutti i prezzi (statistiche)
        stats = db.get_stats()
        return jsonify(stats)

@app.route('/api/trigger-update', methods=['GET', 'POST'])
def trigger_update():
    """Trigger manuale del monitoraggio (GET o POST)"""
    started = _trigger_auto_monitor()

    if started:
        return jsonify({
            'status': 'started',
            'message': 'Monitoraggio avviato in background',
            'timestamp': datetime.now().isoformat()
        })
    else:
        return jsonify({
            'status': 'already_running',
            'message': 'Monitoraggio gia\' in esecuzione, attendere il completamento',
            'timestamp': datetime.now().isoformat()
        })


@app.route('/api/monitor-log')
def get_monitor_log():
    """Mostra il log del monitoraggio"""
    from monitor import monitor_log
    return jsonify({
        'log': monitor_log,
        'count': len(monitor_log),
        'monitor_running': monitor_lock.is_running(),
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/backfill')
def backfill():
    """Backfill storico prezzi: recupera ultimi 50 giorni da Yahoo Finance e salva in DB"""
    if not db.is_available():
        return jsonify({'error': 'Database non disponibile'}), 503

    days = int(request.args.get('days', 50))

    def run_backfill():
        import pandas as pd
        from data_fetcher import FundDataFetcher

        fetcher = FundDataFetcher()
        stats = {'db_saved': 0, 'not_found': 0, 'errors': 0, 'funds_processed': 0}

        try:
            excel_df = pd.read_excel('fondi_monitoraggio.xlsx', sheet_name='Fondi')
        except Exception as e:
            print(f"Backfill: errore lettura Excel: {e}")
            return

        for _, row in excel_df.iterrows():
            isin = str(row.get('ISIN', '')).strip()
            nome = str(row.get('Nome Fondo', ''))[:40]
            if not isin or isin.lower() in ['nan', 'none']:
                continue

            stats['funds_processed'] += 1
            print(f"  Backfill {isin} - {nome}...")

            try:
                hist_df = fetcher.get_historical_nav(isin, days=days + 10)
            except Exception:
                hist_df = pd.DataFrame()

            if hist_df is None or hist_df.empty:
                stats['not_found'] += 1
                continue

            if 'date' not in hist_df.columns or 'nav' not in hist_df.columns:
                stats['not_found'] += 1
                continue

            try:
                hist_df['date'] = pd.to_datetime(hist_df['date']).dt.strftime('%Y-%m-%d')
                hist_df = hist_df.sort_values('date')
            except Exception:
                pass

            for _, r in hist_df.tail(days).iterrows():
                try:
                    ok = db.save_price(isin, str(r['date']), float(r['nav']), 'Yahoo/Backfill')
                    if ok:
                        stats['db_saved'] += 1
                except Exception:
                    stats['errors'] += 1

        print(f"Backfill completato: {stats}")

    thread = threading.Thread(target=run_backfill, daemon=True)
    thread.start()

    return jsonify({
        'status': 'started',
        'message': f'Backfill storico ultimi {days} giorni avviato in background',
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/test-fund')
def test_fund():
    """Test di debug: prova ad analizzare un singolo fondo"""
    import traceback
    result = {'steps': []}

    try:
        # Step 1: carica Excel
        from monitor import FundMonitor
        monitor = FundMonitor()
        result['steps'].append('Excel FundMonitor creato OK')

        df = monitor.load_funds()
        result['steps'].append(f'Excel caricato: {len(df)} fondi')
        result['columns'] = list(df.columns)

        if df.empty:
            result['error'] = 'DataFrame vuoto'
            return jsonify(result)

        # Step 2: prendi il primo fondo
        row = df.iloc[0]
        result['test_fund'] = {
            'isin': str(row['ISIN']),
            'nome': str(row['Nome Fondo']),
            'livello': int(row['Livello'])
        }
        result['steps'].append(f"Fondo selezionato: {row['Nome Fondo']}")

        # Step 3: fetch NAV
        nav = monitor.data_fetcher.get_nav(row['ISIN'])
        result['nav_result'] = nav
        result['steps'].append(f"NAV: {nav}")

        # Step 4: get history from DB
        prices = monitor.db.get_price_series(row['ISIN'], days=100)
        result['price_count'] = len(prices)
        result['steps'].append(f"Storico DB: {len(prices)} prezzi")

        # Step 5: analyze
        analysis = monitor.analyze_fund(row)
        result['analysis'] = str(analysis)
        result['steps'].append('Analisi completata OK')

    except Exception as e:
        result['error'] = str(e)
        result['traceback'] = traceback.format_exc()

    return jsonify(result)


if __name__ == '__main__':
    # Crea cartella data se non esiste
    os.makedirs('data', exist_ok=True)

    # Crea file dati iniziale se non esiste
    if not os.path.exists('data/dashboard_data.json'):
        initial_data = {
            'last_update': datetime.now().isoformat(),
            'summary': {'total_funds': 0, 'buy_signals': 0, 'sell_signals': 0, 'hold_signals': 0},
            'levels': {'1': [], '2': [], '3': []},
            'categories': {}
        }
        with open('data/dashboard_data.json', 'w') as f:
            json.dump(initial_data, f)

    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

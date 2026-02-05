"""
app.py - Web server per la dashboard
=====================================
Serve la dashboard HTML e i dati JSON.
Legge dati direttamente da PostgreSQL (persistente).
"""

from flask import Flask, send_file, send_from_directory, jsonify, request
import os
import json
import threading
from datetime import datetime

from database import PriceDatabase

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

@app.route('/api/status')
def status():
    """Endpoint per verificare lo stato del sistema"""
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

    return jsonify({
        'status': 'ok',
        'json_data': json_status,
        'database': db_stats,
        'database_url_set': bool(os.environ.get('DATABASE_URL'))
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
    result = {
        'env_vars': {
            'DATABASE_URL': bool(os.environ.get('DATABASE_URL')),
            'DATABASE_PUBLIC_URL': bool(os.environ.get('DATABASE_PUBLIC_URL')),
            'PGHOST': os.environ.get('PGHOST', 'NOT SET'),
            'PGDATABASE': os.environ.get('PGDATABASE', 'NOT SET'),
            'PGPORT': os.environ.get('PGPORT', 'NOT SET'),
            'PGUSER': os.environ.get('PGUSER', 'NOT SET'),
        },
        'db_url_resolved': bool(db.database_url),
        'timestamp': datetime.now().isoformat()
    }

    # Test connessione
    try:
        stats = db.get_stats()
        result['connection'] = 'OK'
        result['stats'] = stats
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

@app.route('/api/trigger-update', methods=['POST'])
def trigger_update():
    """Trigger manuale del monitoraggio"""
    def run_monitor_async():
        try:
            from monitor import FundMonitor
            monitor = FundMonitor()
            monitor.run(send_daily_report=False)
        except Exception as e:
            print(f"Errore monitoraggio manuale: {e}")

    thread = threading.Thread(target=run_monitor_async)
    thread.start()

    return jsonify({
        'status': 'started',
        'message': 'Monitoraggio avviato in background',
        'timestamp': datetime.now().isoformat()
    })


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

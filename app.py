"""
app.py - Web server per la dashboard
=====================================
Serve la dashboard HTML e i dati JSON
"""

from flask import Flask, send_file, send_from_directory, jsonify
import os
import json
from datetime import datetime

app = Flask(__name__)

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
    try:
        with open('data/dashboard_data.json', 'r') as f:
            data = json.load(f)
        return jsonify({
            'status': 'ok',
            'last_update': data.get('last_update'),
            'total_funds': data.get('summary', {}).get('total_funds', 0)
        })
    except:
        return jsonify({'status': 'no_data'})

@app.route('/api/funds')
def get_funds():
    """API per ottenere tutti i fondi"""
    try:
        with open('data/dashboard_data.json', 'r') as f:
            return jsonify(json.load(f))
    except:
        return jsonify({'error': 'Data not available'}), 404

if __name__ == '__main__':
    # Crea cartella data se non esiste
    os.makedirs('data', exist_ok=True)
    
    # Crea file dati iniziale se non esiste
    if not os.path.exists('data/dashboard_data.json'):
        initial_data = {
            'last_update': datetime.now().isoformat(),
            'summary': {'total_funds': 70, 'buy_signals': 0, 'sell_signals': 0, 'hold_signals': 70},
            'levels': {'1': [], '2': [], '3': []},
            'categories': {}
        }
        with open('data/dashboard_data.json', 'w') as f:
            json.dump(initial_data, f)
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

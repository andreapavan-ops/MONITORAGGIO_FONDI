"""
main.py - Entry point principale del sistema
=============================================
Avvia:
1. Web server Flask (dashboard)
2. Scheduler per monitoraggio giornaliero
"""

import os
import threading
from datetime import datetime

# Imposta variabili ambiente di default
os.environ.setdefault('MONITOR_HOUR', '18')
os.environ.setdefault('MONITOR_MINUTE', '0')
os.environ.setdefault('EMAIL_RECIPIENT', 'andreapavan67@gmail.com')

from app import app
from scheduler import start_scheduler_thread, run_monitor


def main():
    """Avvia il sistema completo"""
    print("="*60)
    print("🚀 FUND MONITOR SYSTEM - Avvio")
    print("="*60)
    print(f"📅 Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"📧 Email alert: {os.environ.get('EMAIL_RECIPIENT')}")
    print(f"⏰ Orario monitoraggio: {os.environ.get('MONITOR_HOUR')}:{os.environ.get('MONITOR_MINUTE')}")
    print("="*60)
    
    # Crea cartelle necessarie
    os.makedirs('data', exist_ok=True)
    os.makedirs('data/history', exist_ok=True)
    
    # Esegui monitoraggio iniziale se richiesto
    if os.environ.get('RUN_ON_START', 'true').lower() == 'true':
        print("\n▶ Esecuzione monitoraggio iniziale...")
        try:
            run_monitor()
        except Exception as e:
            print(f"⚠️ Errore monitoraggio iniziale: {e}")
    
    # Avvia scheduler in background
    print("\n▶ Avvio scheduler...")
    start_scheduler_thread()
    
    # Avvia web server
    print("\n▶ Avvio web server...")
    port = int(os.environ.get('PORT', 5000))
    print(f"🌐 Dashboard disponibile su http://localhost:{port}")
    print("="*60 + "\n")
    
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()

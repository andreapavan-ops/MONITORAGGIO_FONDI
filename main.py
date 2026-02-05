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

    # Verifica DATABASE_URL
    db_url = os.environ.get('DATABASE_URL')
    if db_url:
        print(f"🗄️ DATABASE_URL: configurato ({db_url[:30]}...)")
    else:
        print(f"⚠️ DATABASE_URL: NON CONFIGURATO - i prezzi non verranno salvati!")
        print(f"   Vai su Railway → Servizio Web → Variables → aggiungi DATABASE_URL")
    print("="*60)

    # Crea cartelle necessarie
    os.makedirs('data', exist_ok=True)
    os.makedirs('data/history', exist_ok=True)
    
    # Avvia web server SUBITO (per superare l'healthcheck di Railway)
    port = int(os.environ.get('PORT', 5000))
    print(f"\n▶ Avvio web server sulla porta {port}...")
    print(f"🌐 Dashboard disponibile su http://localhost:{port}")
    print("="*60 + "\n")

    # Avvia monitoraggio iniziale e scheduler in background DOPO il server
    def startup_background():
        """Esegue monitoraggio iniziale e avvia scheduler in background"""
        import time
        time.sleep(5)  # Aspetta che Flask sia pronto

        # Monitoraggio iniziale
        if os.environ.get('RUN_ON_START', 'true').lower() == 'true':
            print("\n▶ Esecuzione monitoraggio iniziale (background)...")
            try:
                run_monitor()
            except Exception as e:
                print(f"⚠️ Errore monitoraggio iniziale: {e}")

        # Avvia scheduler
        print("\n▶ Avvio scheduler...")
        start_scheduler_thread()

    bg_thread = threading.Thread(target=startup_background, daemon=True)
    bg_thread.start()

    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()

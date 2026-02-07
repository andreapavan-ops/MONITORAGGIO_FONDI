"""
scheduler.py - Scheduler per monitoraggio automatico
=====================================================
Esegue il monitoraggio ogni giorno alle 18:00.
Include fallback: ogni 30 minuti controlla se il monitoraggio
ha gia' girato oggi, e lo lancia se necessario.
"""

import schedule
import time
import os
import json
from datetime import datetime
import threading

from monitor import FundMonitor
import monitor_lock


def _has_run_today():
    """Controlla se il monitoraggio ha gia' girato oggi con successo"""
    try:
        with open('data/dashboard_data.json', 'r') as f:
            data = json.load(f)
        last_update = data.get('last_update')
        total_funds = data.get('summary', {}).get('total_funds', 0)

        # Se ha 0 fondi, non consideriamo che abbia girato
        if total_funds == 0:
            return False

        if last_update:
            last_dt = datetime.fromisoformat(last_update)
            return last_dt.date() == datetime.now().date()
    except:
        pass
    return False


def run_monitor():
    """Esegue il monitoraggio (con lock condiviso per evitare esecuzioni parallele)"""
    if not monitor_lock.try_acquire():
        print(f"⚠️ Scheduler: monitoraggio gia' in esecuzione, skip")
        return

    try:
        print(f"\n⏰ Scheduler: avvio monitoraggio programmato - {datetime.now()}")
        monitor = FundMonitor()
        monitor.run(send_daily_report=True)
    except Exception as e:
        print(f"❌ Errore durante monitoraggio: {e}")
    finally:
        monitor_lock.release()


def fallback_check():
    """Controllo fallback: se il monitoraggio non ha girato oggi, lancialo"""
    hour = int(os.environ.get('MONITOR_HOUR', 18))
    now = datetime.now()

    if now.hour >= hour and not _has_run_today() and not monitor_lock.is_running():
        print(f"\n🔄 Scheduler FALLBACK: monitoraggio non eseguito oggi, lancio ora...")
        run_monitor()
    else:
        # Log silenzioso solo ogni tanto per debug
        pass


def run_scheduler():
    """Avvia lo scheduler con job principale + fallback"""
    # Orario configurabile da variabili ambiente
    hour = int(os.environ.get('MONITOR_HOUR', 18))
    minute = int(os.environ.get('MONITOR_MINUTE', 0))

    schedule_time = f"{hour:02d}:{minute:02d}"

    print(f"📅 Scheduler configurato per le {schedule_time} ogni giorno")
    print(f"   Prossima esecuzione: oggi alle {schedule_time}" if datetime.now().hour < hour else f"   Prossima esecuzione: domani alle {schedule_time}")

    # Job principale: monitoraggio giornaliero
    schedule.every().day.at(schedule_time).do(run_monitor)

    # Job fallback: ogni 30 minuti controlla se ha girato oggi
    schedule.every(30).minutes.do(fallback_check)

    print(f"🔄 Fallback attivo: controllo ogni 30 minuti")

    # Loop infinito
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            print(f"⚠️ Scheduler errore nel loop: {e}")
        time.sleep(60)  # Check ogni minuto


def start_scheduler_thread():
    """Avvia scheduler in un thread separato (per uso con Flask)"""
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    return scheduler_thread


if __name__ == "__main__":
    print("="*50)
    print("🚀 FUND MONITOR SCHEDULER")
    print("="*50)

    # Esegui subito un primo monitoraggio se richiesto
    if os.environ.get('RUN_ON_START', 'false').lower() == 'true':
        print("\n▶ Esecuzione monitoraggio iniziale...")
        run_monitor()

    # Avvia scheduler
    run_scheduler()

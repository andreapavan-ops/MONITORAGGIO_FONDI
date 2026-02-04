"""
scheduler.py - Scheduler per monitoraggio automatico
=====================================================
Esegue il monitoraggio ogni giorno alle 18:00
"""

import schedule
import time
import os
from datetime import datetime
import threading

from monitor import FundMonitor


def run_monitor():
    """Esegue il monitoraggio"""
    print(f"\n⏰ Scheduler: avvio monitoraggio programmato - {datetime.now()}")
    try:
        monitor = FundMonitor()
        monitor.run(send_daily_report=True)
    except Exception as e:
        print(f"❌ Errore durante monitoraggio: {e}")


def run_scheduler():
    """Avvia lo scheduler"""
    # Orario configurabile da variabili ambiente
    hour = int(os.environ.get('MONITOR_HOUR', 18))
    minute = int(os.environ.get('MONITOR_MINUTE', 0))
    
    schedule_time = f"{hour:02d}:{minute:02d}"
    
    print(f"📅 Scheduler configurato per le {schedule_time} ogni giorno")
    print(f"   Prossima esecuzione: oggi alle {schedule_time}" if datetime.now().hour < hour else f"   Prossima esecuzione: domani alle {schedule_time}")
    
    # Schedula il job giornaliero
    schedule.every().day.at(schedule_time).do(run_monitor)
    
    # Loop infinito
    while True:
        schedule.run_pending()
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

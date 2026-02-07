"""
monitor_lock.py - Lock condiviso per evitare monitoraggi paralleli
==================================================================
Usato da app.py e scheduler.py per garantire che un solo
monitoraggio giri alla volta.
"""

import threading
from datetime import datetime

_lock = threading.Lock()
_running = False


def is_running():
    """Controlla se un monitoraggio e' in esecuzione"""
    return _running


def try_acquire():
    """Prova ad acquisire il lock. Ritorna True se acquisito, False se gia' occupato."""
    global _running
    with _lock:
        if _running:
            return False
        _running = True
        return True


def release():
    """Rilascia il lock"""
    global _running
    with _lock:
        _running = False

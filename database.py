"""
database.py - Gestione database PostgreSQL per storico prezzi
==============================================================
Salva e recupera lo storico dei prezzi dei fondi su PostgreSQL (Railway)
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import logging

# Usa psycopg2 per PostgreSQL
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    logging.warning("psycopg2 non installato. Installa con: pip install psycopg2-binary")


class PriceDatabase:
    """Gestisce lo storico prezzi su PostgreSQL"""

    def __init__(self, database_url: str = None):
        """
        Inizializza la connessione al database

        Args:
            database_url: URL di connessione PostgreSQL (default: da variabile ambiente)
        """
        self.database_url = database_url or self._detect_database_url()
        self.connection = None

        if not POSTGRES_AVAILABLE:
            print("❌ psycopg2 non disponibile - installa con: pip install psycopg2-binary")
            return

        if not self.database_url:
            print("⚠️ DATABASE_URL non trovato. Lo storico non verrà salvato su PostgreSQL.")
            return

        print(f"🔗 DATABASE_URL configurato: {self.database_url[:30]}...")

        # Inizializza la tabella se non esiste
        self._init_table()

    @staticmethod
    def _detect_database_url() -> Optional[str]:
        """
        Cerca l'URL del database in diversi modi:
        1. DATABASE_URL (standard)
        2. DATABASE_PUBLIC_URL (Railway public)
        3. Costruisce da PGHOST, PGUSER, PGPASSWORD, PGDATABASE, PGPORT
        """
        # 1. DATABASE_URL diretto
        url = os.environ.get('DATABASE_URL')
        if url:
            print("🔍 Trovato DATABASE_URL")
            return url

        # 2. DATABASE_PUBLIC_URL (Railway)
        url = os.environ.get('DATABASE_PUBLIC_URL')
        if url:
            print("🔍 Trovato DATABASE_PUBLIC_URL")
            return url

        # 3. Costruisci da variabili PG* individuali (Railway le espone sul servizio Postgres)
        pghost = os.environ.get('PGHOST')
        pguser = os.environ.get('PGUSER', 'postgres')
        pgpassword = os.environ.get('PGPASSWORD')
        pgdatabase = os.environ.get('PGDATABASE', 'railway')
        pgport = os.environ.get('PGPORT', '5432')

        if pghost and pgpassword:
            url = f"postgresql://{pguser}:{pgpassword}@{pghost}:{pgport}/{pgdatabase}"
            print(f"🔍 DATABASE_URL costruito da variabili PG*: {pghost}:{pgport}")
            return url

        print("⚠️ Nessuna variabile database trovata (DATABASE_URL, DATABASE_PUBLIC_URL, PGHOST)")
        print("   Aggiungi almeno una di queste variabili al servizio web su Railway")
        return None

    def _get_connection(self):
        """Ottiene una connessione al database"""
        if not self.database_url or not POSTGRES_AVAILABLE:
            if not self.database_url:
                print("⚠️ DATABASE_URL non impostato, impossibile connettersi")
            return None

        try:
            conn = psycopg2.connect(self.database_url, sslmode='require')
            return conn
        except Exception as e:
            print(f"❌ Errore connessione database: {e}")
            # Prova senza SSL (per database locali)
            try:
                conn = psycopg2.connect(self.database_url)
                return conn
            except Exception as e2:
                print(f"❌ Errore connessione anche senza SSL: {e2}")
                return None

    def _init_table(self):
        """Crea la tabella price_history se non esiste"""
        conn = self._get_connection()
        if not conn:
            return

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS price_history (
                        id SERIAL PRIMARY KEY,
                        isin VARCHAR(20) NOT NULL,
                        date DATE NOT NULL,
                        price DECIMAL(12, 4) NOT NULL,
                        source VARCHAR(50),
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE(isin, date)
                    )
                """)
                # Aggiungi vincolo UNIQUE se mancante (tabella già esistente)
                cur.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint
                            WHERE conname = 'price_history_isin_date_key'
                        ) THEN
                            ALTER TABLE price_history ADD CONSTRAINT price_history_isin_date_key UNIQUE (isin, date);
                        END IF;
                    END $$;
                """)
                # Crea indice per query veloci
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_price_history_isin_date
                    ON price_history(isin, date DESC)
                """)
                conn.commit()
                print("✅ Tabella price_history pronta (con vincolo UNIQUE)")
        except Exception as e:
            logging.error(f"Errore creazione tabella: {e}")
        finally:
            conn.close()

    def save_price(self, isin: str, date: str, price: float, source: str = 'FT Markets') -> bool:
        """
        Salva un prezzo nel database

        Args:
            isin: Codice ISIN del fondo
            date: Data nel formato YYYY-MM-DD
            price: Prezzo/NAV
            source: Fonte del dato

        Returns:
            True se salvato con successo
        """
        conn = self._get_connection()
        if not conn:
            print(f"⚠️ DB non disponibile - prezzo {isin} non salvato")
            return False

        try:
            with conn.cursor() as cur:
                # UPSERT: inserisce o aggiorna se esiste già
                cur.execute("""
                    INSERT INTO price_history (isin, date, price, source, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (isin, date)
                    DO UPDATE SET price = EXCLUDED.price, source = EXCLUDED.source, updated_at = NOW()
                """, (isin, date, price, source))
                conn.commit()
                print(f"  💾 Prezzo salvato in DB: {isin} = {price} ({date})")
                return True
        except Exception as e:
            print(f"❌ Errore salvataggio prezzo {isin}: {e}")
            return False
        finally:
            conn.close()

    def get_prices(self, isin: str, days: int = 30) -> pd.DataFrame:
        """
        Recupera lo storico prezzi per un fondo

        Args:
            isin: Codice ISIN del fondo
            days: Numero di giorni da recuperare

        Returns:
            DataFrame con colonne ['date', 'price']
        """
        conn = self._get_connection()
        if not conn:
            return pd.DataFrame(columns=['date', 'price'])

        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT date, price
                    FROM price_history
                    WHERE isin = %s
                    ORDER BY date DESC
                    LIMIT %s
                """, (isin, days))
                rows = cur.fetchall()

                if rows:
                    df = pd.DataFrame(rows)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.sort_values('date').reset_index(drop=True)
                    return df
                return pd.DataFrame(columns=['date', 'price'])
        except Exception as e:
            logging.error(f"Errore recupero prezzi {isin}: {e}")
            return pd.DataFrame(columns=['date', 'price'])
        finally:
            conn.close()

    def get_price_series(self, isin: str, days: int = 30) -> pd.Series:
        """
        Recupera lo storico prezzi come Serie pandas

        Args:
            isin: Codice ISIN del fondo
            days: Numero di giorni

        Returns:
            Serie pandas con index=date e values=price
        """
        df = self.get_prices(isin, days)
        if df.empty:
            return pd.Series(dtype=float)
        return pd.Series(df['price'].values, index=df['date'])

    def get_all_prices(self) -> pd.DataFrame:
        """
        Recupera tutti i prezzi nel database (per debug/export)

        Returns:
            DataFrame con tutti i prezzi
        """
        conn = self._get_connection()
        if not conn:
            return pd.DataFrame()

        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT isin, date, price, source, created_at, updated_at
                    FROM price_history
                    ORDER BY isin, date DESC
                """)
                rows = cur.fetchall()
                return pd.DataFrame(rows) if rows else pd.DataFrame()
        except Exception as e:
            logging.error(f"Errore recupero tutti i prezzi: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def count_prices(self, isin: str = None) -> int:
        """
        Conta i prezzi salvati

        Args:
            isin: Se specificato, conta solo per questo ISIN

        Returns:
            Numero di record
        """
        conn = self._get_connection()
        if not conn:
            return 0

        try:
            with conn.cursor() as cur:
                if isin:
                    cur.execute("SELECT COUNT(*) FROM price_history WHERE isin = %s", (isin,))
                else:
                    cur.execute("SELECT COUNT(*) FROM price_history")
                return cur.fetchone()[0]
        except Exception as e:
            logging.error(f"Errore conteggio prezzi: {e}")
            return 0
        finally:
            conn.close()

    def get_stats(self) -> Dict:
        """
        Statistiche sul database

        Returns:
            Dizionario con statistiche
        """
        conn = self._get_connection()
        if not conn:
            return {'error': 'Database non disponibile'}

        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Conteggio totale
                cur.execute("SELECT COUNT(*) as total FROM price_history")
                total = cur.fetchone()['total']

                # Fondi unici
                cur.execute("SELECT COUNT(DISTINCT isin) as funds FROM price_history")
                funds = cur.fetchone()['funds']

                # Range date
                cur.execute("""
                    SELECT MIN(date) as first_date, MAX(date) as last_date
                    FROM price_history
                """)
                dates = cur.fetchone()

                # Prezzi per fondo
                cur.execute("""
                    SELECT isin, COUNT(*) as count
                    FROM price_history
                    GROUP BY isin
                    ORDER BY count DESC
                """)
                by_fund = cur.fetchall()

                return {
                    'total_records': total,
                    'unique_funds': funds,
                    'first_date': str(dates['first_date']) if dates['first_date'] else None,
                    'last_date': str(dates['last_date']) if dates['last_date'] else None,
                    'records_by_fund': {r['isin']: r['count'] for r in by_fund}
                }
        except Exception as e:
            logging.error(f"Errore statistiche: {e}")
            return {'error': str(e)}
        finally:
            conn.close()


def test_database():
    """Test del modulo database"""
    print("="*50)
    print("TEST DATABASE")
    print("="*50)

    db = PriceDatabase()

    # Test salvataggio
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"\nSalvataggio prezzo test...")
    success = db.save_price('TEST123', today, 100.50, 'Test')
    print(f"  Risultato: {'✅ OK' if success else '❌ ERRORE'}")

    # Test recupero
    print(f"\nRecupero prezzi...")
    prices = db.get_prices('TEST123', 10)
    print(f"  Record trovati: {len(prices)}")

    # Statistiche
    print(f"\nStatistiche database:")
    stats = db.get_stats()
    for k, v in stats.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    test_database()

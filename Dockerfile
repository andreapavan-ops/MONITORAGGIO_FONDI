FROM python:3.13-slim

WORKDIR /app

# Installa dipendenze di sistema (richieste da psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Installa dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia tutto il codice dell'applicazione
COPY . .

# Crea directory dati (poi montate come volume)
RUN mkdir -p data/history

EXPOSE 5000

# Usa python main.py (non gunicorn) perché lo scheduler gira in thread background
CMD ["python", "main.py"]

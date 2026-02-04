# 📊 Fund Monitor System

Sistema automatizzato di monitoraggio fondi a 3 livelli con alert email e dashboard HTML.

## 🎯 Funzionalità

- **Monitoraggio giornaliero** alle 18:00
- **3 livelli di monitoraggio**: Core (L1), Watchlist (L2), Universe (L3)
- **Analisi tecnica**: Media Mobile 15gg, RSI 14, MACD
- **Alert email** per segnali BUY/SELL
- **Dashboard web** con semafori e filtri
- **File Excel** per gestione fondi (aggiungi/rimuovi/promuovi)

---

## 🚀 Deploy su Railway

### 1. Crea account Railway
Vai su [railway.app](https://railway.app) e registrati (gratis con GitHub)

### 2. Nuovo progetto
- Click "New Project"
- Seleziona "Deploy from GitHub repo"
- Connetti il tuo repository

### 3. Configura variabili ambiente
Nel pannello Railway, vai su "Variables" e aggiungi:

```
EMAIL_SENDER=tua-email@gmail.com
EMAIL_PASSWORD=tua-app-password-gmail
EMAIL_RECIPIENT=andreapavan67@gmail.com
MONITOR_HOUR=18
MONITOR_MINUTE=0
RUN_ON_START=true
```

### 4. Setup Gmail per invio email

1. Vai su [Google Account Security](https://myaccount.google.com/security)
2. Attiva **Verifica in 2 passaggi**
3. Vai su **Password per le app**
4. Crea nuova password per "Posta"
5. Usa questa password come `EMAIL_PASSWORD`

### 5. Deploy
Railway farà automaticamente il deploy. La dashboard sarà disponibile all'URL fornito.

---

## 📁 Struttura File

```
fund_monitor_system/
├── main.py                 # Entry point principale
├── monitor.py              # Logica monitoraggio
├── data_fetcher.py         # Recupero NAV fondi
├── technical_analysis.py   # Calcolo indicatori
├── alerts.py               # Sistema email
├── app.py                  # Web server Flask
├── scheduler.py            # Scheduler giornaliero
├── dashboard.html          # Dashboard web
├── fondi_monitoraggio.xlsx # File Excel master
├── requirements.txt        # Dipendenze Python
├── Procfile               # Config Railway
└── data/                  # Dati runtime
    ├── dashboard_data.json
    └── history/           # Storico prezzi
```

---

## 📋 Gestione Fondi (Excel)

### Aprire il file
Scarica `fondi_monitoraggio.xlsx` e aprilo con Excel

### Aggiungere un fondo
1. Vai al foglio "Fondi"
2. Aggiungi nuova riga con:
   - **Livello**: 1, 2, o 3
   - **ISIN**: Codice ISIN del fondo
   - **Nome Fondo**: Nome completo
   - **Casa Gestione**: Es. "Fidelity"
   - **Categoria**: Es. "AI_Tech", "Gold", "Healthcare"
   - **Valuta**: "EUR" o "EUR Hedged"

### Rimuovere un fondo
Cancella la riga corrispondente

### Promuovere/Retrocedere
Cambia il numero nella colonna "Livello":
- `3 → 2`: Promuovi a Watchlist
- `2 → 1`: Promuovi a Core
- `1 → 2`: Retrocedi a Watchlist
- `2 → 3`: Retrocedi a Universe

### Categorie disponibili
- `AI_Tech` - Intelligenza Artificiale e Tecnologia
- `Gold` - Metalli Preziosi
- `Healthcare` - Settore Salute
- `Energy` - Energia
- `Global_Equity` - Azionari Globali
- `Bond_EUR` - Obbligazionari EUR
- `Money_Market` - Monetari

---

## 🚦 Logica Segnali

### Livello 1 (Core Portfolio)
- **SELL** → Alert immediato email
- Monitoraggio completo (MM + RSI + MACD)

### Livello 2 (Watchlist)
- **BUY forte** (≥2 indicatori) → Alert email
- **SELL** → Alert email
- Considera promozione a L1

### Livello 3 (Universe)
- Alert solo se **≥2 indicatori concordano**
- Screening per opportunità
- Considera promozione a L2

---

## 📧 Tipi di Email

1. **Alert BUY** 🟢 - Segnale di acquisto
2. **Alert SELL** 🔴 - Segnale di vendita
3. **Report Giornaliero** 📊 - Riepilogo alle 18:00

---

## 🛠 Esecuzione Locale (Test)

```bash
# Installa dipendenze
pip install -r requirements.txt

# Esegui
python main.py
```

Dashboard su: http://localhost:5000

---

## 💰 Costi Railway

- **Hobby Plan**: ~$5/mese
- Include: 500 ore/mese, 100GB bandwidth
- Sufficiente per questo sistema

---

## 📞 Supporto

Per problemi o domande, controlla:
1. Log su Railway dashboard
2. File `data/dashboard_data.json` per ultimi dati
3. Endpoint `/api/status` per stato sistema

---

## ⚠️ Disclaimer

Questo sistema è solo a scopo informativo. Le decisioni di investimento sono responsabilità dell'utente. I segnali generati non costituiscono consulenza finanziaria.

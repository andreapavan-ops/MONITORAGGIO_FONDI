"""
alerts.py - Sistema di notifiche email per alert di trading
============================================================
Gestisce l'invio di email per:
- Alert di acquisto (segnali BUY)
- Alert di vendita (segnali SELL)
- Report giornaliero
- Alert di promozione/retrocessione livelli

Email via Resend API (HTTPS) — sostituisce Gmail SMTP bloccato da Railway.
"""

from datetime import datetime
from typing import List, Dict, Optional
import os

try:
    import resend as _resend
    RESEND_AVAILABLE = True
except ImportError:
    RESEND_AVAILABLE = False
    print("⚠️  Libreria 'resend' non installata. Installa con: pip install resend")


class AlertSystem:
    """Sistema di alert via email (Resend API)"""

    def __init__(self, sender_email: str = None, sender_password: str = None,
                 recipient_email: str = None):
        """
        Inizializza il sistema di alert.

        Args:
            sender_email:    Indirizzo mittente (deve essere su dominio verificato Resend).
                             Se non specificato, usa EMAIL_SENDER dall'env.
            sender_password: Ignorato (legacy, mantenuto per compatibilità).
            recipient_email: Email destinatario. Default: EMAIL_RECIPIENT dall'env.
        """
        self.sender_email = sender_email or os.getenv('EMAIL_SENDER', 'onboarding@resend.dev')
        self.recipient_email = recipient_email or os.getenv('EMAIL_RECIPIENT', 'andreapavan67@gmail.com')
        self.resend_api_key = os.getenv('RESEND_API_KEY', '')

        if RESEND_AVAILABLE and self.resend_api_key:
            _resend.api_key = self.resend_api_key

    def _send_email(self, subject: str, body_html: str, body_text: str = None) -> bool:
        """
        Invia email tramite Resend API (HTTPS — non bloccato da Railway).

        Returns:
            True se invio riuscito, False altrimenti.
        """
        if not RESEND_AVAILABLE:
            print("⚠️  Resend non disponibile — installa con: pip install resend")
            print(f"   Subject: {subject}")
            return False

        if not self.resend_api_key:
            print("⚠️  RESEND_API_KEY non configurata — email non inviata")
            print(f"   Subject: {subject}")
            return False

        if not self.recipient_email:
            print("⚠️  EMAIL_RECIPIENT non configurata — email non inviata")
            return False

        try:
            params: _resend.Emails.SendParams = {
                "from": f"Fund Monitor <{self.sender_email}>",
                "to": [self.recipient_email],
                "subject": subject,
                "html": body_html,
            }
            if body_text:
                params["text"] = body_text

            _resend.Emails.send(params)
            print(f"✅ Email inviata via Resend: {subject}")
            return True

        except Exception as e:
            print(f"❌ Errore invio email (Resend): {e}")
            return False
    
    def send_l1_digest(self, l1_funds: list) -> bool:
        """
        Email giornaliera: lista di tutti i fondi in Livello 1 con tracking entrata.

        Args:
            l1_funds: Lista di dict con campi:
                nome, isin, casa, categoria,
                entry_date, entry_price, price, days_in_l1, pct_gain
        """
        today = datetime.now().strftime('%d/%m/%Y')
        n = len(l1_funds)
        subject = f"📊 Portfolio L1 — {n} fond{'i' if n != 1 else 'o'} — {today}"

        rows_html = ""
        for i, f in enumerate(l1_funds, start=1):
            entry_date_str = (
                f['entry_date'].strftime('%d/%m/%Y')
                if hasattr(f['entry_date'], 'strftime')
                else str(f['entry_date'])
            )
            entry_price = f.get('entry_price')
            price = f.get('price')
            pct = f.get('pct_gain')
            days = f.get('days_in_l1', 0)

            pct_color = '#00B050' if pct and pct >= 0 else '#DC3545'
            pct_str = f"{pct:+.2f}%" if pct is not None else '–'
            bg = '#f9f9f9' if i % 2 == 0 else 'white'

            rows_html += f"""
            <tr style="background:{bg};">
              <td style="padding:8px;border:1px solid #ddd;text-align:center;color:#666;">{i}</td>
              <td style="padding:8px;border:1px solid #ddd;">
                <strong>{f['nome'][:45]}</strong><br>
                <span style="font-size:11px;color:#888;">{f['casa']} · {f['isin']}</span>
              </td>
              <td style="padding:8px;border:1px solid #ddd;text-align:center;">{entry_date_str}</td>
              <td style="padding:8px;border:1px solid #ddd;text-align:center;">{days}</td>
              <td style="padding:8px;border:1px solid #ddd;text-align:right;">{"€{:.4f}".format(entry_price) if entry_price else '–'}</td>
              <td style="padding:8px;border:1px solid #ddd;text-align:right;">{"€{:.4f}".format(price) if price else '–'}</td>
              <td style="padding:8px;border:1px solid #ddd;text-align:center;font-weight:bold;color:{pct_color};">{pct_str}</td>
            </tr>"""

        body_html = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; background: #f0f2f5;">
          <div style="background: linear-gradient(135deg, #00B050, #007A36); color: white; padding: 25px; text-align: center;">
            <h1 style="margin: 0; font-size: 22px;">📊 Portfolio Livello 1</h1>
            <p style="margin: 6px 0 0 0; opacity: 0.9; font-size: 14px;">
              {datetime.now().strftime('%A %d %B %Y')} &nbsp;·&nbsp; {n} fond{'i' if n != 1 else 'o'} in portafoglio
            </p>
          </div>

          <div style="padding: 20px; background: white;">
            <table style="width:100%;border-collapse:collapse;font-size:13px;">
              <thead>
                <tr style="background:#00B050;color:white;">
                  <th style="padding:8px;border:1px solid #ddd;">#</th>
                  <th style="padding:8px;border:1px solid #ddd;text-align:left;">Fondo</th>
                  <th style="padding:8px;border:1px solid #ddd;">Entrato il</th>
                  <th style="padding:8px;border:1px solid #ddd;">Giorni in L1</th>
                  <th style="padding:8px;border:1px solid #ddd;">Prezzo entrata</th>
                  <th style="padding:8px;border:1px solid #ddd;">Prezzo attuale</th>
                  <th style="padding:8px;border:1px solid #ddd;">Guadagno %</th>
                </tr>
              </thead>
              <tbody>{rows_html}</tbody>
            </table>
          </div>

          <div style="padding: 15px; background: #333; color: #999; text-align: center; font-size: 12px;">
            Fund Monitor System &nbsp;·&nbsp; Prossimo aggiornamento ore 18:00
          </div>
        </body>
        </html>
        """
        return self._send_email(subject, body_html)

    def send_sell_l1_exit(self, fund_info: dict) -> bool:
        """
        Email di uscita da L1: prezzo entrata, prezzo uscita, % guadagno/perdita.

        Args:
            fund_info: dict con nome, isin, casa, categoria,
                       entry_date, entry_price, exit_price, days_in_l1, pct_gain
        """
        pct = fund_info.get('pct_gain')
        pct_str = f"{pct:+.2f}%" if pct is not None else 'N/D'
        pct_color = '#00B050' if pct and pct >= 0 else '#DC3545'
        result_label = 'GUADAGNO' if pct and pct >= 0 else 'PERDITA'

        entry_date_str = (
            fund_info['entry_date'].strftime('%d/%m/%Y')
            if hasattr(fund_info.get('entry_date'), 'strftime')
            else str(fund_info.get('entry_date', '–'))
        )
        today = datetime.now().strftime('%d/%m/%Y')
        subject = f"🔴 Uscita L1 — {fund_info['nome'][:40]} — {today}"

        body_html = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; background: #f0f2f5;">
          <div style="background: linear-gradient(135deg, #DC3545, #AA0000); color: white; padding: 25px; text-align: center;">
            <h1 style="margin: 0; font-size: 22px;">🔴 USCITA DA LIVELLO 1</h1>
            <p style="margin: 6px 0 0 0; opacity: 0.9; font-size: 14px;">{today}</p>
          </div>

          <div style="padding: 20px; background: white;">
            <h2 style="color:#333;margin-top:0;">{fund_info['nome']}</h2>
            <p style="color:#666;margin-top:-10px;">{fund_info['casa']} · {fund_info['categoria']} · {fund_info['isin']}</p>

            <table style="width:100%;border-collapse:collapse;font-size:14px;">
              <tr style="background:#f5f5f5;">
                <td style="padding:12px;border:1px solid #ddd;"><strong>Data entrata in L1</strong></td>
                <td style="padding:12px;border:1px solid #ddd;">{entry_date_str}</td>
              </tr>
              <tr>
                <td style="padding:12px;border:1px solid #ddd;"><strong>Giorni in L1</strong></td>
                <td style="padding:12px;border:1px solid #ddd;">{fund_info.get('days_in_l1', '–')} giorni</td>
              </tr>
              <tr style="background:#f5f5f5;">
                <td style="padding:12px;border:1px solid #ddd;"><strong>Prezzo di entrata</strong></td>
                <td style="padding:12px;border:1px solid #ddd;">{"€{:.4f}".format(fund_info['entry_price']) if fund_info.get('entry_price') else '–'}</td>
              </tr>
              <tr>
                <td style="padding:12px;border:1px solid #ddd;"><strong>Prezzo di uscita</strong></td>
                <td style="padding:12px;border:1px solid #ddd;">{"€{:.4f}".format(fund_info['exit_price']) if fund_info.get('exit_price') else '–'}</td>
              </tr>
              <tr style="background:{pct_color};color:white;">
                <td style="padding:12px;border:1px solid #ddd;"><strong>{result_label}</strong></td>
                <td style="padding:12px;border:1px solid #ddd;font-size:18px;font-weight:bold;">{pct_str}</td>
              </tr>
            </table>
          </div>

          <div style="padding: 15px; background: #333; color: #999; text-align: center; font-size: 12px;">
            Fund Monitor System &nbsp;·&nbsp; {datetime.now().strftime('%d/%m/%Y %H:%M')}
          </div>
        </body>
        </html>
        """
        return self._send_email(subject, body_html)

    def send_buy_alert(self, fund: Dict, analysis: Dict) -> bool:
        """
        Invia alert di acquisto
        
        Args:
            fund: Dizionario con info fondo (isin, nome, categoria, livello)
            analysis: Risultato analisi tecnica
        """
        subject = f"🟢 ALERT BUY - {fund['nome'][:40]}"
        
        body_html = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <div style="background: linear-gradient(135deg, #00B050, #00D060); color: white; padding: 20px; text-align: center;">
                <h1 style="margin: 0;">🟢 SEGNALE DI ACQUISTO</h1>
            </div>
            
            <div style="padding: 20px; background: #f5f5f5;">
                <h2 style="color: #333; margin-top: 0;">{fund['nome']}</h2>
                
                <table style="width: 100%; border-collapse: collapse; background: white;">
                    <tr style="background: #e8e8e8;">
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>ISIN</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{fund['isin']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Categoria</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{fund['categoria']}</td>
                    </tr>
                    <tr style="background: #e8e8e8;">
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Casa Gestione</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{fund['casa']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Livello Attuale</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;">L{fund['livello']}</td>
                    </tr>
                </table>
                
                <h3 style="color: #00B050; margin-top: 20px;">📊 Analisi Tecnica</h3>
                
                <table style="width: 100%; border-collapse: collapse; background: white;">
                    <tr style="background: #00B050; color: white;">
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Prezzo Attuale</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>€{analysis.get('current_price', 0):.2f}</strong></td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd;">Media Mobile 15gg</td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{"€{:.2f}".format(analysis['ma']) if analysis.get('ma') else 'N/A'}</td>
                    </tr>
                    <tr style="background: #e8e8e8;">
                        <td style="padding: 10px; border: 1px solid #ddd;">RSI (14)</td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{"{:.1f}".format(analysis['rsi']) if analysis.get('rsi') else 'N/A'}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd;">Distanza da Max 52w</td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{"{:.1f}%".format(analysis['pct_from_high_52w']) if analysis.get('pct_from_high_52w') else 'N/A'}</td>
                    </tr>
                    <tr style="background: #00B050; color: white;">
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Indicatori Concordi</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>{analysis.get('signal_strength', 0)}/{analysis.get('total_signals', 0)}</strong></td>
                    </tr>
                </table>
                
                <div style="margin-top: 20px; padding: 15px; background: #d4edda; border-left: 4px solid #00B050;">
                    <strong>💡 Suggerimento:</strong> Considera la promozione di questo fondo al Livello {fund['livello'] - 1 if fund['livello'] > 1 else 1}
                </div>
            </div>
            
            <div style="padding: 15px; background: #333; color: #999; text-align: center; font-size: 12px;">
                Fund Monitor System • {datetime.now().strftime('%d/%m/%Y %H:%M')}
            </div>
        </body>
        </html>
        """
        
        return self._send_email(subject, body_html)
    
    def send_sell_alert(self, fund: Dict, analysis: Dict) -> bool:
        """
        Invia alert di vendita
        """
        subject = f"🔴 ALERT SELL - {fund['nome'][:40]}"
        
        body_html = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <div style="background: linear-gradient(135deg, #DC3545, #FF4444); color: white; padding: 20px; text-align: center;">
                <h1 style="margin: 0;">🔴 SEGNALE DI VENDITA</h1>
            </div>
            
            <div style="padding: 20px; background: #f5f5f5;">
                <h2 style="color: #333; margin-top: 0;">{fund['nome']}</h2>
                
                <table style="width: 100%; border-collapse: collapse; background: white;">
                    <tr style="background: #e8e8e8;">
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>ISIN</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{fund['isin']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Categoria</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{fund['categoria']}</td>
                    </tr>
                    <tr style="background: #e8e8e8;">
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Casa Gestione</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{fund['casa']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Livello Attuale</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;">L{fund['livello']}</td>
                    </tr>
                </table>
                
                <h3 style="color: #DC3545; margin-top: 20px;">📊 Analisi Tecnica</h3>
                
                <table style="width: 100%; border-collapse: collapse; background: white;">
                    <tr style="background: #DC3545; color: white;">
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Prezzo Attuale</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>€{analysis.get('current_price', 0):.2f}</strong></td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd;">Media Mobile 15gg</td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{"€{:.2f}".format(analysis['ma']) if analysis.get('ma') else 'N/A'}</td>
                    </tr>
                    <tr style="background: #e8e8e8;">
                        <td style="padding: 10px; border: 1px solid #ddd;">RSI (14)</td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{"{:.1f}".format(analysis['rsi']) if analysis.get('rsi') else 'N/A'}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd;">Distanza da Max 52w</td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{"{:.1f}%".format(analysis['pct_from_high_52w']) if analysis.get('pct_from_high_52w') else 'N/A'}</td>
                    </tr>
                    <tr style="background: #DC3545; color: white;">
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Indicatori Concordi</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>{analysis.get('signal_strength', 0)}/{analysis.get('total_signals', 0)}</strong></td>
                    </tr>
                </table>
                
                <div style="margin-top: 20px; padding: 15px; background: #f8d7da; border-left: 4px solid #DC3545;">
                    <strong>⚠️ Attenzione:</strong> {'Considera la vendita immediata!' if fund['livello'] == 1 else 'Monitora attentamente questo fondo.'}
                </div>
            </div>
            
            <div style="padding: 15px; background: #333; color: #999; text-align: center; font-size: 12px;">
                Fund Monitor System • {datetime.now().strftime('%d/%m/%Y %H:%M')}
            </div>
        </body>
        </html>
        """
        
        return self._send_email(subject, body_html)
    
    def send_daily_report(self, summary: Dict) -> bool:
        """
        Invia report giornaliero con riepilogo di tutti i livelli
        
        Args:
            summary: Dizionario con statistiche giornaliere
        """
        subject = f"📊 Report Giornaliero Fondi - {datetime.now().strftime('%d/%m/%Y')}"
        
        # Costruisci tabelle per ogni livello
        level_tables = ""
        for level in [1, 2, 3]:
            funds = summary.get(f'level_{level}', [])
            if funds:
                level_tables += f"""
                <h3>{'🟢 Livello 1 - Core Portfolio' if level == 1 else '🟡 Livello 2 - Watchlist' if level == 2 else '🔵 Livello 3 - Universe'}</h3>
                <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
                    <tr style="background: {'#00B050' if level == 1 else '#FFC000' if level == 2 else '#4472C4'}; color: white;">
                        <th style="padding: 8px; border: 1px solid #ddd;">Fondo</th>
                        <th style="padding: 8px; border: 1px solid #ddd;">Prezzo</th>
                        <th style="padding: 8px; border: 1px solid #ddd;">RSI</th>
                        <th style="padding: 8px; border: 1px solid #ddd;">Segnale</th>
                    </tr>
                """
                for f in funds[:10]:  # Max 10 fondi per livello nel report
                    sig = f.get('signal', f.get('final_signal', 'HOLD'))
                    signal_color = '#00B050' if sig == 'BUY' else '#DC3545' if sig == 'SELL' else '#FFC000'
                    level_tables += f"""
                    <tr>
                        <td style="padding: 8px; border: 1px solid #ddd;">{f['nome'][:35]}...</td>
                        <td style="padding: 8px; border: 1px solid #ddd; text-align: right;">{"€{:.2f}".format(f['price']) if f.get('price') else '-'}</td>
                        <td style="padding: 8px; border: 1px solid #ddd; text-align: center;">{"{:.0f}".format(f['rsi']) if f.get('rsi') else '-'}</td>
                        <td style="padding: 8px; border: 1px solid #ddd; text-align: center; background: {signal_color}; color: white;">{sig}</td>
                    </tr>
                    """
                level_tables += "</table>"
        
        body_html = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 700px; margin: 0 auto;">
            <div style="background: linear-gradient(135deg, #1F4E79, #2E75B6); color: white; padding: 20px; text-align: center;">
                <h1 style="margin: 0;">📊 Report Giornaliero</h1>
                <p style="margin: 5px 0 0 0;">{datetime.now().strftime('%A %d %B %Y')}</p>
            </div>
            
            <div style="padding: 20px; background: #f5f5f5;">
                <h2 style="color: #1F4E79;">Riepilogo</h2>
                
                <div style="display: flex; gap: 10px; margin-bottom: 20px;">
                    <div style="flex: 1; background: #00B050; color: white; padding: 15px; text-align: center; border-radius: 8px;">
                        <div style="font-size: 24px; font-weight: bold;">{summary.get('buy_signals', 0)}</div>
                        <div>Segnali BUY</div>
                    </div>
                    <div style="flex: 1; background: #FFC000; padding: 15px; text-align: center; border-radius: 8px;">
                        <div style="font-size: 24px; font-weight: bold;">{summary.get('hold_signals', 0)}</div>
                        <div>Segnali HOLD</div>
                    </div>
                    <div style="flex: 1; background: #DC3545; color: white; padding: 15px; text-align: center; border-radius: 8px;">
                        <div style="font-size: 24px; font-weight: bold;">{summary.get('sell_signals', 0)}</div>
                        <div>Segnali SELL</div>
                    </div>
                </div>
                
                {level_tables}
                
            </div>
            
            <div style="padding: 15px; background: #333; color: #999; text-align: center; font-size: 12px;">
                Fund Monitor System • Prossimo aggiornamento ore 18:00
            </div>
        </body>
        </html>
        """
        
        return self._send_email(subject, body_html)
    
    def send_health_report(self, health: dict) -> bool:
        """
        Invia report sullo stato di salute del sistema.

        Args:
            health: Dizionario con dati health dal monitor
        """
        total = health.get('total_funds', 0)
        ok = health.get('funds_ok', 0)
        errors_count = health.get('funds_error', 0)
        with_price = health.get('funds_with_price', 0)
        no_price = health.get('funds_no_price', 0)
        db_ok = health.get('db_available', False)
        errors = health.get('errors', [])

        # Determina stato globale
        if errors_count == 0 and with_price == ok and db_ok:
            status_emoji = "🟢"
            status_text = "TUTTO OK"
            status_color = "#00B050"
        elif errors_count > 0 or no_price > 0:
            status_emoji = "🟡"
            status_text = "ATTENZIONE"
            status_color = "#FFC000"
        else:
            status_emoji = "🔴"
            status_text = "PROBLEMI"
            status_color = "#DC3545"

        # Se tutto perfetto, non mandare email (evita spam)
        if errors_count == 0 and no_price == 0 and db_ok:
            print("✅ Health check OK - email non necessaria")
            return True

        subject = f"{status_emoji} Health Check - {status_text} - {datetime.now().strftime('%d/%m/%Y %H:%M')}"

        # Tabella errori
        errors_html = ""
        if errors:
            errors_rows = "".join(
                f'<tr><td style="padding:6px;border:1px solid #ddd;font-family:monospace;">{e["isin"]}</td>'
                f'<td style="padding:6px;border:1px solid #ddd;color:#DC3545;">{e["error"][:80]}</td></tr>'
                for e in errors
            )
            errors_html = f"""
            <h3 style="color:#DC3545;">Fondi con errore</h3>
            <table style="width:100%;border-collapse:collapse;background:white;">
                <tr style="background:#DC3545;color:white;">
                    <th style="padding:6px;border:1px solid #ddd;">ISIN</th>
                    <th style="padding:6px;border:1px solid #ddd;">Errore</th>
                </tr>
                {errors_rows}
            </table>
            """

        body_html = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <div style="background: {status_color}; color: white; padding: 20px; text-align: center;">
                <h1 style="margin: 0;">{status_emoji} HEALTH CHECK: {status_text}</h1>
                <p style="margin: 5px 0 0 0;">{datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
            </div>

            <div style="padding: 20px; background: #f5f5f5;">
                <table style="width:100%;border-collapse:collapse;background:white;margin-bottom:15px;">
                    <tr>
                        <td style="padding:10px;border:1px solid #ddd;"><strong>Fondi totali</strong></td>
                        <td style="padding:10px;border:1px solid #ddd;text-align:center;">{total}</td>
                    </tr>
                    <tr style="background:#e8e8e8;">
                        <td style="padding:10px;border:1px solid #ddd;"><strong>Analizzati OK</strong></td>
                        <td style="padding:10px;border:1px solid #ddd;text-align:center;color:#00B050;font-weight:bold;">{ok}</td>
                    </tr>
                    <tr>
                        <td style="padding:10px;border:1px solid #ddd;"><strong>Errori analisi</strong></td>
                        <td style="padding:10px;border:1px solid #ddd;text-align:center;color:{'#DC3545' if errors_count > 0 else '#00B050'};font-weight:bold;">{errors_count}</td>
                    </tr>
                    <tr style="background:#e8e8e8;">
                        <td style="padding:10px;border:1px solid #ddd;"><strong>Con prezzo aggiornato</strong></td>
                        <td style="padding:10px;border:1px solid #ddd;text-align:center;">{with_price}</td>
                    </tr>
                    <tr>
                        <td style="padding:10px;border:1px solid #ddd;"><strong>Senza prezzo</strong></td>
                        <td style="padding:10px;border:1px solid #ddd;text-align:center;color:{'#DC3545' if no_price > 0 else '#00B050'};font-weight:bold;">{no_price}</td>
                    </tr>
                    <tr style="background:#e8e8e8;">
                        <td style="padding:10px;border:1px solid #ddd;"><strong>Database PostgreSQL</strong></td>
                        <td style="padding:10px;border:1px solid #ddd;text-align:center;color:{'#00B050' if db_ok else '#DC3545'};font-weight:bold;">{'Connesso' if db_ok else 'Non disponibile'}</td>
                    </tr>
                </table>

                {errors_html}
            </div>

            <div style="padding: 15px; background: #333; color: #999; text-align: center; font-size: 12px;">
                Fund Monitor System - Health Check Automatico
            </div>
        </body>
        </html>
        """

        return self._send_email(subject, body_html)

    def send_buy_digest(self, promoted_funds: list, near_l1_funds: list) -> bool:
        """
        Invia unica email digest giornaliera BUY con:
        - Sezione 1: Fondi promossi a Livello 1 (tutte 4 condizioni L1 Pro soddisfatte)
        - Sezione 2: Top 3 fondi L2 più vicini a L1 con analisi gap quantitativa e previsione

        Args:
            promoted_funds: Fondi con suggested_level==1, con 'analysis' embedded
            near_l1_funds:  Top 3 fondi L2 non promossi, con 'gap_info' embedded
        """
        today = datetime.now().strftime('%d/%m/%Y')
        n_promoted = len(promoted_funds)
        n_near = len(near_l1_funds)

        if n_promoted > 0:
            subject = f"⬆️ {n_promoted} Fondo{'i' if n_promoted > 1 else ''} a L1 (4/4 TRBS) + Top {n_near} L2 — {today}"
        else:
            subject = f"📊 Top {n_near} Fondi L2 Vicini a L1 — {today}"

        # ── Sezione 1: Fondi promossi a L1 ──────────────────────────────────
        if promoted_funds:
            promo_rows = ""
            for f in promoted_funds:
                a = f.get('analysis', {})
                price = a.get('current_price')
                ma    = a.get('ma')
                rsi   = a.get('rsi')
                p1d   = a.get('pct_change_1d')
                p1w   = a.get('pct_change_1w')
                lc    = a.get('level_conditions', {})
                lvl_from = f.get('livello', '?')
                promo_rows += f"""
                <tr>
                  <td style="padding:8px;border:1px solid #ddd;">
                    <strong>{f['nome'][:45]}</strong><br>
                    <span style="font-size:11px;color:#666;">{f['casa']} · {f['categoria'][:35]}</span><br>
                    <span style="font-size:11px;color:#888;">{'✅ Confermato L1' if lvl_from == 1 else f'L{lvl_from} → L1'} · {f['isin']}</span>
                  </td>
                  <td style="padding:8px;border:1px solid #ddd;text-align:center;">{"€{:.4f}".format(price) if price else '–'}</td>
                  <td style="padding:8px;border:1px solid #ddd;text-align:center;">{"€{:.4f}".format(ma) if ma else '–'}</td>
                  <td style="padding:8px;border:1px solid #ddd;text-align:center;">{"{:.0f}".format(rsi) if rsi else '–'}</td>
                  <td style="padding:8px;border:1px solid #ddd;text-align:center;{'color:#00B050;font-weight:bold;' if p1d and p1d > 0 else 'color:#DC3545;font-weight:bold;' if p1d and p1d < 0 else ''}">
                    {"{:+.2f}%".format(p1d) if p1d is not None else '–'}
                  </td>
                  <td style="padding:8px;border:1px solid #ddd;text-align:center;{'color:#00B050;font-weight:bold;' if p1w and p1w > 0 else 'color:#DC3545;font-weight:bold;' if p1w and p1w < 0 else ''}">
                    {"{:+.2f}%".format(p1w) if p1w is not None else '–'}
                  </td>
                </tr>"""

            promo_section = f"""
            <div style="margin-bottom:30px;">
              <h2 style="color:#00B050;border-bottom:3px solid #00B050;padding-bottom:8px;margin-top:0;">
                ⬆️ Fondi a Livello 1 — 4/4 TRBS
                <span style="font-size:13px;font-weight:normal;color:#666;"> — {n_promoted} fondo{'i' if n_promoted > 1 else ''}</span>
              </h2>
              <p style="color:#555;margin-top:0;">
                Questi fondi soddisfano <strong>tutte e 4</strong> le condizioni L1 Pro
                (Trend · Momentum · Volatilità · Setup).
              </p>
              <table style="width:100%;border-collapse:collapse;background:white;font-size:13px;">
                <thead>
                  <tr style="background:#00B050;color:white;">
                    <th style="padding:8px;border:1px solid #ddd;text-align:left;">Fondo</th>
                    <th style="padding:8px;border:1px solid #ddd;">Prezzo</th>
                    <th style="padding:8px;border:1px solid #ddd;">MM20</th>
                    <th style="padding:8px;border:1px solid #ddd;">RSI</th>
                    <th style="padding:8px;border:1px solid #ddd;">1g %</th>
                    <th style="padding:8px;border:1px solid #ddd;">1s %</th>
                  </tr>
                </thead>
                <tbody>{promo_rows}</tbody>
              </table>
            </div>"""
        else:
            promo_section = """
            <div style="margin-bottom:30px;">
              <h2 style="color:#00B050;border-bottom:3px solid #00B050;padding-bottom:8px;margin-top:0;">
                ⬆️ Fondi Promossi a Livello 1
              </h2>
              <p style="color:#999;font-style:italic;">
                Nessun fondo ha raggiunto tutte e 4 le condizioni L1 Pro oggi.
              </p>
            </div>"""

        # ── Sezione 2: Top 3 L2 vicini a L1 ────────────────────────────────
        near_cards = ""
        for rank, f in enumerate(near_l1_funds, start=1):
            gi         = f.get('gap_info', {})
            buy_count  = gi.get('buy_count', 0)
            price      = gi.get('price', 0)
            ma_val     = gi.get('ma', 0)
            rsi_val    = gi.get('rsi', 0)
            pct_1d     = gi.get('pct_1d')
            pct_1w     = gi.get('pct_1w')
            pct_1m     = gi.get('pct_1m')
            conditions = gi.get('conditions', [])

            bar_pct   = int(buy_count / 4 * 100)
            bar_color = '#00B050' if bar_pct >= 75 else '#FFC000' if bar_pct >= 50 else '#FF6600'

            def pct_style(v):
                if v is None: return ''
                return 'color:#00B050;font-weight:bold;' if v > 0 else 'color:#DC3545;font-weight:bold;'

            def fmt_pct(v):
                return "{:+.2f}%".format(v) if v is not None else '–'

            # Righe condizioni
            cond_rows     = ""
            forecast_items = ""
            for cond in conditions:
                ok     = cond.get('ok', False)
                name   = cond.get('name', '')
                if ok:
                    detail = cond.get('detail', '')
                    cond_rows += f"""
                    <tr>
                      <td style="padding:6px 10px;border:1px solid #eee;text-align:center;font-size:14px;">✅</td>
                      <td style="padding:6px 10px;border:1px solid #eee;font-weight:bold;color:#2d7a2d;font-size:12px;">{name}</td>
                      <td style="padding:6px 10px;border:1px solid #eee;color:#2d7a2d;font-size:12px;">{detail}</td>
                    </tr>"""
                else:
                    gap_text = cond.get('gap_text', '')
                    forecast = cond.get('forecast', '')
                    cond_rows += f"""
                    <tr>
                      <td style="padding:6px 10px;border:1px solid #eee;text-align:center;font-size:14px;">❌</td>
                      <td style="padding:6px 10px;border:1px solid #eee;font-weight:bold;color:#DC3545;font-size:12px;">{name}</td>
                      <td style="padding:6px 10px;border:1px solid #eee;color:#DC3545;font-size:12px;">{gap_text}</td>
                    </tr>"""
                    if forecast:
                        forecast_items += f"<li style='margin-bottom:5px;'><strong>{name}</strong>: {forecast}</li>"

            forecast_block = ""
            if forecast_items:
                forecast_block = f"""
                <div style="padding:12px 15px;background:#fffbf0;border-top:1px solid #eee;">
                  <div style="font-size:12px;font-weight:bold;color:#856404;margin-bottom:6px;">
                    📈 ANALISI &amp; PREVISIONE — cosa manca e quanto si è vicini:
                  </div>
                  <ul style="margin:0;padding-left:20px;font-size:12px;color:#555;line-height:1.7;">
                    {forecast_items}
                  </ul>
                </div>"""

            near_cards += f"""
            <div style="background:white;margin-bottom:22px;border-radius:8px;border:1px solid #ddd;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,0.08);">

              <!-- Card header -->
              <div style="background:#FFC000;padding:12px 15px;display:flex;justify-content:space-between;align-items:center;">
                <span style="font-size:16px;font-weight:bold;color:#333;">#{rank} &nbsp;{f['nome'][:50]}</span>
                <span style="background:white;color:#333;padding:4px 12px;border-radius:12px;font-weight:bold;font-size:13px;">
                  {buy_count}/4 ✓
                </span>
              </div>

              <!-- Sottotitolo -->
              <div style="padding:7px 15px;background:#fff8e1;font-size:11px;color:#666;border-bottom:1px solid #ffe082;">
                {f['casa']} &nbsp;·&nbsp; {f['categoria']} &nbsp;·&nbsp; ISIN: {f['isin']}
              </div>

              <!-- Valori tecnici -->
              <div style="padding:12px 15px;border-bottom:1px solid #eee;">
                <table style="width:100%;text-align:center;border-collapse:collapse;">
                  <tr>
                    <td style="padding:6px 4px;">
                      <div style="font-size:10px;color:#999;text-transform:uppercase;">Prezzo</div>
                      <div style="font-size:15px;font-weight:bold;">{"€{:.4f}".format(price) if price else '–'}</div>
                    </td>
                    <td style="padding:6px 4px;">
                      <div style="font-size:10px;color:#999;text-transform:uppercase;">MM20</div>
                      <div style="font-size:15px;font-weight:bold;">{"€{:.4f}".format(ma_val) if ma_val else '–'}</div>
                    </td>
                    <td style="padding:6px 4px;">
                      <div style="font-size:10px;color:#999;text-transform:uppercase;">RSI</div>
                      <div style="font-size:15px;font-weight:bold;">{"{:.0f}".format(rsi_val) if rsi_val else '–'}</div>
                    </td>
                    <td style="padding:6px 4px;">
                      <div style="font-size:10px;color:#999;text-transform:uppercase;">1 giorno</div>
                      <div style="font-size:15px;{pct_style(pct_1d)}">{fmt_pct(pct_1d)}</div>
                    </td>
                    <td style="padding:6px 4px;">
                      <div style="font-size:10px;color:#999;text-transform:uppercase;">1 settimana</div>
                      <div style="font-size:15px;{pct_style(pct_1w)}">{fmt_pct(pct_1w)}</div>
                    </td>
                    <td style="padding:6px 4px;">
                      <div style="font-size:10px;color:#999;text-transform:uppercase;">1 mese</div>
                      <div style="font-size:15px;{pct_style(pct_1m)}">{fmt_pct(pct_1m)}</div>
                    </td>
                  </tr>
                </table>

                <!-- Barra progresso verso L1 -->
                <div style="margin-top:12px;">
                  <div style="display:flex;justify-content:space-between;font-size:11px;color:#999;margin-bottom:4px;">
                    <span>Avanzamento verso L1 Pro</span>
                    <span style="font-weight:bold;color:{bar_color};">{buy_count}/4 condizioni soddisfatte</span>
                  </div>
                  <div style="background:#eee;border-radius:4px;height:8px;">
                    <div style="background:{bar_color};width:{bar_pct}%;height:8px;border-radius:4px;transition:width 0.3s;"></div>
                  </div>
                </div>
              </div>

              <!-- Condizioni L1 Pro -->
              <div style="padding:12px 15px;border-bottom:1px solid #eee;">
                <div style="font-size:12px;font-weight:bold;color:#333;margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px;">
                  Condizioni L1 Pro
                </div>
                <table style="width:100%;border-collapse:collapse;">
                  {cond_rows}
                </table>
              </div>

              {forecast_block}
            </div>"""

        if near_l1_funds:
            near_section_body = near_cards
        else:
            near_section_body = "<p style='color:#999;font-style:italic;'>Nessun fondo L2 disponibile.</p>"

        near_section = f"""
        <div style="margin-bottom:20px;">
          <h2 style="color:#B8860B;border-bottom:3px solid #FFC000;padding-bottom:8px;margin-top:0;">
            📊 Top {n_near} Fondi L2 Più Vicini a L1
            <span style="font-size:13px;font-weight:normal;color:#666;"> — ordinati per condizioni soddisfatte</span>
          </h2>
          <p style="color:#555;margin-top:0;">
            Analisi quantitativa del gap verso Livello 1 Pro. Per ogni condizione mancante
            è indicato il valore attuale, il gap numerico e la previsione su cosa serve.
          </p>
          {near_section_body}
        </div>"""

        body_html = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 740px; margin: 0 auto; background: #f0f2f5;">

          <!-- Header -->
          <div style="background: linear-gradient(135deg, #00B050, #007A36); color: white; padding: 25px; text-align: center;">
            <h1 style="margin: 0; font-size: 22px; letter-spacing: 0.5px;">🟢 DIGEST BUY GIORNALIERO</h1>
            <p style="margin: 6px 0 0 0; opacity: 0.9; font-size: 14px;">
              {datetime.now().strftime('%A %d %B %Y')} &nbsp;·&nbsp; Fund Monitor System
            </p>
          </div>

          <div style="padding: 20px;">
            {promo_section}
            {near_section}
          </div>

          <div style="padding: 15px; background: #333; color: #999; text-align: center; font-size: 12px;">
            Fund Monitor System &nbsp;·&nbsp; Prossimo aggiornamento ore 18:00
          </div>

        </body>
        </html>
        """

        return self._send_email(subject, body_html)

    def send_test_email(self) -> bool:
        """Invia email di test per verificare configurazione"""
        subject = "🧪 Test Fund Monitor System"
        
        body_html = """
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
            <div style="background: #4472C4; color: white; padding: 20px; text-align: center; border-radius: 8px;">
                <h1>✅ Sistema Configurato Correttamente!</h1>
            </div>
            <div style="padding: 20px; background: #f5f5f5; margin-top: 20px; border-radius: 8px;">
                <p>Questa è un'email di test dal Fund Monitor System.</p>
                <p>Se la ricevi, significa che il sistema di alert è configurato correttamente.</p>
                <p><strong>Prossimi passi:</strong></p>
                <ul>
                    <li>Il monitoraggio giornaliero partirà alle ore 18:00</li>
                    <li>Riceverai alert per segnali BUY e SELL</li>
                    <li>Puoi modificare il file Excel per aggiungere/rimuovere fondi</li>
                </ul>
            </div>
        </body>
        </html>
        """
        
        return self._send_email(subject, body_html)


if __name__ == "__main__":
    # Test
    alert = AlertSystem()
    alert.send_test_email()

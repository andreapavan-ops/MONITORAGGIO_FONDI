"""
alerts.py - Sistema di notifiche email per alert di trading
============================================================
Gestisce l'invio di email per:
- Alert di acquisto (segnali BUY)
- Alert di vendita (segnali SELL)
- Report giornaliero
- Alert di promozione/retrocessione livelli
"""

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import List, Dict, Optional
import os


class AlertSystem:
    """Sistema di alert via email"""
    
    def __init__(self, sender_email: str = None, sender_password: str = None, 
                 recipient_email: str = None):
        """
        Inizializza il sistema di alert
        
        Args:
            sender_email: Email mittente (Gmail)
            sender_password: Password app Gmail
            recipient_email: Email destinatario
        """
        self.sender_email = sender_email or os.getenv('EMAIL_SENDER', '')
        self.sender_password = sender_password or os.getenv('EMAIL_PASSWORD', '')
        self.recipient_email = recipient_email or os.getenv('EMAIL_RECIPIENT', 'andreapavan67@gmail.com')
        
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
    
    def _send_email(self, subject: str, body_html: str, body_text: str = None) -> bool:
        """
        Invia email via SMTP
        
        Returns:
            True se invio riuscito, False altrimenti
        """
        if not all([self.sender_email, self.sender_password, self.recipient_email]):
            print("⚠️ Configurazione email incompleta - email non inviata")
            print(f"   Subject: {subject}")
            return False
        
        try:
            # Crea messaggio
            message = MIMEMultipart("alternative")
            message["Subject"] = subject
            message["From"] = self.sender_email
            message["To"] = self.recipient_email
            
            # Corpo testo e HTML
            if body_text:
                part1 = MIMEText(body_text, "plain")
                message.attach(part1)
            
            part2 = MIMEText(body_html, "html")
            message.attach(part2)
            
            # Connessione e invio
            context = ssl.create_default_context()
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls(context=context)
                server.login(self.sender_email, self.sender_password)
                server.sendmail(self.sender_email, self.recipient_email, message.as_string())
            
            print(f"✅ Email inviata: {subject}")
            return True
            
        except Exception as e:
            print(f"❌ Errore invio email: {e}")
            return False
    
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
                    signal_color = '#00B050' if f['signal'] == 'BUY' else '#DC3545' if f['signal'] == 'SELL' else '#FFC000'
                    level_tables += f"""
                    <tr>
                        <td style="padding: 8px; border: 1px solid #ddd;">{f['nome'][:35]}...</td>
                        <td style="padding: 8px; border: 1px solid #ddd; text-align: right;">{"€{:.2f}".format(f['price']) if f.get('price') else '-'}</td>
                        <td style="padding: 8px; border: 1px solid #ddd; text-align: center;">{"{:.0f}".format(f['rsi']) if f.get('rsi') else '-'}</td>
                        <td style="padding: 8px; border: 1px solid #ddd; text-align: center; background: {signal_color}; color: white;">{f.get('signal', 'HOLD')}</td>
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

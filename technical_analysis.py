"""
technical_analysis.py - Modulo per analisi tecnica dei fondi
=============================================================
Calcola indicatori tecnici:
- Media Mobile (MM15)
- RSI (Relative Strength Index)
- Segnali di acquisto/vendita
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional


class TechnicalAnalyzer:
    """Classe per calcolare indicatori tecnici sui fondi"""
    
    def __init__(self, config: dict = None):
        """
        Inizializza l'analizzatore tecnico
        
        Args:
            config: Dizionario di configurazione con:
                - ma_period: Periodo media mobile (default: 15)
                - rsi_period: Periodo RSI (default: 14)
                - rsi_oversold: Soglia ipervenduto (default: 30)
                - rsi_overbought: Soglia ipercomprato (default: 70)
        """
        self.config = config or {}
        self.ma_period = self.config.get('ma_period', 15)
        self.rsi_period = self.config.get('rsi_period', 14)
        self.rsi_oversold = self.config.get('rsi_oversold', 30)
        self.rsi_overbought = self.config.get('rsi_overbought', 70)
    
    def calculate_ma(self, prices: pd.Series, period: int = None) -> pd.Series:
        """
        Calcola la Media Mobile Semplice (SMA)
        
        Args:
            prices: Serie di prezzi
            period: Periodo della media (default: self.ma_period)
        
        Returns:
            Serie con i valori della media mobile
        """
        period = period or self.ma_period
        return prices.rolling(window=period).mean()
    
    def calculate_rsi(self, prices: pd.Series, period: int = None) -> pd.Series:
        """
        Calcola il Relative Strength Index (RSI)
        
        Formula:
        RSI = 100 - (100 / (1 + RS))
        RS = Media guadagni / Media perdite
        
        Args:
            prices: Serie di prezzi
            period: Periodo RSI (default: self.rsi_period)
        
        Returns:
            Serie con i valori RSI
        """
        period = period or self.rsi_period
        
        # Calcola variazioni giornaliere
        delta = prices.diff()
        
        # Separa guadagni e perdite
        gains = delta.where(delta > 0, 0)
        losses = (-delta).where(delta < 0, 0)
        
        # Media mobile esponenziale dei guadagni e perdite
        avg_gain = gains.ewm(com=period-1, min_periods=period).mean()
        avg_loss = losses.ewm(com=period-1, min_periods=period).mean()
        
        # Calcola RS e RSI
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def calculate_macd(self, prices: pd.Series, 
                       fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, pd.Series]:
        """
        Calcola il MACD (Moving Average Convergence Divergence)
        
        Args:
            prices: Serie di prezzi
            fast: Periodo EMA veloce (default: 12)
            slow: Periodo EMA lento (default: 26)
            signal: Periodo linea segnale (default: 9)
        
        Returns:
            Dizionario con 'macd', 'signal', 'histogram'
        """
        ema_fast = prices.ewm(span=fast).mean()
        ema_slow = prices.ewm(span=slow).mean()
        
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal).mean()
        histogram = macd_line - signal_line
        
        return {
            'macd': macd_line,
            'signal': signal_line,
            'histogram': histogram
        }
    
    def get_price_vs_ma_signal(self, current_price: float, ma_value: float) -> str:
        """
        Determina il segnale basato su prezzo vs media mobile
        
        Returns:
            'BUY' se prezzo > MA (trend rialzista)
            'SELL' se prezzo < MA (trend ribassista)
            'HOLD' se dati insufficienti
        """
        if ma_value is None or np.isnan(ma_value):
            return 'HOLD'
        
        pct_diff = (current_price - ma_value) / ma_value * 100
        
        if pct_diff > 2:  # Sopra MA del 2%+
            return 'BUY'
        elif pct_diff < -2:  # Sotto MA del 2%+
            return 'SELL'
        else:
            return 'HOLD'
    
    def get_rsi_signal(self, rsi_value: float) -> str:
        """
        Determina il segnale basato su RSI
        
        Returns:
            'BUY' se RSI < soglia ipervenduto
            'SELL' se RSI > soglia ipercomprato
            'HOLD' altrimenti
        """
        if rsi_value is None or np.isnan(rsi_value):
            return 'HOLD'
        
        if rsi_value < self.rsi_oversold:
            return 'BUY'
        elif rsi_value > self.rsi_overbought:
            return 'SELL'
        else:
            return 'HOLD'
    
    def get_macd_signal(self, macd: float, signal: float, prev_macd: float = None, prev_signal: float = None) -> str:
        """
        Determina il segnale basato su MACD
        
        Returns:
            'BUY' se MACD incrocia al rialzo la signal line
            'SELL' se MACD incrocia al ribasso la signal line
            'HOLD' altrimenti
        """
        if macd is None or signal is None:
            return 'HOLD'
        
        # Se abbiamo dati precedenti, controlliamo l'incrocio
        if prev_macd is not None and prev_signal is not None:
            if prev_macd < prev_signal and macd > signal:
                return 'BUY'  # Incrocio rialzista
            elif prev_macd > prev_signal and macd < signal:
                return 'SELL'  # Incrocio ribassista
        
        # Altrimenti usiamo la posizione relativa
        if macd > signal:
            return 'BUY'
        elif macd < signal:
            return 'SELL'
        
        return 'HOLD'
    
    def get_combined_signal(self, signals: List[str], min_agreement: int = 2) -> Tuple[str, int]:
        """
        Combina più segnali per ottenere un segnale finale
        
        Args:
            signals: Lista di segnali ('BUY', 'SELL', 'HOLD')
            min_agreement: Numero minimo di segnali concordi richiesti
        
        Returns:
            Tupla (segnale_finale, numero_indicatori_concordi)
        """
        buy_count = signals.count('BUY')
        sell_count = signals.count('SELL')
        
        if buy_count >= min_agreement:
            return ('BUY', buy_count)
        elif sell_count >= min_agreement:
            return ('SELL', sell_count)
        else:
            return ('HOLD', 0)
    
    def analyze_fund(self, prices: pd.Series, level: int = 3) -> Dict:
        """
        Esegue analisi tecnica completa su un fondo
        
        Args:
            prices: Serie storica dei prezzi (più recente all'ultimo indice)
            level: Livello del fondo (1, 2, 3) - determina profondità analisi
        
        Returns:
            Dizionario con tutti gli indicatori e segnali
        """
        if len(prices) < self.ma_period:
            days_available = len(prices)
            days_needed = self.ma_period
            return {
                'current_price': prices.iloc[-1] if len(prices) > 0 else None,
                'ma': None,
                'rsi': None,
                'macd': None,
                'final_signal': 'HOLD',
                'signal_strength': 0,
                'data_status': 'insufficient',
                'error': f'Dati insufficienti: {days_available}/{days_needed} giorni. Attendere accumulo storico.'
            }
        
        current_price = prices.iloc[-1]
        
        # Calcola indicatori base (tutti i livelli)
        ma = self.calculate_ma(prices)
        ma_current = ma.iloc[-1]
        
        rsi = self.calculate_rsi(prices)
        rsi_current = rsi.iloc[-1]
        
        # Segnali base
        ma_signal = self.get_price_vs_ma_signal(current_price, ma_current)
        rsi_signal = self.get_rsi_signal(rsi_current)
        
        signals = [ma_signal, rsi_signal]
        
        # Per livelli 1 e 2, aggiungi MACD
        macd_data = None
        macd_signal = 'HOLD'
        
        if level <= 2 and len(prices) >= 26:
            macd_data = self.calculate_macd(prices)
            macd_current = macd_data['macd'].iloc[-1]
            signal_current = macd_data['signal'].iloc[-1]
            
            prev_macd = macd_data['macd'].iloc[-2] if len(macd_data['macd']) > 1 else None
            prev_signal = macd_data['signal'].iloc[-2] if len(macd_data['signal']) > 1 else None
            
            macd_signal = self.get_macd_signal(macd_current, signal_current, prev_macd, prev_signal)
            signals.append(macd_signal)
        
        # Determina segnale combinato
        min_agreement = 2 if level == 3 else 2
        final_signal, strength = self.get_combined_signal(signals, min_agreement)
        
        # Calcola distanza dal max 52 settimane
        max_52w = prices.tail(252).max() if len(prices) >= 252 else prices.max()
        pct_from_high = (current_price - max_52w) / max_52w * 100
        
        return {
            'current_price': round(current_price, 4),
            'ma': round(ma_current, 4) if not np.isnan(ma_current) else None,
            'ma_signal': ma_signal,
            'rsi': round(rsi_current, 2) if not np.isnan(rsi_current) else None,
            'rsi_signal': rsi_signal,
            'macd': {
                'line': round(macd_data['macd'].iloc[-1], 4) if macd_data else None,
                'signal': round(macd_data['signal'].iloc[-1], 4) if macd_data else None,
                'histogram': round(macd_data['histogram'].iloc[-1], 4) if macd_data else None
            },
            'macd_signal': macd_signal,
            'final_signal': final_signal,
            'signal_strength': strength,
            'total_signals': len(signals),
            'pct_from_high_52w': round(pct_from_high, 2),
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M')
        }
    
    def get_signal_emoji(self, signal: str) -> str:
        """Converte segnale in emoji"""
        if signal == 'BUY':
            return '🟢'
        elif signal == 'SELL':
            return '🔴'
        else:
            return '🟡'
    
    def format_analysis_summary(self, analysis: Dict, fund_name: str = '') -> str:
        """Formatta un riassunto dell'analisi per display/email"""
        emoji = self.get_signal_emoji(analysis['final_signal'])
        
        summary = f"""
{emoji} {fund_name}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Prezzo: €{analysis['current_price']:.2f}
MM{self.ma_period}: €{analysis['ma']:.2f} ({analysis['ma_signal']})
RSI: {analysis['rsi']:.1f} ({analysis['rsi_signal']})
MACD: {analysis['macd_signal']}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SEGNALE: {analysis['final_signal']} ({analysis['signal_strength']}/{analysis['total_signals']} indicatori)
Distanza da Max 52w: {analysis['pct_from_high_52w']:.1f}%
"""
        return summary


def test_analyzer():
    """Test dell'analizzatore tecnico"""
    analyzer = TechnicalAnalyzer()
    
    # Genera dati di test
    np.random.seed(42)
    dates = pd.date_range(end=datetime.now(), periods=100, freq='D')
    prices = pd.Series(100 + np.cumsum(np.random.randn(100) * 2), index=dates)
    
    # Analizza
    result = analyzer.analyze_fund(prices, level=1)
    print(analyzer.format_analysis_summary(result, "Test Fund"))


if __name__ == "__main__":
    test_analyzer()

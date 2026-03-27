"""
technical_analysis.py - Modulo per analisi tecnica dei fondi
=============================================================
Calcola indicatori tecnici:
- Media Mobile (MM20)
- Pendenza Media Mobile (Slope)
- RSI (Relative Strength Index)
- Bande di Bollinger
- Logica passaggio automatico livelli 3 → 2 → 1
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional


class TechnicalAnalyzer:
    """Classe per calcolare indicatori tecnici sui fondi"""

    # Profili parametri per tipo di asset
    PROFILES = {
        'equity': {
            'ma_period': 30,
            'rsi_period': 14,
            'rsi_oversold': 30,
            'rsi_overbought': 70,
            'bollinger_period': 20,
            'bollinger_std': 2.0,
            'days_above_ma': 3,
            'rsi_optimal_low': 50,
            'rsi_optimal_high': 72,
            'max_distance_from_ma': 6.0,
            'ma_signal_threshold': 2.0,  # % sopra/sotto MM per segnale BUY/SELL
            'bb_condition': 'upper_half',  # NAV nel 50% superiore delle bande BB
        },
        'bond': {
            'ma_period': 20,
            'rsi_period': 14,
            'rsi_oversold': 38,
            'rsi_overbought': 62,
            'bollinger_period': 20,
            'bollinger_std': 1.5,
            'days_above_ma': 3,
            'rsi_optimal_low': 48,
            'rsi_optimal_high': 58,
            'max_distance_from_ma': 1.5,
            'ma_signal_threshold': 0.5,  # % sopra/sotto MM per segnale BUY/SELL
            'bb_condition': 'above_ma',  # NAV sopra la media BB (= MM20)
        },
        # Profilo intermedio per Corporate, High Yield, Flessibili, EM Bonds
        'bond_hy': {
            'ma_period': 20,
            'rsi_period': 14,
            'rsi_oversold': 33,
            'rsi_overbought': 67,
            'bollinger_period': 20,
            'bollinger_std': 1.8,
            'days_above_ma': 3,
            'rsi_optimal_low': 52,
            'rsi_optimal_high': 63,
            'max_distance_from_ma': 3.0,
            'ma_signal_threshold': 1.0,
            'bb_condition': 'above_ma',  # NAV sopra la media BB (= MM20)
        },
    }

    @staticmethod
    def detect_asset_type(categoria: str) -> str:
        """
        Rileva il tipo di asset dalla categoria del fondo.

        Returns:
            'bond_hy' per Corporate/HY/Flessibili/EM bonds (volatilita' media)
            'bond' per Governativi/Breve Termine/Inflation Linked (bassa volatilita')
            'equity' per tutti gli azionari
        """
        if not categoria:
            return 'equity'
        cat_lower = categoria.lower()

        # Monetari → profilo bond (bassissima volatilita')
        if 'monetar' in cat_lower or 'money market' in cat_lower or 'liquidity' in cat_lower:
            return 'bond'

        # Prima controlla se e' obbligazionario
        is_bond = 'obblig' in cat_lower or 'bond' in cat_lower or 'fixed' in cat_lower or 'reddito' in cat_lower

        if not is_bond:
            return 'equity'

        # Distingui bond ad alta volatilita' (profilo intermedio)
        hy_keywords = ['corporate', 'high yield', 'flessibil', 'flexible', 'emerging',
                       'emergent', 'credit', 'convertib', 'subordinat', 'total return']
        if any(kw in cat_lower for kw in hy_keywords):
            return 'bond_hy'

        # Bond standard (governativi, breve termine, inflation linked)
        return 'bond'

    def __init__(self, config: dict = None, asset_type: str = 'equity'):
        """
        Inizializza l'analizzatore tecnico

        Args:
            config: Dizionario di configurazione (sovrascrive i default del profilo)
            asset_type: Tipo di asset ('equity' o 'bond')
        """
        self.asset_type = asset_type
        profile = self.PROFILES.get(asset_type, self.PROFILES['equity']).copy()

        # Sovrascritture da config
        if config:
            profile.update(config)

        self.ma_period = profile['ma_period']
        self.rsi_period = profile['rsi_period']
        self.rsi_oversold = profile['rsi_oversold']
        self.rsi_overbought = profile['rsi_overbought']
        self.bollinger_period = profile['bollinger_period']
        self.bollinger_std = profile['bollinger_std']
        self.days_above_ma = profile['days_above_ma']
        self.rsi_optimal_low = profile['rsi_optimal_low']
        self.rsi_optimal_high = profile['rsi_optimal_high']
        self.max_distance_from_ma = profile['max_distance_from_ma']
        self.ma_signal_threshold = profile.get('ma_signal_threshold', 2.0)
        self.bb_condition = profile.get('bb_condition', 'upper_half')
    
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

    def calculate_ma_slope(self, ma: pd.Series, days: int = 3) -> float:
        """
        Calcola la pendenza della Media Mobile

        Args:
            ma: Serie della media mobile
            days: Numero di giorni per calcolare la pendenza

        Returns:
            Pendenza (positiva = trend rialzista, negativa = ribassista)
        """
        if len(ma) < days + 1:
            return 0.0
        recent_ma = ma.dropna().tail(days + 1)
        if len(recent_ma) < 2:
            return 0.0
        # Pendenza = (MA oggi - MA N giorni fa) / MA N giorni fa * 100
        slope = (recent_ma.iloc[-1] - recent_ma.iloc[0]) / recent_ma.iloc[0] * 100
        return slope

    def calculate_bollinger_bands(self, prices: pd.Series, period: int = None,
                                   std_dev: float = None) -> Dict[str, pd.Series]:
        """
        Calcola le Bande di Bollinger

        Args:
            prices: Serie di prezzi
            period: Periodo per la media (default: self.bollinger_period)
            std_dev: Numero di deviazioni standard (default: self.bollinger_std)

        Returns:
            Dizionario con 'middle' (SMA), 'upper', 'lower', 'width'
        """
        period = period or self.bollinger_period
        std_dev = std_dev or self.bollinger_std

        middle = prices.rolling(window=period).mean()
        std = prices.rolling(window=period).std()

        upper = middle + (std * std_dev)
        lower = middle - (std * std_dev)
        # Width = ampiezza delle bande (per rilevare squeeze/espansione)
        width = (upper - lower) / middle * 100

        return {
            'middle': middle,
            'upper': upper,
            'lower': lower,
            'width': width
        }

    def is_bollinger_expanding(self, bollinger: Dict[str, pd.Series], days: int = 3) -> bool:
        """
        Verifica se le Bande di Bollinger si stanno espandendo

        Args:
            bollinger: Dizionario con dati Bollinger
            days: Giorni da confrontare

        Returns:
            True se le bande si stanno allargando
        """
        width = bollinger['width'].dropna()
        if len(width) < days + 1:
            return False
        recent_width = width.tail(days + 1)
        # Espansione = width attuale > width di N giorni fa
        return recent_width.iloc[-1] > recent_width.iloc[0]

    def count_days_above_ma(self, prices: pd.Series, ma: pd.Series, max_days: int = 10) -> int:
        """
        Conta i giorni consecutivi in cui il prezzo è sopra la MM

        Args:
            prices: Serie di prezzi
            ma: Serie della media mobile
            max_days: Massimo giorni da controllare

        Returns:
            Numero di giorni consecutivi sopra la MM
        """
        if len(prices) < 2 or len(ma) < 2:
            return 0

        count = 0
        for i in range(1, min(max_days + 1, len(prices))):
            idx = -i
            if len(prices) >= abs(idx) and len(ma) >= abs(idx):
                price = prices.iloc[idx]
                ma_val = ma.iloc[idx]
                if pd.notna(ma_val) and price > ma_val:
                    count += 1
                else:
                    break
            else:
                break
        return count

    def count_rising_days(self, prices: pd.Series, window: int = 5) -> int:
        """
        Conta quanti degli ultimi N giorni il NAV e' salito (non consecutivi).
        Piu' robusto dei giorni consecutivi: basta 1 giorno di pausa
        per non azzerare il conteggio.

        Args:
            prices: Serie di prezzi
            window: Finestra di giorni da controllare

        Returns:
            Numero di giorni in salita su window
        """
        if len(prices) < 2:
            return 0
        count = 0
        for i in range(1, min(window + 1, len(prices))):
            if prices.iloc[-i] > prices.iloc[-i - 1]:
                count += 1
            else:
                break
        return count

    def suggest_level(self, prices: pd.Series, current_level: int = 3) -> Dict:
        """
        Suggerisce il livello appropriato per un fondo - Schema L1 Pro

        Logica L1 Pro (4 condizioni):
        1. TREND: Prezzo > MM30 per 3+ gg, slope positivo
        2. MOMENTUM: RSI 55-68
        3. DISTANZA: NAV < 6% sopra MM30 (non tirato)
        4. SETUP-B: Prezzo oggi > Prezzo 5 giorni fa

        Livelli:
        - Livello 3: Prezzo sotto MM (monitoraggio passivo)
        - Livello 2: Prezzo > MM per 3+ giorni consecutivi
        - Livello 1: Tutte e 4 le condizioni L1 Pro soddisfatte

        Args:
            prices: Serie storica dei prezzi
            current_level: Livello attuale del fondo

        Returns:
            Dizionario con livello suggerito e motivazione
        """
        if len(prices) < self.ma_period:
            return {
                'suggested_level': current_level,
                'reason': 'Dati insufficienti per analisi',
                'conditions': {}
            }

        # Calcola indicatori
        ma = self.calculate_ma(prices)
        ma_current = ma.iloc[-1]
        current_price = prices.iloc[-1]

        rsi = self.calculate_rsi(prices)
        rsi_current = rsi.iloc[-1] if len(rsi) > 0 and pd.notna(rsi.iloc[-1]) else 50

        ma_slope = self.calculate_ma_slope(ma)
        days_above = self.count_days_above_ma(prices, ma)

        bollinger = self.calculate_bollinger_bands(prices)
        bb_upper = bollinger['upper'].iloc[-1] if pd.notna(bollinger['upper'].iloc[-1]) else None

        # Distanza % dal MM20
        distance_from_ma = ((current_price - ma_current) / ma_current * 100) if pd.notna(ma_current) and ma_current != 0 else 0

        # Giorni in salita su ultimi 5
        rising_days = self.count_rising_days(prices, window=5)

        # === CONDIZIONI L1 PRO ===
        price_above_ma = current_price > ma_current if pd.notna(ma_current) else False
        price_above_ma_3days = days_above >= self.days_above_ma
        slope_positive = ma_slope > 0
        distance_ok = distance_from_ma < self.max_distance_from_ma  # < 6%

        # Condizione 1: TREND (prezzo > MM30 3+gg, slope+)
        trend_ok = price_above_ma_3days and slope_positive

        # Condizione 2: MOMENTUM (RSI 55-68)
        rsi_optimal = self.rsi_optimal_low <= rsi_current <= self.rsi_optimal_high

        # Condizione 3: VOLATILITA (posizione nelle Bande di Bollinger)
        # Equity: NAV nel 50% superiore delle bande (midpoint tra MA e banda superiore)
        # Bond/Bond_HY: NAV sopra la media BB (= MM20)
        if self.bb_condition == 'upper_half' and bb_upper is not None and ma_current is not None:
            bb_midpoint = (ma_current + bb_upper) / 2
            nav_above_upper_bb = current_price >= bb_midpoint
        elif self.bb_condition == 'above_ma' and ma_current is not None:
            nav_above_upper_bb = current_price > ma_current
        else:
            nav_above_upper_bb = False

        # Condizione 4a: SETUP-A (NAV in salita 3+ giorni consecutivi)
        nav_rising_original = rising_days >= 3

        # Condizione 4b: SETUP-B (prezzo oggi > prezzo 5 giorni fa — direzione netta)
        if len(prices) >= 6:
            price_5d_ago = prices.iloc[-6]
            nav_rising_alt = float(current_price) > float(price_5d_ago)
            pct_vs_5d = ((float(current_price) - float(price_5d_ago)) / float(price_5d_ago) * 100) if float(price_5d_ago) != 0 else 0.0
        else:
            nav_rising_alt = False
            pct_vs_5d = 0.0

        # Solo Setup B (più veloce, compensa lentezza MM30)
        nav_rising = nav_rising_alt

        conditions = {
            'price_above_ma': price_above_ma,
            'days_above_ma': days_above,
            'price_above_ma_3days': price_above_ma_3days,
            'ma_slope': round(ma_slope, 3),
            'slope_positive': slope_positive,
            'distance_from_ma': round(distance_from_ma, 2),
            'distance_ok': distance_ok,
            'rsi': round(rsi_current, 1),
            'rsi_optimal': rsi_optimal,
            'bb_upper': round(bb_upper, 4) if bb_upper else None,
            'nav_above_upper_bb': nav_above_upper_bb,
            'rising_days': rising_days,
            'nav_rising_original': nav_rising_original,
            'nav_rising_alt': nav_rising_alt,
            'pct_vs_5d': round(pct_vs_5d, 2),
            'nav_rising': nav_rising,  # OR combinato (usato per buy_count)
            # Le 4 condizioni aggregate per buy_count
            'trend_ok': trend_ok,
        }

        # Determina livello suggerito
        if current_level == 1:
            # USCITA ASIMMETRICA: in L1 si resta finché prezzo > MM20.
            # Solo una rottura della MM20 giustifica l'uscita (non il deterioramento di RSI/BB).
            if not price_above_ma:
                suggested = 3
                reason = f'Uscita L1: Prezzo sceso sotto MM30 dopo {days_above} giorni'
            else:
                suggested = 1
                reason = f'Mantenuto L1 — Prezzo sopra MM30 ({distance_from_ma:.1f}% dalla MM, RSI {rsi_current:.0f})'
        elif not price_above_ma:
            suggested = 3
            reason = 'Prezzo sotto Media Mobile'
        elif trend_ok and rsi_optimal and distance_ok and nav_rising:
            suggested = 1
            reason = f'BUY ALERT L1 Pro: Trend OK (dist {distance_from_ma:.1f}%), RSI {rsi_current:.0f}, dist<6%, +{pct_vs_5d:.1f}% vs 5gg'
        elif price_above_ma_3days:
            suggested = 2
            reason = f'Prezzo sopra MM da {days_above} giorni consecutivi'
        elif price_above_ma:
            suggested = 3
            reason = f'Prezzo sopra MM da {days_above} giorni (servono {self.days_above_ma})'
        else:
            suggested = 3
            reason = 'Monitoraggio passivo'

        return {
            'suggested_level': suggested,
            'current_level': current_level,
            'level_change': suggested != current_level,
            'reason': reason,
            'conditions': conditions
        }
    
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
        threshold = self.ma_signal_threshold

        if pct_diff > threshold:
            return 'BUY'
        elif pct_diff < -threshold:
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
            level: Livello attuale del fondo (1, 2, 3)

        Returns:
            Dizionario con tutti gli indicatori, segnali e livello suggerito
        """
        if len(prices) < self.ma_period:
            days_available = len(prices)
            days_needed = self.ma_period
            # Calcola le percentuali possibili anche con dati insufficienti per MM
            pct_1d = round(float((prices.iloc[-1] - prices.iloc[-2]) / prices.iloc[-2] * 100), 2) if len(prices) >= 2 else None
            pct_1w = round(float((prices.iloc[-1] - prices.iloc[-6]) / prices.iloc[-6] * 100), 2) if len(prices) >= 6 else None
            pct_1m = round(float((prices.iloc[-1] - prices.iloc[-22]) / prices.iloc[-22] * 100), 2) if len(prices) >= 22 else None
            return {
                'current_price': float(prices.iloc[-1]) if len(prices) > 0 else None,
                'ma': None,
                'ma_slope': None,
                'rsi': None,
                'bollinger': None,
                'days_above_ma': 0,
                'final_signal': 'HOLD',
                'signal_strength': 0,
                'suggested_level': level,
                'level_change': False,
                'level_reason': f'Dati insufficienti: {days_available}/{days_needed} giorni',
                'buy_count': 0,
                'pct_change_1d': pct_1d,
                'pct_change_1w': pct_1w,
                'pct_change_1m': pct_1m,
                'data_status': 'insufficient',
                'error': f'Dati insufficienti: {days_available}/{days_needed} giorni. Attendere accumulo storico.'
            }

        current_price = prices.iloc[-1]

        # Variazioni percentuali (float() per evitare numpy.float64)
        pct_1d = round(float((prices.iloc[-1] - prices.iloc[-2]) / prices.iloc[-2] * 100), 2) if len(prices) >= 2 else None
        pct_1w = round(float((prices.iloc[-1] - prices.iloc[-6]) / prices.iloc[-6] * 100), 2) if len(prices) >= 6 else None
        pct_1m = round(float((prices.iloc[-1] - prices.iloc[-22]) / prices.iloc[-22] * 100), 2) if len(prices) >= 22 else None

        # Calcola indicatori
        ma = self.calculate_ma(prices)
        ma_current = ma.iloc[-1]
        ma_slope = self.calculate_ma_slope(ma)

        rsi = self.calculate_rsi(prices)
        rsi_current = rsi.iloc[-1]

        bollinger = self.calculate_bollinger_bands(prices)
        bb_width = bollinger['width'].iloc[-1] if pd.notna(bollinger['width'].iloc[-1]) else 0
        bb_expanding = self.is_bollinger_expanding(bollinger)

        days_above = self.count_days_above_ma(prices, ma)

        # Segnali per compatibilità
        ma_signal = self.get_price_vs_ma_signal(current_price, ma_current)
        rsi_signal = self.get_rsi_signal(rsi_current)
        signals = [ma_signal, rsi_signal]

        # MACD per livelli 1 e 2
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

        # Segnale combinato
        final_signal, strength = self.get_combined_signal(signals, min_agreement=2)

        # Suggerimento livello automatico
        level_suggestion = self.suggest_level(prices, current_level=level)

        # Conteggio condizioni BUY L1 Pro (4 condizioni)
        lc = level_suggestion['conditions']
        buy_count = sum([
            lc.get('trend_ok', False),           # 1. Trend: >MM30 3gg + slope+
            lc.get('rsi_optimal', False),         # 2. Momentum: RSI 55-68
            lc.get('distance_ok', False),         # 3. Distanza: <6% dalla MM30
            lc.get('nav_rising_alt', False)       # 4. Setup B: prezzo oggi > prezzo 5gg fa
        ])

        # Calcola distanza dal max 52 settimane
        max_52w = prices.tail(252).max() if len(prices) >= 252 else prices.max()
        pct_from_high = (current_price - max_52w) / max_52w * 100

        return {
            'current_price': round(current_price, 4),
            'ma': round(ma_current, 4) if pd.notna(ma_current) else None,
            'ma_slope': round(ma_slope, 3),
            'ma_signal': ma_signal,
            'rsi': round(rsi_current, 2) if pd.notna(rsi_current) else None,
            'rsi_signal': rsi_signal,
            'bollinger': {
                'upper': round(bollinger['upper'].iloc[-1], 4) if pd.notna(bollinger['upper'].iloc[-1]) else None,
                'middle': round(bollinger['middle'].iloc[-1], 4) if pd.notna(bollinger['middle'].iloc[-1]) else None,
                'lower': round(bollinger['lower'].iloc[-1], 4) if pd.notna(bollinger['lower'].iloc[-1]) else None,
                'width': round(bb_width, 2),
                'expanding': bb_expanding
            },
            'days_above_ma': days_above,
            'macd': {
                'line': round(macd_data['macd'].iloc[-1], 4) if macd_data else None,
                'signal': round(macd_data['signal'].iloc[-1], 4) if macd_data else None,
                'histogram': round(macd_data['histogram'].iloc[-1], 4) if macd_data else None
            },
            'macd_signal': macd_signal,
            'final_signal': final_signal,
            'signal_strength': strength,
            'total_signals': len(signals),
            'suggested_level': level_suggestion['suggested_level'],
            'level_change': level_suggestion['level_change'],
            'level_reason': level_suggestion['reason'],
            'level_conditions': level_suggestion['conditions'],
            'buy_count': buy_count,
            'pct_change_1d': pct_1d,
            'pct_change_1w': pct_1w,
            'pct_change_1m': pct_1m,
            'pct_from_high_52w': round(pct_from_high, 2),
            'data_status': 'ok',
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
        
        price = analysis.get('current_price')
        ma = analysis.get('ma')
        rsi = analysis.get('rsi')
        pct = analysis.get('pct_from_high_52w')

        summary = f"""
{emoji} {fund_name}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Prezzo: €{price:.2f if price else 'N/A'}
MM{self.ma_period}: €{ma:.2f if ma else 'N/A'} ({analysis.get('ma_signal', 'N/A')})
RSI: {rsi:.1f if rsi else 'N/A'} ({analysis.get('rsi_signal', 'N/A')})
MACD: {analysis.get('macd_signal', 'N/A')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SEGNALE: {analysis.get('final_signal', 'N/A')} ({analysis.get('signal_strength', 0)}/{analysis.get('total_signals', 0)} indicatori)
Distanza da Max 52w: {pct:.1f if pct else 'N/A'}%
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

# ==========================================================
# [volatility_engine.py] - Part 1/2 부 (상반부)
# ⚠️ V3.2 패치: 기초지수 1년 ATR 절대 진폭 고정 및 공포지수 방향타 스위치 엔진 탑재
# 💡 [V24.09 패치] 야후 파이낸스 교착(Deadlock) 방어용 timeout=5 전면 이식 완료
# 💡 [V24.11 패치] 클래스 래퍼(VolatilityEngine) 구조 도입 및 calculate_weight 공통 인터페이스 신설
# ==========================================================
import yfinance as yf
import pandas as pd
import numpy as np
import os
import json
import tempfile

CACHE_FILE = "data/volatility_cache.json"

def _load_cache(key, default_val):
    """ 🛡️ 통신 장애 시 직전 영업일의 1년 평균값을 로드하는 1차 방어막 """
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                data = json.load(f)
                val = data.get(key)
                if val is not None and float(val) > 0:
                    return float(val)
        except Exception:
            pass
    return default_val

def _save_cache(key, value):
    """ 🛡️ 원자적 쓰기(fsync)를 통해 무결성이 보장된 로컬 캐시 저장 """
    data = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                data = json.load(f)
        except Exception:
            pass
    
    data[key] = value
    
    try:
        dir_name = os.path.dirname(CACHE_FILE)
        if dir_name and not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
            
        fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(data, f)
            f.flush()
            os.fsync(fd)
        os.replace(temp_path, CACHE_FILE)
    except Exception as e:
        print(f"⚠️ [Engine] 캐시 저장 실패: {e}")

def _calculate_1y_atr(ticker, cache_key, default_atr):
    """ 💡 기초지수의 최근 1년(252일) ATR14 평균값을 동적으로 연산하여 반환 """
    try:
        df = yf.download(ticker, period="2y", interval="1d", progress=False, timeout=5)
        if df.empty:
            return _load_cache(cache_key, default_atr)
            
        if hasattr(df.columns, 'droplevel'):
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
                
        df['Prev_Close'] = df['Close'].shift(1)
        
        tr1 = df['High'] - df['Low']
        tr2 = (df['High'] - df['Prev_Close']).abs()
        tr3 = (df['Low'] - df['Prev_Close']).abs()
        
        df['TR'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df['ATR14'] = df['TR'].rolling(window=14).mean()
        df['ATR14_pct'] = (df['ATR14'] / df['Close']) * 100
        
        df_valid = df.dropna(subset=['ATR14_pct'])
        df_1y = df_valid.tail(252)
        
        if df_1y.empty:
            return _load_cache(cache_key, default_atr)
            
        atr_1y_avg = float(df_1y['ATR14_pct'].mean())
        if pd.isna(atr_1y_avg) or atr_1y_avg <= 0:
            raise ValueError("Invalid ATR")
            
        _save_cache(cache_key, atr_1y_avg)
        return atr_1y_avg
        
    except Exception as e:
        print(f"⚠️ [Engine] {ticker} ATR 연산 오류: {e}")
        return _load_cache(cache_key, default_atr)
# ==========================================================
# [volatility_engine.py] - Part 2/2 부 (하반부)
# ⚠️ V3.2 패치: 기초지수 1년 ATR 절대 진폭 고정 및 공포지수 방향타 스위치 엔진 탑재
# 💡 [V24.11 패치] 클래스 래퍼(VolatilityEngine) 구조 도입 및 calculate_weight 인터페이스 신설
# ==========================================================

def get_tqqq_target_drop():
    """ [ TQQQ 스나이퍼 ] 실시간 VXN과 QQQ 1년 ATR을 결합하여 타격선 계산 """
    try:
        vxn_data = yf.download("^VXN", period="2y", interval="1d", progress=False, timeout=5)
        if vxn_data.empty: 
            return round(-(1.65 * 3), 2)
            
        if hasattr(vxn_data.columns, 'droplevel'):
            if isinstance(vxn_data.columns, pd.MultiIndex):
                vxn_data.columns = vxn_data.columns.droplevel(1)
                
        valid_closes = vxn_data['Close'].dropna()
        valid_closes_1y = valid_closes.tail(252)
        
        if valid_closes_1y.empty:
            return round(-(1.65 * 3), 2)
            
        current_vxn = float(valid_closes_1y.iloc[-1])
        
        try:
            mean_vxn = float(valid_closes_1y.mean())
            if pd.isna(mean_vxn) or mean_vxn <= 0:
                raise ValueError("Invalid Mean")
            _save_cache("VXN_MEAN", mean_vxn)
        except Exception:
            mean_vxn = _load_cache("VXN_MEAN", 20.0)
        
        weight = current_vxn / mean_vxn
        
        # 💡 [V3.2 패치] 1배수 기초지수 QQQ의 1년 ATR * 3배 동적 스케일링 (가중치 배제 절대 진폭 고정)
        qqq_1y_atr = _calculate_1y_atr("QQQ", "QQQ_ATR_1Y", 1.65)
        base_amp = round(-(qqq_1y_atr * 3), 2)
        
        target_drop = base_amp
        return target_drop
        
    except Exception as e:
        print(f"❌ VXN 스캔 오류: {e}")
        return round(-(1.65 * 3), 2)

def get_soxl_target_drop():
    """ [ SOXL 스나이퍼 ] SOXX HV와 SOXX 1년 ATR을 결합하여 타격선 계산 """
    try:
        soxx_data = yf.download("SOXX", period="2y", interval="1d", progress=False, timeout=5)
        if soxx_data.empty or len(soxx_data) < 21: 
            return round(-(2.93 * 3), 2)
        
        if hasattr(soxx_data.columns, 'droplevel'):
            if isinstance(soxx_data.columns, pd.MultiIndex):
                soxx_data.columns = soxx_data.columns.droplevel(1)
                
        closes = soxx_data['Close'].dropna()
        log_returns = np.log(closes / closes.shift(1))
        hv_20d = log_returns.rolling(window=20).std() * np.sqrt(252) * 100
        
        valid_hvs = hv_20d.dropna()
        valid_hvs_1y = valid_hvs.tail(252)
        
        if valid_hvs_1y.empty:
            return round(-(2.93 * 3), 2)
            
        latest_hv = float(valid_hvs_1y.iloc[-1])
        
        try:
            mean_hv = float(valid_hvs_1y.mean())
            if pd.isna(mean_hv) or mean_hv <= 0:
                raise ValueError("Invalid Mean")
            _save_cache("SOXX_HV_MEAN", mean_hv)
        except Exception:
            mean_hv = _load_cache("SOXX_HV_MEAN", 25.0)
        
        weight = latest_hv / mean_hv
        
        # 💡 [V3.2 패치] 1배수 기초지수 SOXX의 1년 ATR * 3배 동적 스케일링 (가중치 배제 절대 진폭 고정)
        soxx_1y_atr = _calculate_1y_atr("SOXX", "SOXX_ATR_1Y", 2.93)
        base_amp = round(-(soxx_1y_atr * 3), 2)
        
        target_drop = base_amp
        return target_drop
        
    except Exception as e:
        print(f"❌ SOXX HV 연산 오류: {e}")
        return round(-(2.93 * 3), 2)

def get_tqqq_target_drop_full():
    """ 💡 [텔레그램 UI 표시용] TQQQ 상세 데이터 반환 (4개 파라미터 리턴) """
    try:
        vxn_data = yf.download("^VXN", period="2y", interval="1d", progress=False, timeout=5)
        
        if vxn_data.empty: 
            fallback_amp = round(-(1.65 * 3), 2)
            return 0.0, 1.0, fallback_amp, fallback_amp
            
        if hasattr(vxn_data.columns, 'droplevel'):
            if isinstance(vxn_data.columns, pd.MultiIndex):
                vxn_data.columns = vxn_data.columns.droplevel(1)
                
        valid_closes = vxn_data['Close'].dropna()
        valid_closes_1y = valid_closes.tail(252)
        
        if valid_closes_1y.empty:
            fallback_amp = round(-(1.65 * 3), 2)
            return 0.0, 1.0, fallback_amp, fallback_amp
            
        current_vxn = float(valid_closes_1y.iloc[-1])
        
        try:
            mean_vxn = float(valid_closes_1y.mean())
            if pd.isna(mean_vxn) or mean_vxn <= 0:
                raise ValueError("Invalid Mean")
            _save_cache("VXN_MEAN", mean_vxn)
        except Exception:
            mean_vxn = _load_cache("VXN_MEAN", 20.0)
            
        weight = current_vxn / mean_vxn
        
        # 💡 [V3.2 패치] 절대 진폭(base_amp)을 target_drop과 1:1로 일치시켜 마스터 스위치 로직과 분리
        qqq_1y_atr = _calculate_1y_atr("QQQ", "QQQ_ATR_1Y", 1.65)
        base_amp = round(-(qqq_1y_atr * 3), 2)
        target_drop = base_amp
        
        return current_vxn, weight, target_drop, base_amp
        
    except Exception as e:
        print(f"❌ VXN 상세 스캔 오류: {e}")
        fallback_amp = round(-(1.65 * 3), 2)
        return 0.0, 1.0, fallback_amp, fallback_amp

def get_soxl_target_drop_full():
    """ 💡 [텔레그램 UI 표시용] SOXL 상세 데이터 반환 (4개 파라미터 리턴) """
    try:
        soxx_data = yf.download("SOXX", period="2y", interval="1d", progress=False, timeout=5)
        if soxx_data.empty or len(soxx_data) < 21: 
            fallback_amp = round(-(2.93 * 3), 2)
            return 0.0, 1.0, fallback_amp, fallback_amp
        
        if hasattr(soxx_data.columns, 'droplevel'):
            if isinstance(soxx_data.columns, pd.MultiIndex):
                soxx_data.columns = soxx_data.columns.droplevel(1)
                
        closes = soxx_data['Close'].dropna()
        log_returns = np.log(closes / closes.shift(1))
        hv_20d = log_returns.rolling(window=20).std() * np.sqrt(252) * 100
        
        valid_hvs = hv_20d.dropna()
        valid_hvs_1y = valid_hvs.tail(252)
        
        if valid_hvs_1y.empty:
            fallback_amp = round(-(2.93 * 3), 2)
            return 0.0, 1.0, fallback_amp, fallback_amp
            
        latest_hv = float(valid_hvs_1y.iloc[-1])
        
        try:
            mean_hv = float(valid_hvs_1y.mean())
            if pd.isna(mean_hv) or mean_hv <= 0:
                raise ValueError("Invalid Mean")
            _save_cache("SOXX_HV_MEAN", mean_hv)
        except Exception:
            mean_hv = _load_cache("SOXX_HV_MEAN", 25.0)
        
        weight = latest_hv / mean_hv
        
        # 💡 [V3.2 패치] 절대 진폭(base_amp)을 target_drop과 1:1로 일치시켜 마스터 스위치 로직과 분리
        soxx_1y_atr = _calculate_1y_atr("SOXX", "SOXX_ATR_1Y", 2.93)
        base_amp = round(-(soxx_1y_atr * 3), 2)
        target_drop = base_amp
        
        return latest_hv, weight, target_drop, base_amp
        
    except Exception as e:
        print(f"❌ SOXX HV 상세 연산 오류: {e}")
        fallback_amp = round(-(2.93 * 3), 2)
        return 0.0, 1.0, fallback_amp, fallback_amp

# ==========================================================
# 💡 [V24.11 핵심 수술] main.py와 완벽한 통신을 위한 클래스 래퍼 신설
# ==========================================================
class VolatilityEngine:
    def __init__(self):
        pass
        
    def calculate_weight(self, ticker):
        """ 
        main.py의 scheduled_volatility_scan 함수가 호출하는 공통 인터페이스.
        기존 0.85/1.15 하드코딩을 대체하여 팩트 기반의 가중치를 반환합니다.
        """
        try:
            if ticker == "TQQQ":
                _, weight, _, _ = get_tqqq_target_drop_full()
                return {'weight': float(weight)}
            elif ticker == "SOXL":
                _, weight, _, _ = get_soxl_target_drop_full()
                return {'weight': float(weight)}
            else:
                return {'weight': 1.0}
        except Exception as e:
            print(f"⚠️ [VolatilityEngine] {ticker} 가중치 산출 래퍼 오류: {e}")
            return {'weight': 1.0}

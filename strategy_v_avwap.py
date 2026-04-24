# ==========================================================
# [strategy_v_avwap.py]
# 💡 V-REV 하이브리드 전용 차세대 AVWAP 스나이퍼 플러그인 (Dual-Referencing)
# ⚠️ 초공격형 당일 청산 암살자 (V-REV 잉여 현금 100% 몰빵 & -3% 하드스탑)
# ⚠️ 옵션 B 아키텍처: 기초자산(SOXX) 시그널 스캔 + 파생상품(SOXL) 미시구조 타격
# 🚨 [V28.50 NEW] 사용자 맞춤형 조기 퇴근(Early Exit) 듀얼 코어 이식 완비
# 🚨 [PEP 8 포맷팅 패치] 미사용 변수(time_0930) 소각 (Ruff F841 교정 완료)
# 🚨 [V25.23 디커플링] KIS API 하드코딩 종속성 적출 및 범용 1분봉 컬럼 정규화 완비
# 🚨 [V27.06 긴급 수술] NameError (#ffffff) 소각 및 ZeroDivision 방어막 구축
# 🚨 [V27.07 그랜드 수술] 코파일럿 합작 - 20MA NaN 붕괴, VWAP 침묵, 10시 누수, 소수점 주문 4대 맹점 전면 철거
# 🚨 [V27.16 핫픽스] 20MA 시차 왜곡 차단, RVOL 정수 파싱, 소수점 매도 차단 및 ZeroDivision 영구 차단 완비
# 🚨 [V29.03 팩트 수술] 기억상실(Amnesia) 엣지 케이스 방어막: 서버 재부팅 시 AVWAP 상태(매수/셧다운)가 증발하는 현상을 원천 차단하기 위해 파일 기반 영속성 저장(Persistence) 엔진 탑재.
# MODIFIED: [V29.12 핫픽스] 스케줄러 매개변수 불일치 런타임 붕괴 원천 차단 및 Safe Casting 다형성(Polymorphism) 지원
# MODIFIED: [V29.13 핫픽스] 데이터 기아(Data Starvation) 방어막 이식 및 다형성 맵핑 2차 강화
# NEW: [UI 패치] 텔레그램 /sync 실시간 레이더 렌더링을 위한 팩트 데이터(gap_pct) 반환 패키징
# 🚨 [V30.05 팩트 수술] AVWAP 암살자 모니터링 타임라인 1시간 연장 (14:00 -> 15:00 EST) 및 조건문 교정 완비.
# MODIFIED: [V30.09 핫픽스] pytz 영구 적출 및 ZoneInfo('America/New_York') 이식으로 LMT 버그 차단
# ==========================================================
import logging
import datetime
# MODIFIED: [LMT 오차 방어를 위해 pytz를 적출하고 ZoneInfo 도입]
# import pytz
from zoneinfo import ZoneInfo
import math
import yfinance as yf
import pandas as pd
import json
import os
import tempfile

class VAvwapHybridPlugin:
    def __init__(self):
        self.plugin_name = "AVWAP_HYBRID_DUAL"
        self.leverage = 3.0             
        self.base_stop_loss_pct = 0.01  
        self.base_target_pct = 0.01     
        self.base_dip_buy_pct = 0.0067  
        
    # ==========================================================
    # 🚨 [V29.03 NEW] AVWAP 상태 영속성(Persistence) 듀얼 캐시 엔진
    # 서버 다운, 데몬 재시작 등에도 암살자가 타점과 셧다운 여부를 기억하도록 박제
    # ==========================================================
    def _get_state_file(self, ticker, now_est):
        today_str = now_est.strftime('%Y%m%d')
        return f"data/avwap_state_{today_str}_{ticker}.json"

    def load_state(self, ticker, now_est):
        file_path = self._get_state_file(ticker, now_est)
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def save_state(self, ticker, now_est, state_data):
        file_path = self._get_state_file(ticker, now_est)
        try:
            dir_name = os.path.dirname(file_path)
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
            
            # 원자적(Atomic) 쓰기로 파일 깨짐 원천 차단
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, file_path)
        except Exception as e:
            logging.error(f"🚨 [V_AVWAP] 상태 저장 실패: {e}")

    def fetch_macro_context(self, base_ticker):
        try:
            tkr = yf.Ticker(base_ticker)
            df_daily = tkr.history(period="2mo", interval="1d", timeout=5)
            df_30m = tkr.history(period="60d", interval="30m", timeout=5)

            today_est = datetime.datetime.now(ZoneInfo('America/New_York')).date()
            if df_daily.index.tz is None:
                df_daily.index = df_daily.index.tz_localize('UTC').tz_convert('America/New_York')
            else:
                df_daily.index = df_daily.index.tz_convert('America/New_York')

            df_past = df_daily[df_daily.index.date < today_est]

            if df_past.empty or len(df_past) < 20 or df_30m.empty:
                return None

            prev_close = float(df_past['Close'].iloc[-1])
            ma_20 = float(df_past['Close'].rolling(window=20).mean().iloc[-1])

            if math.isnan(ma_20) or math.isnan(prev_close):
                return None

            if df_30m.index.tz is None:
                df_30m.index = df_30m.index.tz_localize('UTC').tz_convert('America/New_York')
            else:
                df_30m.index = df_30m.index.tz_convert('America/New_York')

            first_30m = df_30m[df_30m.index.time == datetime.time(9, 30)]
            past_first_30m = first_30m[first_30m.index.date < today_est]
            
            if len(past_first_30m) >= 20:
                avg_vol_20 = float(past_first_30m['Volume'].tail(20).mean())
            elif len(past_first_30m) > 0:
                avg_vol_20 = float(past_first_30m['Volume'].mean())
            else:
                avg_vol_20 = 0.0

            return {
                "prev_close": prev_close,
                "ma_20": ma_20,
                "avg_vol_20": avg_vol_20
            }
            
        except Exception as e:
            logging.error(f"🚨 [V_AVWAP] YF 기초자산 매크로 컨텍스트 추출 실패 ({base_ticker}): {e}")
            return None

    def get_decision(self, base_ticker=None, exec_ticker=None, base_curr_p=0.0, exec_curr_p=0.0, base_day_open=0.0, avwap_avg_price=0.0, avwap_qty=0, avwap_alloc_cash=0.0, context_data=None, df_1min_base=None, now_est=None, early_exit_mode=False, early_target_profit=0.025, **kwargs):
        
        df_1min_base = df_1min_base if df_1min_base is not None else kwargs.get('base_df')
        avwap_qty = avwap_qty if avwap_qty != 0 else kwargs.get('current_qty', 0)
        
        base_curr_p = base_curr_p if base_curr_p > 0 else kwargs.get('base_curr_p', 0.0)
        exec_curr_p = exec_curr_p if exec_curr_p > 0 else kwargs.get('exec_curr_p', 0.0)
        base_day_open = base_day_open if base_day_open > 0 else kwargs.get('base_day_open', 0.0)
        avwap_avg_price = avwap_avg_price if avwap_avg_price > 0 else kwargs.get('avwap_avg_price', kwargs.get('avg_price', 0.0))
        avwap_alloc_cash = avwap_alloc_cash if avwap_alloc_cash > 0 else kwargs.get('alloc_cash', kwargs.get('avwap_alloc_cash', 0.0))
        
        if now_est is None:
            now_est = datetime.datetime.now(ZoneInfo('America/New_York'))
            
        if base_curr_p <= 0.0 and df_1min_base is not None and not df_1min_base.empty:
            try: base_curr_p = float(df_1min_base['close'].iloc[-1])
            except Exception: pass
            
        curr_time = now_est.time()
        
        time_1000 = datetime.time(10, 0)
        # 🚨 [V30.05 팩트 수술] 매수 윈도우 시간 연장 (14:00 -> 15:00)
        time_1500 = datetime.time(15, 0)
        time_1430 = datetime.time(14, 30)
        time_1555 = datetime.time(15, 55)

        base_vwap = base_curr_p
        base_current_30m_vol = 0.0
        vwap_success = False 
        
        if df_1min_base is not None and not df_1min_base.empty:
            try:
                df = df_1min_base.copy()
                df['tp'] = (df['high'].astype(float) + df['low'].astype(float) + df['close'].astype(float)) / 3.0
                df['vol'] = df['volume'].astype(float)
                df['vol_tp'] = df['tp'] * df['vol']
                
                cum_vol = df['vol'].sum()
                if cum_vol > 0:
                    base_vwap = df['vol_tp'].sum() / cum_vol
                    vwap_success = True
                
                if 'time_est' in df.columns:
                    def _to_hhmiss_int(t):
                        if isinstance(t, (datetime.time, datetime.datetime)):
                            return t.hour * 10000 + t.minute * 100 + t.second
                        if isinstance(t, pd.Timestamp):
                            return t.hour * 10000 + t.minute * 100 + t.second
                        s = str(t).replace(':', '').replace(' ', '')[:6].zfill(6)
                        try:
                            return int(s)
                        except ValueError:
                            return -1

                    df['time_int'] = df['time_est'].apply(_to_hhmiss_int)
                    mask_30m = (df['time_int'] >= 93000) & (df['time_int'] < 100000)
                    base_current_30m_vol = df.loc[mask_30m, 'vol'].sum()
            except Exception as e:
                logging.error(f"🚨 [V_AVWAP] 기초자산 1분봉 VWAP 연산 실패: {e}")

        def _build_res(action, reason, qty=0, target_price=0.0):
            gap_pct = ((base_curr_p - base_vwap) / base_vwap * 100.0) if base_vwap > 0 else 0.0
            return {
                'action': action,
                'reason': reason,
                'qty': qty,
                'target_price': target_price,
                'vwap': base_vwap,
                'base_curr_p': base_curr_p,
                'gap_pct': gap_pct
            }

        if not vwap_success and avwap_qty == 0:
            return _build_res('WAIT', 'VWAP_데이터_결측_동결')

        safe_qty = int(math.floor(float(avwap_qty)))
        if safe_qty > 0:
            safe_avg = avwap_avg_price if avwap_avg_price > 0 else exec_curr_p
            
            if safe_avg <= 0:
                logging.error("🚨 [V_AVWAP] safe_avg <= 0: 가격 데이터 결측, 하드스탑 강제 집행")
                return _build_res('SELL', 'CORRUPT_PRICE_HARD_STOP', qty=safe_qty, target_price=0.0)
                
            exec_return = (exec_curr_p - safe_avg) / safe_avg
            base_equivalent_return = exec_return / self.leverage
            
            if base_equivalent_return <= -self.base_stop_loss_pct:
                return _build_res('SELL', 'HARD_STOP_DUAL', qty=safe_qty, target_price=0.0)
            
            if early_exit_mode and (exec_return >= early_target_profit):
                return _build_res('SELL', f'EARLY_PROFIT_TAKE_DUAL_{early_target_profit*100:.1f}%', qty=safe_qty, target_price=0.0)

            if curr_time >= time_1555:
                return _build_res('SELL', 'TIME_STOP', qty=safe_qty, target_price=0.0)
                
            if vwap_success and curr_time >= time_1430 and base_curr_p >= base_vwap * (1 + self.base_target_pct):
                return _build_res('SELL', 'SQUEEZE_TARGET_DUAL', qty=safe_qty, target_price=0.0)
                
            return _build_res('HOLD', '보유중_관망')

        if not context_data:
            return _build_res('WAIT', '매크로_데이터_수집대기')

        if base_day_open <= 0:
            return _build_res('WAIT', '시가_데이터_결측_대기')

        prev_c = context_data['prev_close']
        ma_20 = context_data['ma_20']
        avg_vol_20 = context_data['avg_vol_20']

        is_bull_regime = (prev_c > ma_20) and (base_day_open > ma_20)
        if not is_bull_regime:
            return _build_res('SHUTDOWN', '기초자산_역배열_하락장_영구동결')
            
        if base_day_open <= prev_c * (1 - self.base_dip_buy_pct):
            return _build_res('SHUTDOWN', '기초자산_시가_갭하락_영구동결')
            
        if curr_time >= time_1000:
            if avg_vol_20 > 0 and base_current_30m_vol >= (avg_vol_20 * 2.0) and base_curr_p < base_vwap:
                return _build_res('SHUTDOWN', '기초자산_RVOL_스파이크_영구동결')
                
        # 🚨 [V30.05 팩트 수술] 매수 윈도우 스캔 구간 조건문 교정
        if time_1000 <= curr_time <= time_1500:
            if base_curr_p <= base_vwap * (1 - self.base_dip_buy_pct):
                if exec_curr_p > 0 and avwap_alloc_cash > 0:
                    buy_qty = int(math.floor(avwap_alloc_cash / exec_curr_p))
                    if buy_qty > 0:
                        return _build_res('BUY', 'VWAP_BOUNCE_DUAL', qty=buy_qty, target_price=exec_curr_p)
                return _build_res('WAIT', '예산_부족_관망')
                    
        return _build_res('WAIT', '타점_대기중')

# ==========================================================
# FILE: strategy_v_avwap.py
# ==========================================================
# 🚨 MODIFIED: [Lost Update 궁극 방어] 파일 읽기/쓰기 연산에 GlobalThrottle.get_file_lock()을 100% 팩트 래핑 완료.
# 🚨 MODIFIED: [API Thundering Herd 방어] YF API 호출 직전 time.sleep(0.06) 땜질 코드를 영구 소각하고 GlobalThrottle.wait_api_sync() 중앙 통제 락온.
# 🚨 NEW: [04:07 타임쉴드 조건부 해제망 결속] 개장 직후 노이즈를 회피하기 위해 04:07까지 절대 진입 금지 락온. 04:07~04:30 구간은 '1분 연속 VWAP 상회' 시 진입, 04:30 이후는 '즉각 상향 돌파' 시 진입하도록 하이브리드 타점 추적망 100% 팩트 교정 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
import time 
import yfinance as yf
import pandas as pd
import numpy as np 
import json
import os
import tempfile
import html
from global_throttle import GlobalThrottle # 🚨 NEW: 중앙 통제소 결속

class VAvwapHybridPlugin:
    def __init__(self):
        self.plugin_name = "AVWAP_BREAKOUT_OBSERVER"

    def _safe_float(self, value):
        try:
            val = float(str(value or 0.0).replace(',', ''))
            if math.isnan(val) or math.isinf(f_val):
                return 0.0
            return val
        except Exception:
            return 0.0

    def _flatten_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if isinstance(df.columns, pd.MultiIndex):
            if 'Ticker' in df.columns.names:
                df.columns = df.columns.droplevel('Ticker')
            elif df.columns.nlevels == 2:
                price_fields = {'Close', 'High', 'Low', 'Open', 'Volume', 'Adj Close'}
                level0_vals = set(df.columns.get_level_values(0))
                drop_level = 0 if not level0_vals.intersection(price_fields) else 1
                df.columns = df.columns.droplevel(drop_level)
        return df

    def _get_logical_date_str(self, now_est):
        if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 4):
            target_date = now_est - datetime.timedelta(days=1)
        elif now_est.time() >= datetime.time(16, 0):
            target_date = now_est + datetime.timedelta(days=1)
        else:
            target_date = now_est
            
        if target_date.weekday() == 5: 
            target_date += datetime.timedelta(days=2)
        elif target_date.weekday() == 6: 
            target_date += datetime.timedelta(days=1)
            
        return target_date.strftime('%Y-%m-%d')

    def _get_state_file(self, ticker, now_est):
        return f"data/avwap_state_persistent_{ticker}.json"

    def load_state(self, ticker, now_est):
        file_path = self._get_state_file(ticker, now_est)
        today_str = self._get_logical_date_str(now_est)
        data = {}

        # 🚨 MODIFIED: File Mutex 락온
        with GlobalThrottle.get_file_lock(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except OSError:
                pass
            except json.JSONDecodeError:
                pass
                
            if not isinstance(data, dict):
                data = {}

            if data.get('date') != today_str:
                data = {
                    'date': today_str
                }
                
                dir_name = os.path.dirname(file_path) or '.'
                try: os.makedirs(dir_name, exist_ok=True)
                except OSError: pass

                fd = None
                temp_path = None
                try:
                    fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
                    with os.fdopen(fd, 'w', encoding='utf-8') as f:
                        fd = None
                        json.dump(data, f, ensure_ascii=False, indent=4)
                        f.flush()
                        os.fsync(f.fileno()) 
                    os.replace(temp_path, file_path)
                    temp_path = None
                except Exception as e:
                    if fd is not None:
                        try: os.close(fd)
                        except OSError: pass
                    if temp_path:
                        try: os.remove(temp_path)
                        except OSError: pass
                    logging.error(f"🚨 [V_AVWAP] 관측기 초기화 상태 저장 실패: {e}")
            
            return data

    def save_state(self, ticker, now_est, state_data):
        if not isinstance(state_data, dict):
            state_data = {}
            
        file_path = self._get_state_file(ticker, now_est)
        today_str = self._get_logical_date_str(now_est)

        merged_data = {'date': today_str}

        # 🚨 MODIFIED: File Mutex 락온
        with GlobalThrottle.get_file_lock(file_path):
            dir_name = os.path.dirname(file_path) or '.'
            try:
                os.makedirs(dir_name, exist_ok=True)
            except OSError:
                pass

            fd = None
            temp_path = None
            try:
                fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    fd = None
                    json.dump(merged_data, f, ensure_ascii=False, indent=4)
                    f.flush()
                    os.fsync(f.fileno()) 
                os.replace(temp_path, file_path)
                temp_path = None
            except Exception as e:
                if fd is not None:
                    try: os.close(fd)
                    except OSError: pass
                if temp_path:
                    try: os.remove(temp_path)
                    except OSError: pass
                logging.error(f"🚨 [V_AVWAP] 관측기 상태 저장 실패 (원자적 쓰기 에러): {e}")

    def apply_stock_split(self, ticker, ratio, now_est):
        pass

    def fetch_macro_context(self, base_ticker):
        for attempt in range(3):
            try:
                GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 중앙 통제소 락온
                tkr = yf.Ticker(base_ticker)
                df_1m = tkr.history(period="5d", interval="1m", prepost=False, timeout=5)
    
                prev_vwap = 0.0
                prev_close = 0.0
    
                est = ZoneInfo('America/New_York')
                now_est = datetime.datetime.now(est)
 
                if now_est.hour < 4:
                    today_est = (now_est - datetime.timedelta(days=1)).date()
                else:
                    today_est = now_est.date()
    
                if not df_1m.empty:
                    df_1m = self._flatten_columns(df_1m)
    
                    if df_1m.index.tz is None:
                        df_1m.index = df_1m.index.tz_localize('UTC').tz_convert(est)
                    else:
                        df_1m.index = df_1m.index.tz_convert(est)
    
                    df_past_1m = df_1m[df_1m.index.date < today_est].copy()
    
                    if not df_past_1m.empty:
                        last_date = df_past_1m.index.date[-1]
                        df_prev_day = df_past_1m[df_past_1m.index.date == last_date].copy()
                        df_prev_day = df_prev_day.between_time('09:30', '15:59')
    
                        if not df_prev_day.empty:
                            df_prev_day['High'] = df_prev_day['High'].ffill().bfill()
                            df_prev_day['Low'] = df_prev_day['Low'].ffill().bfill()
                            df_prev_day['Close'] = df_prev_day['Close'].ffill().bfill()
                            df_prev_day['Volume'] = df_prev_day['Volume'].ffill().bfill().fillna(0)

                            prev_close = self._safe_float(df_prev_day['Close'].iloc[-1])
     
                            df_prev_day['tp'] = (df_prev_day['High'].astype(float) + df_prev_day['Low'].astype(float) + df_prev_day['Close'].astype(float)) / 3.0
                            df_prev_day['vol'] = df_prev_day['Volume'].astype(float)
                            df_prev_day['vol_tp'] = df_prev_day['tp'] * df_prev_day['vol']
    
                            cum_vol = df_prev_day['vol'].sum()
                            if cum_vol > 0:
                                prev_vwap = self._safe_float(df_prev_day['vol_tp'].sum() / cum_vol)
                            else:
                                prev_vwap = prev_close
    
                if prev_vwap == 0.0:
                    prev_vwap = prev_close
                   
                return {
                    "prev_close": prev_close,
                    "prev_vwap": prev_vwap,
                    "avg_vol_20": 0.0 
                }
    
            except Exception as e:
                logging.debug(f"⚠️ [V_AVWAP] YF 기초자산 매크로 컨텍스트 추출 오류 (시도 {attempt+1}/3): {e}")
                if attempt == 2: return None
                time.sleep(1.0 * (2 ** attempt))

    def get_decision(self, base_ticker=None, exec_ticker=None, base_curr_p=0.0, exec_curr_p=0.0, base_day_open=0.0, avwap_avg_price=0.0, avwap_qty=0, avwap_alloc_cash=0.0, context_data=None, df_1min_base=None, df_1min_exec=None, now_est=None, avwap_state=None, regime_data=None, is_simulation=False, sortie_mode="SINGLE", **kwargs):
        is_holiday = kwargs.get('is_holiday', False)
        now_est = now_est or datetime.datetime.now(ZoneInfo('America/New_York'))
        today_est_date = now_est.date()
        curr_t = now_est.time()

        def _build_res(action, reason, tp=0.0, session_vwap=0.0):
            return {
                'action': 'OBSERVING' if is_simulation else action,
                'raw_action': action,
                'reason': html.escape(str(reason)),
                'target_price': self._safe_float(tp),
                'session_vwap': self._safe_float(session_vwap)
            }

        exec_curr_p = self._safe_float(exec_curr_p)

        if exec_curr_p <= 0.0:
            return _build_res('OBSERVING', '현재가(exec_curr_p) 데이터 결측 (0.0). 관망 유지.')
            
        if exec_ticker != "SOXL":
            return _build_res('OBSERVING', 'SOXL 전용 모듈 (타 종목 차단)')

        if now_est.weekday() >= 5 or is_holiday:
            return _build_res('OBSERVING', '미국 증시 휴장일 (관측 오프라인)')

        # 🚨 NEW: [04:07 타임쉴드 절대 락온] 개장 직후 노이즈를 7분간 완벽히 튕겨냅니다.
        if curr_t < datetime.time(4, 7):
            return _build_res('OBSERVING', '개장 직후 타임쉴드 가동 (04:07 이전 진입 금지)')
        elif curr_t < datetime.time(9, 30):
            session_name = "1세션(프리장)"
            start_time_str = '040000'
            end_time_str = '092959'
        elif curr_t <= datetime.time(16, 0):
            session_name = "2세션(정규장)"
            start_time_str = '093000'
            end_time_str = '160000'
        else:
            return _build_res('OBSERVING', '정규장 마감 (애프터장 관망)')

        avwap_qty = int(self._safe_float(avwap_qty))
        avwap_avg_price = self._safe_float(avwap_avg_price)

        session_vwap = 0.0
        df_session_valid = False
        if df_1min_exec is not None and not df_1min_exec.empty and 'time_est' in df_1min_exec.columns:
            df_today = df_1min_exec[df_1min_exec.index.date == today_est_date].copy()
            df_session = df_today[(df_today['time_est'] >= start_time_str) & (df_today['time_est'] <= end_time_str)].copy()

            if not df_session.empty:
                df_session['high'] = df_session['high'].ffill().bfill()
                df_session['low'] = df_session['low'].ffill().bfill()
                df_session['close'] = df_session['close'].ffill().bfill()
                df_session['volume'] = df_session['volume'].ffill().bfill().fillna(0)

                tp = (df_session['high'].astype(float) + df_session['low'].astype(float) + df_session['close'].astype(float)) / 3.0
                vol = df_session['volume'].astype(float)
                vol_tp = tp * vol

                c_vol = vol.sum()
                if c_vol > 0:
                    session_vwap = self._safe_float(vol_tp.sum() / c_vol)
                else:
                    session_vwap = self._safe_float(tp.mean())
                df_session_valid = True

        if session_vwap <= 0.0:
            return _build_res('OBSERVING', f'{session_name} 실시간 VWAP 연산 대기중')

        if avwap_qty > 0:
            sell_target_price = math.ceil(avwap_avg_price * 1.01 * 100) / 100.0 if avwap_avg_price > 0 else 0.0
            return _build_res('OBSERVING', f'{session_name} 교전 중 (+1.0% 전량 익절 대기)', tp=sell_target_price, session_vwap=session_vwap)
        else:
            if curr_t >= datetime.time(9, 30):
                return _build_res('OBSERVING', '프리장 미진입으로 인한 진입 차단 (조기 퇴근)', tp=session_vwap, session_vwap=session_vwap)

            # 🚨 NEW: [하이브리드 타점 락온] 04:07~04:30은 1분 유지 조건, 이후는 즉각 돌파 판별
            if curr_t < datetime.time(4, 30):
                if df_session_valid and len(df_session) >= 2:
                    is_sustained = (self._safe_float(df_session['low'].iloc[-2]) > session_vwap) and (self._safe_float(df_session['low'].iloc[-1]) > session_vwap)
                    if is_sustained and exec_curr_p >= session_vwap:
                        return _build_res('BREAKOUT_BUY', f'{session_name} 04:30 이전 VWAP 1분 상회 검증 완료', tp=session_vwap, session_vwap=session_vwap)
                    else:
                        return _build_res('OBSERVING', f'{session_name} 04:07~04:30 이전 VWAP 1분 상회 조건 감시 중', tp=session_vwap, session_vwap=session_vwap)
                else:
                    return _build_res('OBSERVING', f'{session_name} 초기 1분봉 데이터 축적 중', tp=session_vwap, session_vwap=session_vwap)
            else:
                if exec_curr_p >= session_vwap:
                    return _build_res('BREAKOUT_BUY', f'{session_name} 실시간 VWAP(${session_vwap:.2f}) 상향 돌파 요격 인가', tp=session_vwap, session_vwap=session_vwap)
                else:
                    return _build_res('OBSERVING', f'{session_name} 실시간 VWAP(${session_vwap:.2f}) 하회 중 (돌파 감시)', tp=session_vwap, session_vwap=session_vwap)

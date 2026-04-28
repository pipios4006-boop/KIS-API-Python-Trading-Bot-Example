# ==========================================================
# [strategy_v_avwap.py]
# 💡 V-REV 하이브리드 전용 차세대 AVWAP 스나이퍼 플러그인 (Dual-Referencing)
# ⚠️ 초공격형 당일 청산 암살자 (V-REV 잉여 현금 100% 몰빵 & -8% 하드스탑)
# 🚨 [V29.03 팩트 수술] 기억상실(Amnesia) 엣지 케이스 방어막 (Persistence 엔진 탑재)
# 🚨 [V30.09 핫픽스] pytz 영구 적출 및 ZoneInfo('America/New_York') 이식
# 🚨 MODIFIED: [V31.50 그랜드 수술] 20MA 방어막 영구 소각 및 '전일 정규장 VWAP' 산출 엔진 탑재 완료.
# 🚨 MODIFIED: [V32.00 백테스트 팩트 락온] 동적 파라미터 전면 소각. +4.0% 고정 익절 및 -8.0% 하드스탑 하드코딩 완료.
# 🚨 MODIFIED: [V32.00 방어막] 2차 손절망(재진입) 환각을 영구 차단하는 13계명 백신 주석 이식 완료.
# 🚨 MODIFIED: [V41.XX 파격적 수술] 0% 쿨다운, 갭 타격, 손절 셧다운 전면 폐기 & 무제한 VWAP 모멘텀 돌파 엔진 이식.
# 🚨 MODIFIED: [V42.12 그랜드 핫픽스] 부등호 논리 완벽 원상 복구! (당일 > 5분평균 = 상승 롱 / 당일 < 5분평균 = 하락 숏)
# ==========================================================
import logging
import datetime
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
        # 🚨 [팩트 락온] 백테스트 챔피언 파라미터 (Option B) 하드코딩
        self.base_stop_loss_pct = 0.08 / 3.0  # 레버리지 3배 환산 시 -8.0% 하드스탑 고정
        self.base_target_pct = 0.04           # +4.0% 고정 익절
        
    def _get_logical_date_str(self, now_est):
        if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 5):
            target_date = now_est - datetime.timedelta(days=1)
        else:
            target_date = now_est
        return target_date.strftime('%Y%m%d')

    def _get_state_file(self, ticker, now_est):
        today_str = self._get_logical_date_str(now_est)
        return f"data/avwap_state_{today_str}_{ticker}.json"

    def load_state(self, ticker, now_est):
        file_path = self._get_state_file(ticker, now_est)
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        # NEW: [V41.XX] 0% 리셋 플래그 소각, shutdown 초기값 False 유지
        return {"executed_buy": False, "shutdown": False, "strikes": 0}

    def save_state(self, ticker, now_est, state_data):
        file_path = self._get_state_file(ticker, now_est)
        try:
            dir_name = os.path.dirname(file_path)
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
            
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
            df_1m = tkr.history(period="5d", interval="1m", prepost=False, timeout=5)
            
            prev_vwap = 0.0
            prev_close = 0.0
            
            est = ZoneInfo('America/New_York')
            now_est = datetime.datetime.now(est)
            
            if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 5):
                today_est = (now_est - datetime.timedelta(days=1)).date()
            else:
                today_est = now_est.date()

            if not df_1m.empty:
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
                        prev_close = float(df_prev_day['Close'].iloc[-1])
                        
                        df_prev_day['tp'] = (df_prev_day['High'].astype(float) + df_prev_day['Low'].astype(float) + df_prev_day['Close'].astype(float)) / 3.0
                        df_prev_day['vol'] = df_prev_day['Volume'].astype(float)
                        df_prev_day['vol_tp'] = df_prev_day['tp'] * df_prev_day['vol']
                        
                        cum_vol = df_prev_day['vol'].sum()
                        if cum_vol > 0:
                            prev_vwap = df_prev_day['vol_tp'].sum() / cum_vol
                        else:
                            prev_vwap = prev_close

            # MODIFIED: [V41.XX] RVOL 스파이크 차단 팩트는 소각되었으나 기존 아키텍처 보존을 위해 연산은 유지
            df_30m = tkr.history(period="60d", interval="30m", timeout=5)
            avg_vol_20 = 0.0

            if not df_30m.empty:
                if df_30m.index.tz is None:
                    df_30m.index = df_30m.index.tz_localize('UTC').tz_convert(est)
                else:
                    df_30m.index = df_30m.index.tz_convert(est)

                first_30m = df_30m[df_30m.index.time == datetime.time(9, 30)]
                past_first_30m = first_30m[first_30m.index.date < today_est]
                
                if len(past_first_30m) >= 20:
                    avg_vol_20 = float(past_first_30m['Volume'].tail(20).mean())
                elif len(past_first_30m) > 0:
                    avg_vol_20 = float(past_first_30m['Volume'].mean())

            if prev_vwap == 0.0:
                prev_vwap = prev_close

            return {
                "prev_close": prev_close,
                "prev_vwap": prev_vwap,
                "avg_vol_20": avg_vol_20
            }
            
        except Exception as e:
            logging.error(f"🚨 [V_AVWAP] YF 기초자산 매크로 컨텍스트 추출 실패 ({base_ticker}): {e}")
            return None

    def get_decision(self, base_ticker=None, exec_ticker=None, base_curr_p=0.0, exec_curr_p=0.0, base_day_open=0.0, avwap_avg_price=0.0, avwap_qty=0, avwap_alloc_cash=0.0, context_data=None, df_1min_base=None, now_est=None, avwap_state=None, **kwargs):
        
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

        avwap_state = avwap_state or {}
        
        curr_time = now_est.time()
        
        # NEW: [V41.XX] 모멘텀 무제한 타격 스캔 윈도우 락온
        time_1020 = datetime.time(10, 20)
        time_1500 = datetime.time(15, 0)
        time_1555 = datetime.time(15, 55)

        base_vwap = base_curr_p
        vwap_success = False 
        avg_vwap_5m = base_curr_p
        
        # 🚨 [V40.XX 옴니 매트릭스] 인버스(Inverse) 종목 여부 판독
        is_inverse = exec_ticker.upper() in ["SOXS", "SQQQ", "SPXU"]
        
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
                
                # NEW: [V41.XX] 당일 실시간 5분 평균 VWAP 연산 파이프라인
                if len(df) >= 5:
                    recent_5 = df.tail(5)
                    sum_vol_5 = recent_5['vol'].sum()
                    if sum_vol_5 > 0:
                        avg_vwap_5m = recent_5['vol_tp'].sum() / sum_vol_5
                else:
                    if cum_vol > 0:
                        avg_vwap_5m = base_vwap

            except Exception as e:
                logging.error(f"🚨 [V_AVWAP] 기초자산 1분봉 VWAP/5MA 연산 실패: {e}")

        # NEW: [V41.XX] 낡은 rolling_tp 및 gap_pct 렌더링을 5분 VWAP 평균(avg_vwap_5m)으로 완전 대체
        def _build_res(action, reason, qty=0, target_price=0.0):
            return {
                'action': action,
                'reason': reason,
                'qty': qty,
                'target_price': target_price,
                'vwap': base_vwap,
                'base_curr_p': base_curr_p,
                'avg_vwap_5m': avg_vwap_5m,
                'prev_vwap': context_data.get('prev_vwap', 0.0) if context_data else 0.0
            }

        if not vwap_success and avwap_qty == 0:
            return _build_res('WAIT', 'VWAP_데이터_결측_동결')

        safe_qty = int(math.floor(float(avwap_qty)))

        # ---------------------------------------------------------
        # 1. 매도 (보유 중일 때) 로직
        # ---------------------------------------------------------
        if safe_qty > 0:
            safe_avg = avwap_avg_price if avwap_avg_price > 0 else exec_curr_p
            
            if safe_avg <= 0:
                logging.error("🚨 [V_AVWAP] safe_avg <= 0: 가격 데이터 결측, 하드스탑 강제 집행")
                return _build_res('SELL', 'CORRUPT_PRICE_HARD_STOP', qty=safe_qty, target_price=0.0)
                
            # 익절 및 손절 연산은 계좌 실제 수익률을 추적하므로 SOXL/SOXS 구분 없이 팩트 기반 공통 연산
            exec_return = (exec_curr_p - safe_avg) / safe_avg
            base_equivalent_return = exec_return / self.leverage
            
            if base_equivalent_return <= -self.base_stop_loss_pct:
                # 🚨 [팩트 락온] -8.0% 하드스탑
                reason = f'HARD_STOP_손절(-8.0%)_즉각재진입가능'
                return _build_res('SELL', reason, qty=safe_qty, target_price=0.0)
            
            if exec_return >= self.base_target_pct:
                # 🚨 [팩트 락온] +4.0% 고정 익절
                reason = f'MULTI_STRIKE_TAKE(+4.0%)'
                return _build_res('SELL', reason, qty=safe_qty, target_price=0.0)

            if curr_time >= time_1555:
                # NEW: [V41.XX] 15:55 타임스탑 강제 청산 시에만 익일 오버나이트 갭하락 방어를 위해 셧다운 락온
                avwap_state["shutdown"] = True
                self.save_state(exec_ticker, now_est, avwap_state)
                return _build_res('SELL', 'TIME_STOP_오버나이트동결', qty=safe_qty, target_price=0.0)
                
            return _build_res('HOLD', '보유중_관망')

        # ---------------------------------------------------------
        # 2. 매수 (포지션 0주 일 때) 로직
        # ---------------------------------------------------------
        if not context_data:
            return _build_res('WAIT', '매크로_데이터_수집대기')

        if avwap_state.get('shutdown', False):
            return _build_res('WAIT', '15:55_강제청산_당일영구동결')

        if curr_time < time_1020:
            return _build_res('WAIT', '10:20_이전_타임쉴드_대기')
            
        if curr_time > time_1500:
            return _build_res('WAIT', '15:00_이후_신규진입_차단')

        prev_vwap = context_data.get('prev_vwap', 0.0)

        # 🚨 [V42.12 핫픽스] 부등호 완벽 원상 복구! (당일 > 5분평균 = 상승 롱 / 당일 < 5분평균 = 하락 숏)
        if not is_inverse:
            trigger_condition = (base_vwap > prev_vwap) and (base_vwap > avg_vwap_5m)
        else:
            trigger_condition = (base_vwap < prev_vwap) and (base_vwap < avg_vwap_5m)

        if trigger_condition:
            if exec_curr_p > 0 and avwap_alloc_cash > 0:
                buy_qty = int(math.floor(avwap_alloc_cash / exec_curr_p))
                if buy_qty > 0:
                    return _build_res('BUY', f'VWAP_MOMENTUM_BREAKOUT', qty=buy_qty, target_price=exec_curr_p)
            return _build_res('WAIT', '순수현금예산_부족_관망')
            
        return _build_res('WAIT', '타점_대기중')

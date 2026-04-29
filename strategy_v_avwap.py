# ==========================================================
# [strategy_v_avwap.py] - 🌟 V44.03 잔여 체력 락온 🌟
# 💡 V-REV 하이브리드 전용 차세대 AVWAP 스나이퍼 플러그인 (Dual-Referencing)
# ⚠️ 초공격형 당일 청산 암살자 (V-REV 잉여 현금 100% 몰빵 & -8% 하드스탑)
# 🚨 [V29.03 팩트 수술] 기억상실(Amnesia) 엣지 케이스 방어막 (Persistence 엔진 탑재)
# 🚨 [V30.09 핫픽스] pytz 영구 적출 및 ZoneInfo('America/New_York') 이식
# 🚨 MODIFIED: [V31.50 그랜드 수술] 20MA 방어막 영구 소각 및 '전일 정규장 VWAP' 산출 엔진 탑재 완료.
# 🚨 MODIFIED: [V32.00 방어막] 2차 손절망(재진입) 환각을 영구 차단하는 13계명 백신 주석 이식 완료.
# 🚨 MODIFIED: [V41.XX 파격적 수술] 0% 쿨다운, 갭 타격, 손절 셧다운 전면 폐기 & 무제한 VWAP 모멘텀 돌파 엔진 이식.
# 🚨 MODIFIED: [V42.12 그랜드 핫픽스] 부등호 논리 완벽 원상 복구! (당일 > 5분평균 = 상승 롱 / 당일 < 5분평균 = 하락 숏)
# 🚨 MODIFIED: [V43.00 작전 통제실 복구] 사용자가 설정한 커스텀 목표 수익률(Target) 수신 및 조기퇴근/다중출장 모드 연동 엔진 대수술 완료.
# 🚨 MODIFIED: [V43.07] 체력 소진율(ATR5) 연동 목표 수익률 자율주행(Auto) 익절 렌더링 엔진 완벽 융합 완료.
# 🚨 MODIFIED: [V44.03 체력 보존 락온] 매수(BUY) 트리거 최상단에 5일 ATR 기반 잔여 체력 검증 파이프라인을 이식하여 상승/하락 여력이 2.0% 미만일 경우 즉시 방아쇠를 강제 파기(WAIT)하는 무결점 락온 확립.
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
        # 🚨 [팩트 락온] 백테스트 챔피언 파라미터 하드코딩 유지
        self.base_stop_loss_pct = 0.08 / 3.0  # 레버리지 3배 환산 시 -8.0% 하드스탑 고정
        
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
        
        user_target_pct = kwargs.get('target_profit', 4.0)
        is_multi_strike = kwargs.get('is_multi_strike', False)
        
        target_mode = kwargs.get('target_mode', 'AUTO')
        
        # 🚨 [V44.03] 스나이퍼에서 수집된 진폭 체력 스캔 팩트 파라미터 수신 완료
        atr5 = kwargs.get('atr5', 0.0)
        day_low = kwargs.get('day_low', 0.0)
        prev_c = kwargs.get('prev_close', 0.0)

        if now_est is None:
            now_est = datetime.datetime.now(ZoneInfo('America/New_York'))
            
        if base_curr_p <= 0.0 and df_1min_base is not None and not df_1min_base.empty:
            try: base_curr_p = float(df_1min_base['close'].iloc[-1])
            except Exception: pass

        avwap_state = avwap_state or {}
        
        curr_time = now_est.time()
        time_1020 = datetime.time(10, 20)
        time_1500 = datetime.time(15, 0)
        time_1555 = datetime.time(15, 55)

        base_vwap = base_curr_p
        vwap_success = False 
        avg_vwap_5m = base_curr_p
        
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
                
            exec_return = (exec_curr_p - safe_avg) / safe_avg
            base_equivalent_return = exec_return / self.leverage
            
            if base_equivalent_return <= -self.base_stop_loss_pct:
                avwap_state["shutdown"] = True
                self.save_state(exec_ticker, now_est, avwap_state)
                reason = f'HARD_STOP_손절(-8.0%)_당일영구동결'
                return _build_res('SELL', reason, qty=safe_qty, target_price=0.0)
            
            final_target_pct = user_target_pct
            
            if target_mode == "AUTO" and atr5 > 0 and day_low > 0 and prev_c > 0:
                atr5_price = prev_c * (atr5 / 100.0)
                exh_5 = ((safe_avg - day_low) / atr5_price * 100) if atr5_price > 0 else 0
                
                if exh_5 >= 90: final_target_pct = 2.0
                elif exh_5 >= 80: final_target_pct = 3.0
                elif exh_5 >= 70: final_target_pct = 4.0
            
            final_target_ratio = final_target_pct / 100.0
            
            if exec_return >= final_target_ratio:
                if not is_multi_strike:
                    avwap_state["shutdown"] = True
                    self.save_state(exec_ticker, now_est, avwap_state)
                    reason = f'조기퇴근_익절(+{final_target_pct:.1f}%)_당일영구동결'
                else:
                    reason = f'MULTI_STRIKE_TAKE(+{final_target_pct:.1f}%)_즉각재진입가능'
                return _build_res('SELL', reason, qty=safe_qty, target_price=0.0)

            if curr_time >= time_1555:
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
            return _build_res('WAIT', '작전완수_또는_강제청산으로_인한_당일영구동결')

        if curr_time < time_1020:
            return _build_res('WAIT', '10:20_이전_타임쉴드_대기')
            
        if curr_time > time_1500:
            return _build_res('WAIT', '15:00_이후_신규진입_차단')

        prev_vwap = context_data.get('prev_vwap', 0.0)

        if not is_inverse:
            trigger_condition = (base_vwap > prev_vwap) and (base_vwap > avg_vwap_5m)
        else:
            trigger_condition = (base_vwap < prev_vwap) and (base_vwap < avg_vwap_5m)

        if trigger_condition:
            # 🚨 [V44.03 AVWAP 체력 소진 방어막 락온]
            # 이미 당일 저가 대비 크게 올라 최소 2.0%의 수익조차 보장하지 못할 정도로 
            # 5일 평균 진폭(ATR5) 체력이 모두 고갈되었다면, 방아쇠를 강제로 회수하고(WAIT)
            # 고점 휩소의 희생양이 되는 것을 영구 차단합니다.
            if atr5 > 0 and prev_c > 0 and day_low > 0 and exec_curr_p > 0:
                actual_gap_dollar = exec_curr_p - day_low
                actual_gap_pct = (actual_gap_dollar / prev_c) * 100.0
                rem_5_pct = atr5 - actual_gap_pct
                
                if rem_5_pct < 2.0:
                    return _build_res('WAIT', f'ATR5_잔여체력_고갈(최소2.0%보장_현재{rem_5_pct:.1f}%)_관망')
                    
            if exec_curr_p > 0 and avwap_alloc_cash > 0:
                buy_qty = int(math.floor(avwap_alloc_cash / exec_curr_p))
                if buy_qty > 0:
                    return _build_res('BUY', f'VWAP_MOMENTUM_BREAKOUT', qty=buy_qty, target_price=exec_curr_p)
            return _build_res('WAIT', '순수현금예산_부족_관망')
            
        return _build_res('WAIT', '타점_대기중')

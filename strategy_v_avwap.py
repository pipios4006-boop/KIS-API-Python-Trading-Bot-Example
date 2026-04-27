# ==========================================================
# [strategy_v_avwap.py]
# 💡 V-REV 하이브리드 전용 차세대 AVWAP 스나이퍼 플러그인 (Dual-Referencing)
# ⚠️ 초공격형 당일 청산 암살자 (V-REV 잉여 현금 100% 몰빵 & -6% 하드스탑)
# 🚨 [V29.03 팩트 수술] 기억상실(Amnesia) 엣지 케이스 방어막 (Persistence 엔진 탑재)
# 🚨 [V30.09 핫픽스] pytz 영구 적출 및 ZoneInfo('America/New_York') 이식
# 🚨 MODIFIED: [V31.50 그랜드 수술] 20MA 방어막 영구 소각 및 '전일 정규장 VWAP' 산출 엔진 탑재 완료.
# 🚨 MODIFIED: [V32.00 백테스트 팩트 락온] 동적 파라미터 전면 소각. Gap < 0 시 무조건 스윕 및 +2.0% 고정 익절 하드코딩.
# 🚨 MODIFIED: [V32.00 방어막] 2차 손절망(재진입) 환각을 영구 차단하는 13계명 백신 주석 이식 완료.
# NEW: [자정 경계 스냅샷/캐시 증발(Cinderella) 타임 패러독스 완벽 방어] 논리적 거래일 시프트 엔진 이식
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
        self.base_stop_loss_pct = 0.02  # 레버리지 3배 환산 시 -6.0% 하드스탑 고정
        self.base_target_pct = 0.02     # 🚨 [프롬프트 락온] 레버리지 3배 환산 시 +2.0% 고정 익절 (백테스트 동기화)
        
    # NEW: [자정 경계 스냅샷/캐시 증발(Cinderella) 타임 패러독스 완벽 방어]
    # 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
    def _get_logical_date_str(self, now_est):
        if now_est.time() < datetime.time(4, 5):
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
        return {}

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
            
            # 전일 VWAP 산출을 위해 5일 치 1분봉 데이터 로드
            df_1m = tkr.history(period="5d", interval="1m", prepost=False, timeout=5)
            
            prev_vwap = 0.0
            prev_close = 0.0
            
            est = ZoneInfo('America/New_York')
            now_est = datetime.datetime.now(est)
            
            # MODIFIED: [타임 패러독스 방어] 04:05 EST 이전은 전날로 간주
            if now_est.time() < datetime.time(4, 5):
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
                    
                    # 정규장 시간 핀셋 필터링 (09:30 ~ 15:59)
                    df_prev_day = df_prev_day.between_time('09:30', '15:59')
                    
                    if not df_prev_day.empty:
                        prev_close = float(df_prev_day['Close'].iloc[-1])
                        
                        # 전일 정통 VWAP 공식 적용 (누적 거래대금 / 누적 거래량)
                        df_prev_day['tp'] = (df_prev_day['High'].astype(float) + df_prev_day['Low'].astype(float) + df_prev_day['Close'].astype(float)) / 3.0
                        df_prev_day['vol'] = df_prev_day['Volume'].astype(float)
                        df_prev_day['vol_tp'] = df_prev_day['tp'] * df_prev_day['vol']
                        
                        cum_vol = df_prev_day['vol'].sum()
                        if cum_vol > 0:
                            prev_vwap = df_prev_day['vol_tp'].sum() / cum_vol
                        else:
                            prev_vwap = prev_close

            # 10시 이전 RVOL 스파이크 감지용 30분봉 데이터는 유지
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

    # MODIFIED: [V32.00] 동적 파라미터 소각 및 하드코딩 룰 적용
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
        strikes = avwap_state.get('strikes', 0)
        cooldown_active = avwap_state.get('cooldown_active', False)
            
        curr_time = now_est.time()
        
        time_1000 = datetime.time(10, 0)
        time_1500 = datetime.time(15, 0)
        time_1555 = datetime.time(15, 55)

        base_vwap = base_curr_p
        vwap_success = False 
        
        rolling_tp = base_curr_p
        
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
                
                # 롤링 5분 TP 갭 스캔 엔진
                if len(df) >= 5:
                    recent_5 = df.tail(5)
                    high_5m = float(recent_5['high'].max())
                    low_5m = float(recent_5['low'].min())
                    close_1m = float(recent_5['close'].iloc[-1])
                    rolling_tp = (high_5m + low_5m + close_1m) / 3.0
                
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

        gap_pct = ((rolling_tp - base_vwap) / base_vwap * 100.0) if base_vwap > 0 else 0.0

        def _build_res(action, reason, qty=0, target_price=0.0):
            return {
                'action': action,
                'reason': reason,
                'qty': qty,
                'target_price': target_price,
                'vwap': base_vwap,
                'base_curr_p': base_curr_p,
                'rolling_tp': rolling_tp,
                'gap_pct': gap_pct,
                'prev_vwap': context_data.get('prev_vwap', 0.0) if context_data else 0.0
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
            
            # 🚨 [AI 에이전트 절대 주의 - 13계명 위반 방어막]
            # 14차 백테스트의 2차 손절망(-3.0%) 재진입(다중 손절) 로직은 
            # "손절 피격 시 당일 매매 영구 동결(Daily Stop Lock-down)"이라는 13계명을 정면 위반하는 환각입니다.
            # 단 1회의 -6.0% 하드스탑 도달 시 묻지도 따지지도 않고 SHUTDOWN 반환하여 당일 락온을 보장하십시오.
            if base_equivalent_return <= -self.base_stop_loss_pct:
                return _build_res('SHUTDOWN', 'HARD_STOP_손절(-6.0%)_당일영구동결', qty=safe_qty, target_price=0.0)
            
            # 🚨 [프롬프트 절대 락온] 다중 출장 고정 타겟 (+2.0% 도달 시 즉각 전량 익절)
            if exec_return >= self.base_target_pct:
                reason = f'MULTI_STRIKE_TAKE(+2.0%)'
                return _build_res('SELL', reason, qty=safe_qty, target_price=0.0)

            if curr_time >= time_1555:
                return _build_res('SELL', 'TIME_STOP', qty=safe_qty, target_price=0.0)
                
            return _build_res('HOLD', '보유중_관망')

        if not context_data:
            return _build_res('WAIT', '매크로_데이터_수집대기')

        if base_day_open <= 0:
            return _build_res('WAIT', '시가_데이터_결측_대기')

        prev_vwap = context_data.get('prev_vwap', 0.0)
        prev_c = context_data.get('prev_close', 0.0)
        avg_vol_20 = context_data.get('avg_vol_20', 0.0)

        # 🚨 [진성 상승장 필터] 전일 정규장 VWAP 대비 당일 실시간 VWAP이 낮을 경우 강제 관망
        if prev_vwap > 0 and base_vwap < prev_vwap:
            return _build_res('WAIT', f'당일VWAP_전일대비_하락_관망(당일:${base_vwap:.2f} < 전일:${prev_vwap:.2f})')

        # 기초자산 시가 갭 하락 차단
        if base_day_open <= prev_c * (1 - 0.0067):
            return _build_res('SHUTDOWN', '기초자산_시가_갭하락_영구동결')
            
        if curr_time >= time_1000:
            if avg_vol_20 > 0 and base_current_30m_vol >= (avg_vol_20 * 2.0) and base_curr_p < base_vwap:
                return _build_res('SHUTDOWN', '기초자산_RVOL_스파이크_영구동결')

        if cooldown_active:
            # 🚨 자연 쿨다운 적용: 갭 해소 시(-0.2% 이상 회복) 대기 해제
            if gap_pct >= -0.2:
                return _build_res('COOLDOWN_RELEASE', 'VWAP_회복_재장전_완료')
            else:
                return _build_res('WAIT', f'다중타격_자연쿨다운_대기중 (현재갭 {gap_pct:.2f}%)')
                
        if time_1000 <= curr_time <= time_1500:
            # 🚨 [프롬프트 절대 락온] 단돈 0.01%라도 하향 돌파(음수 갭) 시 즉각 무지성 시장가 스윕 타격
            if gap_pct < 0:
                if exec_curr_p > 0 and avwap_alloc_cash > 0:
                    buy_qty = int(math.floor(avwap_alloc_cash / exec_curr_p))
                    if buy_qty > 0:
                        return _build_res('BUY', f'VWAP_GAP_STRIKE({gap_pct:.2f}%)', qty=buy_qty, target_price=exec_curr_p)
                return _build_res('WAIT', '순수현금예산_부족_관망')
                    
        return _build_res('WAIT', '타점_대기중')

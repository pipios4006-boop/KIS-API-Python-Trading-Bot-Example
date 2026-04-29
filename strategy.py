# ==========================================================
# [strategy.py] - 🌟 2대 코어 + 하이브리드 라우터 완성본 🌟
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# 🚨 MODIFIED: [V32.00 그랜드 수술] 불필요한 AVWAP 동적 파라미터 수신 배선 완전 소각
# NEW: [V40.XX 옴니 매트릭스 절대 헌법] TQQQ(V14) / SOXS(V-REV) 런타임 강제 라우팅(Bypass) 쉴드 이식
# 🚨 MODIFIED: [V40.XX 옴니 매트릭스 전면 수술] 후행성 60MA/120MA 엔진 전면 소각 및
# 전일 VWAP vs 당일 실시간 VWAP 동행 지표(Coincident Indicator) 듀얼 모멘텀 엔진 수신 및 라우팅 락온
# 🚨 MODIFIED: [V43.00 작전 통제실 복구] AVWAP 사용자가 설정하는 커스텀 목표 수익률(Target) 및 근무 모드(조기퇴근/다중출장) 파라미터를 하위 플러그인(strategy_v_avwap)으로 전달하는 라우터 배선 복구 완료.
# 🚨 MODIFIED: [V44.03 AVWAP 매수 방어] **kwargs 배선 개통으로 5일 ATR 등 신규 파라미터 주입 호환성 확보
# ==========================================================
import logging
import pandas as pd
from zoneinfo import ZoneInfo
from strategy_v14 import V14Strategy
from strategy_v_avwap import VAvwapHybridPlugin  
from strategy_reversion import ReversionStrategy
from strategy_v14_vwap import V14VwapStrategy

class InfiniteStrategy:
    def __init__(self, config):
        self.cfg = config
        self.v14_plugin = V14Strategy(config)
        self.v_avwap_plugin = VAvwapHybridPlugin()
        self.v_rev_plugin = ReversionStrategy()
        self.v14_vwap_plugin = V14VwapStrategy(config)

    def analyze_vwap_dominance(self, df):
        if df is None or len(df) < 10:
            return {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}
            
        try:
            if 'High' in df.columns and 'Low' in df.columns:
                typical_price = (df['High'] + df['Low'] + df['Close']) / 3.0
            else:
                typical_price = df['Close']
                
            vol_x_price = typical_price * df['Volume']
            total_vol = df['Volume'].sum()
            
            if total_vol == 0:
                return {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}
                
            vwap_price = vol_x_price.sum() / total_vol
            
            df_temp = pd.DataFrame()
            df_temp['Volume'] = df['Volume']
            df_temp['Vol_x_Price'] = vol_x_price
            df_temp['Cum_Vol'] = df_temp['Volume'].cumsum()
            df_temp['Cum_Vol_Price'] = df_temp['Vol_x_Price'].cumsum()
            df_temp['Running_VWAP'] = df_temp['Cum_Vol_Price'] / df_temp['Cum_Vol']
            
            idx_10pct = int(len(df_temp) * 0.1)
            vwap_start = df_temp['Running_VWAP'].iloc[idx_10pct]
            vwap_end = df_temp['Running_VWAP'].iloc[-1]
            vwap_slope = vwap_end - vwap_start
            
            vol_above = df[df['Close'] > vwap_price]['Volume'].sum()
            vol_below = df[df['Close'] <= vwap_price]['Volume'].sum()
            
            vol_above_pct = vol_above / total_vol if total_vol > 0 else 0
            vol_below_pct = vol_below / total_vol if total_vol > 0 else 0
            
            daily_open = df['Open'].iloc[0] if 'Open' in df.columns else df['Close'].iloc[0]
            daily_close = df['Close'].iloc[-1]
            
            is_up_day = daily_close > daily_open
            is_down_day = daily_close < daily_open
            
            is_strong_up = is_up_day and (vwap_slope > 0) and (vol_above_pct > 0.60)
            is_strong_down = is_down_day and (vwap_slope < 0) and (vol_below_pct > 0.60)
            
            return {
                "vwap_price": round(vwap_price, 2),
                "is_strong_up": bool(is_strong_up),
                "is_strong_down": bool(is_strong_down),
                "vol_above_pct": round(vol_above_pct, 4),
                "vwap_slope": round(vwap_slope, 4)
            }
        except Exception as e:
            return {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}

    def apply_omni_matrix_filter(self, ticker, qty, regime_data):
        """
        VWAP 동행 지표 기반의 국면 데이터(regime_data)를 해석하여,
        현재 요청된 티커(SOXL 또는 SOXS)가 당일 신규 매수 가능한지 판별합니다.
        보유 수량(qty)이 1주라도 있다면 1층 청산(SELL)은 무조건 허용합니다.
        """
        if not regime_data or regime_data.get("status") != "success":
            return {"allow_buy": False, "allow_sell": qty > 0, "msg": "VWAP 모멘텀 판별 불가 (안전 대기)"}

        target_ticker = regime_data.get("target_ticker", "NONE")
        regime = regime_data.get("regime", "SIDEWAYS")
        desc = regime_data.get("desc", regime)

        # 횡보장 휩소 구간 (방향성 충돌): 신규 매수 전면 차단 (암살자 퇴직 모드)
        if target_ticker == "NONE" or regime == "SIDEWAYS":
            return {"allow_buy": False, "allow_sell": qty > 0, "msg": f"{desc} - 암살자 퇴직 (신규 진입 차단)"}

        # 듀얼 모멘텀 공수 일치 여부 확인
        if ticker.upper() == target_ticker.upper():
            return {"allow_buy": True, "allow_sell": True, "msg": f"{desc} - {ticker.upper()} 진입 락온"}
        else:
            return {"allow_buy": False, "allow_sell": qty > 0, "msg": f"{desc} - {ticker.upper()} 진입 차단 (타겟: {target_ticker})"}

    def get_plan(self, ticker, current_price, avg_price, qty, prev_close, ma_5day=0.0, market_type="REG", available_cash=0, is_simulation=False, vwap_status=None, is_snapshot_mode=False, regime_data=None):
        version = self.cfg.get_version(ticker)
        
        # 🚨 [V40.XX 절대 헌법] SOXS = V-REV 전용, TQQQ = V14 전용 강제 락온(Bypass)
        if ticker.upper() == "SOXS" and version != "V_REV":
            logging.warning(f"🚨 [{ticker}] 절대 헌법 위반 감지. V_REV 모드로 강제 라우팅합니다.")
            self.cfg.set_version(ticker, "V_REV")
            version = "V_REV"
        elif ticker.upper() == "TQQQ" and version != "V14":
            logging.warning(f"🚨 [{ticker}] 절대 헌법 위반 감지. V14 모드로 강제 라우팅합니다.")
            self.cfg.set_version(ticker, "V14")
            version = "V14"

        if version in ["V13", "V17", "V_VWAP", "V_AVWAP"]:
            logging.warning(f"[{ticker}] 폐기된 레거시 모드({version}) 감지. V14 엔진으로 강제 라우팅합니다.")
            self.cfg.set_version(ticker, "V14")
            version = "V14"

        is_vwap_enabled = getattr(self.cfg, 'get_manual_vwap_mode', lambda x: False)(ticker)
        
        # 기본 플랜 산출
        if version == "V14" and is_vwap_enabled:
            plan = self.v14_vwap_plugin.get_plan(
                ticker=ticker, current_price=current_price, avg_price=avg_price, qty=qty,
                prev_close=prev_close, ma_5day=ma_5day, market_type=market_type,
                available_cash=available_cash, is_simulation=is_simulation,
                is_snapshot_mode=is_snapshot_mode
            )
        elif version == "V_REV":
            plan = {
                'core_orders': [], 'bonus_orders': [], 'orders': [],
                't_val': 0.0, 'is_reverse': False, 'star_price': 0.0, 'one_portion': 0.0
            }
        else:
            plan = self.v14_plugin.get_plan(
                ticker=ticker, current_price=current_price, avg_price=avg_price, qty=qty,
                prev_close=prev_close, ma_5day=ma_5day, market_type=market_type,
                available_cash=available_cash, is_simulation=is_simulation, vwap_status=vwap_status
            )
            
        # [V40.XX] 옴니 매트릭스 필터 적용 (매수 락온 및 청산 패스)
        if regime_data is not None:
            omni_filter = self.apply_omni_matrix_filter(ticker, qty, regime_data)
            if not omni_filter["allow_buy"]:
                plan['core_orders'] = [o for o in plan.get('core_orders', []) if o.get('side') != 'BUY']
                plan['bonus_orders'] = [o for o in plan.get('bonus_orders', []) if o.get('side') != 'BUY']
                plan['orders'] = [o for o in plan.get('orders', []) if o.get('side') != 'BUY']
                plan['omni_msg'] = omni_filter["msg"]
                
        return plan

    def capture_vrev_snapshot(self, ticker, clear_price, avg_price, qty):
        if qty <= 0: return None
        
        raw_total_buy = avg_price * qty
        raw_total_sell = clear_price * qty
        
        fee_rate = self.cfg.get_fee(ticker) / 100.0
        net_invested = raw_total_buy * (1.0 + fee_rate)
        net_revenue = raw_total_sell * (1.0 - fee_rate)
        
        realized_pnl = net_revenue - net_invested
        realized_pnl_pct = (realized_pnl / net_invested) * 100 if net_invested > 0 else 0.0
        
        return {
            "ticker": ticker,
            "clear_price": clear_price,
            "avg_price": avg_price,
            "cleared_qty": qty,
            "realized_pnl": realized_pnl,
            "realized_pnl_pct": realized_pnl_pct,
            "captured_at": pd.Timestamp.now(tz=ZoneInfo('America/New_York'))
        }

    def load_avwap_state(self, ticker, now_est):
        if hasattr(self.v_avwap_plugin, 'load_state'):
            return self.v_avwap_plugin.load_state(ticker, now_est)
        return {}

    def save_avwap_state(self, ticker, now_est, state_data):
        if hasattr(self.v_avwap_plugin, 'save_state'):
            self.v_avwap_plugin.save_state(ticker, now_est, state_data)

    def fetch_avwap_macro(self, base_ticker):
        return self.v_avwap_plugin.fetch_macro_context(base_ticker)

    def get_avwap_decision(self, base_ticker, exec_ticker, base_curr_p, exec_curr_p, base_day_open, avg_price, qty, alloc_cash, context_data, df_1min_base, now_est, avwap_state=None, regime_data=None, **kwargs):
        
        if regime_data is not None:
            omni_filter = self.apply_omni_matrix_filter(exec_ticker, qty, regime_data)
            if not omni_filter["allow_buy"] and qty == 0:
                return {
                    "action": "HOLD",
                    "qty": 0,
                    "price": 0.0,
                    "msg": f"⛔ AVWAP 셧다운: {omni_filter['msg']}"
                }

        # 🚨 [V43.00 복원] config에서 커스텀 파라미터(목표 수익률, 근무 모드)를 동적으로 추출하여 플러그인에 전달
        target_profit = getattr(self.cfg, 'get_avwap_target_profit', lambda x: 4.0)(exec_ticker)
        is_multi_strike = getattr(self.cfg, 'get_avwap_multi_strike_mode', lambda x: False)(exec_ticker)

        # 🚨 [V44.03] 스나이퍼에서 수신한 체력 스캔 팩트 파라미터(**kwargs) 플러그인으로 바이패스
        return self.v_avwap_plugin.get_decision(
            base_ticker=base_ticker, exec_ticker=exec_ticker, base_curr_p=base_curr_p, exec_curr_p=exec_curr_p, 
            base_day_open=base_day_open, avwap_avg_price=avg_price, avwap_qty=qty, avwap_alloc_cash=alloc_cash, 
            context_data=context_data, df_1min_base=df_1min_base, now_est=now_est, avwap_state=avwap_state,
            target_profit=target_profit, is_multi_strike=is_multi_strike, **kwargs
        )

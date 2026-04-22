# ==========================================================
# [strategy.py] - 🌟 2대 코어 + 하이브리드 라우터 완성본 🌟
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# 💡 [V24.15 대수술] V_VWAP 영구 소각 및 2대 코어(V14, V-REV) 체제 확립
# 💡 [V24.18 하이브리드] VAvwapHybridPlugin 의존성 이름 교정 및 샌드박스 유지
# 🚨 [V25.08 팩트 동기화] V-REV 종목 지시서 누수(DI Leak) 방어를 위한 지능형 동적 라우터(Dynamic Router) 구축
# 🚨 [V25.19 핫픽스] 레거시 모드 감지 시 로컬 version 변수 미업데이트 맹점 팩트 교정
# 🚀 [V26.02 핵심 수술] V14 오리지널 모드 내 LOC/VWAP 집행 방식 이원화 라우팅 탑재
# 🚀 [V26.07 확정 순수익 렌더링 패치] V-REV 메모리 스냅샷 수수료(0.5%) 완벽 차감 이식
# MODIFIED: [V28.25 그랜드 수술] V-REV 메모리 스냅샷에 동적 수수료 팩트 역산 엔진 이식 완료
# 🚨 [V28.51 팩트 수술] 정규장 스케줄러 TypeError 붕괴 및 AVWAP 스나이퍼 크래시 원천 차단 (파라미터 디커플링 파이프라인 100% 개통)
# ==========================================================
import logging
import pandas as pd
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

    # 🚨 [V28.51 팩트 교정] TypeError 방어막: 스케줄러가 던지는 is_snapshot_mode 파라미터를 
    # 무결하게 수신하여 하위 엔진으로 패스하도록 시그니처 대수술 완료.
    def get_plan(self, ticker, current_price, avg_price, qty, prev_close, ma_5day=0.0, market_type="REG", available_cash=0, is_simulation=False, vwap_status=None, is_snapshot_mode=False):
        version = self.cfg.get_version(ticker)
        
        if version in ["V13", "V17", "V_VWAP", "V_AVWAP"]:
            logging.warning(f"[{ticker}] 폐기된 레거시 모드({version}) 감지. V14 엔진으로 강제 라우팅합니다.")
            self.cfg.set_version(ticker, "V14")
            version = "V14"

        is_vwap_enabled = getattr(self.cfg, 'get_manual_vwap_mode', lambda x: False)(ticker)
        if version == "V14" and is_vwap_enabled:
            return self.v14_vwap_plugin.get_plan(
                ticker=ticker, current_price=current_price, avg_price=avg_price, qty=qty,
                prev_close=prev_close, ma_5day=ma_5day, market_type=market_type,
                available_cash=available_cash, is_simulation=is_simulation,
                is_snapshot_mode=is_snapshot_mode
            )

        if version == "V_REV":
            return {
                'core_orders': [], 'bonus_orders': [], 'orders': [],
                't_val': 0.0, 'is_reverse': False, 'star_price': 0.0, 'one_portion': 0.0
            }

        return self.v14_plugin.get_plan(
            ticker=ticker, current_price=current_price, avg_price=avg_price, qty=qty,
            prev_close=prev_close, ma_5day=ma_5day, market_type=market_type,
            available_cash=available_cash, is_simulation=is_simulation, vwap_status=vwap_status
        )

    # MODIFIED: [V28.25] V-REV 메모리 스냅샷에 동적 수수료 팩트 역산 로직 이식
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
            "captured_at": pd.Timestamp.now(tz='Asia/Seoul')
        }

    def fetch_avwap_macro(self, base_ticker):
        return self.v_avwap_plugin.fetch_macro_context(base_ticker)

    # 🚨 [V28.51 팩트 교정] AVWAP 스나이퍼 크래시 쉴드: 조기퇴근 모드의 
    # early_exit_mode 및 early_target_profit 인젝션 100% 개통 완료.
    def get_avwap_decision(self, base_ticker, exec_ticker, base_curr_p, exec_curr_p, base_day_open, avg_price, qty, alloc_cash, context_data, df_1min_base, now_est, early_exit_mode=False, early_target_profit=0.025):
        return self.v_avwap_plugin.get_decision(
            base_ticker, exec_ticker, base_curr_p, exec_curr_p, base_day_open, avg_price, qty, alloc_cash, context_data, df_1min_base, now_est,
            early_exit_mode=early_exit_mode, early_target_profit=early_target_profit
        )

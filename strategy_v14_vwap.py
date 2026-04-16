# ==========================================================
# [strategy_v14_vwap.py]
# 💡 오리지널 V14(무매4) 공식 & VWAP 타임 슬라이싱 하이브리드 플러그인
# ⚠️ 수술 내역: 
# 1. V14의 T값/별값/예산 산출 로직과 V-REV의 VWAP 슬라이싱 엔진 융합
# 2. 17:05 프리장 오픈 시 '예방적 LOC 덫'을 Fail-Safe로 자동 장전
# 3. 장 마감 30분 전(15:30 EST)부터 1분 단위 유동성 가중치 분할 타격
# 🚨 [V26.02 팩트 동기화] 비파괴 보정(CALIB) 및 Safe Casting 완벽 이식
# 🚨 [V26.02 핫픽스] UI 렌더링 누락 버그(별%, 진행상태) 팩트 복원
# 🚀 [V26.03 영속성 캐시 이식] 서버 재시작 시 잔차 증발(기억상실)을 방어하는 L1/L2 듀얼 캐싱 엔진 탑재
# 🚀 [V27.01 지시서 스냅샷] 매일 17:05 확정 지시서를 박제하여 장중 잔고 변이에 따른 타점 왜곡 원천 차단
# 🚨 [V27.03 핫픽스] 스냅샷 로드 시 내부 날짜 검사(Validation) 전면 폐기로 무한루프 영구 방어
# 🚨 [V27.04 자전거래 방어] 별값매수 타점을 별값매도 대비 -$0.01 차감(Decoupling)하여 동시 타격 시 주문 거절 맹점 소각
# 🚨 [V27.05 그랜드 수술] 기억상실, 자전거래 하극상, API Reject(소수점 주문), 인자 누락 등 5대 치명적 맹점 전면 철거
# 🚨 [V27.06 코파일럿 합작] VWAP 매도 제논의 역설(목표량 실시간 축소) 앵커링 수술 및 fsync 객체 무결성 강화
# ==========================================================
import math
import logging
import os
import json
import tempfile
from datetime import datetime

class V14VwapStrategy:
    def __init__(self, config):
        self.cfg = config
        self.residual = {"BUY_AVG": {}, "BUY_STAR": {}, "SELL_STAR": {}, "SELL_TARGET": {}}
        self.executed = {"BUY_BUDGET": {}, "SELL_QTY": {}}
        self.state_loaded = {}
        
        self.U_CURVE_WEIGHTS = [
            0.0252, 0.0213, 0.0192, 0.0210, 0.0189, 0.0187, 0.0228, 0.0203, 0.0200, 0.0209,
            0.0254, 0.0217, 0.0225, 0.0211, 0.0228, 0.0281, 0.0262, 0.0240, 0.0236, 0.0256,
            0.0434, 0.0294, 0.0327, 0.0362, 0.0549, 0.0566, 0.0407, 0.0470, 0.0582, 0.1515
        ]

    def _get_state_file(self, ticker):
        today_str = datetime.now().strftime("%Y-%m-%d")
        return f"data/vwap_state_V14_{today_str}_{ticker}.json"

    def _get_snapshot_file(self, ticker):
        today_str = datetime.now().strftime("%Y-%m-%d")
        return f"data/daily_snapshot_V14VWAP_{today_str}_{ticker}.json"

    def _load_state_if_needed(self, ticker):
        today_str = datetime.now().strftime("%Y-%m-%d")
        if self.state_loaded.get(ticker) == today_str:
            return 
            
        state_file = self._get_state_file(ticker)
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for k in self.residual.keys():
                        self.residual[k][ticker] = float(data.get("residual", {}).get(k, 0.0))
                    for k in self.executed.keys():
                        raw_val = data.get("executed", {}).get(k, 0)
                        self.executed[k][ticker] = int(raw_val) if k == "SELL_QTY" else float(raw_val)
                    self.state_loaded[ticker] = today_str
                    return
            except Exception:
                pass
                
        for k in self.residual.keys():
            self.residual[k][ticker] = 0.0
        self.executed["BUY_BUDGET"][ticker] = 0.0
        self.executed["SELL_QTY"][ticker] = 0
        self.state_loaded[ticker] = today_str

    def _save_state(self, ticker):
        today_str = datetime.now().strftime("%Y-%m-%d")
        state_file = self._get_state_file(ticker)
        data = {
            "date": today_str,
            "residual": {k: float(self.residual[k].get(ticker, 0.0)) for k in self.residual.keys()},
            "executed": {
                "BUY_BUDGET": float(self.executed.get("BUY_BUDGET", {}).get(ticker, 0.0)),
                "SELL_QTY": int(self.executed.get("SELL_QTY", {}).get(ticker, 0))
            }
        }
        try:
            dir_name = os.path.dirname(state_file)
            os.makedirs(dir_name, exist_ok=True) # 🚨 [수술 완료] TOCTOU 방어: if 조건문 철거
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno()) # 🚨 [수술 완료] fd 대신 f.fileno() 사용으로 파일 객체 무결성 확보
            os.replace(temp_path, state_file)
        except Exception:
            pass

    def save_daily_snapshot(self, ticker, plan_data):
        today_str = datetime.now().strftime("%Y-%m-%d")
        snap_file = self._get_snapshot_file(ticker)
        data = {
            "date": today_str,
            "plan": plan_data
        }
        try:
            dir_name = os.path.dirname(snap_file)
            os.makedirs(dir_name, exist_ok=True)
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno()) # 🚨 [수술 완료] 파일 무결성 확보
            os.replace(temp_path, snap_file)
        except Exception:
            pass

    def load_daily_snapshot(self, ticker):
        snap_file = self._get_snapshot_file(ticker)
        if os.path.exists(snap_file):
            try:
                with open(snap_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get("plan")
            except Exception:
                pass
        return None

    def _ceil(self, val): return math.ceil(val * 100) / 100.0
    def _floor(self, val): return math.floor(val * 100) / 100.0

    def reset_residual(self, ticker):
        self._load_state_if_needed(ticker)
        for k in self.residual: self.residual[k][ticker] = 0.0
        self.executed["BUY_BUDGET"][ticker] = 0.0
        self.executed["SELL_QTY"][ticker] = 0
        self._save_state(ticker)

    def record_execution(self, ticker, side, qty, exec_price):
        self._load_state_if_needed(ticker)
        if side == "BUY":
            spent = float(qty * exec_price)
            self.executed["BUY_BUDGET"][ticker] = float(self.executed["BUY_BUDGET"].get(ticker, 0.0)) + spent
        else:
            self.executed["SELL_QTY"][ticker] = int(self.executed["SELL_QTY"].get(ticker, 0)) + int(qty)
        self._save_state(ticker)

    def get_plan(self, ticker, current_price, avg_price, qty, prev_close, ma_5day=0.0, market_type="REG", available_cash=0, is_simulation=False, is_snapshot_mode=False):
        if not is_snapshot_mode:
            cached_plan = self.load_daily_snapshot(ticker)
            if cached_plan:
                return cached_plan

        split = self.cfg.get_split_count(ticker)
        target_ratio = self.cfg.get_target_profit(ticker) / 100.0
        t_val, _ = self.cfg.get_absolute_t_val(ticker, qty, avg_price)
        
        depreciation_factor = 2.0 / split if split > 0 else 0.1
        star_ratio = target_ratio - (target_ratio * depreciation_factor * t_val)
        star_price = self._ceil(avg_price * (1 + star_ratio)) if avg_price > 0 else 0
        target_price = self._ceil(avg_price * (1 + target_ratio)) if avg_price > 0 else 0
        
        buy_star_price = round(star_price - 0.01, 2) if star_price > 0.01 else 0.0

        _, dynamic_budget, _ = self.cfg.calculate_v14_state(ticker)
        
        core_orders = []
        process_status = "예방적방어선"
        
        if qty == 0:
            p_buy = self._ceil(prev_close * 1.15)
            q_buy = math.floor(dynamic_budget / p_buy) if p_buy > 0 else 0
            if q_buy > 0: core_orders.append({"side": "BUY", "price": p_buy, "qty": q_buy, "type": "LOC", "desc": "🆕새출발(VWAP대기)"})
            process_status = "✨새출발"
        else:
            p_avg = self._ceil(avg_price)
            if t_val < (split / 2):
                q_avg = math.floor((dynamic_budget * 0.5) / p_avg) if p_avg > 0 else 0
                q_star = math.floor((dynamic_budget * 0.5) / buy_star_price) if buy_star_price > 0 else 0
                if q_avg > 0: core_orders.append({"side": "BUY", "price": p_avg, "qty": q_avg, "type": "LOC", "desc": "⚓평단매수(V)"})
                if q_star > 0: core_orders.append({"side": "BUY", "price": buy_star_price, "qty": q_star, "type": "LOC", "desc": "💫별값매수(V)"})
            else:
                q_star = math.floor(dynamic_budget / buy_star_price) if buy_star_price > 0 else 0
                if q_star > 0: core_orders.append({"side": "BUY", "price": buy_star_price, "qty": q_star, "type": "LOC", "desc": "💫별값매수(V)"})
            
            q_sell = math.ceil(qty / 4)
            if q_sell > 0:
                core_orders.append({"side": "SELL", "price": star_price, "qty": q_sell, "type": "LOC", "desc": "🌟별값매도(V)"})
                if qty - q_sell > 0:
                    core_orders.append({"side": "SELL", "price": target_price, "qty": qty - q_sell, "type": "LIMIT", "desc": "🎯목표매도(V)"})

        plan_result = {
            'core_orders': core_orders, 'bonus_orders': [], 'orders': core_orders,
            't_val': t_val, 'one_portion': dynamic_budget, 'star_price': star_price,
            'buy_star_price': buy_star_price,
            'star_ratio': star_ratio,
            'target_price': target_price, 'is_reverse': False,
            'process_status': process_status,
            'tracking_info': {},
            'initial_qty': int(qty) # 🚨 [수술 완료] 스냅샷 앵커링: 장전 당시의 최초 수량을 영구 박제
        }
        
        self.save_daily_snapshot(ticker, plan_result)
            
        return plan_result

    def get_dynamic_plan(self, ticker, curr_p, prev_c, current_weight, min_idx, alloc_cash, qty, avg_price):
        self._load_state_if_needed(ticker)
        
        plan_static = self.get_plan(
            ticker=ticker,
            current_price=curr_p,
            avg_price=avg_price,
            qty=qty,
            prev_close=prev_c,
            available_cash=alloc_cash,
            is_simulation=True,
            is_snapshot_mode=False
        )
        star_price = float(plan_static['star_price'])
        buy_star_price = float(plan_static.get('buy_star_price', round(star_price - 0.01, 2) if star_price > 0.01 else 0.0))
        target_price = float(plan_static['target_price'])
        total_budget = float(plan_static['one_portion'])
        
        # 🚨 [수술 완료] 박제된 초기 수량을 로드하여 매도 시 제논의 역설(목표 수량 실시간 축소) 버그 원천 차단
        initial_qty = int(plan_static.get('initial_qty', qty))
        
        rem_weight = sum(self.U_CURVE_WEIGHTS[min_idx:])
        slice_ratio = current_weight / rem_weight if rem_weight > 0 else 1.0
        
        orders = []
        
        total_spent = float(self.executed["BUY_BUDGET"].get(ticker, 0.0))
        rem_budget = max(0.0, total_budget - total_spent)
        
        if rem_budget > 0:
            slice_budget = rem_budget * slice_ratio
            if buy_star_price > 0 and curr_p <= buy_star_price:
                exact_qty = (slice_budget / curr_p) + float(self.residual["BUY_STAR"].get(ticker, 0.0))
                alloc_qty = int(math.floor(exact_qty))
                self.residual["BUY_STAR"][ticker] = float(exact_qty - alloc_qty)
                if alloc_qty > 0:
                    orders.append({"side": "BUY", "qty": alloc_qty, "price": buy_star_price, "desc": "VWAP분할매수"})

        # 🚨 [수술 완료] 실시간 qty 대신 initial_qty를 앵커로 사용하여 1/4 목표량을 절대 사수
        rem_sell_qty = int(math.ceil(initial_qty / 4)) - int(self.executed["SELL_QTY"].get(ticker, 0))
        if rem_sell_qty > 0 and star_price > 0:
            if curr_p >= star_price:
                exact_s_qty = float(rem_sell_qty * slice_ratio) + float(self.residual["SELL_STAR"].get(ticker, 0.0))
                alloc_s_qty = int(min(math.floor(exact_s_qty), rem_sell_qty))
                self.residual["SELL_STAR"][ticker] = float(exact_s_qty - alloc_s_qty)
                if alloc_s_qty > 0:
                    orders.append({"side": "SELL", "qty": alloc_s_qty, "price": star_price, "desc": "VWAP분할익절"})

        self._save_state(ticker)
        return {"orders": orders, "trigger_loc": False}

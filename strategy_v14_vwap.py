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

    # NEW: [V27.01] 일일 지시서 스냅샷 파일 경로 생성
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
                    if data.get("date") == today_str:
                        for k in self.residual.keys():
                            self.residual[k][ticker] = data.get("residual", {}).get(k, 0.0)
                        for k in self.executed.keys():
                            self.executed[k][ticker] = data.get("executed", {}).get(k, 0.0)
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
            "residual": {k: self.residual[k].get(ticker, 0.0) for k in self.residual.keys()},
            "executed": {k: self.executed[k].get(ticker, 0.0) for k in self.executed.keys()}
        }
        try:
            dir_name = os.path.dirname(state_file)
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(fd)
            os.replace(temp_path, state_file)
        except Exception:
            pass

    # NEW: [V27.01] 17:05 KST 정규장 스케줄러가 호출하여 그날의 확정 지시서를 파일에 박제
    def save_daily_snapshot(self, ticker, plan_data):
        today_str = datetime.now().strftime("%Y-%m-%d")
        snap_file = self._get_snapshot_file(ticker)
        data = {
            "date": today_str,
            "plan": plan_data
        }
        try:
            dir_name = os.path.dirname(snap_file)
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(fd)
            os.replace(temp_path, snap_file)
        except Exception:
            pass

    # NEW: [V27.01] /sync 조회 또는 VWAP 엔진이 박제된 지시서를 우선적으로 로드
    def load_daily_snapshot(self, ticker):
        today_str = datetime.now().strftime("%Y-%m-%d")
        snap_file = self._get_snapshot_file(ticker)
        if os.path.exists(snap_file):
            try:
                with open(snap_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if data.get("date") == today_str:
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
            spent = qty * exec_price
            self.executed["BUY_BUDGET"][ticker] = self.executed["BUY_BUDGET"].get(ticker, 0.0) + spent
        else:
            self.executed["SELL_QTY"][ticker] = self.executed["SELL_QTY"].get(ticker, 0) + qty
        self._save_state(ticker)

    def get_plan(self, ticker, current_price, avg_price, qty, prev_close, ma_5day=0.0, market_type="REG", available_cash=0, is_simulation=False, is_snapshot_mode=False):
        # NEW: [V27.01] 스냅샷 로드 모드일 경우 박제된 지시서를 최우선 반환
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
                q_star = math.floor((dynamic_budget * 0.5) / star_price) if star_price > 0 else 0
                if q_avg > 0: core_orders.append({"side": "BUY", "price": p_avg, "qty": q_avg, "type": "LOC", "desc": "⚓평단매수(V)"})
                if q_star > 0: core_orders.append({"side": "BUY", "price": star_price, "qty": q_star, "type": "LOC", "desc": "💫별값매수(V)"})
            else:
                q_star = math.floor(dynamic_budget / star_price) if star_price > 0 else 0
                if q_star > 0: core_orders.append({"side": "BUY", "price": star_price, "qty": q_star, "type": "LOC", "desc": "💫별값매수(V)"})
            
            q_sell = math.ceil(qty / 4)
            if q_sell > 0:
                core_orders.append({"side": "SELL", "price": star_price, "qty": q_sell, "type": "LOC", "desc": "🌟별값매도(V)"})
                if qty - q_sell > 0:
                    core_orders.append({"side": "SELL", "price": target_price, "qty": qty - q_sell, "type": "LIMIT", "desc": "🎯목표매도(V)"})

        plan_result = {
            'core_orders': core_orders, 'bonus_orders': [], 'orders': core_orders,
            't_val': t_val, 'one_portion': dynamic_budget, 'star_price': star_price,
            'star_ratio': star_ratio,
            'target_price': target_price, 'is_reverse': False,
            'process_status': process_status,
            'tracking_info': {}
        }
        
        # NEW: [V27.01] 스냅샷 모드로 호출된 경우 결과 반환 직전 파일에 박제
        if is_snapshot_mode:
            self.save_daily_snapshot(ticker, plan_result)
            
        return plan_result

    def get_dynamic_plan(self, ticker, curr_p, prev_c, current_weight, min_idx, alloc_cash, qty, avg_price):
        self._load_state_if_needed(ticker)
        
        plan_static = self.get_plan(ticker, curr_p, avg_price, qty, prev_c, is_simulation=True, is_snapshot_mode=False)
        star_price = plan_static['star_price']
        target_price = plan_static['target_price']
        total_budget = plan_static['one_portion']
        
        rem_weight = sum(self.U_CURVE_WEIGHTS[min_idx:])
        slice_ratio = current_weight / rem_weight if rem_weight > 0 else 1.0
        
        orders = []
        
        total_spent = self.executed["BUY_BUDGET"].get(ticker, 0.0)
        rem_budget = max(0.0, total_budget - total_spent)
        
        if rem_budget > 0:
            slice_budget = rem_budget * slice_ratio
            if star_price > 0 and curr_p <= star_price:
                exact_qty = (slice_budget / curr_p) + self.residual["BUY_STAR"].get(ticker, 0.0)
                alloc_qty = math.floor(exact_qty)
                self.residual["BUY_STAR"][ticker] = exact_qty - alloc_qty
                if alloc_qty > 0:
                    orders.append({"side": "BUY", "qty": alloc_qty, "price": star_price, "desc": "VWAP분할매수"})

        rem_sell_qty = math.ceil(qty / 4) - self.executed["SELL_QTY"].get(ticker, 0)
        if rem_sell_qty > 0 and star_price > 0:
            if curr_p >= star_price:
                exact_s_qty = (rem_sell_qty * slice_ratio) + self.residual["SELL_STAR"].get(ticker, 0.0)
                alloc_s_qty = min(math.floor(exact_s_qty), rem_sell_qty)
                self.residual["SELL_STAR"][ticker] = exact_s_qty - alloc_s_qty
                if alloc_s_qty > 0:
                    orders.append({"side": "SELL", "qty": alloc_s_qty, "price": star_price, "desc": "VWAP분할익절"})

        self._save_state(ticker)
        return {"orders": orders, "trigger_loc": False}

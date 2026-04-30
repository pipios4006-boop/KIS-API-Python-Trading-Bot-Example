# ==========================================================
# [strategy_reversion.py] - 🌟 V44.25 예산 탈취(Stealing) 락온 🌟
# ⚠️ V-REV 하이브리드 엔진 전용 수학적 타격 모듈
# 💡 5년 백테스트 기반 VWAP 유동성 정밀 가중치(U_CURVE_WEIGHTS) 적용 완료
# 💡 [V24.16 팩트 동기화] 0주 새출발 디커플링 타점 (Buy1: 0.999, Buy2: /0.935) 원본 유지
# 💡 [V24.16 팩트 동기화] 하락장 방어 매수 Buy2 타점 (0.9725) 교정
# 💡 [V24.16 팩트 동기화] 1층 전량 익절 타점 고유 매수가 기반(layer_price * 1.006) 원복
# 🚨 [V25.13 디커플링 스왑 패치] UI와 동일하게 Buy1과 Buy2의 타점을 고가->저가 순으로 스왑 연동
# 🚨 [V25.14 팩트 동기화] 1층 물귀신 덤핑 차단 및 지층별 평단가 완벽 분리 개별 탈출(Decoupling) 이식
# 🚨 [V25.15 잔여물량 격리] SELL_L1 / SELL_UPPER / SELL_JACKPOT 독립 큐(Residual) 분리 및 줍줍 무손실 복원 완료
# 🚨 [V25.17 잔재 소각] 수동 통제망(Telegram) 전환에 따른 자동 긴급 수혈(get_emergency_liquidation_qty) 레거시 함수 영구 삭제
# 🚨 [V25.20 엣지 케이스 패치] 0주 새출발 시 줍줍(Sweep) 타점 생성 원천 차단 (단일 라우터 방어막 이식)
# 🚀 [V26.03 영속성 캐시 이식] 서버 재시작 시 잔차 증발(기억상실)을 방어하는 L1/L2 듀얼 캐싱 엔진 탑재
# 🚀 [V27.01 지시서 스냅샷] 매일 17:05 확정 지시서를 박제하여 장중 잔고 변이에 따른 타점 왜곡 원천 차단
# 🚨 [V27.03 핫픽스] 스냅샷 로드 시 내부 날짜 검사(Validation) 전면 폐기로 무한루프 영구 방어
# 🚨 [V27.05 그랜드 수술] API Reject 방어(소수점 덤핑 차단), ZeroDivision 방어 및 Safe Casting 완벽 이식
# 🚨 [V27.15 코파일럿 합작] FD 누수 방어, 스냅샷 덮어쓰기 락온, 0달러 로트 배제 및 TypeError 런타임 붕괴 방어막 이식 완료
# MODIFIED: [V28.08 그랜드 수술] 스냅샷 영구 박제에 따른 VWAP 디커플링 방어막 완벽 이식 (0주 새출발 타임 패러독스 영구 소각)
# MODIFIED: [V28.19 타임존 락온] datetime.now()를 EST(미국 동부) 기준으로 강제 고정하여 KST 자정 경계 스냅샷 증발 버그 완벽 수술
# MODIFIED: [V28.20 무조건 진입 투트랙] 0주 새출발 시 VWAP 런타임 타격에서 Buy1 상한선 방어막 철거 (스냅샷 락온과 완벽 무결점 디커플링 이식)
# NEW: [V28.22 AI 환각 방어 백신 이식] 공수 교대 로직에 AI 에이전트 오판 차단 경고 주석 하드코딩
# NEW: [V28.27 자전거래 락온 방어막] 매도 단가 역전 시 매수 단가 강제 캡핑(Capping) 적용하여 API Reject 엣지 케이스 완벽 수술
# MODIFIED: [V28.28 하이브리드 병합] 0주 -> 1주 전환 시 텔레그램 스냅샷(UI) 렌더링 디커플링 맹점 완벽 수술 (매도 팩트 동적 덮어쓰기 이식)
# MODIFIED: [V28.42] U_CURVE_WEIGHTS 동기화(합산 1.0) 및 0주 새출발 Buy1 타점 고정 락온 수술 완료
# MODIFIED: [V28.43] 0주 새출발 예산 분리 팩트 체크 및 안심 주석 하드코딩 (Buy1: 무제한 50% 20주 / Buy2: 조건부 50% 21주 락온)
# MODIFIED: [V28.44] 0주 새출발 Buy1 상한제 완전 철거 (50% 예산 20주 무조건 매수 락온 및 타점 붕괴 영구 방어)
# 🚨 [V29.06 팩트 증명] 한투 평단가 하방 오염 100% 영구 차단 검증. 본 엔진은 외부 평단가(actual_avg) 개입을 일절 불허하며 오직 큐(q_data) 기반 순수 역산 평단가만 사용함이 검증됨.
# MODIFIED: [V29.07] 0주 새출발 VWAP 타점 붕괴 및 호가 스프레드(Ask) 스킵 맹점 100% 영구 차단 (스냅샷 앵커 복원)
# MODIFIED: [V29.09] 0주 새출발 시각적 디커플링 차단 (스냅샷 강제 덮어쓰기) 및 0주 타점 역배선(Swap) 팩트 교정 수술 완료
# 🚨 [V30.07 NEW] 0주 새출발 당일 매도 영구 동결 락온 이식:
# 당일 0주로 스냅샷이 박제된 세션(is_zero_start_fact=True)에서는 1주가 부분 체결되더라도
# 정규장(REG) 내의 모든 SELL 지시를 100% 강제 소각하고 오직 애프터마켓(AFTER)에서만 덫을 놓도록
# get_dynamic_plan 렌더링 파이프라인에 강력한 필터링 방어막 이식.
# MODIFIED: [V30.09 핫픽스] pytz 영구 적출 및 ZoneInfo('America/New_York') 이식으로 LMT 버그 차단
# NEW: [자정 경계 스냅샷/캐시 증발(Cinderella) 타임 패러독스 완벽 방어] 런타임 붕괴(AttributeError) 차단 정수 기반 락온
# NEW: [V40.XX 옴니 매트릭스] V-REV 내부 U_CURVE 배열 영구 소각 및 vwap_data.py 동적 30분 재정규화 파이프라인 연결 완료
# 🚨 MODIFIED: [V43.28 그랜드 수술] BUY 슬라이싱 누수(부족 매수) 방어. 조건 불만족 스킵 시 예산을 무조건 잔차 달러 버킷(Residual)에 이월시켜 100% 소진을 락온.
# 🚨 MODIFIED: [V43.28 엣지 케이스 수술] SELL 이중 차감 조기 종료 방어. LIFO 큐(total_q) 자체가 실시간 팩트이므로 executed 차감을 영구 소각하여 멱등성 확보.
# 🚨 MODIFIED: [V44.08 팩트 교정] V-REV 매수 예산 잔차 버킷 이월 시 발생하는 수량(Qty) 소수점 섞임 차원 붕괴 영구 방어 완료 (순수 달러($) 캐싱 보장)
# 🚨 MODIFIED: [V44.11 팩트 교정] 0주 새출발 시 1층 예산 100% 강제 진입을 보장하기 위해 Buy1 상한선을 15% 할증(* 1.15)으로 상향 락온.
# 🚨 MODIFIED: [V44.25 예산 탈취(Stealing) 런타임 붕괴 방어막 이식] Buy1이 Buy2의 미사용 예산을 훔쳐와 무한 타격(34주 체결 등)하는 차원 붕괴를 영구 소각.
# ==========================================================
import math
import os
import json
import tempfile
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# NEW: [V40.XX 옴니 매트릭스] U-Curve 플러그인 로드 방어막
try:
    from vwap_data import VWAP_PROFILES
except ImportError:
    VWAP_PROFILES = {}
    logging.warning("⚠️ [V-REV] vwap_data.py 플러그인을 찾을 수 없습니다.")

class ReversionStrategy:
    def __init__(self):
        self.residual = {
            "BUY1": {}, "BUY2": {}, 
            "SELL_L1": {}, "SELL_UPPER": {}, "SELL_JACKPOT": {}
        }
        self.executed = {"BUY_BUDGET": {}, "SELL_QTY": {}}
        self.state_loaded = {}
        self.was_holding = {}

    def _get_logical_date_str(self):
        now_est = datetime.now(ZoneInfo('America/New_York'))
        if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 5):
            target_date = now_est - timedelta(days=1)
        else:
            target_date = now_est
        return target_date.strftime("%Y-%m-%d")

    def _get_state_file(self, ticker):
        today_str = self._get_logical_date_str()
        return f"data/vwap_state_REV_{today_str}_{ticker}.json"

    def _get_snapshot_file(self, ticker):
        today_str = self._get_logical_date_str()
        return f"data/daily_snapshot_REV_{today_str}_{ticker}.json"

    def _load_state_if_needed(self, ticker):
        today_str = self._get_logical_date_str()
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
                    self.was_holding[ticker] = bool(data.get("was_holding", False))
                    self.state_loaded[ticker] = today_str
                    return
            except Exception:
                pass
                
        for k in self.residual.keys():
            self.residual[k][ticker] = 0.0
        self.executed["BUY_BUDGET"][ticker] = 0.0
        self.executed["SELL_QTY"][ticker] = 0
        self.was_holding[ticker] = False
        self.state_loaded[ticker] = today_str

    def _save_state(self, ticker):
        today_str = self._get_logical_date_str()
        state_file = self._get_state_file(ticker)
        data = {
            "date": today_str,
            "residual": {k: float(self.residual[k].get(ticker, 0.0)) for k in self.residual.keys()},
            "executed": {
                "BUY_BUDGET": float(self.executed.get("BUY_BUDGET", {}).get(ticker, 0.0)),
                "SELL_QTY": int(self.executed.get("SELL_QTY", {}).get(ticker, 0))
            },
            "was_holding": bool(self.was_holding.get(ticker, False))
        }
        temp_path = None
        try:
            dir_name = os.path.dirname(state_file)
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, state_file)
            temp_path = None
        except Exception:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass

    def save_daily_snapshot(self, ticker, plan_data):
        snap_file = self._get_snapshot_file(ticker)
        if os.path.exists(snap_file):
            return
            
        today_str = self._get_logical_date_str()
        data = {
            "date": today_str,
            "plan": plan_data
        }
        temp_path = None
        try:
            dir_name = os.path.dirname(snap_file)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, snap_file)
            temp_path = None
        except Exception:
            if temp_path and os.path.exists(temp_path):
                try: os.unlink(temp_path)
                except OSError: pass

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

    # 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
    # 17:05에 장전된 LOC는 체결 확정이 아니므로 절대 record_execution으로 예산을 선차감하지 말 것.
    # 공수 교대 시 reset_residual은 소수점 잔차만 초기화해야 하며, 이미 집행된 executed 예산을 소각하면 이중 합산(Double Spending) 버그가 발생하므로 절대 건드리지 말 것.
    def reset_residual(self, ticker):
        self._load_state_if_needed(ticker)
        self.residual["BUY1"][ticker] = 0.0
        self.residual["BUY2"][ticker] = 0.0
        self.residual["SELL_L1"][ticker] = 0.0
        self.residual["SELL_UPPER"][ticker] = 0.0
        self.residual["SELL_JACKPOT"][ticker] = 0.0
        self._save_state(ticker)

    def record_execution(self, ticker, side, qty, exec_price):
        self._load_state_if_needed(ticker)
        safe_qty = int(float(qty or 0))
        safe_price = float(exec_price or 0.0)
        
        if side == "BUY":
            spent = safe_qty * safe_price
            self.executed["BUY_BUDGET"][ticker] = float(self.executed.get("BUY_BUDGET", {}).get(ticker, 0.0)) + spent
        else:
            self.executed["SELL_QTY"][ticker] = int(self.executed.get("SELL_QTY", {}).get(ticker, 0)) + safe_qty
        self._save_state(ticker)

    # 🚨 [V30.07] market_type 파라미터 추가 (기본값 "REG")
    def get_dynamic_plan(self, ticker, curr_p, prev_c, current_weight, vwap_status, min_idx, alloc_cash, q_data, is_snapshot_mode=False, market_type="REG"):
        self._load_state_if_needed(ticker)

        valid_q_data = [item for item in q_data if float(item.get('price', 0.0)) > 0]
        total_q = sum(int(item.get("qty", 0)) for item in valid_q_data)
        total_inv = sum(float(item.get('qty', 0)) * float(item.get('price', 0.0)) for item in valid_q_data)
        avg_price = (total_inv / total_q) if total_q > 0 else 0.0
        
        dates_in_queue = sorted(list(set(item.get('date') for item in valid_q_data if item.get('date'))), reverse=True)
        l1_qty, l1_price = 0, 0.0
        
        if dates_in_queue:
            lots_1 = [item for item in valid_q_data if item.get('date') == dates_in_queue[0]]
            l1_qty = sum(int(item.get('qty', 0)) for item in lots_1)
            l1_price = sum(float(item.get('qty', 0)) * float(item.get('price', 0.0)) for item in lots_1) / l1_qty if l1_qty > 0 else 0.0
            
        upper_qty = total_q - l1_qty
        upper_inv = total_inv - (l1_qty * l1_price)
        upper_avg = upper_inv / upper_qty if upper_qty > 0 else 0.0

        trigger_jackpot = round(avg_price * 1.010, 2)
        trigger_l1 = round(l1_price * 1.006, 2)
        trigger_upper = round(upper_avg * 1.005, 2) if upper_qty > 0 else 0.0

        cached_plan = self.load_daily_snapshot(ticker)
        
        if is_snapshot_mode:
            is_zero_start_session = (total_q == 0)
        else:
            is_zero_start_session = cached_plan.get("is_zero_start", cached_plan.get("snapshot_total_q", cached_plan.get("total_q", -1)) == 0) if cached_plan else (total_q == 0)

        # NEW: [V40.XX] 동적 U-Curve 30분 재정규화 파이프라인 세팅
        profile = VWAP_PROFILES.get(ticker, {})
        # 🚨 MODIFIED: [V43.28 핫픽스] 스케줄러 기상 시간(15:27)과 엇박자를 교정하여 27분부터 스캔 궤적 매핑
        target_keys = [f"15:{str(m).zfill(2)}" for m in range(27, 60)]
        total_target_vol = sum(profile.get(k, 0.0) for k in target_keys)
        
        now_est = datetime.now(ZoneInfo('America/New_York'))
        time_str = now_est.strftime('%H:%M')

        # 🚨 MODIFIED: [V40.XX 수술] min_idx가 아닌 time_str 매핑으로 VWAP 가동/대기 판별
        if not is_snapshot_mode and time_str not in target_keys:
            if cached_plan:
                if total_q == 0:
                    # MODIFIED: [V44.11 0주 새출발 15% 상한가 락온] 0주 진입 시 예산 100% 강제 진입을 위해 Buy1 타점을 15% 할증(1.15배)으로 상향 팩트 교정
                    p1_trigger_fact = round(prev_c * 1.15, 2)
                    p2_trigger_fact = round(prev_c * 0.999, 2)
                    b1_budget = alloc_cash * 0.5
                    b2_budget = alloc_cash - b1_budget
                    
                    q1 = math.floor(b1_budget / p1_trigger_fact) if p1_trigger_fact > 0 else 0
                    q2 = math.floor(b2_budget / p2_trigger_fact) if p2_trigger_fact > 0 else 0
                    
                    new_buy_orders = []
                    if q1 > 0: new_buy_orders.append({"side": "BUY", "qty": q1, "price": p1_trigger_fact})
                    if q2 > 0: new_buy_orders.append({"side": "BUY", "qty": q2, "price": p2_trigger_fact})
                    
                    cached_plan["orders"] = new_buy_orders
                    cached_plan["total_q"] = 0
                else:
                    buy_orders = [o for o in cached_plan.get("orders", []) if o.get("side") == "BUY"]
                    sell_orders = []
                    
                    rem_qty_total = max(0, int(total_q) - int(self.executed.get("SELL_QTY", {}).get(ticker, 0)))
                    if rem_qty_total > 0:
                        sell_orders.append({"side": "SELL", "qty": rem_qty_total, "price": trigger_jackpot})
                        
                        available_l1 = min(l1_qty, rem_qty_total)
                        l1_queued = 0
                        if available_l1 > 0:
                            sell_orders.append({"side": "SELL", "qty": available_l1, "price": trigger_l1})
                            l1_queued = available_l1
                            
                        available_upper = min(upper_qty, rem_qty_total - l1_queued)
                        if available_upper > 0:
                            sell_orders.append({"side": "SELL", "qty": available_upper, "price": trigger_upper})
                    
                    cached_plan["orders"] = buy_orders + sell_orders
                    cached_plan["snapshot_total_q"] = cached_plan.get("snapshot_total_q", cached_plan.get("total_q", 0)) 
                    cached_plan["total_q"] = total_q
                
                if is_zero_start_session and market_type != "AFTER":
                    cached_plan["orders"] = [o for o in cached_plan.get("orders", []) if o.get("side") != "SELL"]
                    
                return cached_plan

        # 정규장 30분 미만 & VWAP 구간 밖일 경우 HOLD
        if time_str not in target_keys:
            if not vwap_status.get('is_strong_up') and not vwap_status.get('is_strong_down'):
                return {"orders": [], "trigger_loc": False, "total_q": total_q}

        if is_zero_start_session or total_q == 0:
            side = "BUY"
            # MODIFIED: [V44.11 0주 새출발 15% 상한가 락온] 0주 진입 시 예산 100% 강제 진입을 위해 Buy1 타점을 15% 할증(1.15배)으로 상향 팩트 교정
            p1_trigger = round(prev_c * 1.15, 2)
            p2_trigger = round(prev_c * 0.999, 2)
        else:
            side = "SELL" if curr_p > prev_c else "BUY"
            p1_trigger = round(prev_c * 0.995, 2)
            p2_trigger = round(prev_c * 0.9725, 2)

        if total_q > 0:
            active_sell_targets = [t for t in [trigger_jackpot, trigger_l1, trigger_upper] if t > 0]
            if active_sell_targets:
                min_sell = min(active_sell_targets)
                if p1_trigger >= min_sell:
                    p1_trigger = max(0.01, round(min_sell - 0.01, 2))
                if p2_trigger >= min_sell:
                    p2_trigger = max(0.01, round(min_sell - 0.01, 2))

        is_strong_up = vwap_status.get('is_strong_up', False)
        is_strong_down = vwap_status.get('is_strong_down', False)
        trigger_loc = is_strong_up or is_strong_down 

        orders = []

        if trigger_loc or is_snapshot_mode:
            total_spent = float(self.executed["BUY_BUDGET"].get(ticker, 0.0))
            rem_budget = max(0.0, float(alloc_cash) - total_spent)
            if rem_budget > 0:
                b1_budget = rem_budget * 0.5
                b2_budget = rem_budget - b1_budget
                
                q1 = math.floor(b1_budget / p1_trigger) if p1_trigger > 0 else 0
                q2 = math.floor(b2_budget / p2_trigger) if p2_trigger > 0 else 0
                
                if q1 > 0: orders.append({"side": "BUY", "qty": q1, "price": p1_trigger})
                if q2 > 0: orders.append({"side": "BUY", "qty": q2, "price": p2_trigger})
                
                if total_q > 0:
                    max_n = 5
                    if curr_p > 0:
                        required_n = math.ceil(b2_budget / curr_p) - q2
                        if required_n > 5:
                            max_n = min(required_n, 50)
                    
                    for n in range(1, max_n + 1):
                        if (q2 + n) > 0:
                            grid_p2 = round(b2_budget / (q2 + n), 2)
                            if grid_p2 >= 0.01 and grid_p2 < p2_trigger:
                                orders.append({"side": "BUY", "qty": 1, "price": grid_p2})
                
            rem_qty_total = max(0, int(total_q) - int(self.executed["SELL_QTY"].get(ticker, 0)))
            if rem_qty_total > 0:
                if curr_p >= trigger_jackpot:
                    orders.append({"side": "SELL", "qty": rem_qty_total, "price": trigger_jackpot})
                else:
                    available_l1 = min(l1_qty, rem_qty_total)
                    l1_queued = 0
                    if available_l1 > 0 and curr_p >= trigger_l1:
                        orders.append({"side": "SELL", "qty": available_l1, "price": trigger_l1})
                        l1_queued = available_l1
                        
                    available_upper = min(upper_qty, rem_qty_total - l1_queued)
                    if available_upper > 0 and trigger_upper > 0 and curr_p >= trigger_upper:
                        orders.append({"side": "SELL", "qty": available_upper, "price": trigger_upper})
            
            plan_result = {
                "orders": orders, 
                "trigger_loc": True, 
                "total_q": total_q,
                "is_zero_start": is_zero_start_session
            }
            
            if is_zero_start_session and market_type != "AFTER":
                plan_result["orders"] = [o for o in plan_result.get("orders", []) if o.get("side") != "SELL"]
            
            if is_snapshot_mode:
                self.save_daily_snapshot(ticker, plan_result)
                
            return plan_result

        # NEW: [V40.XX] V-REV 동적 재정규화 분할 연산 파이프라인
        rem_weight = 0.0
        if time_str in target_keys:
            start_idx = target_keys.index(time_str)
            rem_vol = sum(profile.get(k, 0.0) for k in target_keys[start_idx:])
            rem_weight = (rem_vol / total_target_vol) if total_target_vol > 0 else (30 - start_idx) / 30.0
            
            # 🚨 MODIFIED: [V43.28 핫픽스] 매수(BUY)도 매도(SELL)와 동일하게 동적 이월(Carry-over) 재정규화 비율 적용
            slice_ratio_sell = current_weight / rem_weight if rem_weight > 0 else 1.0
            slice_ratio_buy = current_weight / rem_weight if rem_weight > 0 else 1.0
        else:
            slice_ratio_sell = 0.0
            slice_ratio_buy = 0.0

        if side == "BUY":
            total_spent = float(self.executed["BUY_BUDGET"].get(ticker, 0.0))
            rem_budget = max(0.0, float(alloc_cash) - total_spent)
            
            if rem_budget <= 0:
                return {"orders": [], "trigger_loc": False, "total_q": total_q}
            
            # 🚨 MODIFIED: [V44.25 예산 탈취(Stealing) 런타임 붕괴 방어막]
            # rem_budget을 반으로 가르는 기존 방식은 Buy2의 미사용 예산을 Buy1이 훔쳐서 무한 타격(34주 체결 등)하는 차원 붕괴를 유발.
            # 전체 예산(alloc_cash)에 현재 분봉의 절대 가중치(current_weight)를 곱해 순수 1분 할당량을 도출하고,
            # 이를 정확히 50%씩 쪼개어 각각의 고립된 잔차 버킷(Residual)에 이월시키는 완벽한 디커플링 이식.
            raw_b1_slice = (float(alloc_cash) * 0.5) * current_weight
            raw_b2_slice = (float(alloc_cash) * 0.5) * current_weight
            
            b1_bucket = float(self.residual["BUY1"].get(ticker, 0.0)) + raw_b1_slice
            b2_bucket = float(self.residual["BUY2"].get(ticker, 0.0)) + raw_b2_slice

            b1_budget_slice = min(b1_bucket, rem_budget)
            b2_budget_slice = min(b2_bucket, max(0.0, rem_budget - b1_budget_slice))

            if curr_p > 0:
                if is_zero_start_session or curr_p <= p1_trigger:
                    alloc_q1 = int(math.floor(b1_budget_slice / curr_p))
                    # 수량의 소수점이 아닌 '사용하고 남은 달러'를 버킷에 돌려줌
                    self.residual["BUY1"][ticker] = b1_bucket - (alloc_q1 * curr_p)
                    if alloc_q1 > 0:
                        orders.append({"side": "BUY", "qty": alloc_q1, "price": p1_trigger})
                else:
                    # 조건 미충족 시 예산 100% 다음 분봉으로 이월 (달러 유지)
                    self.residual["BUY1"][ticker] = b1_bucket
                    
                if curr_p <= p2_trigger:
                    alloc_q2 = int(math.floor(b2_budget_slice / curr_p))
                    # 수량의 소수점이 아닌 '사용하고 남은 달러'를 버킷에 돌려줌
                    self.residual["BUY2"][ticker] = b2_bucket - (alloc_q2 * curr_p)
                    if alloc_q2 > 0:
                        orders.append({"side": "BUY", "qty": alloc_q2, "price": p2_trigger})
                else:
                    # 조건 미충족 시 예산 100% 다음 분봉으로 이월 (달러 유지)
                    self.residual["BUY2"][ticker] = b2_bucket
            else:
                self.residual["BUY1"][ticker] = b1_bucket
                self.residual["BUY2"][ticker] = b2_bucket

        else: # SELL
            # 🚨 MODIFIED: [V43.28 핫픽스] 큐(Queue) 팩트 기반 이중 차감 맹점 원천 차단
            # queue_ledger.pop_lots 에 의해 total_q 자체가 실시간으로 줄어들고 있으므로 
            # executed["SELL_QTY"]를 다시 빼는 이중 차감 버그(매도 조기 종료 현상)를 영구 소각함.
            rem_qty_total = total_q
            
            if rem_qty_total <= 0:
                return {"orders": [], "trigger_loc": False, "total_q": total_q}

            if slice_ratio_sell > 0:
                if curr_p >= trigger_jackpot:
                    exact_qs = float(rem_qty_total * slice_ratio_sell) + float(self.residual["SELL_JACKPOT"].get(ticker, 0.0))
                    alloc_qs = int(min(math.floor(exact_qs), rem_qty_total))
                    self.residual["SELL_JACKPOT"][ticker] = float(exact_qs - alloc_qs)
                    if alloc_qs > 0:
                        orders.append({"side": "SELL", "qty": alloc_qs, "price": trigger_jackpot})
                else:
                    if l1_qty > 0 and curr_p >= trigger_l1:
                        # 1층 물량이 이미 매도된 상태를 고려하여 1층 잔여량 정밀 스캔
                        sold_so_far = int(total_q) - rem_qty_total
                        rem_l1_qty = max(0, l1_qty - sold_so_far)
                        if rem_l1_qty > 0:
                            exact_l1 = float(rem_l1_qty * slice_ratio_sell) + float(self.residual["SELL_L1"].get(ticker, 0.0))
                            alloc_l1 = int(min(math.floor(exact_l1), rem_l1_qty))
                            self.residual["SELL_L1"][ticker] = float(exact_l1 - alloc_l1)
                            if alloc_l1 > 0:
                                orders.append({"side": "SELL", "qty": alloc_l1, "price": trigger_l1})
                                rem_qty_total -= alloc_l1

                    if upper_qty > 0 and trigger_upper > 0 and curr_p >= trigger_upper and rem_qty_total > 0:
                        exact_upper = float(rem_qty_total * slice_ratio_sell) + float(self.residual["SELL_UPPER"].get(ticker, 0.0))
                        alloc_upper = int(min(math.floor(exact_upper), rem_qty_total))
                        self.residual["SELL_UPPER"][ticker] = float(exact_upper - alloc_upper)
                        if alloc_upper > 0:
                            orders.append({"side": "SELL", "qty": alloc_upper, "price": trigger_upper})

        if is_zero_start_session and market_type != "AFTER":
            orders = [o for o in orders if o.get("side") != "SELL"]

        self._save_state(ticker)
        return {"orders": orders, "trigger_loc": False, "total_q": total_q}

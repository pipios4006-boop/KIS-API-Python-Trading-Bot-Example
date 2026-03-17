# 파일명: strategy.py
import math

class InfiniteStrategy:
    # [V14.2] 전략 클래스 - V14.1 로직 온전히 유지
    def __init__(self, config):
        self.cfg = config

    def _ceil(self, val): return math.ceil(val * 100) / 100.0
    def _floor(self, val): return math.floor(val * 100) / 100.0

    def get_plan(self, ticker, current_price, avg_price, qty, prev_close, ma_5day=0.0, market_type="REG", available_cash=0, is_simulation=False, force_turbo_off=False):
        orders = []
        process_status = "" 
        
        # 🚀 [V16.8 핵심] 타 종목이 금고에 잠가둔 격리금 확인 및 나의 진짜 예산 도출
        other_locked_cash = self.cfg.get_total_locked_cash(exclude_ticker=ticker)
        real_available_cash = max(0, available_cash - other_locked_cash)
        
        seed = self.cfg.get_seed(ticker)
        split = self.cfg.get_split_count(ticker)      
        target_pct_val = self.cfg.get_target_profit(ticker) 
        target_ratio = target_pct_val / 100.0
        version = self.cfg.get_version(ticker)
        
        # [V14.1 리버스모드] 상태값 불러오기
        rev_state = self.cfg.get_reverse_state(ticker)
        is_reverse = rev_state["is_active"]
        rev_day = rev_state["day_count"]
        # [V14.9] 저장된 탈출 목표 꼬리표 불러오기
        exit_target = rev_state.get("exit_target", 0.0)

        if version == "V14":
            _, one_portion_amt, rem_cash = self.cfg.calculate_v14_state(ticker)
            
            # 🔥 [V15.7] T값(진행률)의 완전한 독립: 장부 내역을 의존하지 않고 오직 KIS 팩트(총수량 × 평단가)로 오버라이드 계산
            t_val = round((qty * avg_price) / one_portion_amt, 4) if one_portion_amt > 0 else 0.0
            
            # 🔥 [V14.1 조기 리버스모드] 우선순위에서 밀려 예수금이 부족한지 사전 감지
            # [V14.4 버그수정] 프리마켓 순찰(PRE_CHECK) 시 지갑 잔액이 넘어오지 않아 예산 부족으로 오인하는 현상 방어
            # 🚀 [V16.8 변경] available_cash 대신 real_available_cash를 사용하여 철저한 예산 심판
            is_money_short_check = False if (is_simulation or market_type == "PRE_CHECK") else (real_available_cash < one_portion_amt)
            
            # 🔥 [V14.9 리버스모드 진입 판별기 및 동적 꼬리표 부착]
            if not is_reverse and (t_val > (split - 1) or (qty > 0 and is_money_short_check)):
                is_reverse = True # 소진 발생 또는 예산 부족, 리버스모드 진입
                rev_day = 1 # 첫날 진입
                
                # 진입 순간의 현재 손실률 파악
                current_return = (current_price - avg_price) / avg_price * 100.0 if avg_price > 0 else 0.0
                default_exit = -15.0 if ticker == "TQQQ" else -20.0
                
                # 핑퐁 방지: 이미 손실이 -20% 이내라면, 0%(평단가 회복)를 탈출 목표로 세팅!
                if current_return >= default_exit:
                    exit_target = 0.0
                else:
                    exit_target = default_exit

                # 정규장 판별 시에만 파일에 확정 저장
                if market_type == "REG":
                    self.cfg.set_reverse_state(ticker, True, rev_day, exit_target)
            
            # 🔥 [V14.10] 0% 꼬리표 감옥(Trap) 탈출 동적 알고리즘!
            if is_reverse and current_price > 0 and avg_price > 0:
                current_return = (current_price - avg_price) / avg_price * 100.0
                default_exit = -15.0 if ticker == "TQQQ" else -20.0
                
                # 얕게 물려서 0% 꼬리표를 달았지만, 이후 폭락장을 만나 깊게 물렸다면?
                # 0% 감옥을 허물어 버리고, 정상적인 라오어님의 탈출 룰(-15%, -20%)로 업그레이드!
                if exit_target == 0.0 and current_return < default_exit:
                    exit_target = default_exit
                
                # 설정된 목표 도달 시 감옥 탈출!
                if current_return >= exit_target:
                    is_reverse = False
                    rev_day = 0
                    if market_type == "REG":
                        # 탈출 시 상태와 꼬리표 초기화
                        self.cfg.set_reverse_state(ticker, False, 0, 0.0)
                        # 🚀 [V16.8 추가] 탈출 시 휘발성 에스크로 금고 완벽 소각
                        self.cfg.clear_escrow_cash(ticker)
        else:
            # ConfigManager의 검증된 로직으로 단일화 (ValueError 발생 시 상위에서 처리)
            one_portion_amt = self.cfg.get_one_portion(ticker)
            t_val = self.cfg.calculate_t_val(ticker, qty, avg_price)

        # split이 0인 경우에 대한 최종 방어
        depreciation_factor = 2.0 / split if split > 0 else 0.1
        star_ratio = target_ratio - (target_ratio * depreciation_factor * t_val)
        
        # [V14.1 리버스모드] 리버스일 경우 별지점은 5일선 평균으로 강제 변경
        if is_reverse:
            star_price = round(ma_5day, 2)
            # 🔥 [V14.11 버그수정] 장부상 남은 잔금/4 를 쓰되, 실제 지갑에 남은 할당 금액(available_cash)을 절대 초과할 수 없도록 강제 방어! (마이너스 통장 방지)
            # 🚀 [V15 / V16.8 변경] 리버스 예산 분배 최적화 및 마이너스 통장 방지를 위해 real_available_cash 기준 방어!
            active_tickers_count = len(self.cfg.get_active_tickers())
            if active_tickers_count == 0: active_tickers_count = 1
            raw_portion = (rem_cash / 4.0) / active_tickers_count if rem_cash > 0 else 0.0 
            one_portion_amt = min(raw_portion, real_available_cash) if not is_simulation else raw_portion
        else:
            star_price = self._ceil(avg_price * (1 + star_ratio)) if avg_price > 0 else 0
            
        target_price = self._ceil(avg_price * (1 + target_ratio)) if avg_price > 0 else 0
        is_last_lap = (split - 1) < t_val < split
        
        # 🚀 [V16.8 변경] available_cash 대신 real_available_cash 사용
        if is_simulation: is_money_short = False
        else: is_money_short = real_available_cash < one_portion_amt

        base_price = current_price if current_price > 0 else prev_close
        if base_price <= 0: return {"orders": [], "t_val": t_val, "one_portion": one_portion_amt, "process_status": "⛔가격오류", "is_reverse": is_reverse, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash}

        if market_type == "PRE_CHECK":
            process_status = "🌅프리마켓"
            # [V14.1 리버스모드] 리버스 중에는 프리마켓 익절 감시를 하지 않음
            if qty > 0 and target_price > 0 and current_price >= target_price and not is_reverse:
                orders.append({"side": "SELL", "price": current_price, "qty": qty, "type": "LIMIT", "desc": "🌅프리:목표돌파익절"})
            return {"orders": orders, "t_val": t_val, "one_portion": one_portion_amt, "process_status": process_status, "is_reverse": is_reverse, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash}

        if market_type == "REG":
            if qty == 0:
                process_status = "✨새출발"
                buy_price = round(self._ceil(base_price * 1.15) - 0.01, 2)
                buy_qty = math.floor(one_portion_amt / buy_price) if buy_price > 0 else 0
                if buy_qty > 0:
                    orders.append({"side": "BUY", "price": buy_price, "qty": buy_qty, "type": "LOC", "desc": "🆕새출발"})
                return {"orders": orders, "t_val": t_val, "one_portion": one_portion_amt, "process_status": process_status, "is_reverse": False, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash}

            # --------------------------------------------------------
            # 🔥 [V14.1 리버스모드] 정규장 하이브리드 지시 로직
            # --------------------------------------------------------
            if is_reverse:
                # 20분할은 전체 개수를 10등분, 40분할은 20등분
                sell_divisor = 10 if split <= 20 else 20
                sell_qty = math.floor(qty / sell_divisor) 

                # 🔥 [V16.7 추가] 긴급 자금 수혈 로직: 에스크로 잔금이 1주 살 돈보다 적다면 MOC 강제 매도
                is_emergency_cash_needed = (real_available_cash < base_price) and (rev_day > 1)

                if rev_day == 1 or is_emergency_cash_needed:
                    process_status = "🩸리버스(긴급수혈)" if is_emergency_cash_needed else "🚨리버스(1일차)"
                    # 첫날이거나 자금이 고갈된 경우, 매수 없이 무조건 MOC 매도만 진행하여 자금 확보
                    if sell_qty > 0:
                        # [V14.6] 주문명 간소화 (리버스MOC매도 -> 의무매도) + 수혈매도 추가
                        desc_str = "🩸수혈매도(MOC)" if is_emergency_cash_needed else "🛡️의무매도"
                        orders.append({"side": "SELL", "price": 0, "qty": sell_qty, "type": "MOC", "desc": desc_str})
                else:
                    process_status = f"🔄리버스({rev_day}일차)"
                    buy_qty = 0
                    buy_price = 0
                    # 둘째날 이후: 잔금/4 로 쿼터 매수 시도
                    if one_portion_amt > 0 and star_price > 0:
                        # 별지점 아래에서 매수 시도 (LOC)
                        buy_price = round(star_price - 0.01, 2)
                        # 🔥 [V14.10] 0달러 크래시(Division By Zero) 완벽 방어
                        if buy_price > 0: 
                            buy_qty = math.floor(one_portion_amt / buy_price)
                            if buy_qty > 0:
                                # [V14.6] 주문명 간소화 (리버스매수 -> 잔금매수)
                                orders.append({"side": "BUY", "price": buy_price, "qty": buy_qty, "type": "LOC", "desc": "⚓잔금매수"})
                    
                    # 5일선 별지점 위에서 LOC 매도 시도
                    if sell_qty > 0 and star_price > 0:
                        # [V14.6] 주문명 간소화 (리버스매도 -> 별값매도)
                        orders.append({"side": "SELL", "price": star_price, "qty": sell_qty, "type": "LOC", "desc": "🌟별값매도"})

                    # 🚀 [V15] 리버스 줍줍 모드 추가 (잔액/N 예산 내에서 가동)
                    if one_portion_amt > 0 and buy_price > 0:
                        for i in range(1, 6):
                            target_qty = buy_qty + i 
                            raw_jup_price = self._floor(one_portion_amt / target_qty)
                            capped_jup_price = min(raw_jup_price, buy_price - 0.01)
                            jup_price = round(capped_jup_price, 2)
                            if jup_price > 0:
                                orders.append({"side": "BUY", "price": jup_price, "qty": 1, "type": "LOC", "desc": f"🧹리버스줍줍({i})" })
                
                # [V14.9] 정규장 사이클을 돌 때마다 상태 갱신 (동적 꼬리표 유지)
                if market_type == "REG":
                    self.cfg.set_reverse_state(ticker, True, rev_day, exit_target)
                        
                return {"orders": orders, "t_val": t_val, "one_portion": one_portion_amt, "process_status": process_status, "is_reverse": is_reverse, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash}

            # --------------------------------------------------------
            # 기존 V13 / V14 일반 모드 지시 로직
            # --------------------------------------------------------
            if is_last_lap: process_status = "🏁마지막회차"
            elif is_money_short: process_status = "🛡️방어모드(부족)"
            elif t_val < (split / 2): process_status = "🌓전반전"
            else: process_status = "🌕후반전"

            can_buy = not is_money_short and not is_last_lap
            is_turbo_active = False if force_turbo_off else self.cfg.get_turbo_mode()
                
            if is_turbo_active and not is_last_lap:
                # 🚀 [V16.8 변경] available_cash 대신 real_available_cash 사용
                if is_simulation or real_available_cash >= one_portion_amt:
                    ref_price = min(avg_price, prev_close)
                    turbo_price = round(self._ceil(ref_price * 0.95) - 0.01, 2)
                    turbo_qty = math.floor(one_portion_amt / turbo_price) if turbo_price > 0 else 0
                    if turbo_qty > 0:
                        orders.append({"side": "BUY", "price": turbo_price, "qty": turbo_qty, "type": "LOC", "desc": "🏎️가속매수"})

            standard_buy_qty = 0 
            N = math.floor(one_portion_amt / avg_price) if avg_price > 0 else 0
            
            if can_buy:
                p_avg = round(self._ceil(avg_price) - 0.01, 2)
                p_star = round(star_price - 0.01, 2)

                if t_val < (split / 2):
                    half_amt = one_portion_amt * 0.5
                    q_avg_init = math.floor(half_amt / p_avg) if p_avg > 0 else 0
                    q_star = math.floor(half_amt / p_star) if p_star > 0 else 0
                    total_basic = q_avg_init + q_star
                    if total_basic < N: q_avg = q_avg_init + (N - total_basic)
                    else: q_avg = q_avg_init
                    
                    if q_avg > 0:
                        orders.append({"side": "BUY", "price": p_avg, "qty": q_avg, "type": "LOC", "desc": "⚓평단매수"})
                        standard_buy_qty += q_avg
                    if q_star > 0:
                        orders.append({"side": "BUY", "price": p_star, "qty": q_star, "type": "LOC", "desc": "💫별값매수"})
                        standard_buy_qty += q_star
                else: 
                    if p_star > 0:
                        q_star = math.floor(one_portion_amt / p_star)
                        if q_star > 0:
                            orders.append({"side": "BUY", "price": p_star, "qty": q_star, "type": "LOC", "desc": "💫별값매수"})
                            standard_buy_qty += q_star

            if one_portion_amt > 0 and (is_simulation or not is_money_short):
                base_qty_for_jupjup = standard_buy_qty if standard_buy_qty > 0 else N
                if base_qty_for_jupjup == 0: base_qty_for_jupjup = 1
                for i in range(1, 6):
                    target_qty = base_qty_for_jupjup + i 
                    raw_jup_price = self._floor(one_portion_amt / target_qty)
                    capped_jup_price = min(raw_jup_price, avg_price - 0.01)
                    jup_price = round(capped_jup_price, 2)
                    if jup_price > 0:
                        orders.append({"side": "BUY", "price": jup_price, "qty": 1, "type": "LOC", "desc": f"🧹줍줍({i})" })

            q_qty = math.ceil(qty / 4)
            r_qty = qty - q_qty 

            buy_orders_exist = (len([o for o in orders if o['side']=='BUY']) > 0)
            force_moc = is_last_lap or (is_money_short and not buy_orders_exist)
            
            if force_moc:
                orders.append({"side": "SELL", "price": 0, "qty": q_qty, "type": "MOC", "desc": "🛡️쿼터MOC"})
            else:
                if star_price > 0:
                    orders.append({"side": "SELL", "price": star_price, "qty": q_qty, "type": "LOC", "desc": "🌟쿼터매도"})

            if target_price > 0:
                orders.append({"side": "SELL", "price": target_price, "qty": r_qty, "type": "LIMIT", "desc": "🎯목표익절"})

            return {"orders": orders, "t_val": t_val, "one_portion": one_portion_amt, "process_status": process_status, "is_reverse": False, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash}
            
        return {"orders": orders, "t_val": t_val, "one_portion": one_portion_amt, "process_status": "대기", "is_reverse": is_reverse, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash}

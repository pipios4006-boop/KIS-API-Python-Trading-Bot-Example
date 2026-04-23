# ==========================================================
# [scheduler_trade.py] - 🌟 100% 통합 전투 사령부 (V29.21) 🌟 (Part 1/2)
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# 🚨 [V27.13 그랜드 수술] 코파일럿 합작 5대 엣지 케이스 완벽 수술 완료
# 🚀 [V28.01 그랜드 수술] 서머타임 데드락 방어 윈도우 65분 확장, 결측치 락온 차단, tx_lock 런타임 붕괴 가드 완비
# 🚀 [V28.02 그랜드 수술] 이중 매도 방어, 큐 증발 Fallback, UX 모순 팩트 교정
# 🚀 [V28.04 그랜드 수술] KIS API LOC 중복 응답 뻥튀기 맹점 원천 차단 (odno 기반 병합 엔진)
# 🚀 [V28.05 그랜드 수술] VWAP 앵커 영속화(Lock-on) 및 기억상실(Amnesia) 오발탄 방어막 이식
# 🚀 [V28.07 그랜드 수술] 스냅샷 강제 은폐 적출 및 VWAP 디커플링 무결성 확보
# 🚀 [V28.13 그랜드 수술] 애프터마켓 스냅샷 소각 맹점 적출 및 24시간 디커플링 보존
# 🚀 [V28.30 그랜드 수술] 애프터마켓 로터리 덫 휴장일 오발탄(False Fire) 원천 차단 쉴드 이식
# 🚀 [V28.31 그랜드 수술] V14 상방 스나이퍼 코어 100% 이식 및 V-REV 락다운 복 복원 완료
# 🚀 [V28.37 그랜드 수술] 스윕 피니셔 발화 후 '잔고 증발' 오발탄 원천 차단: sweep_msg_sent 플래그 교차 참조 바이패스 가드 이식
# MODIFIED: [V28.41] U_CURVE_WEIGHTS 배열 합산 불일치(0.9596)로 인한 예산 누수 버그 완벽 수술
# 🚨 [V28.50 NEW] AVWAP 조기퇴근 모드(Early Exit) 파이프라인 배선 개통 및 타겟 수익률 팩트 캐스팅
# 🚨 [V28.51 팩트 수술] 정규장 스케줄러 통신 지연(가짜 에러) 진단망 이식
# 🚨 [V29.03 팩트 수술] AVWAP 영속성(Persistence) 듀얼 캐시 동기화 이식 완료
# MODIFIED: [V29.08 핫픽스] AVWAP 암살자 런타임 라우팅 누수(AttributeError) 팩트 교정 완료
# MODIFIED: [V29.09 핫픽스] 0주 새출발 예방적 LOC 덫 타점 역배선(Swap) 팩트 교정 완료
# MODIFIED: [V29.10 핫픽스] 스나이퍼 모니터링 들여쓰기(Indentation) 붕괴 복구 및 SyntaxError 원천 차단
# MODIFIED: [V29.11 핫픽스] AVWAP 엔진 get_decision() 매개변수 불일치(TypeError) 팩트 교정 완료
# MODIFIED: [V29.12 핫픽스] 스나이퍼 모니터링 AttributeError(get_master_switch) Safe Casting 방어막 이식
# MODIFIED: [V29.13 핫픽스] 스나이퍼 모니터링 이중 락(Nested tx_lock) 데드락 붕괴 현상 원천 적출 완료
# MODIFIED: [V29.14 핫픽스] InfiniteStrategy check_sniper_condition 모듈 누락(AttributeError) Safe Bypass 방어막 이식
# MODIFIED: [V29.15 핫픽스] AVWAP 데이터 기아(Data Starvation) 원천 차단 및 필수 파라미터 배선 개통
# MODIFIED: [V29.17 그랜드 수술] AVWAP 딕셔너리 맵핑 불일치(Zero-Price), 스레드 마비(은닉메서드 오호출) 및 L1 기억상실 이중타격 맹점 완벽 적출 수술
# MODIFIED: [V29.20 팩트 수술] 미체결 스캔 문자열 참조 런타임 붕괴(AttributeError) 완벽 적출 및 시가 갭하락 시맨틱 오판 방어망(yfinance Open 팩트 파싱) 개통 완료
# MODIFIED: [V29.21 그랜드 수술] 미체결 스캔 시 예방적 LOC 덫(34) 혼선에 의한 스나이퍼 마비를 원천 차단하기 위해 순수 지정가(00) 팩트 필터링 도입. 
# MODIFIED: [V29.21 그랜드 수술] AVWAP 및 스나이퍼 타격 후 8초 다중 교차 검증(ODNO 기반 팩트 스캔) 엔진을 전면 이식하고 미체결 잔량 취소(Nuke) 및 실체결량(ccld_qty) 듀얼 캐싱 처리 완료.
# MODIFIED: [V29.21 핫픽스] get_execution_history 호출 시 날짜 파라미터 누락 버그를 today_est_str 팩트 주입으로 완벽 교정.
# ==========================================================
import logging
import datetime
import pytz
import asyncio
import traceback
import math
import os
import time
import json
import random
import glob
import yfinance as yf
import pandas_market_calendars as mcal

from scheduler_core import is_market_open, get_budget_allocation, get_target_hour

# ==========================================================
# 1. 🔫 스나이퍼 모니터링 (하이브리드 AVWAP 및 V14 상방 스나이퍼)
# ==========================================================
# 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
# 코파일럿 등이 제안하는 외부 KIS API 래퍼(동기식 블로킹) 모듈을 무지성으로 병합하지 마세요.
# asyncio 루프 내에서 requests나 동기 함수를 직접 호출하면 전체 스케줄러가 100% 교착(Deadlock)에 빠집니다.
# 반드시 기존의 asyncio.to_thread 래핑과 내부 broker.py 엔진만을 사용해 비동기 무결성을 유지하세요.
async def scheduled_sniper_monitor(context):
    if not is_market_open(): return
    
    est = pytz.timezone('US/Eastern')
    now_est = datetime.datetime.now(est)
    
    if context.job.data.get('tx_lock') is None:
        logging.warning("⚠️ [sniper_monitor] tx_lock 미초기화. 이번 사이클 스킵.")
        return
    
    try:
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
        if schedule.empty: return
        
        market_open = schedule.iloc[0]['market_open'].astimezone(est)
        market_close = schedule.iloc[0]['market_close'].astimezone(est)
    except Exception:
        if now_est.weekday() < 5:
            market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else: return
    
    pre_start = market_open - datetime.timedelta(hours=5, minutes=30)
    start_monitor = pre_start + datetime.timedelta(minutes=1)
    end_monitor = market_close - datetime.timedelta(minutes=1)
    
    if not (start_monitor <= now_est <= end_monitor):
        return

    is_regular_session = market_open <= now_est <= market_close
    
    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    
    base_map = app_data.get('base_map', {'SOXL': 'SOXX', 'TQQQ': 'QQQ'})
    chat_id = context.job.chat_id
    
    tracking_cache = app_data.setdefault('sniper_tracking', {})
    
    today_est_str = now_est.strftime('%Y%m%d')
    if tracking_cache.get('date') != today_est_str:
        tracking_cache.clear()
        tracking_cache['date'] = today_est_str
        try:
            for _f in glob.glob("data/sniper_cache_*.json"): os.remove(_f)
        except: pass
            
    async def _do_sniper():
        async with tx_lock:
            cash, holdings = await asyncio.to_thread(broker.get_account_balance)
            if holdings is None: return
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}
            avwap_free_cash = cash
            
            for t in cfg.get_active_tickers():
                version = cfg.get_version(t)
                
                if version == "V_REV":
                    h = safe_holdings.get(t) or {}
                    actual_qty = int(float(h.get('qty', 0)))
                    q_ledger = app_data.get('queue_ledger')
                    if q_ledger:
                        q_data = q_ledger.get_queue(t)
                        total_q = sum(item.get("qty", 0) for item in q_data)
                        
                        if actual_qty == 0 and total_q > 0:
                            _vwap_cache_ref = app_data.get('vwap_cache', {})
                            if _vwap_cache_ref.get(f"REV_{t}_sweep_msg_sent"):
                                continue
                                
                            if not tracking_cache.get(f"REV_{t}_panic_sell_warn"):
                                tracking_cache[f"REV_{t}_panic_sell_warn"] = True
                                await context.bot.send_message(
                                    chat_id=chat_id,
                                    text=f"🚨 <b>[비상] [{t}] 뇌동매매로 인한 잔고 증발이 감지되었습니다.</b>\n"
                                         f"▫️ 봇의 매매가 일시 정지됩니다.\n"
                                         f"▫️ 시드 오염을 막기 위해 즉시 <code>/reset</code> 커맨드를 실행하여 장부를 소각하십시오.",
                                    parse_mode='HTML'
                                )
                            continue
                
                if version == "V_REV":
                    if not cfg.get_avwap_hybrid_mode(t): continue
                    
                    if not tracking_cache.get(f"AVWAP_INIT_{t}"):
                        try:
                            saved_state = strategy.v_avwap_plugin.load_state(t, now_est)
                            if saved_state:
                                tracking_cache[f"AVWAP_BOUGHT_{t}"] = saved_state.get('bought', False)
                                tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = saved_state.get('shutdown', False)
                                tracking_cache[f"AVWAP_QTY_{t}"] = saved_state.get('qty', 0)
                                tracking_cache[f"AVWAP_AVG_{t}"] = saved_state.get('avg_price', 0.0)
                        except Exception as e:
                            logging.error(f"AVWAP 상태 복구 실패: {e}")
                        tracking_cache[f"AVWAP_INIT_{t}"] = True
                        
                    if tracking_cache.get(f"AVWAP_SHUTDOWN_{t}"): continue
                    
                    target_base = base_map.get(t, t)
                    
                    if f"AVWAP_CTX_{t}" not in tracking_cache or tracking_cache[f"AVWAP_CTX_{t}"] is None:
                        ctx_data = await asyncio.to_thread(strategy.fetch_avwap_macro, target_base)
                        if ctx_data is not None:
                            tracking_cache[f"AVWAP_CTX_{t}"] = ctx_data
                        else:
                            continue 
                    
                    ctx_data = tracking_cache.get(f"AVWAP_CTX_{t}")
                    avwap_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
                    avwap_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
                    
                    exec_curr_p = float(await asyncio.to_thread(broker.get_current_price, t) or 0.0)
                    if exec_curr_p <= 0: continue
                    
                    base_curr_p = float(await asyncio.to_thread(broker.get_current_price, target_base) or 0.0)
                    if base_curr_p <= 0: continue
                    
                    def _fetch_open(tkr):
                        try:
                            st = yf.Ticker(tkr)
                            h = st.history(period="1d", interval="1m", prepost=False, timeout=5)
                            if not h.empty: return float(h['Open'].dropna().iloc[0])
                        except: pass
                        return 0.0
                    
                    base_day_open = float(await asyncio.to_thread(_fetch_open, target_base) or 0.0)
                    
                    if base_day_open <= 0:
                        continue 
                    
                    df_1min_base = None
                    try: df_1min_base = await asyncio.to_thread(broker.get_1min_candles_df, target_base)
                    except: pass
                    
                    early_exit_mode = cfg.get_avwap_early_exit_mode(t)
                    early_target_profit = cfg.get_avwap_early_target(t) / 100.0
                    
                    decision = strategy.v_avwap_plugin.get_decision(
                        base_ticker=target_base,
                        exec_ticker=t,
                        base_curr_p=base_curr_p,
                        exec_curr_p=exec_curr_p,
                        base_day_open=base_day_open,
                        avwap_avg_price=avwap_avg,
                        avwap_qty=avwap_qty,
                        avwap_alloc_cash=avwap_free_cash,
                        context_data=ctx_data,
                        df_1min_base=df_1min_base,
                        now_est=now_est,
                        early_exit_mode=early_exit_mode,
                        early_target_profit=early_target_profit
                    )
                    
                    action = decision.get("action")
                    reason = decision.get("reason", "")
                    
                    if action == "BUY":
                        alloc_cash = decision.get("alloc_cash", 0)
                        price = float(decision.get("target_price", decision.get("price", 0.0)))
                        qty = int(decision.get("qty", 0))
                        
                        if qty > 0 and price > 0:
                            # MODIFIED: [V29.21 그랜드 수술] 예방적 LOC 덫(34) 혼선 차단을 위해 순수 지정가(00) 미체결만 바이패스 검증
                            has_unfilled = False
                            for _ in range(4):
                                unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                if isinstance(unfilled, list) and any(
                                    o.get('sll_buy_dvsn_cd') == '02' and str(o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '').strip().zfill(2) == '00' 
                                    for o in unfilled
                                ):
                                    has_unfilled = True
                                    break
                                await asyncio.sleep(2.0)
                            
                            if has_unfilled:
                                continue
                                
                            res = await asyncio.to_thread(broker.send_order, t, "BUY", qty, price, "LIMIT")
                            odno = res.get('odno', '') if isinstance(res, dict) else ''
                            
                            if res and res.get('rt_cd') == '0' and odno:
                                # MODIFIED: [V29.21 그랜드 수술] 8초(4회) 다중 교차 검증 및 미체결 잔여 강제 Nuke 로직 이식
                                ccld_qty = 0
                                for _ in range(4):
                                    await asyncio.sleep(2.0)
                                    unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                    safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                    
                                    my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                    if my_order:
                                        ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                    else:
                                        ccld_qty = qty
                                        break
                                
                                if ccld_qty < qty:
                                    try:
                                        await asyncio.to_thread(broker.cancel_order, t, odno)
                                        await asyncio.sleep(0.5)
                                    except Exception as e_cancel:
                                        logging.warning(f"⚠️ [{t}] AVWAP 매수 잔여 취소 실패: {e_cancel}")
                                
                                if ccld_qty > 0:
                                    msg = f"⚔️ <b>[AVWAP] 단타 암살자 딥매수 타격 성공!</b>\n▫️ 타겟: {t}\n▫️ 타점: ${price}\n▫️ 팩트 체결수량: {ccld_qty}주 (목표 {qty}주)\n▫️ 사유: {reason}"
                                    if ccld_qty < qty:
                                        msg += f"\n▫️ 미체결 {qty - ccld_qty}주는 안전을 위해 즉각 취소(Nuke)되었습니다."
                                        
                                    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                    
                                    old_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
                                    old_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
                                    new_qty = old_qty + ccld_qty
                                    new_avg = ((old_qty * old_avg) + (ccld_qty * price)) / new_qty if new_qty > 0 else 0.0

                                    tracking_cache[f"AVWAP_BOUGHT_{t}"] = True
                                    tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = False
                                    tracking_cache[f"AVWAP_QTY_{t}"] = new_qty
                                    tracking_cache[f"AVWAP_AVG_{t}"] = round(new_avg, 4)
                                    
                                    state_data = {
                                        "bought": True,
                                        "shutdown": False,
                                        "qty": new_qty,
                                        "avg_price": round(new_avg, 4)
                                    }
                                    await asyncio.to_thread(strategy.v_avwap_plugin.save_state, t, now_est, state_data)
                    
                    elif action == "SELL":
                        price = float(decision.get("target_price", decision.get("price", 0.0)))
                        qty = int(decision.get("qty", 0))
                        
                        exec_price = price
                        if exec_price <= 0.0:
                            bid_price = float(await asyncio.to_thread(broker.get_bid_price, t) or 0.0)
                            exec_price = bid_price if bid_price > 0 else exec_curr_p
                            
                        if qty > 0:
                            # MODIFIED: [V29.21 그랜드 수술] 예방적 LOC 덫 혼선 차단
                            has_unfilled = False
                            for _ in range(4):
                                unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                if isinstance(unfilled, list) and any(
                                    o.get('sll_buy_dvsn_cd') == '01' and str(o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '').strip().zfill(2) == '00' 
                                    for o in unfilled
                                ):
                                    has_unfilled = True
                                    break
                                await asyncio.sleep(2.0)
                            
                            if has_unfilled:
                                continue

                            res = await asyncio.to_thread(broker.send_order, t, "SELL", qty, exec_price, "LIMIT")
                            odno = res.get('odno', '') if isinstance(res, dict) else ''
                            
                            if res and res.get('rt_cd') == '0' and odno:
                                # MODIFIED: [V29.21 그랜드 수술] 8초(4회) 다중 교차 검증 및 미체결 즉각 Nuke 로직 이식
                                ccld_qty = 0
                                for _ in range(4):
                                    await asyncio.sleep(2.0)
                                    unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                    safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                    
                                    my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                    if my_order:
                                        ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                    else:
                                        ccld_qty = qty
                                        break
                                
                                if ccld_qty < qty:
                                    try:
                                        await asyncio.to_thread(broker.cancel_order, t, odno)
                                        await asyncio.sleep(0.5)
                                    except Exception as e_cancel:
                                        logging.warning(f"⚠️ [{t}] AVWAP 매도 잔여 취소 실패: {e_cancel}")
                                
                                if ccld_qty > 0:
                                    msg = f"⚔️ <b>[AVWAP] 암살자 덤핑 타격!</b>\n▫️ 타겟: {t}\n▫️ 타점: ${exec_price}\n▫️ 팩트 체결수량: {ccld_qty}주 (목표 {qty}주)\n▫️ 사유: {reason}"
                                    
                                    old_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
                                    new_qty = max(0, old_qty - ccld_qty)
                                    
                                    if new_qty == 0:
                                        msg += "\n🛡️ 금일 해당 종목의 100% 청산 완료, 단타 작전을 영구 셧다운합니다."
                                        shutdown_flag = True
                                        new_avg = 0.0
                                    else:
                                        msg += f"\n⚠️ 잔량 {new_qty}주 발생 (미체결 강제 취소됨, 다음 1분봉 루프에서 재시도)"
                                        shutdown_flag = tracking_cache.get(f"AVWAP_SHUTDOWN_{t}", False)
                                        new_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)

                                    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                    
                                    tracking_cache[f"AVWAP_BOUGHT_{t}"] = (new_qty > 0)
                                    tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = shutdown_flag
                                    tracking_cache[f"AVWAP_QTY_{t}"] = new_qty
                                    tracking_cache[f"AVWAP_AVG_{t}"] = new_avg
                                    
                                    state_data = {
                                        'bought': tracking_cache[f"AVWAP_BOUGHT_{t}"],
                                        'shutdown': shutdown_flag,
                                        'qty': new_qty,
                                        'avg_price': new_avg
                                    }
                                    await asyncio.to_thread(strategy.v_avwap_plugin.save_state, t, now_est, state_data)

                    elif action == "SHUTDOWN":
                        if not tracking_cache.get(f"AVWAP_SHUTDOWN_{t}"):
                            tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = True
                            state_data = {
                                "bought": tracking_cache.get(f"AVWAP_BOUGHT_{t}", False),
                                "shutdown": True,
                                "qty": tracking_cache.get(f"AVWAP_QTY_{t}", 0),
                                "avg_price": tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
                            }
                            await asyncio.to_thread(strategy.v_avwap_plugin.save_state, t, now_est, state_data)
                            msg = f"🛡️ <b>[AVWAP] 암살자 작전 영구 셧다운(동결)</b>\n▫️ 타겟: {t}\n▫️ 사유: {reason}"
                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

                master_switch = getattr(cfg, 'get_master_switch', lambda x: "ALL")(t)
                sniper_buy_locked = getattr(cfg, 'get_sniper_buy_locked', lambda x: False)(t)
                sniper_sell_locked = getattr(cfg, 'get_sniper_sell_locked', lambda x: False)(t)

                curr_p = await asyncio.to_thread(broker.get_current_price, t)
                if curr_p is None or float(curr_p) <= 0:
                    continue

                sniper_func = getattr(strategy, 'check_sniper_condition', None)
                if sniper_func:
                    res = await asyncio.to_thread(sniper_func, t, cfg, broker, chat_id)
                else:
                    res = {"action": "HOLD", "reason": "스나이퍼 모듈 누락(Bypass)", "limit_price": 0.0}
                    
                action = res.get("action")
                reason = res.get("reason", "")
                limit_p = res.get("limit_price", 0.0)

                is_rev = (cfg.get_version(t) == "V_REV")

                if action == "BUY" and not is_rev and not sniper_buy_locked and master_switch != "UP_ONLY":
                    qty = res.get("qty", 0)
                    if qty > 0:
                        cancelled = await asyncio.to_thread(broker.cancel_targeted_orders, t, "02", "03")
                        await asyncio.sleep(1.0)
                        
                        # MODIFIED: [V29.21 그랜드 수술] 지정가(00) 주문 필터링으로 LOC 덫 혼선 차단
                        has_unfilled = False
                        for _ in range(4):
                            unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                            if isinstance(unfilled, list) and any(
                                o.get('sll_buy_dvsn_cd') == '02' and str(o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '').strip().zfill(2) == '00' 
                                for o in unfilled
                            ):
                                has_unfilled = True
                                break
                            await asyncio.sleep(2.0)
                        
                        if has_unfilled:
                            continue
                            
                        order_res = await asyncio.to_thread(broker.send_order, t, "BUY", qty, limit_p, "LIMIT")
                        odno = order_res.get('odno', '') if isinstance(order_res, dict) else ''
                        
                        if order_res and order_res.get('rt_cd') == '0' and odno:
                            ccld_qty = 0
                            for _ in range(4):
                                await asyncio.sleep(2.0)
                                unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                
                                my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                if my_order:
                                    ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                else:
                                    ccld_qty = qty
                                    break
                                    
                            if ccld_qty < qty:
                                try:
                                    await asyncio.to_thread(broker.cancel_order, t, odno)
                                    await asyncio.sleep(1.0)
                                except: pass

                            if ccld_qty > 0:
                                if hasattr(cfg, 'set_sniper_buy_locked'):
                                    cfg.set_sniper_buy_locked(t, True)
                                    
                                exec_history = await asyncio.to_thread(broker.get_execution_history, t, today_est_str, today_est_str)
                                
                                def get_actual_execution_price(history, side_code, target_odno):
                                    if not history: return 0.0
                                    for ex in history:
                                        if ex.get('sll_buy_dvsn_cd') == side_code and ex.get('odno') == target_odno:
                                            p = float(ex.get('ft_ccld_unpr3', '0'))
                                            if p > 0: return p
                                            
                                    target_recs = [ex for ex in history if ex.get('sll_buy_dvsn_cd') == side_code]
                                    for ex in target_recs:
                                        p = float(ex.get('ft_ccld_unpr3', '0'))
                                        if p > 0: return p
                                    return 0.0
                                    
                                actual_exec_price = get_actual_execution_price(exec_history, "02", odno)
                                display_price = actual_exec_price if actual_exec_price > 0 else limit_p
                                
                                msg = f"🚨 <b>[{t}] 스나이퍼 딥-매수(Intercept) 명중!</b>\n▫️ 타겟가: ${limit_p}\n▫️ 팩트 단가: ${display_price}\n▫️ 체결수량: {ccld_qty}주 (요청: {qty}주)\n▫️ 사유: {reason}\n▫️ 하방 방어망이 잠깁니다 (상방 독립 유지)."
                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                
                upward_mode = getattr(cfg, 'get_upward_sniper_mode', lambda x: False)(t)
                is_upward_active = upward_mode and not is_rev and not sniper_sell_locked and master_switch != "DOWN_ONLY"

                if is_upward_active and action in ["SELL_QUARTER", "SELL_JACKPOT"]:
                    qty = res.get("qty", 0)
                    if qty > 0:
                        cancelled = await asyncio.to_thread(broker.cancel_targeted_orders, t, "01", "03")
                        await asyncio.sleep(1.0)
                        
                        # MODIFIED: [V29.21 그랜드 수술] 지정가(00) 주문 필터링으로 LOC 덫 혼선 차단
                        has_unfilled = False
                        for _ in range(4):
                            unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                            if isinstance(unfilled, list) and any(
                                o.get('sll_buy_dvsn_cd') == '01' and str(o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '').strip().zfill(2) == '00' 
                                for o in unfilled
                            ):
                                has_unfilled = True
                                break
                            await asyncio.sleep(2.0)
                        
                        if has_unfilled:
                            continue
                            
                        order_res = await asyncio.to_thread(broker.send_order, t, "SELL", qty, limit_p, "LIMIT")
                        odno = order_res.get('odno', '') if isinstance(order_res, dict) else ''
                        
                        if order_res and order_res.get('rt_cd') == '0' and odno:
                            ccld_qty = 0
                            for _ in range(4):
                                await asyncio.sleep(2.0)
                                unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                
                                my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                if my_order:
                                    ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                else:
                                    ccld_qty = qty
                                    break
                                    
                            if ccld_qty < qty:
                                try:
                                    await asyncio.to_thread(broker.cancel_order, t, odno)
                                    await asyncio.sleep(1.0)
                                except: pass

                            if ccld_qty > 0:
                                if hasattr(cfg, 'set_sniper_sell_locked'):
                                    cfg.set_sniper_sell_locked(t, True)
                                    
                                exec_history = await asyncio.to_thread(broker.get_execution_history, t, today_est_str, today_est_str)
                                
                                def get_actual_execution_price(history, side_code, target_odno):
                                    if not history: return 0.0
                                    for ex in history:
                                        if ex.get('sll_buy_dvsn_cd') == side_code and ex.get('odno') == target_odno:
                                            p = float(ex.get('ft_ccld_unpr3', '0'))
                                            if p > 0: return p
                                            
                                    target_recs = [ex for ex in history if ex.get('sll_buy_dvsn_cd') == side_code]
                                    for ex in target_recs:
                                        p = float(ex.get('ft_ccld_unpr3', '0'))
                                        if p > 0: return p
                                    return 0.0
                                    
                                actual_exec_price = get_actual_execution_price(exec_history, "01", odno)
                                display_price = actual_exec_price if actual_exec_price > 0 else limit_p
                                    
                                msg = f"🦇 <b>[{t}] 스나이퍼 상방 기습({action}) 명중!</b>\n▫️ 타겟가: ${limit_p}\n▫️ 팩트 단가: ${display_price}\n▫️ 체결수량: {ccld_qty}주 (요청: {qty}주)\n▫️ 사유: {reason}\n▫️ 상방 감시망이 잠깁니다 (하방 독립 유지)."
                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

    try:
        await asyncio.wait_for(_do_sniper(), timeout=45.0)
    except Exception as e:
        logging.error(f"🚨 스나이퍼 타임아웃 에러: {e}", exc_info=True)
# ==========================================================
# [scheduler_trade.py] - 🌟 100% 통합 전투 사령부 (V29.21) 🌟 (Part 2/2)
# ==========================================================

# ==========================================================
# 2. 🛡️ Fail-Safe: 선제적 LOC 취소 (VWAP 엔진 기상과 동기화 완료)
# ==========================================================
async def scheduled_vwap_init_and_cancel(context):
    if not is_market_open(): return
    
    est = pytz.timezone('US/Eastern')
    now_est = datetime.datetime.now(est)
    
    try:
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
        if schedule.empty: return
        market_close = schedule.iloc[0]['market_close'].astimezone(est)
    except Exception:
        market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        
    vwap_start_time = market_close - datetime.timedelta(minutes=33)
    vwap_end_time = market_close 
    
    if not (vwap_start_time <= now_est <= vwap_end_time):
        return
    
    app_data = context.job.data
    cfg, broker, tx_lock = app_data['cfg'], app_data['broker'], app_data['tx_lock']
    chat_id = context.job.chat_id
    
    vwap_cache = app_data.setdefault('vwap_cache', {})
    today_str = now_est.strftime('%Y%m%d')
    if vwap_cache.get('date') != today_str:
        vwap_cache.clear()
        vwap_cache['date'] = today_str
        
    async def _do_init():
        async with tx_lock:
            for t in cfg.get_active_tickers():
                version = cfg.get_version(t)
                is_manual_vwap = getattr(cfg, 'get_manual_vwap_mode', lambda x: False)(t)
                
                if version == "V_REV" and is_manual_vwap:
                    continue
                
                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    if not vwap_cache.get(f"REV_{t}_nuked"):
                        try:
                            await asyncio.to_thread(broker.cancel_all_orders_safe, t, "BUY")
                            await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                            vwap_cache[f"REV_{t}_nuked"] = True
                            
                            msg = f"🌅 <b>[{t}] 장 마감 33분 전 엔진 기상 (Fail-Safe 전환)</b>\n"
                            msg += f"▫️ 프리장에 선제 전송해둔 '예방적 양방향 LOC 덫'을 전량 취소(Nuke)했습니다.\n"
                            msg += f"▫️ 1분 단위 정밀 타격(VWAP 슬라이싱) 모드로 교전 수칙을 변경합니다. ⚔️"
                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_notification=True)
                            await asyncio.sleep(1.0)
                        except Exception as e:
                            logging.error(f"🚨 Fail-Safe 초기화(Nuke) 에러: {e}", exc_info=True)
                            vwap_cache[f"REV_{t}_nuked"] = False 
                    
    try:
        await asyncio.wait_for(_do_init(), timeout=45.0)
    except Exception as e:
        logging.error(f"🚨 Fail-Safe 타임아웃 에러: {e}", exc_info=True)

# ==========================================================
# 3. 🎯 VWAP 1분 단위 정밀 타격 엔진
# ==========================================================
async def scheduled_vwap_trade(context):
    if not is_market_open(): return
    
    est = pytz.timezone('US/Eastern')
    now_est = datetime.datetime.now(est)
    
    if context.job.data.get('tx_lock') is None:
        logging.warning("⚠️ [vwap_trade] tx_lock 미초기화. 이번 사이클 스킵.")
        return
        
    try:
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
        if schedule.empty: return
        market_close = schedule.iloc[0]['market_close'].astimezone(est)
    except Exception:
        market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        
    vwap_start_time = market_close - datetime.timedelta(minutes=33)
    vwap_end_time = market_close 
    
    if not (vwap_start_time <= now_est <= vwap_end_time):
        return
        
    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    chat_id = context.job.chat_id
    
    vwap_cache = app_data.setdefault('vwap_cache', {})
    today_str = now_est.strftime('%Y%m%d')
    
    if vwap_cache.get('date') != today_str:
        vwap_cache.clear()
        vwap_cache['date'] = today_str

    U_CURVE_WEIGHTS = [
        0.0308, 0.0220, 0.0190, 0.0228, 0.0179, 0.0191, 0.0199, 0.0190, 0.0187, 0.0213,
        0.0216, 0.0234, 0.0231, 0.0210, 0.0205, 0.0252, 0.0225, 0.0228, 0.0238, 0.0229,
        0.0259, 0.0284, 0.0331, 0.0385, 0.0400, 0.0461, 0.0553, 0.0620, 0.0750, 0.1584
    ]
    
    minutes_to_close = int(max(0, (market_close - now_est).total_seconds()) / 60)
    min_idx = 33 - minutes_to_close
    if min_idx < 0: min_idx = 0
    if min_idx > 29: min_idx = 29
    current_weight = U_CURVE_WEIGHTS[min_idx]
        
    async def _do_vwap():
        async with tx_lock:
            cash, holdings = await asyncio.to_thread(broker.get_account_balance)
            if holdings is None: return
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}
            
            for t in cfg.get_active_tickers():
                version = cfg.get_version(t)
                is_manual_vwap = getattr(cfg, 'get_manual_vwap_mode', lambda x: False)(t)
                
                if version == "V_REV" and is_manual_vwap:
                    continue

                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    if not vwap_cache.get(f"REV_{t}_nuked"):
                        try:
                            await asyncio.to_thread(broker.cancel_all_orders_safe, t, "BUY")
                            await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                            vwap_cache[f"REV_{t}_nuked"] = True
                            msg = f"🌅 <b>[{t}] 하이브리드 타임 슬라이싱 기상 (자가 치유 가동)</b>\n"
                            msg += f"▫️ 장 마감 33분 전 진입을 확인하여 기존 LOC 덫 강제 취소(Nuke)했습니다.\n"
                            msg += f"▫️ 스케줄러 누락을 완벽히 극복하고 1분 단위 정밀 타격을 즉각 개시합니다. ⚔️"
                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_notification=True)
                            await asyncio.sleep(1.0)
                        except Exception as e:
                            logging.error(f"🚨 자가 치유 Nuke 실패: {e}", exc_info=True)
                            continue

                    curr_p = float(await asyncio.to_thread(broker.get_current_price, t) or 0.0)
                    
                    if not vwap_cache.get(f"REV_{t}_anchor_prev_c"):
                        prev_c_live = float(await asyncio.to_thread(broker.get_previous_close, t) or 0.0)
                        if prev_c_live > 0:
                            vwap_cache[f"REV_{t}_anchor_prev_c"] = prev_c_live
                    prev_c = float(vwap_cache.get(f"REV_{t}_anchor_prev_c") or 0.0)

                    if curr_p <= 0 or prev_c <= 0: continue

                    if version == "V_REV":
                        strategy_rev = app_data.get('strategy_rev')
                        queue_ledger = app_data.get('queue_ledger')
                        if not strategy_rev or not queue_ledger: continue
                        
                        h = safe_holdings.get(t) or {}
                        actual_qty = int(float(h.get('qty', 0)))
                        
                        q_data = queue_ledger.get_queue(t)
                        total_q = sum(item.get("qty", 0) for item in q_data)
                        
                        if actual_qty == 0 and total_q > 0:
                            if vwap_cache.get(f"REV_{t}_sweep_msg_sent"):
                                continue
                                
                            if not vwap_cache.get(f"REV_{t}_panic_sell_warn"):
                                vwap_cache[f"REV_{t}_panic_sell_warn"] = True
                                await context.bot.send_message(
                                    chat_id=chat_id,
                                    text=f"🚨 <b>[비상] [{t}] 수동매매로 인한 잔고 증발이 감지되었습니다.</b>\n"
                                         f"▫️ 봇의 매매가 일시 정지됩니다.\n"
                                         f"▫️ 시드 오염을 막기 위해 즉시 <code>/reset</code> 커맨드를 실행하여 장부를 소각하십시오.",
                                    parse_mode='HTML'
                                )
                            continue
                        
                        cached_plan = strategy_rev.load_daily_snapshot(t)
                        is_zero_start = (cached_plan and cached_plan.get("total_q", -1) == 0)
                        virtual_q_data = [] if is_zero_start else q_data
                        
                        strategy_rev._load_state_if_needed(t)
                        held_in_cache = vwap_cache.get(f"REV_{t}_was_holding", False)
                        held_in_file = strategy_rev.was_holding.get(t, False)
                        if (held_in_cache or held_in_file) and total_q == 0:
                            continue
                            
                        if total_q > 0:
                            vwap_cache[f"REV_{t}_was_holding"] = True
                            if not strategy_rev.was_holding.get(t, False):
                                strategy_rev.was_holding[t] = True
                                strategy_rev._save_state(t)
                            
                        if total_q > 0:
                            avg_price = sum(item.get("qty", 0) * item.get("price", 0.0) for item in q_data) / total_q
                            jackpot_trigger = avg_price * 1.010
                        else:
                            avg_price = 0.0
                            jackpot_trigger = float('inf')
                        
                        dates_in_queue = sorted(list(set(item.get('date') for item in q_data if item.get('date'))), reverse=True)
                        layer_1_qty = 0
                        layer_1_trigger = round(prev_c * 1.006, 2)
                        if dates_in_queue:
                            lots_for_date = [item for item in q_data if item.get('date') == dates_in_queue[0]]
                            layer_1_qty = sum(item.get('qty', 0) for item in lots_for_date)
                            if layer_1_qty > 0:
                                layer_1_price = sum(item.get('qty', 0) * item.get('price', 0.0) for item in lots_for_date) / layer_1_qty
                                layer_1_trigger = round(layer_1_price * 1.006, 2)
                        
                        if not is_zero_start and minutes_to_close <= 3:
                            target_sweep_qty = 0
                            sweep_type = ""
                            
                            if total_q > 0 and curr_p >= jackpot_trigger:
                                target_sweep_qty = total_q
                                sweep_type = "잭팟 전량"
                            elif layer_1_qty > 0 and curr_p >= layer_1_trigger:
                                target_sweep_qty = layer_1_qty
                                sweep_type = "1층 잔여물량"
                                
                            if target_sweep_qty > 0:
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                                await asyncio.sleep(0.5)
                                
                                _, live_holdings = await asyncio.to_thread(broker.get_account_balance)
                                safe_live_holdings = live_holdings if isinstance(live_holdings, dict) else {}
                                
                                if safe_live_holdings and t in safe_live_holdings:
                                    h_live = safe_live_holdings[t]
                                    sellable_qty = int(float(h_live.get('ord_psbl_qty', h_live.get('qty', 0))))
                                    actual_sweep_qty = min(target_sweep_qty, sellable_qty)
                                    
                                    if actual_sweep_qty > 0:
                                        bid_price = float(await asyncio.to_thread(broker.get_bid_price, t) or 0.0)
                                        exec_price = bid_price if bid_price > 0 else curr_p
                                        
                                        # MODIFIED: [V29.21 핫픽스] 동기 함수 블로킹에 의한 루프 마비 방지 (asyncio.to_thread 래핑)
                                        res = await asyncio.to_thread(broker.send_order, t, "SELL", actual_sweep_qty, exec_price, "LIMIT")
                                        odno = res.get('odno', '') if isinstance(res, dict) else ''
                                        
                                        if res and res.get('rt_cd') == '0' and odno:
                                            if not vwap_cache.get(f"REV_{t}_sweep_msg_sent"):
                                                msg = f"🌪️ <b>[{t}] V-REV 본대 {sweep_type} 3분 가속 스윕(Sweep) 개시!</b>\n"
                                                if "잭팟" in sweep_type:
                                                    msg += f"▫️ 장 마감 3분 전 데드존 철거. 잭팟 커트라인({jackpot_trigger:.2f}) 돌파를 확인했습니다.\n"
                                                else:
                                                    msg += f"▫️ 장 마감 3분 전 데드존 철거. 1층 앵커({layer_1_trigger:.2f}) 방어를 확인했습니다.\n"
                                                msg += f"▫️ 매도 가능 잔량이 0이 될 때까지 매 1분마다 지속 덤핑합니다! (현재 <b>{actual_sweep_qty}주</b> 매수호가 폭격) 🏆"
                                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                                vwap_cache[f"REV_{t}_sweep_msg_sent"] = True
                                            
                                            ccld_qty = 0
                                            for _ in range(4):
                                                await asyncio.sleep(2.0)
                                                unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                                safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                                
                                                my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                                if my_order:
                                                    ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                                else:
                                                    ccld_qty = actual_sweep_qty
                                                    break
                                            
                                            if ccld_qty < actual_sweep_qty:
                                                try:
                                                    await asyncio.to_thread(broker.cancel_order, t, odno)
                                                    await asyncio.sleep(0.5)
                                                except Exception as e_cancel:
                                                    logging.warning(f"⚠️ [{t}] 스윕 잔여 주문 취소 실패: {e_cancel}")
                                                    
                                            if ccld_qty > 0:
                                                strategy_rev.record_execution(t, "SELL", ccld_qty, exec_price)
                                                q_snap_before_pop = list(q_data)
                                                queue_ledger.pop_lots(t, ccld_qty)
                                                remaining_after_pop = queue_ledger.get_queue(t)
                                                remaining_qty_after = sum(item.get('qty', 0) for item in remaining_after_pop)
                                                if remaining_qty_after == 0 and total_q > 0:
                                                    try:
                                                        pending_file = f"data/pending_grad_{t}.json"
                                                        pending_data = {
                                                            "q_data_before": q_snap_before_pop,
                                                            "exec_price": exec_price,
                                                            "total_q": total_q
                                                        }
                                                        with open(pending_file, 'w', encoding='utf-8') as _pf:
                                                            json.dump(pending_data, _pf)
                                                    except Exception as pg_e:
                                                        logging.error(f"🚨 [{t}] pending_grad 마커 파일 저장 실패: {pg_e}")
                                    else:
                                        if not vwap_cache.get(f"REV_{t}_sweep_skip_msg"):
                                            msg = f"⚠️ <b>[{t}] 스윕 피니셔 덤핑 생략 (MOC 락다운 감지)</b>\n▫️ 조건이 달성되었으나, 대상 물량이 수동 긴급 수혈(MOC) 등 취소 불가 상태로 미국 거래소에 묶여 있어 스윕 덤핑을 자동 스킵합니다."
                                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                            vwap_cache[f"REV_{t}_sweep_skip_msg"] = True
                                            
                            if target_sweep_qty > 0 or (total_q > 0 and curr_p >= jackpot_trigger):
                                continue 
                        
                        try:
                            df_1min = await asyncio.to_thread(broker.get_1min_candles_df, t)
                            vwap_status = strategy.analyze_vwap_dominance(df_1min)
                        except Exception:
                            vwap_status = {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}
                        
                        current_regime = "BUY" if is_zero_start else ("SELL" if curr_p > prev_c else "BUY")
                        last_regime = vwap_cache.get(f"REV_{t}_regime")
                        
                        if not is_zero_start and last_regime and last_regime != current_regime:
                            await context.bot.send_message(
                                chat_id=chat_id, 
                                text=f"🔄 <b>[{t}] 실시간 공수 교대 발동!</b>\n"
                                     f"▫️ <b>[{last_regime} ➡️ {current_regime}]</b> 모드로 두뇌를 전환하며 궤도를 수정합니다.", 
                                parse_mode='HTML', disable_notification=True
                            )
                            try:
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "BUY")
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                                strategy_rev.reset_residual(t) 
                            except Exception as e:
                                err_msg = f"🛑 <b>[FATAL ERROR] {t} 공수 교대 중 기존 덫 취소 실패!</b>\n▫️ 2중 예산 소진 방어를 위해 당일 남은 V-REV 교전을 강제 중단(Hard-Lock)합니다.\n▫️ 상세 오류: {e}"
                                await context.bot.send_message(chat_id=chat_id, text=err_msg, parse_mode='HTML')
                                continue
                                
                        vwap_cache[f"REV_{t}_regime"] = current_regime
                        
                        if vwap_cache.get(f"REV_{t}_loc_fired"):
                            continue

                        rev_daily_budget = float(cfg.get_seed(t) or 0.0) * 0.15
                        
                        rev_plan = None
                        try:
                            rev_plan = strategy_rev.get_dynamic_plan(
                                ticker=t, curr_p=curr_p, prev_c=prev_c, 
                                current_weight=current_weight, vwap_status=vwap_status, 
                                min_idx=min_idx, alloc_cash=rev_daily_budget, q_data=virtual_q_data,
                                is_snapshot_mode=False
                            )
                        except Exception as plan_e:
                            logging.error(f"🚨 [{t}] get_dynamic_plan 실행 에러 (해당 티커 건너뜀): {plan_e}")
                        
                        if rev_plan is None:
                            continue
                        if not is_zero_start and rev_plan.get('trigger_loc') and minutes_to_close >= 15:
                            vwap_cache[f"REV_{t}_loc_fired"] = True
                            msg = f"🛡️ <b>[{t}] 60% 거래량 지배력 감지 (추세장 전환)</b>\n"
                            msg += f"▫️ 기관급 자금 쏠림으로 인해 위험한 1분 단위 타임 슬라이싱(VWAP)을 전면 중단합니다.\n"
                            msg += f"▫️ <b>잔여 할당량 전량을 양방향 LOC 방어선으로 전환 배치 완료!</b>\n"
                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_notification=True)
                            
                            for o in rev_plan.get('orders', []):
                                if o['qty'] > 0:
                                    # MODIFIED: [V29.21 핫픽스] 동기 함수 블로킹에 의한 루프 마비 방지 (asyncio.to_thread 래핑)
                                    await asyncio.to_thread(broker.send_order, t, o['side'], o['qty'], o['price'], "LOC")
                                    await asyncio.sleep(0.2)
                            continue
                            
                        target_orders = rev_plan.get('orders', [])

                    elif version == "V14":
                        h = safe_holdings.get(t, {'qty':0, 'avg':0.0})
                        actual_qty = int(h.get('qty', 0))
                        actual_avg = float(h.get('avg', 0.0))
                        
                        v14_vwap_plugin = strategy.v14_vwap_plugin
                        
                        plan = v14_vwap_plugin.get_dynamic_plan(
                            ticker=t, current_price=curr_p, prev_c=prev_c, 
                            current_weight=current_weight, min_idx=min_idx, 
                            alloc_cash=0.0, qty=actual_qty, avg_price=actual_avg
                        )
                        target_orders = plan.get('orders', [])

                    for o in target_orders:
                        slice_qty = o['qty']
                        if slice_qty <= 0: continue
                        
                        target_price = o['price']
                        side = o['side']
                        
                        ask_price = float(await asyncio.to_thread(broker.get_ask_price, t) or 0.0)
                        bid_price = float(await asyncio.to_thread(broker.get_bid_price, t) or 0.0)
                        exec_price = ask_price if side == "BUY" else bid_price
                        if exec_price <= 0: exec_price = curr_p
                        
                        if side == "BUY" and exec_price > target_price: continue
                        if side == "SELL" and exec_price < target_price: continue
                        
                        # MODIFIED: [V29.21 핫픽스] 동기 함수 블로킹에 의한 루프 마비 방지 (asyncio.to_thread 래핑)
                        res = await asyncio.to_thread(broker.send_order, t, side, slice_qty, exec_price, "LIMIT")
                        odno = res.get('odno', '') if isinstance(res, dict) else ''
                        
                        if res and res.get('rt_cd') == '0' and odno:
                            ccld_qty = 0
                            for _ in range(4):
                                await asyncio.sleep(2.0)
                                unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                
                                my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                if my_order:
                                    ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                else:
                                    ccld_qty = slice_qty
                                    break
                                    
                            if ccld_qty < slice_qty:
                                try:
                                    await asyncio.to_thread(broker.cancel_order, t, odno)
                                    await asyncio.sleep(1.0)
                                except: pass
                                
                            if ccld_qty > 0:
                                if version == "V_REV":
                                    strategy_rev.record_execution(t, side, ccld_qty, exec_price)
                                    if side == "BUY": queue_ledger.add_lot(t, ccld_qty, exec_price, "VWAP_BUY")
                                    elif side == "SELL": queue_ledger.pop_lots(t, ccld_qty)
                                elif version == "V14":
                                    v14_vwap_plugin.record_execution(t, side, ccld_qty, exec_price)
                                    
                            await asyncio.sleep(0.2)

    try:
        await asyncio.wait_for(_do_vwap(), timeout=45.0)
    except Exception as e:
        logging.error(f"🚨 VWAP 스케줄러 에러: {e}", exc_info=True)

# ==========================================================
# 4. 🌅 정규장 오픈 (17:05) 전송 (V14 통합 & V-REV 예방 방어선)
# ==========================================================
async def scheduled_regular_trade(context):
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.datetime.now(kst)
    target_hour, _ = get_target_hour()
    chat_id = context.job.chat_id
    
    now_minutes = now.hour * 60 + now.minute
    target_minutes = target_hour * 60 + 5
    
    if abs(now_minutes - target_minutes) > 65 and abs(now_minutes - target_minutes) < (24*60 - 65):
        return
        
    if not is_market_open():
        return
    
    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    strategy_rev = app_data.get('strategy_rev')
    queue_ledger = app_data.get('queue_ledger')
    
    if tx_lock is None:
        logging.warning("⚠️ [regular_trade] tx_lock 미초기화. 이번 사이클 스킵.")
        await context.bot.send_message(chat_id=chat_id, text="⚠️ <b>[시스템 경고]</b> tx_lock 미초기화로 정규장 주문을 1회 스킵합니다.", parse_mode='HTML')
        return
    
    jitter_seconds = random.randint(0, 180)

    await context.bot.send_message(
        chat_id=chat_id, 
        text=f"🌃 <b>[{target_hour}:05] 통합 주문 장전 및 스냅샷 박제!</b>\n"
             f"🛡️ 서버 접속 부하 방지를 위해 <b>{jitter_seconds}초</b> 대기 후 안전하게 주문 전송을 시도합니다.", 
        parse_mode='HTML'
    )

    await asyncio.sleep(jitter_seconds)

    MAX_RETRIES = 15
    RETRY_DELAY = 60

    async def _do_regular_trade():
        est = pytz.timezone('US/Eastern')
        _now_est = datetime.datetime.now(est)
        today_str_est = _now_est.strftime("%Y-%m-%d")
        
        async with tx_lock:
            cash, holdings = await asyncio.to_thread(broker.get_account_balance)
            if holdings is None:
                return False, "❌ 계좌 정보를 불러오지 못했습니다."
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}

            sorted_tickers, allocated_cash = get_budget_allocation(cash, cfg.get_active_tickers(), cfg)
            
            plans = {}
            msgs = {t: "" for t in sorted_tickers}
            
            all_success_map = {t: True for t in sorted_tickers}
            v_rev_tickers = [] 

            for t in sorted_tickers:
                if cfg.check_lock(t, "REG"):
                    skip_msg = (
                        f"⚠️ <b>[{t}] REG 잠금 미해제 — 주문 스킵</b>\n"
                        f"▫️ 전날 REG 잠금이 자정 초기화 시 해제되지 않아 오늘 17:05 주문 루프에서 제외되었습니다.\n"
                        f"▫️ 수동으로 잠금 해제 후 [🚀 V-REV 방어선 수동 장전] 버튼을 눌러 재장전하십시오."
                    )
                    await context.bot.send_message(chat_id, skip_msg, parse_mode='HTML')
                    continue
                
                h = safe_holdings.get(t) or {}
                safe_avg = float(h.get('avg') or 0.0)
                safe_qty = int(float(h.get('qty') or 0))

                curr_p = 0.0
                prev_c = 0.0
                for _api_retry in range(3):
                    curr_p = float(await asyncio.to_thread(broker.get_current_price, t) or 0.0)
                    prev_c = float(await asyncio.to_thread(broker.get_previous_close, t) or 0.0)
                    if curr_p > 0 and prev_c > 0:
                        break
                    await asyncio.sleep(2.0)

                if curr_p <= 0 or prev_c <= 0:
                    msgs[t] += (
                        f"🚨 <b>[{t}] 전일 종가/현재가 API 3회 결측 감지!</b>\n"
                        f"▫️ 매수 방어선을 장전하지 못하고 다음 종목으로 넘어갑니다(continue 바이패스).\n"
                    )
                    all_success_map[t] = False
                    await context.bot.send_message(chat_id, msgs[t], parse_mode='HTML')
                    continue
                
                version = cfg.get_version(t)
                is_manual_vwap = getattr(cfg, 'get_manual_vwap_mode', lambda x: False)(t)

                if version == "V_REV" and is_manual_vwap:
                    msgs[t] += f"🛡️ <b>[{t}] V-REV 수동 시그널 모 가동 중</b>\n"
                    msgs[t] += "▫️ 봇 자동 주문이 락다운되었습니다. V앱에서 장 마감 30분 전 세팅으로 수동 장전하십시오.\n"
                    await context.bot.send_message(chat_id, msgs[t], parse_mode='HTML')
                    continue

                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    loc_orders = []
                    
                    if version == "V_REV":
                        q_data = queue_ledger.get_queue(t)
                        v_rev_q_qty = sum(item.get("qty", 0) for item in q_data)
                        rev_budget = float(cfg.get_seed(t) or 0.0) * 0.15
                        half_portion_cash = rev_budget * 0.5
                        
                        if q_data and safe_qty > 0:
                            dates_in_queue = sorted(list(set(item.get('date') for item in q_data if item.get('date'))), reverse=True)
                            l1_qty = 0
                            l1_price = 0.0
                            if dates_in_queue:
                                lots_1 = [item for item in q_data if item.get('date') == dates_in_queue[0]]
                                l1_qty_raw = sum(item.get('qty', 0) for item in lots_1)
                                
                                if l1_qty_raw > safe_qty:
                                    await context.bot.send_message(
                                        chat_id, 
                                        f"⚠️ <b>[{t}] 큐/브로커 원장 불일치!</b>\n"
                                        f"▫️ 내부 큐: {l1_qty_raw}주 / 실제 보유: {safe_qty}주\n"
                                        f"▫️ 초과 매도 방지를 위해 실제 보유량({safe_qty}주)으로 1층 수량을 조정합니다.",
                                        parse_mode='HTML'
                                    )
                                l1_qty = min(l1_qty_raw, safe_qty)
                                
                                if l1_qty > 0:
                                    l1_price = sum(item.get('qty', 0) * item.get('price', 0.0) for item in lots_1) / l1_qty
                            
                            target_l1 = round(l1_price * 1.006, 2)
                            if l1_qty > 0:
                                loc_orders.append({'side': 'SELL', 'qty': l1_qty, 'price': target_l1, 'type': 'LOC', 'desc': '[1층 단독]'})
                                
                            upper_qty = safe_qty - l1_qty 
                            if upper_qty > 0:
                                total_inv = safe_qty * safe_avg
                                l1_inv = l1_qty * l1_price
                                upper_invested = total_inv - l1_inv
                                upper_avg = upper_invested / upper_qty if upper_invested > 0 else safe_avg
                                    
                                target_upper = round(upper_avg * 1.005, 2)
                                loc_orders.append({'side': 'SELL', 'qty': upper_qty, 'price': target_upper, 'type': 'LOC', 'desc': '[상위 재고]'})
                                
                        elif not q_data and safe_qty > 0:
                            await context.bot.send_message(
                                chat_id,
                                f"⚠️ <b>[{t}] V-REV 큐 증발 감지 — Fallback 매도선 생성</b>\n"
                                f"▫️ 내부 큐: 비어있음 / 실제 보유: {safe_qty}주\n"
                                f"▫️ 서버 재시작 등으로 큐가 초기화된 것으로 판단됩니다.\n"
                                f"▫️ 총 보유 평단가({safe_avg:.2f}) × 1.005 기준 전량 Fallback LOC 매도선을 설정합니다.",
                                parse_mode='HTML'
                            )
                            fallback_target = round(safe_avg * 1.005, 2) if safe_avg > 0 else 0.0
                            if fallback_target >= 0.01:
                                loc_orders.append({'side': 'SELL', 'qty': safe_qty, 'price': fallback_target, 'type': 'LOC', 'desc': '[Fallback 전량]'})
                        
                        b1_price = round(prev_c / 0.935 if v_rev_q_qty == 0 else prev_c * 0.995, 2)
                        b2_price = round(prev_c * 0.999 if v_rev_q_qty == 0 else prev_c * 0.9725, 2)
                        
                        b1_qty = math.floor(half_portion_cash / b1_price) if b1_price > 0 else 0
                        b2_qty = math.floor(half_portion_cash / b2_price) if b2_price > 0 else 0
                        
                        if b1_qty > 0:
                            loc_orders.append({'side': 'BUY', 'qty': b1_qty, 'price': b1_price, 'type': 'LOC', 'desc': '예방적 매수(Buy1)'})
                        if b2_qty > 0:
                            loc_orders.append({'side': 'BUY', 'qty': b2_qty, 'price': b2_price, 'type': 'LOC', 'desc': '예방적 매수(Buy2)'})

                            if safe_qty > 0 and v_rev_q_qty > 0:
                                for n in range(1, 6):
                                    grid_p = round(half_portion_cash / (b2_qty + n), 2)
                                    if grid_p >= 0.01 and grid_p < b2_price:
                                        loc_orders.append({'side': 'BUY', 'qty': 1, 'price': grid_p, 'type': 'LOC', 'desc': f'예방적 줍줍({n})'})
                                        
                        msgs[t] += f"🛡️ <b>[{t}] V-REV 예방적 양방향 LOC 방어선 장전 완료</b>\n"
                        
                        plan_result = {"orders": loc_orders, "trigger_loc": True, "total_q": v_rev_q_qty}
                        if hasattr(strategy_rev, 'save_daily_snapshot'):
                            strategy_rev.save_daily_snapshot(t, plan_result)

                        if safe_qty == 0 or v_rev_q_qty == 0:
                            msgs[t] += "🚫 <code>[0주 새출발] 기준 평단가 부재로 줍줍 생략 (1층 확보에 예산 100% 집중)</code>\n"

                    elif version == "V14":
                        ma_5day = float(await asyncio.to_thread(broker.get_5day_ma, t) or 0.0)
                        v14_vwap_plugin = strategy.v14_vwap_plugin
                        
                        v14_plan = v14_vwap_plugin.get_dynamic_plan(
                            ticker=t, current_price=curr_p, avg_price=safe_avg, qty=safe_qty, 
                            prev_close=prev_c, ma_5day=ma_5day, market_type="REG", 
                            available_cash=allocated_cash.get(t, 0.0), is_simulation=False,
                            is_snapshot_mode=True
                        )
                        loc_orders = v14_plan.get('core_orders', [])
                        msgs[t] += f"🛡️ <b>[{t}] 무매4(VWAP) 예방적 LOC 덫 장전 완료</b>\n"

                    sell_success_count = 0
                    for o in loc_orders:
                        res = await asyncio.to_thread(broker.send_order, t, o['side'], o['qty'], o['price'], o['type'])
                        is_success = res.get('rt_cd') == '0'
                        if not is_success: all_success_map[t] = False
                        if is_success and o['side'] == 'SELL':
                            sell_success_count += 1
                            
                        err_msg = res.get('msg1', '오류')
                        status_icon = '✅' if is_success else f'❌({err_msg})'
                        msgs[t] += f"└ {o['desc']} {o['qty']}주 (${o['price']}): {status_icon}\n"
                        await asyncio.sleep(0.2)
                        
                    if all_success_map[t] and len(loc_orders) > 0:
                        cfg.set_lock(t, "REG")
                        msgs[t] += "\n🔒 <b>방어선 전송 완료 (매매 잠금 설정됨)</b>"
                    elif sell_success_count > 0:
                        cfg.set_lock(t, "REG")
                        msgs[t] += "\n⚠️ <b>일부 방어선 구축 실패 (반쪽짜리 잠금 설정됨)</b>"
                    elif len(loc_orders) == 0:
                        msgs[t] += "\n⚠️ <b>전송할 방어선(예산/수량)이 없습니다.</b>"
                    else:
                        msgs[t] += "\n⚠️ <b>일부 방어선 구축 실패 (잠금 보류)</b>"
                        
                    await context.bot.send_message(chat_id, msgs[t], parse_mode='HTML')
                    v_rev_tickers.append((t, version))
                    continue
                
                ma_5day = float(await asyncio.to_thread(broker.get_5day_ma, t) or 0.0)
                plan = strategy.get_plan(t, curr_p, safe_avg, safe_qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash.get(t, 0.0), is_snapshot_mode=True)
                
                if hasattr(strategy, 'v14_plugin') and hasattr(strategy.v14_plugin, 'save_daily_snapshot'):
                    strategy.v14_plugin.save_daily_snapshot(t, plan)
                    
                plans[t] = plan
                if plan.get('core_orders', []) or plan.get('orders', []):
                    is_rev = plan.get('is_reverse', False)
                    msgs[t] += f"🔄 <b>[{t}] 리버스 주문 실행</b>\n" if is_rev else f"💎 <b>[{t}] 정규장 주문 실행</b>\n"

            for t, ver in v_rev_tickers:
                mod_name = "V-REV" if ver == "V_REV" else "무매4(VWAP)"
                msg = f"🎺 <b>[{t}] {mod_name} 예방적 방어망 장전 완료</b>\n"
                msg += f"▫️ 프리장이 개장했습니다! 시스템 다운 등 최악의 블랙스완을 대비하여 <b>지층별 분리 종가(LOC) 덫</b>을 KIS 서버에 선제 전송했습니다.\n"
                msg += f"▫️ 서버가 무사하다면 장 후반(04:30 KST)에 스스로 깨어나 이 덫을 거두고 추세(60% 허들)를 스캔하여 새로운 최적 전술로 교체합니다! 편안한 밤 보내십시오! 🌙💤\n"
                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

            for t in sorted_tickers:
                if t not in plans: continue
                target_orders = plans[t].get('core_orders', plans[t].get('orders', []))
                for o in target_orders:
                    res = await asyncio.to_thread(broker.send_order, t, o['side'], o['qty'], o['price'], o['type'])
                    if res.get('rt_cd') != '0': all_success_map[t] = False
                    
                    err_msg = res.get('msg1', '오류')
                    status_icon = '✅' if res.get('rt_cd') == '0' else f'❌({err_msg})'
                    msgs[t] += f"└ 1차 필수: {o['desc']} {o['qty']}주: {status_icon}\n"
                    await asyncio.sleep(0.2) 

            for t in sorted_tickers:
                if t not in plans: continue
                target_bonus = plans[t].get('bonus_orders', [])
                for o in target_bonus:
                    res = await asyncio.to_thread(broker.send_order, t, o['side'], o['qty'], o['price'], o['type'])
                    msgs[t] += f"└ 2차 보너스: {o['desc']} {o['qty']}주: {'✅' if res.get('rt_cd')=='0' else '❌(잔금패스)'}\n"
                    await asyncio.sleep(0.2) 

            for t in sorted_tickers:
                if t not in plans: continue
                target_orders = plans[t].get('core_orders', plans[t].get('orders', []))
                target_bonus = plans[t].get('bonus_orders', [])
                if not target_orders and not target_bonus: continue
                
                if all_success_map[t] and len(target_orders) > 0:
                    cfg.set_lock(t, "REG")
                    msgs[t] += "\n🔒 <b>필수 주문 정상 전송 완료 (잠금 설정됨)</b>"
                elif not all_success_map[t] and len(target_orders) > 0:
                    msgs[t] += "\n⚠️ <b>일부 필수 주문 실패 (매매 잠금 보류)</b>"
                elif len(target_bonus) > 0:
                    cfg.set_lock(t, "REG")
                    msgs[t] += "\n🔒 <b>보너스 주문만 전송 완료 (잠금 설정됨)</b>"
                    
                if not any(tx[0] == t for tx in v_rev_tickers): 
                    await context.bot.send_message(chat_id=chat_id, text=msgs[t], parse_mode='HTML')

            return True, "SUCCESS"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            success, fail_reason = await asyncio.wait_for(_do_regular_trade(), timeout=300.0)
            if success:
                if attempt > 1:
                    await context.bot.send_message(chat_id=chat_id, text=f"✅ <b>[통신 복구] {attempt}번째 재시도 끝에 전송을 완수했습니다!</b>", parse_mode='HTML')
                return 
        except Exception as e:
            logging.error(f"정규장 전송 에러 ({attempt}/{MAX_RETRIES}): {e}", exc_info=True)
            if attempt == 1:
                await context.bot.send_message(
                    chat_id=chat_id, 
                    text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 재시도합니다! 🛡️\n<code>사유: {type(e).__name__}: {e}</code>", 
                    parse_mode='HTML'
                )
        else:
            logging.warning(f"정규장 조건 미충족 ({attempt}/{MAX_RETRIES}): {fail_reason}")
            if attempt == 1:
                await context.bot.send_message(
                    chat_id=chat_id, 
                    text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 재시도합니다! 🛡️\n<code>사유: {fail_reason}</code>", 
                    parse_mode='HTML'
                )

        if attempt < MAX_RETRIES:
            if attempt != 1 and attempt % 5 == 0:
                await context.bot.send_message(chat_id=chat_id, text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 재시도합니다! 🛡️", parse_mode='HTML')
            await asyncio.sleep(RETRY_DELAY)

    await context.bot.send_message(chat_id=chat_id, text="🚨 <b>[긴급 에러] 통신 복구 최종 실패. 수동 점검 요망!</b>", parse_mode='HTML')

# ==========================================================
# 5. 🌙 애프터마켓 로터리 덫 (16:05 EST / 05:05 KST)
# ==========================================================
async def scheduled_after_market_lottery(context):
    if not is_market_open(): return
    
    app_data = context.job.data
    cfg, broker, tx_lock = app_data['cfg'], app_data['broker'], app_data['tx_lock']
    chat_id = context.job.chat_id

    async def _do_lottery():
        async with tx_lock:
            cash, holdings = await asyncio.to_thread(broker.get_account_balance)
            if holdings is None: return
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}

            for t in cfg.get_active_tickers():
                version = cfg.get_version(t)
                if version != "V_REV": continue

                is_manual_vwap = getattr(cfg, 'get_manual_vwap_mode', lambda x: False)(t)
                if is_manual_vwap: continue

                h = safe_holdings.get(t) or {}
                qty = int(float(h.get('qty') or 0))
                avg_price = float(h.get('avg') or 0.0)

                if qty > 0 and avg_price > 0:
                    target_price = math.ceil(avg_price * 1.030 * 100) / 100.0
                    await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                    await asyncio.sleep(0.5)

                    # MODIFIED: [V29.21 핫픽스] 동기 함수 블로킹에 의한 루프 마비 방지 (asyncio.to_thread 래핑)
                    res = await asyncio.to_thread(broker.send_order, t, "SELL", qty, target_price, "AFTER_LIMIT")
                    if res.get('rt_cd') == '0':
                        msg = f"🌙 <b>[{t}] 애프터마켓 3% 로터리 덫(Lottery Trap) 장전 완료</b>\n▫️ 대상 물량: <b>{qty}주</b>\n▫️ 타겟 타겟 가격: <b>${target_price:.2f}</b>"
                        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_notification=True)
                    else:
                        fail_msg = f"❌ <b>[{t}] 애프터마켓 덫 장전 실패:</b> {res.get('msg1', '에러')}"
                        await context.bot.send_message(chat_id=chat_id, text=fail_msg, parse_mode='HTML')

                    await asyncio.sleep(0.2)
    try:
        await asyncio.wait_for(_do_lottery(), timeout=60.0)
    except Exception as e:
        logging.error(f"🚨 애프터마켓 로터리 덫 에러: {e}", exc_info=True)

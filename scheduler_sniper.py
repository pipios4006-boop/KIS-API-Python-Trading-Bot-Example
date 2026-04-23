# ==========================================================
# [scheduler_sniper.py] - 🌟 100% 분할 캡슐화 완성본 (V30.00) 🌟
# ⚠️ 단일 책임 원칙(SRP) 적용: 1분 단위 스나이퍼 감시망 전담 코어
# 💡 [역할] 하이브리드 AVWAP 암살자 및 V14 상방 스나이퍼 1분봉 정밀 타격
# 🚨 기존 scheduler_trade.py에서 100% 비파괴적으로 분리 독립 완료
# ==========================================================
import logging
import datetime
import pytz
import asyncio
import traceback
import math
import os
import glob
import yfinance as yf
import pandas_market_calendars as mcal

# 🚨 공통 유틸리티 코어 참조
from scheduler_core import is_market_open

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

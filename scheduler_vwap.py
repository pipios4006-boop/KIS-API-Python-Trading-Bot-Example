# ==========================================================
# [scheduler_vwap.py] - 🌟 100% 분할 캡슐화 완성본 (V30.00) 🌟
# ⚠️ 단일 책임 원칙(SRP) 적용: 장 마감 전 VWAP 전담 코어
# 💡 [역할] Fail-Safe (선제적 LOC 취소) 및 VWAP 1분 단위 타임 슬라이싱
# 🚨 기존 scheduler_trade.py에서 100% 비파괴적으로 분리 독립 완료
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
import pandas_market_calendars as mcal

# 🚨 공통 유틸리티 코어 참조
from scheduler_core import is_market_open

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

# ==========================================================
# [scheduler_trade.py] (1부 / 2부)
# ⚠️ 수술 내역: 
# 1. 🚨 [타임존 락온] 스나이퍼 기상 통제 및 15:30 EST 통제에 엄격한 미국 동부 시간 적용 완료
# 2. 🚨 [거짓 잭팟 차단] 프리장 비정상 스파이크 호가(Glitch Tick) 방어용 거래량 이중 필터 탑재
# 3. V-REV 잭팟 잔량 강제 청산 (Sweep Finisher) 유지
# 4. [V-REV 데드존 구축] 목표가 미만 시 매도 보류 및 잔량 누적(Carry-over) 적용
# 5. [애프터마켓 3% 로터리 덫 신설] KST 05:05 (16:05 EST) 잔여 물량 전량 +3% 지정가 전송
# 💡 [V24.15 대수술] V_VWAP 스케줄러 100% 영구 소각 및 2대 코어(V14, V-REV) 최적화
# 💡 [V24.16 팩트 동기화] 스윕 피니셔 1층 잔량 타점 고유 단가(layer_1_price) 앵커 교정
# 💡 [V24.17 수술] 긴급 수혈(Emergency MOC) 자동 스케줄러 영구 적출 (수동 격발로 전환)
# 💡 [V24.18 수술] V-REV 15:45 EST LOC 마감(Cut-off) 타임락(Time-Lock) 방어막 이식
# ==========================================================
import os
import logging
import datetime
import pytz
import time
import math
import asyncio
import glob
import json
import pandas_market_calendars as mcal
import random

from scheduler_core import is_market_open, get_budget_allocation, get_target_hour

# ==========================================================
# 1. 🔫 스나이퍼 모니터링 (상/하방 기습 및 트레일링)
# ==========================================================
async def scheduled_sniper_monitor(context):
    if not is_market_open(): return
    
    # 💡 [핵심 수술] 엄격한 EST/EDT 타임존 락온
    est = pytz.timezone('US/Eastern')
    now_est = datetime.datetime.now(est)
    
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
    
    pre_start = market_open.replace(hour=4, minute=0)
    start_monitor = pre_start + datetime.timedelta(minutes=1)
    end_monitor = market_close - datetime.timedelta(minutes=15)
    
    if not (start_monitor <= now_est <= end_monitor):
        return

    is_regular_session = market_open <= now_est <= market_close
    
    is_sniper_active_time = False
    switch_time = market_open + datetime.timedelta(minutes=50)
    if now_est >= switch_time:
        is_sniper_active_time = True

    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    chat_id = context.job.chat_id
    
    tracking_cache = app_data.setdefault('sniper_tracking', {})
    
    today_est_str = now_est.strftime('%Y%m%d')
    saved_date = tracking_cache.get('date')
    
    if saved_date != today_est_str:
        tracking_cache.clear()
        tracking_cache['date'] = today_est_str
        
        if saved_date is not None:
            try:
                for _f in glob.glob("data/sniper_cache_*.json"):
                    os.remove(_f)
            except: pass
            
    async def _do_sniper():
        async with tx_lock:
            cash, holdings = broker.get_account_balance()
            if holdings is None: return
            
            sorted_tickers, allocated_cash = get_budget_allocation(cash, cfg.get_active_tickers(), cfg)
            
            for t in cfg.get_active_tickers():
                version = cfg.get_version(t)
                
                # 💡 [다이어트 완료] V_REV는 스나이퍼 로직에서 완전히 제외되며 V_VWAP 관련 찌꺼기 완벽 소각
                if version == "V_REV":
                    continue
                
                is_upward_sniper_on = cfg.get_upward_sniper_mode(t)
                
                if not is_upward_sniper_on:
                    continue
                
                lock_sell = cfg.check_lock(t, "SNIPER_SELL")
                
                if lock_sell:
                    continue
                
                h = holdings.get(t) or {}
                qty = int(float(h.get('qty') or 0))
                avg_price = float(h.get('avg') or 0.0)
                if qty == 0: continue
                
                curr_p = float(await asyncio.to_thread(broker.get_current_price, t) or 0.0)
                prev_c = float(await asyncio.to_thread(broker.get_previous_close, t) or 0.0)
                if curr_p <= 0: continue
                
                df_1min = None
                try:
                    df_1min = await asyncio.to_thread(broker.get_1min_candles_df, t)
                    vwap_status = strategy.analyze_vwap_dominance(df_1min)
                except Exception:
                    vwap_status = {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}

                actual_day_high, _ = await asyncio.to_thread(broker.get_day_high_low, t)
                actual_day_high = float(actual_day_high or 0.0)
                
                tracking_info = tracking_cache.setdefault(t, {
                    'is_tracking': False, 'lowest_price': float('inf'), 'day_high': 0.0, 'armed_price': 0.0, 'alerted': False,
                    'is_trailing': False, 'peak_price': 0.0, 'trailing_armed': False, 'trigger_price': 0.0
                })
                
                if actual_day_high > tracking_info['day_high']:
                    tracking_info['day_high'] = actual_day_high

                is_glitch = False
                if not is_regular_session:
                    if df_1min is not None and not df_1min.empty:
                        recent_vol = df_1min['volume'].tail(2).sum()
                        if recent_vol < 5:
                            is_glitch = True
                    else:
                        is_glitch = True

                if target_pct_val := cfg.get_target_profit(t):
                    target_price = math.ceil(avg_price * (1 + target_pct_val / 100.0) * 100) / 100.0
                    split = cfg.get_split_count(t)
                    t_val, _ = cfg.get_absolute_t_val(t, qty, avg_price)
                    
                    depreciation_factor = 2.0 / split if split > 0 else 0.1
                    star_ratio = (target_pct_val / 100.0) - ((target_pct_val / 100.0) * depreciation_factor * t_val)
                    star_price = math.ceil(avg_price * (1 + star_ratio) * 100) / 100.0
                    
                    if not lock_sell and curr_p >= target_price:
                        if not is_glitch:
                            await asyncio.to_thread(broker.cancel_all_orders_safe, t, side="SELL")
                            await asyncio.sleep(1.0)
                            
                            rem_qty = qty
                            hunt_success = False
                            
                            for attempt in range(3):
                                if rem_qty <= 0: hunt_success = True; break
                                bid_price = float(await asyncio.to_thread(broker.get_bid_price, t) or 0.0)
                                if bid_price > 0 and bid_price >= target_price:
                                    res = broker.send_order(t, "SELL", rem_qty, bid_price, "LIMIT")
                                    if res.get('rt_cd') == '0':
                                        rem_qty = 0
                                        hunt_success = True
                                        break
                                await asyncio.sleep(0.5)
                                
                            if hunt_success:
                                cfg.set_lock(t, "SNIPER_SELL")
                                await context.bot.send_message(chat_id=chat_id, text=f"🎉 <b>[{t}] 12% 잭팟 강제 익절 명중!</b>", parse_mode='HTML')
                                continue
                
                is_rev = cfg.get_reverse_state(t).get("is_active", False)
                sell_divisor = 10 if split <= 20 else 20
                
                if is_rev:
                    q_qty = max(4, math.floor(qty / sell_divisor)) if qty >= 4 else qty
                    ma_5day = float(await asyncio.to_thread(broker.get_5day_ma, t) or 0.0)
                    base_trigger = round(ma_5day, 2) if ma_5day > 0 else (math.ceil(avg_price * 100) / 100.0)
                else:
                    safe_floor_price = math.ceil(avg_price * 1.005 * 100) / 100.0
                    q_qty = math.ceil(qty / 4)
                    base_trigger = max(star_price, safe_floor_price)
                
                if not lock_sell and curr_p >= base_trigger:
                    if not is_glitch:
                        if not tracking_info['trailing_armed']:
                            tracking_info['trailing_armed'] = True
                            tracking_info['is_trailing'] = True
                            tracking_info['peak_price'] = curr_p
                            tracking_info['trigger_price'] = base_trigger
                            await context.bot.send_message(chat_id=chat_id, text=f"🦇 <b>[{t}] 상방 트레일링 스나이퍼 락온!</b>", parse_mode='HTML')
                
                if not lock_sell and tracking_info['trailing_armed']:
                    if curr_p > tracking_info['peak_price']:
                        tracking_info['peak_price'] = curr_p
                        
                    trailing_drop = 1.5 if t == "SOXL" else 1.0
                    if vwap_status.get('is_strong_up', False): trailing_drop = max(trailing_drop, 3.0)
                        
                    drop_trigger = tracking_info['peak_price'] * (1 - (trailing_drop / 100.0))
                    
                    candle = await asyncio.to_thread(broker.get_current_5min_candle, t)
                    c_vwap = candle.get('vwap', 0.0) if candle else 0.0
                    is_vwap_death_cross = (c_vwap > 0 and curr_p < c_vwap)
                    
                    if curr_p <= drop_trigger or is_vwap_death_cross:
                        intercept_price = max(drop_trigger, base_trigger)
                        if is_vwap_death_cross and curr_p > drop_trigger: intercept_price = max(curr_p, base_trigger)
                        intercept_price = math.floor(intercept_price * 100) / 100.0
                        
                        await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                        await asyncio.sleep(1.0) 
                        
                        rem_qty = q_qty
                        hunt_success = False
                        
                        for attempt in range(3):
                            if rem_qty <= 0: hunt_success = True; break
                            bid_price = float(await asyncio.to_thread(broker.get_bid_price, t) or 0.0)
                            if bid_price > 0 and bid_price >= intercept_price:
                                res = broker.send_order(t, "SELL", rem_qty, bid_price, "LIMIT")
                                if res.get('rt_cd') == '0':
                                    rem_qty = 0
                                    hunt_success = True
                                    break
                            await asyncio.sleep(0.5)
                                
                        if hunt_success:
                            cfg.set_lock(t, "SNIPER_SELL")
                            tracking_info['is_trailing'] = False
                            await context.bot.send_message(chat_id=chat_id, text=f"💥 <b>[{t}] 상방 쿼터 스나이퍼 명중! (기습 익절)</b>", parse_mode='HTML')
                            continue
                            
    try:
        await asyncio.wait_for(_do_sniper(), timeout=45.0)
    except asyncio.TimeoutError: pass
    except Exception as e: logging.error(f"🚨 스나이퍼 에러: {e}")

# ==========================================================
# 2. 🛡️ Fail-Safe: 선제적 LOC 취소 & V-REV 타임 슬라이싱 전환 (15:30 EST)
# ==========================================================
async def scheduled_vwap_init_and_cancel(context):
    if not is_market_open(): return
    
    app_data = context.job.data
    cfg, broker, tx_lock = app_data['cfg'], app_data['broker'], app_data['tx_lock']
    chat_id = context.job.chat_id
    
    vwap_cache = app_data.setdefault('vwap_cache', {})
    today_str = datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%Y%m%d')
    if vwap_cache.get('date') != today_str:
        vwap_cache.clear()
        vwap_cache['date'] = today_str
        
    async def _do_init():
        async with tx_lock:
            for t in cfg.get_active_tickers():
                if cfg.get_version(t) == "V_REV":
                    try:
                        await asyncio.to_thread(broker.cancel_all_orders_safe, t, "BUY")
                        await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                        vwap_cache[f"REV_{t}_nuked"] = True
                        
                        msg = f"🌅 <b>[{t}] 15:30 EST 엔진 기상 (Fail-Safe 전환)</b>\n"
                        msg += f"▫️ 프리장에 선제 전송해둔 '예방적 양방향 LOC 덫'을 전량 취소(Nuke)합니다.\n"
                        msg += f"▫️ 시스템 다운 위기를 무사히 넘겼습니다! 이제부터 1분 단위 정밀 타격(VWAP 슬라이싱) 모드로 교전 수칙을 변경합니다. ⚔️"
                        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_notification=True)
                        await asyncio.sleep(1.0)
                    except Exception as e:
                        err_msg = f"🛑 <b>[FATAL ERROR] {t} LOC 덫 취소 실패!</b>\n▫️ 2중 예산 소진 방어를 위해 금일 V-REV 엔진의 신규 발사를 영구 차단(Hard-Lock)합니다.\n▫️ 상세 오류: {e}"
                        await context.bot.send_message(chat_id=chat_id, text=err_msg, parse_mode='HTML')
                        vwap_cache[f"REV_{t}_nuked"] = False 
                    
    try:
        await asyncio.wait_for(_do_init(), timeout=45.0)
    except Exception as e:
        logging.error(f"🚨 Fail-Safe 초기화 에러: {e}")

# ==========================================================
# [scheduler_trade.py] (2부 / 2부)
# 💡 [V24.15 대수술] 후반부 스케줄러 최적화 및 V_VWAP 종속성 100% 탈피
# 💡 [V24.16 팩트 동기화] 스윕 피니셔 1층 잔량 타점 고유 단가(layer_1_price) 앵커 교정
# 💡 [V24.18 수술] V-REV 15:45 EST LOC 마감(Cut-off) 타임락(Time-Lock) 방어막 이식
# ==========================================================

# ==========================================================
# 3. ⏱️ 1분봉 정밀 타격 (V-REV 전용 타임 슬라이싱)
# ==========================================================
async def scheduled_vwap_trade(context):
    if not is_market_open(): return
    
    # 💡 [핵심 수술] 엄격한 EST 타임존 락온
    est = pytz.timezone('US/Eastern')
    now_est = datetime.datetime.now(est)
    
    if not (now_est.hour == 15 and 30 <= now_est.minute <= 59):
        return
        
    app_data = context.job.data
    # 💡 [다이어트 완료] vwap_strategy 의존성 100% 적출
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
        0.0259, 0.0284, 0.0331, 0.0385, 0.0400, 0.0461, 0.0553, 0.0620, 0.0750, 0.1180
    ]
    min_idx = now_est.minute - 30
    current_weight = U_CURVE_WEIGHTS[min_idx] if 0 <= min_idx < 30 else 0.0
        
    async def _do_vwap():
        async with tx_lock:
            cash, holdings = broker.get_account_balance()
            if holdings is None: return
            
            _, allocated_cash = get_budget_allocation(cash, cfg.get_active_tickers(), cfg)

            for t in cfg.get_active_tickers():
                # ----------------------------------------------------------
                # 🟢 V-REV 하이브리드 엔진 로직 (VWAP 슬라이싱 단독 가동)
                # ----------------------------------------------------------
                if cfg.get_version(t) == "V_REV":
                    if not vwap_cache.get(f"REV_{t}_nuked"):
                        continue
                        
                    strategy_rev = app_data.get('strategy_rev')
                    queue_ledger = app_data.get('queue_ledger')
                    if not strategy_rev or not queue_ledger: continue
                    
                    # 💡 [핵심 수술] 현재가/전일종가 Safe Casting
                    curr_p = float(await asyncio.to_thread(broker.get_current_price, t) or 0.0)
                    prev_c = float(await asyncio.to_thread(broker.get_previous_close, t) or 0.0)
                    if curr_p <= 0 or prev_c <= 0: continue
                    
                    # ==========================================================
                    # 🌪️ [V-REV 잭팟 & 1층 잔량 스윕 피니셔] 장 마감 1~2분 전(58~59분) 강제 청산
                    # ==========================================================
                    q_data = queue_ledger.get_queue(t)
                    total_q = sum(item.get("qty", 0) for item in q_data)
                    avg_price = (sum(item.get("qty", 0) * item.get("price", 0.0) for item in q_data) / total_q) if total_q > 0 else 0.0
                    jackpot_trigger = avg_price * 1.010
                    
                    # 💡 [V24.16 수술] 1층 물량 및 타점 산출 (고유 단가 앵커 적용)
                    dates_in_queue = sorted(list(set(item.get('date') for item in q_data if item.get('date'))), reverse=True)
                    layer_1_qty = 0
                    layer_1_trigger = round(prev_c * 1.006, 2)
                    if dates_in_queue:
                        lots_for_date = [item for item in q_data if item.get('date') == dates_in_queue[0]]
                        layer_1_qty = sum(item.get('qty', 0) for item in lots_for_date)
                        if layer_1_qty > 0:
                            layer_1_price = sum(item.get('qty', 0) * item.get('price', 0.0) for item in lots_for_date) / layer_1_qty
                            layer_1_trigger = round(layer_1_price * 1.006, 2)
                    
                    if now_est.minute >= 58 and not vwap_cache.get(f"REV_{t}_sweep_finished"):
                        target_sweep_qty = 0
                        sweep_type = ""
                        
                        if total_q > 0 and curr_p >= jackpot_trigger:
                            target_sweep_qty = total_q
                            sweep_type = "잭팟 전량"
                        elif layer_1_qty > 0 and curr_p >= layer_1_trigger:
                            target_sweep_qty = layer_1_qty
                            sweep_type = "1층 잔여물량"
                            
                        if target_sweep_qty > 0:
                            vwap_cache[f"REV_{t}_sweep_finished"] = True
                            
                            await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                            await asyncio.sleep(0.5)
                            
                            # 💡 [핵심 수술] 호가 Safe Casting
                            bid_price = float(await asyncio.to_thread(broker.get_bid_price, t) or 0.0)
                            exec_price = bid_price if bid_price > 0 else curr_p
                            
                            res = broker.send_order(t, "SELL", target_sweep_qty, exec_price, "LIMIT")
                            odno = res.get('odno', '')
                            
                            if res.get('rt_cd') == '0' and odno:
                                msg = f"🌪️ <b>[{t}] {sweep_type} 강제 청산 (Sweep Finisher) 발동!</b>\n"
                                if sweep_type == "잭팟 전량":
                                    msg += f"▫️ 장 마감을 2분 앞두고 잭팟 커트라인({jackpot_trigger:.2f}) 돌파를 확인했습니다.\n"
                                else:
                                    msg += f"▫️ 장 마감을 2분 앞두고 1층 앵커({layer_1_trigger:.2f}) 방어를 확인했습니다.\n"
                                msg += f"▫️ 미체결 잔량 <b>{target_sweep_qty}주</b>를 시장 매수호가(${exec_price:.2f})로 전량 폭격하여 지층을 완벽하게 소각합니다! 🏆"
                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                
                                ccld_qty = 0
                                for _ in range(4):
                                    await asyncio.sleep(2.0)
                                    execs = await asyncio.to_thread(broker.get_execution_history, t, today_str, today_str)
                                    my_execs = [ex for ex in execs if ex.get('odno') == odno]
                                    if my_execs:
                                        ccld_qty = sum(int(float(ex.get('ft_ccld_qty') or 0)) for ex in my_execs)
                                        if ccld_qty >= target_sweep_qty: break
                                        
                                if ccld_qty > 0:
                                    strategy_rev.record_execution(t, "SELL", ccld_qty, exec_price)
                                    queue_ledger.pop_lots(t, ccld_qty)
                        
                        if target_sweep_qty > 0 or (total_q > 0 and curr_p >= jackpot_trigger):
                            continue 
                    # ==========================================================
                    
                    try:
                        df_1min = await asyncio.to_thread(broker.get_1min_candles_df, t)
                        vwap_status = strategy.analyze_vwap_dominance(df_1min)
                    except Exception:
                        vwap_status = {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}
                    
                    current_regime = "SELL" if curr_p > prev_c else "BUY"
                    last_regime = vwap_cache.get(f"REV_{t}_regime")
                    
                    if last_regime and last_regime != current_regime:
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
                    
                    rev_plan = strategy_rev.get_dynamic_plan(
                        ticker=t, curr_p=curr_p, prev_c=prev_c, 
                        current_weight=current_weight, vwap_status=vwap_status, 
                        min_idx=min_idx, alloc_cash=rev_daily_budget, q_data=q_data
                    )
                    
                    # 💡 [V24.18 수술] LOC 규정 마감 시간(15:45 EST) 초과 시 trigger_loc 강제 무시 (Time-Lock)
                    if rev_plan.get('trigger_loc') and now_est.minute <= 45:
                        vwap_cache[f"REV_{t}_loc_fired"] = True
                        msg = f"🛡️ <b>[{t}] 60% 거래량 지배력 감지 (추세장 전환)</b>\n"
                        msg += f"▫️ 기관급 자금 쏠림으로 인해 위험한 1분 단위 타임 슬라이싱(VWAP)을 전면 중단합니다.\n"
                        msg += f"▫️ <b>잔여 할당량 전량을 양방향 LOC 방어선으로 전환 배치 완료!</b>\n"
                        msg += f"<i>(💡 15:45 EST 규정 마감 전 안전하게 전송되었습니다)</i>"
                        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_notification=True)
                        
                        for o in rev_plan.get('orders', []):
                            if o['qty'] > 0:
                                broker.send_order(t, o['side'], o['qty'], o['price'], "LOC")
                                await asyncio.sleep(0.2)
                        continue
                        
                    for o in rev_plan.get('orders', []):
                        slice_qty = o['qty']
                        if slice_qty <= 0: continue
                        
                        target_price = o['price']
                        side = o['side']
                        
                        # 💡 [핵심 수술] 호가 Safe Casting
                        ask_price = float(await asyncio.to_thread(broker.get_ask_price, t) or 0.0)
                        bid_price = float(await asyncio.to_thread(broker.get_bid_price, t) or 0.0)
                        exec_price = ask_price if side == "BUY" else bid_price
                        if exec_price <= 0: exec_price = curr_p
                        
                        if side == "BUY" and exec_price > target_price: continue
                        if side == "SELL" and exec_price < target_price: continue
                        
                        res = broker.send_order(t, side, slice_qty, exec_price, "LIMIT")
                        odno = res.get('odno', '')
                        
                        if res.get('rt_cd') == '0' and odno:
                            ccld_qty = 0
                            for _ in range(4):
                                await asyncio.sleep(2.0)
                                unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                my_order = next((ox for ox in unfilled_check if ox.get('odno') == odno), None)
                                if my_order:
                                    ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                    break
                                    
                                execs = await asyncio.to_thread(broker.get_execution_history, t, today_str, today_str)
                                my_execs = [ex for ex in execs if ex.get('odno') == odno]
                                if my_execs:
                                    ccld_qty = sum(int(float(ex.get('ft_ccld_qty') or 0)) for ex in my_execs)
                                    if ccld_qty >= slice_qty: break
                                    
                            if ccld_qty < slice_qty:
                                await asyncio.to_thread(broker.cancel_order, t, odno)
                                await asyncio.sleep(1.0)
                                
                            if ccld_qty > 0:
                                strategy_rev.record_execution(t, side, ccld_qty, exec_price)
                                if side == "BUY":
                                    queue_ledger.add_lot(t, ccld_qty, exec_price, "VWAP_BUY")
                                elif side == "SELL":
                                    queue_ledger.pop_lots(t, ccld_qty)
                                    
                            await asyncio.sleep(0.2)

    try:
        await asyncio.wait_for(_do_vwap(), timeout=45.0)
    except Exception as e:
        logging.error(f"🚨 VWAP 스케줄러 에러: {e}")

# ==========================================================
# 4. 🌅 정규장 오픈 (17:05) 전송 (기존 V17 찌꺼기 100% 소각)
# ==========================================================
async def scheduled_regular_trade(context):
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.datetime.now(kst)
    target_hour, _ = get_target_hour()
    chat_id = context.job.chat_id
    
    now_minutes = now.hour * 60 + now.minute
    target_minutes = target_hour * 60 + 5
    
    if abs(now_minutes - target_minutes) > 2 and abs(now_minutes - target_minutes) < (24*60 - 2):
        return
        
    if not is_market_open():
        return
    
    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    strategy_rev = app_data.get('strategy_rev')
    queue_ledger = app_data.get('queue_ledger')
    
    jitter_seconds = random.randint(0, 180)

    await context.bot.send_message(
        chat_id=chat_id, 
        text=f"🌃 <b>[{target_hour}:05] 통합 주문 장전!</b>\n"
             f"🛡️ 서버 접속 부하 방지를 위해 <b>{jitter_seconds}초</b> 대기 후 안전하게 주문 전송을 시도합니다.", 
        parse_mode='HTML'
    )

    await asyncio.sleep(jitter_seconds)

    MAX_RETRIES = 15
    RETRY_DELAY = 60

    async def _do_regular_trade():
        async with tx_lock:
            cash, holdings = broker.get_account_balance()
            if holdings is None:
                return False, "❌ 계좌 정보를 불러오지 못했습니다."

            sorted_tickers, allocated_cash = get_budget_allocation(cash, cfg.get_active_tickers(), cfg)
            
            plans = {}
            msgs = {t: "" for t in sorted_tickers}
            all_success = {t: True for t in sorted_tickers}
            v_rev_tickers = []

            for t in sorted_tickers:
                if cfg.check_lock(t, "REG"): continue
                
                h = holdings.get(t) or {}
                # 💡 [핵심 수술] 정규장 주문 변수 Safe Casting
                curr_p = float(await asyncio.to_thread(broker.get_current_price, t) or 0.0)
                prev_c = float(await asyncio.to_thread(broker.get_previous_close, t) or 0.0)
                
                if cfg.get_version(t) == "V_REV":
                    q_data = queue_ledger.get_queue(t)
                    rev_budget = float(cfg.get_seed(t) or 0.0) * 0.15
                    
                    dummy_vwap = {"is_strong_up": True, "is_strong_down": True} 
                    
                    rev_plan = strategy_rev.get_dynamic_plan(
                        ticker=t, curr_p=curr_p, prev_c=prev_c, 
                        current_weight=1.0, vwap_status=dummy_vwap, 
                        min_idx=0, alloc_cash=rev_budget, q_data=q_data
                    )
                    
                    loc_orders = []
                    for o in rev_plan.get('orders', []):
                        o['type'] = 'LOC'
                        o['desc'] = f"예방적 {o['side']} 방어선"
                        loc_orders.append(o)
                        
                    plans[t] = {'core_orders': loc_orders, 'bonus_orders': [], 'is_reverse': False}
                    msgs[t] += f"🛡️ <b>[{t}] V-REV 예방적 LOC (Fail-Safe) 실행</b>\n"
                    v_rev_tickers.append(t)
                    continue
                
                ma_5day = float(await asyncio.to_thread(broker.get_5day_ma, t) or 0.0)
                
                safe_avg = float(h.get('avg') or 0.0)
                safe_qty = int(float(h.get('qty') or 0))
                
                # 💡 중앙 라우터 플러그인(V14) 호출
                plan = strategy.get_plan(t, curr_p, safe_avg, safe_qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash.get(t, 0.0))
                plans[t] = plan
                
                if plan.get('core_orders', []) or plan.get('orders', []):
                    is_rev = plan.get('is_reverse', False)
                    # 💡 [다이어트 완료] VWAP 출력 텍스트 원천 삭제
                    ver_txt = "정규장 주문"
                    msgs[t] += f"🔄 <b>[{t}] 리버스 주문 실행</b>\n" if is_rev else f"💎 <b>[{t}] {ver_txt} 실행</b>\n"

            for t in v_rev_tickers:
                msg = f"🎺 <b>[{t}] V-REV 예방적 방어망 장전 완료</b>\n"
                msg += f"▫️ 프리장이 개장했습니다! 시스템 다운 등 최악의 블랙스완을 대비하여 <b>양방향 종가(LOC) 덫</b>을 KIS 서버에 선제 전송했습니다.\n"
                msg += f"▫️ 서버가 무사하다면 장 후반(04:30 KST)에 스스로 깨어나 이 덫을 거두고 추세(60% 허들)를 스캔하여 새로운 최적 전술로 교체합니다! 편안한 밤 보내십시오! 🌙💤\n"
                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

            for t in sorted_tickers:
                if t not in plans: continue
                # 💡 [다이어트 완료] 라우터가 계산한 오리지널 오더를 그대로 전송
                target_orders = plans[t].get('core_orders', plans[t].get('orders', []))
                if not target_orders: continue
                
                for o in target_orders:
                    res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                    is_success = res.get('rt_cd') == '0'
                    if not is_success: all_success[t] = False
                    err_msg = res.get('msg1')
                    msgs[t] += f"└ 1차 필수: {o['desc']} {o['qty']}주: {'✅' if is_success else f'❌({err_msg})'}\n"
                    await asyncio.sleep(0.2) 

            for t in sorted_tickers:
                if t not in plans: continue
                target_bonus = plans[t].get('bonus_orders', [])
                if not target_bonus: continue
                
                for o in target_bonus:
                    res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                    is_success = res.get('rt_cd') == '0'
                    msgs[t] += f"└ 2차 보너스: {o['desc']} {o['qty']}주: {'✅' if is_success else '❌(잔금패스)'}\n"
                    await asyncio.sleep(0.2) 

            for t in sorted_tickers:
                if t not in plans: continue
                target_orders = plans[t].get('core_orders', plans[t].get('orders', []))
                target_bonus = plans[t].get('bonus_orders', [])
                
                if not target_orders and not target_bonus: continue
                
                if all_success[t] and len(target_orders) > 0:
                    cfg.set_lock(t, "REG")
                    msgs[t] += "\n🔒 <b>필수 주문 정상 전송 완료 (잠금 설정됨)</b>"
                elif not all_success[t] and len(target_orders) > 0:
                    msgs[t] += "\n⚠️ <b>일부 필수 주문 실패 (매매 잠금 보류)</b>"
                elif len(target_bonus) > 0:
                    cfg.set_lock(t, "REG")
                    msgs[t] += "\n🔒 <b>보너스 주문만 전송 완료 (잠금 설정됨)</b>"
                    
                if t not in v_rev_tickers: 
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
            logging.error(f"정규장 전송 에러 ({attempt}/{MAX_RETRIES}): {e}")

        if attempt < MAX_RETRIES:
            if attempt == 1 or attempt % 5 == 0:
                await context.bot.send_message(chat_id=chat_id, text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 재시도합니다! 🛡️", parse_mode='HTML')
            await asyncio.sleep(RETRY_DELAY)

    await context.bot.send_message(chat_id=chat_id, text="🚨 <b>[긴급 에러] 통신 복구 최종 실패. 수동 점검 요망!</b>", parse_mode='HTML')

# ==========================================================
# 5. 🌙 애프터마켓 로터리 덫 (16:05 EST / 05:05 KST)
# ==========================================================
async def scheduled_after_market_lottery(context):
    app_data = context.job.data
    cfg, broker, tx_lock = app_data['cfg'], app_data['broker'], app_data['tx_lock']
    chat_id = context.job.chat_id

    async def _do_lottery():
        async with tx_lock:
            cash, holdings = broker.get_account_balance()
            if holdings is None: return

            for t in cfg.get_active_tickers():
                if cfg.get_version(t) != "V_REV":
                    continue

                h = holdings.get(t) or {}
                # 💡 [핵심 수술] 수량 및 평단가 Safe Casting
                qty = int(float(h.get('qty') or 0))
                avg_price = float(h.get('avg') or 0.0)

                if qty > 0 and avg_price > 0:
                    target_price = math.ceil(avg_price * 1.030 * 100) / 100.0

                    await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                    await asyncio.sleep(0.5)

                    res = broker.send_order(t, "SELL", qty, target_price, "AFTER_LIMIT")
                    
                    if res.get('rt_cd') == '0':
                        msg = f"🌙 <b>[{t}] 애프터마켓 3% 로터리 덫(Lottery Trap) 장전 완료</b>\n"
                        msg += f"▫️ 대상 물량: <b>{qty}주</b> 전량\n"
                        msg += f"▫️ 타겟 가격: <b>${target_price:.2f}</b> (총 평단가 +3%)\n"
                        msg += f"▫️ 정규장 마감 후 유휴 주식을 활용하여 시간 외 폭등을 포획합니다. 미체결 시 내일 아침 자동 소멸됩니다! 🎣"
                        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_notification=True)
                    await asyncio.sleep(0.2)

    try:
        await asyncio.wait_for(_do_lottery(), timeout=60.0)
    except Exception as e:
        logging.error(f"🚨 애프터마켓 로터리 덫 에러: {e}")

# ==========================================================
# [scheduler_trade.py] - 🌟 100% 통합 완성본 🌟 (Part 1)
# ⚠️ 수술 내역: 
# 🚨 [V25.03 긴급 수술] 타임존 Glitch 방어 및 상대적 시간 윈도우(Relative Time Window) 도입
# 💡 하드코딩된 '15시' 비교를 폐기하고 '장 마감(market_close) 30분 전' 앵커 시스템으로 전면 교체
# 🚨 [V25.04 패치] 듀얼 레퍼런싱 데이터(SOXX/SOXL) 이원화 호출 및 파이프라인 연동
# 🚨 [V25.19 핫픽스] 서머타임(DST) 경계일 프리마켓 시간 연산 에러(NonExistentTimeError) 수학적 교정
# 🚨 [V25.19 핫픽스] 듀얼 레퍼런싱 base_map 매핑 누락 시 파생상품 오호출 맹점 방어
# 🚨 [V25.20 핫픽스] 잭팟 스윕 피니셔 MOC 락다운 충돌 방어 및 순수 매도 가능 잔량 디커플링 연산 이식
# 🚨 [V25.24 타임라인 시프트] VWAP 슬라이싱 시간을 3분 앞당겨(장 마감 33분 전~4분 전) 막판 미체결 맹점 원천 차단
# 🚨 [V25.25 에러 팩트 보고] 애프터마켓 로터리 덫 거절 사유 텔레그램 타전 방어막 신설
# 🚨 [V25.26 락다운 확장] 당일 0주 졸업 후 유령 매수(Phantom Buy) 방지를 위한 Daily Buy-Lock 완벽 이식
# 🚀 [V26.02 핵심 수술] V14 무매 오리지널의 VWAP 슬라이싱 스케줄러 통합 확장
# 🚨 [V26.03 핫픽스] V-REV 자동매매 시 줍줍(Grid) 누락 복원 및 수동 모드 스케줄러 100% 락다운 이식
# 🚨 [V26.04 스위프 패치] VWAP 엔진 데드존(15:56~15:58) 철거 및 3분간(15:57~16:00)의 지속적 클린업 스윕 피니셔로 통합
# 🚀 [V27.01 지시서 스냅샷] 매일 17:05 확정 지시서를 박제하고, 16:05 EST에 초기화하는 파이프라인 이식 완료
# 🚨 [V27.13 그랜드 수술] 코파일럿 합작 - all_success 딕셔너리 붕괴 차단, 상위 평단가 음수 오류 해결, 스냅샷 은폐 사고(finally) 방어 및 0주 새출발 확정 매수(/ 0.935) 원본 완벽 복구
# 🚨 [V27.14 핫픽스] AVWAP 예산 중복 소진 차단, 스윕 잔여 주문 취소, VWAP 예외 전파 붕괴 방어막 이식
# 🚀 [V28.01 핫픽스] 정규장 스케줄러 타임 윈도우 15분 확장 (APScheduler 지연 붕괴 원천 차단)
# 🚀 [V28.02 그랜드 수술] 코파일럿 엣지 케이스 3대 결함(DST 데드락 65분 확장, prev_c 결측 텔레그램 타전, tx_lock 콜드스타트 가드) 전면 수술 완료
# 🚀 [V28.03 그랜드 수술] 엣지 케이스 4대 결함(이중 매도 방지, 클램핑 수학 모순 교정, 큐 증발 폴백, 잠금 스킵 타전) 및 UX 렌더링 충돌 교정 완비
# 🚀 [V28.04 그랜드 수술] VWAP 엔진 내 prev_c 앵커(Lock-on) 고정 및 was_holding 영속성 듀얼 캐싱 방어막 이식 완비
# 🚨 [V28.07 그랜드 수술] 장중 스냅샷 강제 은폐(Hide) 및 파괴 레거시 전면 적출 (0주 새출발 기억상실 하극상 버그 영구 차단)
# MODIFIED: [V28.08 세션 오염 방어] 내일 날짜 스냅샷 복사(Copy) 레거시 100% 영구 적출. 야간 청소 스케줄러 기절 시 발생하는 어제의 0주 스냅샷 환각 버그 차단 완료.
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
# 1. 🔫 스나이퍼 모니터링 (하이브리드 AVWAP 및 V14 상방 스나이퍼)
# ==========================================================
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
            avwap_free_cash = cash
            
            for t in cfg.get_active_tickers():
                version = cfg.get_version(t)
                
                if version == "V_REV":
                    if not cfg.get_avwap_hybrid_mode(t): continue
                    if tracking_cache.get(f"AVWAP_SHUTDOWN_{t}"): continue
                    
                    target_base = base_map.get(t, t)
                    
                    if f"AVWAP_CTX_{t}" not in tracking_cache:
                        ctx_data = await asyncio.to_thread(strategy.fetch_avwap_macro, target_base)
                        tracking_cache[f"AVWAP_CTX_{t}"] = ctx_data
                    
                    ctx_data = tracking_cache.get(f"AVWAP_CTX_{t}")
                    avwap_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
                    avwap_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
                    
                    exec_curr_p = float(await asyncio.to_thread(broker.get_current_price, t) or 0.0)
                    if exec_curr_p <= 0: continue
                    
                    base_curr_p = float(await asyncio.to_thread(broker.get_current_price, target_base) or 0.0)
                    if base_curr_p <= 0: continue
                    
                    base_day_open, _ = await asyncio.to_thread(broker.get_day_high_low, target_base)
                    base_day_open = float(base_day_open or 0.0)
                    
                    df_1min_base = None
                    try: df_1min_base = await asyncio.to_thread(broker.get_1min_candles_df, target_base)
                    except: pass
                    
                    decision = strategy.get_avwap_decision(
                        target_base, t, base_curr_p, exec_curr_p, base_day_open, avwap_avg, avwap_qty, avwap_free_cash, ctx_data, df_1min_base, now_est
                    )
                    
                    action, reason = decision.get('action'), decision.get('reason')
                    
                    if action == 'SHUTDOWN':
                        tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = True
                        await context.bot.send_message(chat_id=chat_id, text=f"🛑 <b>[{t}] 하이브리드 AVWAP 당일 작전 종료</b>\n▫️ 사유: {reason}", parse_mode='HTML')
                        
                    elif action == 'BUY' and not tracking_cache.get(f"AVWAP_BOUGHT_{t}"):
                        b_qty = decision.get('qty', 0)
                        if b_qty > 0:
                            ask_p = float(await asyncio.to_thread(broker.get_ask_price, t) or exec_curr_p)
                            res = broker.send_order(t, "BUY", b_qty, ask_p, "LIMIT")
                            if res.get('rt_cd') == '0':
                                tracking_cache[f"AVWAP_BOUGHT_{t}"], tracking_cache[f"AVWAP_QTY_{t}"], tracking_cache[f"AVWAP_AVG_{t}"] = True, b_qty, ask_p
                                committed = b_qty * ask_p
                                avwap_free_cash = max(0.0, avwap_free_cash - committed)
                                
                                await context.bot.send_message(chat_id=chat_id, text=f"🎯 <b>[{t}] 하이브리드 AVWAP 딥매수 작렬!</b>\n▫️ 수량: {b_qty}주 / 단가: ${ask_p:.2f}", parse_mode='HTML')
                    
                    elif action == 'SELL' and avwap_qty > 0:
                        bid_p = float(await asyncio.to_thread(broker.get_bid_price, t) or exec_curr_p)
                        res = broker.send_order(t, "SELL", avwap_qty, bid_p, "LIMIT")
                        if res.get('rt_cd') == '0':
                            tracking_cache[f"AVWAP_SHUTDOWN_{t}"], tracking_cache[f"AVWAP_QTY_{t}"], tracking_cache[f"AVWAP_AVG_{t}"] = True, 0, 0.0
                            await context.bot.send_message(chat_id=chat_id, text=f"🏆 <b>[{t}] 하이브리드 AVWAP 독립물량 청산 완료!</b>", parse_mode='HTML')
                    continue

    try:
        await asyncio.wait_for(_do_sniper(), timeout=45.0)
    except Exception as e:
        logging.error(f"🚨 스나이퍼 타임아웃 에러: {e}")

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
                            logging.error(f"🚨 Fail-Safe 초기화(Nuke) 에러: {e}")
                            vwap_cache[f"REV_{t}_nuked"] = False 
                    
    try:
        await asyncio.wait_for(_do_init(), timeout=45.0)
    except Exception as e:
        logging.error(f"🚨 Fail-Safe 타임아웃 에러: {e}")

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
        0.0259, 0.0284, 0.0331, 0.0385, 0.0400, 0.0461, 0.0553, 0.0620, 0.0750, 0.1180
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
                            logging.error(f"🚨 자가 치유 Nuke 실패: {e}")
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
                        
                        q_data = queue_ledger.get_queue(t)
                        total_q = sum(item.get("qty", 0) for item in q_data)
                        
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
                                if live_holdings and t in live_holdings:
                                    sellable_qty = int(float(live_holdings[t].get('ord_psbl_qty', live_holdings[t].get('qty', 0))))
                                    actual_sweep_qty = min(target_sweep_qty, sellable_qty)
                                    
                                    if actual_sweep_qty > 0:
                                        bid_price = float(await asyncio.to_thread(broker.get_bid_price, t) or 0.0)
                                        exec_price = bid_price if bid_price > 0 else curr_p
                                        
                                        res = broker.send_order(t, "SELL", actual_sweep_qty, exec_price, "LIMIT")
                                        odno = res.get('odno', '')
                                        
                                        if res.get('rt_cd') == '0' and odno:
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
                                                execs = await asyncio.to_thread(broker.get_execution_history, t, today_str, today_str)
                                                my_execs = [ex for ex in execs if ex.get('odno') == odno]
                                                if my_execs:
                                                    ccld_qty = sum(int(float(ex.get('ft_ccld_qty') or 0)) for ex in my_execs)
                                                    if ccld_qty >= actual_sweep_qty: break
                                            
                                            if ccld_qty < actual_sweep_qty:
                                                try:
                                                    await asyncio.to_thread(broker.cancel_order, t, odno)
                                                    await asyncio.sleep(0.5)
                                                except Exception as e_cancel:
                                                    logging.warning(f"⚠️ [{t}] 스윕 잔여 주문 취소 실패: {e_cancel}")
                                                    
                                            if ccld_qty > 0:
                                                strategy_rev.record_execution(t, "SELL", ccld_qty, exec_price)
                                                queue_ledger.pop_lots(t, ccld_qty)
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
                                    broker.send_order(t, o['side'], o['qty'], o['price'], "LOC")
                                    await asyncio.sleep(0.2)
                            continue
                            
                        target_orders = rev_plan.get('orders', [])

                    elif version == "V14":
                        h = holdings.get(t, {'qty':0, 'avg':0.0})
                        actual_qty = int(h.get('qty', 0))
                        actual_avg = float(h.get('avg', 0.0))
                        
                        v14_vwap_plugin = strategy.v14_vwap_plugin
                        
                        plan = v14_vwap_plugin.get_dynamic_plan(
                            ticker=t, curr_p=curr_p, prev_c=prev_c, 
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
        logging.error(f"🚨 VWAP 스케줄러 에러: {e}")
# ==========================================================
# [scheduler_trade.py] - 🌟 100% 통합 완성본 🌟 (Part 2)
# ==========================================================

# ... (앞선 1부 코드의 scheduled_vwap_trade 함수 끝부분에 이어집니다) ...

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

            sorted_tickers, allocated_cash = get_budget_allocation(cash, cfg.get_active_tickers(), cfg)
            
            plans = {}
            msgs = {t: "" for t in sorted_tickers}
            
            all_success_map = {t: True for t in sorted_tickers}
            v_rev_tickers = [] 

            for t in sorted_tickers:
                if cfg.check_lock(t, "REG"):
                    logging.warning(f"⚠️ [{t}] REG 잠금이 해제되지 않은 채 17:05 루프 진입. 자정 초기화 실패 가능성 있음. 해당 종목 주문 전체 스킵.")
                    skip_msg = (
                        f"⚠️ <b>[{t}] REG 잠금 미해제 — 주문 스킵</b>\n"
                        f"▫️ 전날 REG 잠금이 자정 초기화 시 해제되지 않아 오늘 17:05 주문 루프에서 제외되었습니다.\n"
                        f"▫️ 수동으로 잠금 해제 후 [🚀 V-REV 방어선 수동 장전] 버튼을 눌러 재장전하십시오."
                    )
                    await context.bot.send_message(chat_id, skip_msg, parse_mode='HTML')
                    continue
                
                h = holdings.get(t) or {}
                curr_p = float(await asyncio.to_thread(broker.get_current_price, t) or 0.0)
                prev_c = float(await asyncio.to_thread(broker.get_previous_close, t) or 0.0)
                safe_avg = float(h.get('avg') or 0.0)
                safe_qty = int(float(h.get('qty') or 0))
                
                version = cfg.get_version(t)
                is_manual_vwap = getattr(cfg, 'get_manual_vwap_mode', lambda x: False)(t)

                if version == "V_REV" and is_manual_vwap:
                    msgs[t] += f"🛡️ <b>[{t}] V-REV 수동 시그널 모드 가동 중</b>\n"
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
                                    logging.error(f"🚨 [{t}] 큐 원장 불일치 감지! 큐 l1={l1_qty_raw}주 > 실제 보유={safe_qty}주. 실제 보유량으로 강제 클램프합니다.")
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
                                
                                if upper_invested > 0:
                                    upper_avg = upper_invested / upper_qty
                                else:
                                    upper_avg = safe_avg
                                    
                                target_upper = round(upper_avg * 1.005, 2)
                                loc_orders.append({'side': 'SELL', 'qty': upper_qty, 'price': target_upper, 'type': 'LOC', 'desc': '[상위 재고]'})
                                
                        elif not q_data and safe_qty > 0:
                            logging.warning(f"⚠️ [{t}] V-REV 큐 데이터 부재 + 실제 보유 {safe_qty}주 감지! Fallback LOC 매도 방어선을 생성합니다.")
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
                        
                        if prev_c > 0:
                            b1_price = round(prev_c * 0.999 if v_rev_q_qty == 0 else prev_c * 0.995, 2)
                            b2_price = round(prev_c / 0.935 if v_rev_q_qty == 0 else prev_c * 0.9725, 2)
                            
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
                        else:
                            logging.error(f"🚨 [{t}] V-REV 전일 종가(prev_c) 취득 실패(0). 매수 방어선(Buy1/Buy2/줍줍) 미장전!")
                            msgs[t] += (
                                f"🚨 <b>[{t}] 전일 종가 API 결측치 감지!</b>\n"
                                f"▫️ prev_c = 0 수신 → 매수 방어선(Buy1/Buy2/줍줍)을 <b>장전하지 못했습니다.</b>\n"
                                f"▫️ 수동으로 [🚀 V-REV 방어선 수동 장전] 버튼을 눌러 재장전하거나 다음 날 확인하십시오.\n"
                            )
                            msgs[t] += f"⚠️ <b>[{t}] V-REV 예방적 LOC 방어선 부분 장전 (매도 전용)</b>\n"
                            all_success_map[t] = False
                        
                        plan_result = {
                            "orders": loc_orders,
                            "trigger_loc": True,
                            "total_q": v_rev_q_qty
                        }
                        # MODIFIED: [V28.08 세션 오염 방어] 내일 날짜로 스냅샷을 강제 복사하던 레거시 로직 전면 삭제
                        if hasattr(strategy_rev, 'save_daily_snapshot'):
                            strategy_rev.save_daily_snapshot(t, plan_result)

                        if safe_qty == 0 or v_rev_q_qty == 0:
                            msgs[t] += "🚫 <code>[0주 새출발] 기준 평단가 부재로 줍줍 생략 (1층 확보에 예산 100% 집중)</code>\n"

                    elif version == "V14":
                        ma_5day = float(await asyncio.to_thread(broker.get_5day_ma, t) or 0.0)
                        v14_vwap_plugin = strategy.v14_vwap_plugin
                        
                        v14_plan = v14_vwap_plugin.get_plan(
                            ticker=t, current_price=curr_p, avg_price=safe_avg, qty=safe_qty, 
                            prev_close=prev_c, ma_5day=ma_5day, market_type="REG", 
                            available_cash=allocated_cash.get(t, 0.0), is_simulation=False,
                            is_snapshot_mode=True
                        )
                        loc_orders = v14_plan.get('core_orders', [])
                        
                        # MODIFIED: [V28.08 세션 오염 방어] V14 분기 내일 날짜 스냅샷 복사 레거시 전면 삭제
                        msgs[t] += f"🛡️ <b>[{t}] 무매4(VWAP) 예방적 LOC 덫 장전 완료</b>\n"

                    sell_success_count = 0
                    for o in loc_orders:
                        res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
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
                plan = strategy.get_plan(t, curr_p, safe_avg, safe_qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash.get(t, 0.0))
                plans[t] = plan
                
                if plan.get('core_orders', []) or plan.get('orders', []):
                    is_rev = plan.get('is_reverse', False)
                    ver_txt = "정규장 주문"
                    msgs[t] += f"🔄 <b>[{t}] 리버스 주문 실행</b>\n" if is_rev else f"💎 <b>[{t}] {ver_txt} 실행</b>\n"

            for t, ver in v_rev_tickers:
                mod_name = "V-REV" if ver == "V_REV" else "무매4(VWAP)"
                msg = f"🎺 <b>[{t}] {mod_name} 예방적 방어망 장전 완료</b>\n"
                msg += f"▫️ 프리장이 개장했습니다! 시스템 다운 등 최악의 블랙스완을 대비하여 <b>지층별 분리 종가(LOC) 덫</b>을 KIS 서버에 선제 전송했습니다.\n"
                msg += f"▫️ 서버가 무사하다면 장 후반(04:30 KST)에 스스로 깨어나 이 덫을 거두고 추세(60% 허들)를 스캔하여 새로운 최적 전술로 교체합니다! 편안한 밤 보내십시오! 🌙💤\n"
                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

            for t in sorted_tickers:
                if t not in plans: continue
                target_orders = plans[t].get('core_orders', plans[t].get('orders', []))
                if not target_orders: continue
                
                for o in target_orders:
                    res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                    is_success = res.get('rt_cd') == '0'
                    if not is_success: all_success_map[t] = False
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
            cash, holdings = await asyncio.to_thread(broker.get_account_balance)
            if holdings is None: return

            for t in cfg.get_active_tickers():
                version = cfg.get_version(t)
                if version != "V_REV":
                    continue

                is_manual_vwap = getattr(cfg, 'get_manual_vwap_mode', lambda x: False)(t)
                if is_manual_vwap:
                    continue

                h = holdings.get(t) or {}
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
                    else:
                        err_msg = res.get('msg1', '알 수 없는 KIS 시스템 에러')
                        fail_msg = f"❌ <b>[{t}] 애프터마켓 덫(Lottery Trap) 장전 실패</b>\n"
                        fail_msg += f"▫️ 사유: {err_msg}\n"
                        fail_msg += f"▫️ 증권사 서버 거절 또는 통신 오류가 발생했습니다. 수동으로 장후 지정가를 장전해 주십시오."
                        await context.bot.send_message(chat_id=chat_id, text=fail_msg, parse_mode='HTML')

                    await asyncio.sleep(0.2)
                    
            try:
                for t in cfg.get_active_tickers():
                    for prefix in ["REV", "V14VWAP"]:
                        for f in glob.glob(f"data/daily_snapshot_{prefix}_*_{t}.json"):
                            try: os.remove(f)
                            except: pass
                logging.info("🧹 [스냅샷 초기화] 익일 새로운 작전 수립을 위해 당일 지시서 스냅샷 파일을 모두 소각했습니다.")
            except Exception as e:
                logging.error(f"🚨 스냅샷 파일 초기화 실패: {e}")

    try:
        await asyncio.wait_for(_do_lottery(), timeout=60.0)
    except Exception as e:
        logging.error(f"🚨 애프터마켓 로터리 덫 에러: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"🚨 <b>애프터마켓 로터리 덫 치명적 에러 발생!</b>\n▫️ 상세 내역: {e}", parse_mode='HTML')

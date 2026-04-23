# ==========================================================
# [scheduler_regular.py] - 🌟 100% 분할 캡슐화 완성본 (V30.00) 🌟
# ⚠️ 단일 책임 원칙(SRP) 적용: 정규장 통합 주문 및 방어선 장전 전담 코어
# 💡 [역할] 17:05 KST 예방적 LOC 덫 전송 및 V14 오리지널 주문 집행
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
import random
import pandas_market_calendars as mcal

# 🚨 공통 유틸리티 코어 참조
from scheduler_core import is_market_open, get_budget_allocation, get_target_hour

# ==========================================================
# 4. 🌅 정규장 오픈 (17:05) 전송 (V14 통합 & V-REV 예방 방어선)
# ==========================================================
# 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
# 코파일럿 등이 제안하는 외부 KIS API 래퍼(동기식 블로킹) 모듈을 무지성으로 병합하지 마세요.
# asyncio 루프 내에서 requests나 동기 함수를 직접 호출하면 전체 스케줄러가 100% 교착(Deadlock)에 빠집니다.
# 반드시 기존의 asyncio.to_thread 래핑과 내부 broker.py 엔진만을 사용해 비동기 무결성을 유지하세요.
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

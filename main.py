import os
import logging
import datetime
import pytz
import time
import math
import asyncio
import pandas_market_calendars as mcal
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from dotenv import load_dotenv

from config import ConfigManager
from broker import KoreaInvestmentBroker
from strategy import InfiniteStrategy
from telegram_bot import TelegramController

if not os.path.exists('data'):
    os.makedirs('data')
if not os.path.exists('logs'):
    os.makedirs('logs')

load_dotenv() 

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
# 🦇 [V19.10] 환경변수 미검증 방어: ADMIN_CHAT_ID가 없을 경우 None 처리 후 텔레그램 컨트롤러에서 차단
try:
    ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID")) if os.getenv("ADMIN_CHAT_ID") else None
except ValueError:
    ADMIN_CHAT_ID = None

# 🦇 [V19.10] 환경변수 미검증 방어: 한투 API 필수 키 누락 시 프로그램 기동 중단 (크래시 방어)
APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
CANO = os.getenv("CANO")
ACNT_PRDT_CD = os.getenv("ACNT_PRDT_CD", "01")

if not all([APP_KEY, APP_SECRET, CANO]):
    print("❌ [치명적 오류] .env 파일에 한투 API 필수 키(APP_KEY, APP_SECRET, CANO)가 누락되었습니다. 봇을 종료합니다.")
    exit(1)

log_filename = f"logs/bot_app_{datetime.datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO,
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def is_dst_active():
    est = pytz.timezone('US/Eastern')
    return datetime.datetime.now(est).dst() != datetime.timedelta(0)

def get_target_hour():
    return (17, "🌞 서머타임 적용(여름)") if is_dst_active() else (18, "❄️ 서머타임 해제(겨울)")

def is_market_open():
    est = pytz.timezone('US/Eastern')
    today = datetime.datetime.now(est).date()
    nyse = mcal.get_calendar('NYSE')
    return not nyse.schedule(start_date=today, end_date=today).empty

# 🦇 [V19.10] 예산 배분 로직 일치화 (심각한 논리 버그 수정)
# main.py의 스나이퍼 및 정규장 스케줄러가 리버스 모드 종목의 예산을 0으로 처리하도록 telegram_bot과 동일한 로직 적용
def get_budget_allocation(cash, tickers, cfg):
    sorted_tickers = sorted(tickers, key=lambda x: 0 if x == "SOXL" else (1 if x == "TQQQ" else 2))
    allocated = {}
    force_turbo_off = False
    rem_cash = cash
    
    for tx in sorted_tickers:
        rev_state = cfg.get_reverse_state(tx)
        is_rev = rev_state.get("is_active", False)
        
        if is_rev:
            portion = 0.0
        else:
            split = cfg.get_split_count(tx)
            portion = cfg.get_seed(tx) / split if split > 0 else 0
            
        if rem_cash >= portion:
            allocated[tx] = rem_cash
            rem_cash -= portion
        else: 
            allocated[tx] = 0
            if not is_rev:
                force_turbo_off = True 
                
    return sorted_tickers, allocated, force_turbo_off

# 🦇 [V19.10] 스나이퍼 모니터 코드 3중 중복 제거: 실제 체결가 역산 공통 헬퍼 함수
def get_actual_execution_price(execs, target_qty, side_cd):
    if not execs: return 0.0
    
    execs.sort(key=lambda x: x.get('ord_tmd', '000000'), reverse=True)
    matched_qty = 0
    total_amt = 0.0
    for ex in execs:
        if ex.get('sll_buy_dvsn_cd') == side_cd: 
            eqty = int(float(ex.get('ft_ccld_qty', '0')))
            eprice = float(ex.get('ft_ccld_unpr3', '0'))
            if matched_qty + eqty <= target_qty:
                total_amt += eqty * eprice
                matched_qty += eqty
            elif matched_qty < target_qty:
                rem = target_qty - matched_qty
                total_amt += rem * eprice
                matched_qty += rem
            
            if matched_qty >= target_qty:
                break
    
    if matched_qty > 0:
        return math.floor((total_amt / matched_qty) * 100) / 100.0
    return 0.0

async def scheduled_token_check(context):
    context.job.data['broker']._get_access_token(force=True)

async def scheduled_force_reset(context):
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.datetime.now(kst)
    target_hour, _ = get_target_hour()
    
    if now.hour != target_hour: return
    if not is_market_open(): return
    
    app_data = context.job.data
    app_data['cfg'].reset_locks()
    
    for t in app_data['cfg'].get_active_tickers():
        app_data['cfg'].increment_reverse_day(t)
        
        bb_lower = await asyncio.to_thread(app_data['broker'].get_bb_lower, t)
        app_data['cfg'].set_daily_bb_lower(t, bb_lower)
        
    await context.bot.send_message(chat_id=context.job.chat_id, text=f"🔓 <b>[{target_hour}:00] 시스템 초기화 완료 (매매 잠금 해제 & 하단 스나이퍼 장전 & 리버스 카운트 누적)</b>", parse_mode='HTML')

async def scheduled_premarket_monitor(context):
    if not is_market_open(): return
    app_data = context.job.data
    
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.datetime.now(kst)
    target_hour, _ = get_target_hour()
    
    if not (now.hour == target_hour and 0 <= now.minute < 30):
        return

    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']

    async with tx_lock:
        _, holdings = broker.get_account_balance()
        if holdings is None: return 

        for t in cfg.get_active_tickers():
            h = holdings.get(t, {'qty': 0, 'avg': 0})
            if int(h['qty']) == 0: continue 

            curr_p = await asyncio.to_thread(broker.get_current_price, t)
            prev_c = await asyncio.to_thread(broker.get_previous_close, t)
            
            plan = strategy.get_plan(t, curr_p, float(h['avg']), int(h['qty']), prev_c, market_type="PRE_CHECK")
            
            if plan['orders']:
                msg = f"🌅 <b>[{t}] 대박! 프리마켓 목표 달성 🎉</b>\n⚡ 전량 익절 주문을 실행합니다!"
                broker.cancel_all_orders_safe(t)
                for o in plan['orders']:
                    res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                    # 🦇 [V19.10] 구버전 파이썬 f-string 중첩 따옴표 SyntaxError 크래시 방어
                    err_msg = res.get('msg1')
                    is_success = res.get('rt_cd') == '0'
                    msg += f"\n└ {o['desc']}: {'✅' if is_success else f'❌({err_msg})'}"
                    await asyncio.sleep(0.2) 
                await context.bot.send_message(chat_id=context.job.chat_id, text=msg, parse_mode='HTML')

async def scheduled_sniper_monitor(context):
    if not is_market_open(): return
    
    est = pytz.timezone('US/Eastern')
    now_est = datetime.datetime.now(est)
    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
    if schedule.empty: return
    
    market_open = schedule.iloc[0]['market_open'].astimezone(est)
    market_close = schedule.iloc[0]['market_close'].astimezone(est)
    
    pre_start = market_open.replace(hour=4, minute=0)
    start_monitor = pre_start + datetime.timedelta(minutes=1)
    end_monitor = market_close - datetime.timedelta(minutes=15)
    
    if not (start_monitor <= now_est <= end_monitor):
        return

    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    chat_id = context.job.chat_id
    
    async with tx_lock:
        cash, holdings = broker.get_account_balance()
        if holdings is None: return
        
        sorted_tickers, allocated_cash, force_turbo_off = get_budget_allocation(cash, cfg.get_active_tickers(), cfg)
        
        for t in cfg.get_active_tickers():
            if cfg.get_version(t) != "V17": continue
            if cfg.check_lock(t, "SNIPER"): continue 
            
            h = holdings.get(t, {'qty': 0, 'avg': 0})
            qty = int(h['qty'])
            avg_price = float(h['avg'])
            if qty == 0: continue
            
            curr_p = await asyncio.to_thread(broker.get_current_price, t)
            prev_c = await asyncio.to_thread(broker.get_previous_close, t)
            if curr_p <= 0: continue
            
            real_time_bb_lower = await asyncio.to_thread(broker.get_bb_lower, t, current_price=curr_p)
            
            sniper_pct = cfg.get_sniper_trigger(t)
            hybrid_multiplier = (100.0 - sniper_pct) / 100.0
            hybrid_base = min(avg_price, prev_c) * hybrid_multiplier
            
            raw_target_price = max(real_time_bb_lower, hybrid_base)
            target_buy_price = math.floor(raw_target_price * 100) / 100.0
            
            trigger_reason = "실시간 블밴 하한가" if real_time_bb_lower >= hybrid_base else f"-{sniper_pct}% 고정 하한가"
            
            if target_buy_price > 0 and curr_p <= target_buy_price and curr_p < avg_price:
                
                is_rev = cfg.get_reverse_state(t).get("is_active", False)
                if is_rev:
                    sniper_budget = cfg.get_escrow_cash(t) / 4.0
                else:
                    split = cfg.get_split_count(t)
                    sniper_budget = cfg.get_seed(t) / split if split > 0 else 0
                
                if sniper_budget < curr_p:
                    continue 

                await asyncio.to_thread(broker.cancel_all_orders_safe, t, side="BUY")
                await asyncio.sleep(1.0)
                
                hunt_success = False
                
                for attempt in range(3):
                    if target_buy_price > 0:
                        buy_qty = math.floor(sniper_budget / target_buy_price)
                        if buy_qty > 0:
                            res = broker.send_order(t, "BUY", buy_qty, target_buy_price, "LIMIT")
                            if res.get('rt_cd') == '0':
                                await asyncio.sleep(5.0)
                                unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                buy_unfilled = [o for o in unfilled if o.get('sll_buy_dvsn_cd') == '02']
                                
                                if not buy_unfilled:
                                    cfg.set_lock(t, "SNIPER") 
                                    
                                    # 🦇 [V19.10] 타임존 경계 오류 수정: KST 대신 EST 기준 날짜 사용
                                    today_est_str = datetime.datetime.now(est).strftime('%Y%m%d')
                                    execs = await asyncio.to_thread(broker.get_execution_history, t, today_est_str, today_est_str)
                                    
                                    actual_buy_price = target_buy_price
                                    if execs:
                                        computed_price = get_actual_execution_price(execs, buy_qty, '02')
                                        if computed_price > 0: actual_buy_price = computed_price

                                    msg = f"💥 <b>[{t}] V19.1 하이브리드 덫 명중! ({trigger_reason} 이탈)</b>\n"
                                    msg += f"📉 실시간 현재가(${curr_p:.2f})가 <b>[{trigger_reason}]</b> 기준선(${target_buy_price:.2f})마저 뚫었습니다!\n"
                                    msg += f"🎯 <b>최적의 실제 체결 단가(${actual_buy_price:.2f})에 {buy_qty}주 지정가 덫이 완벽하게 체결</b>되었습니다!\n"
                                    msg += "🔫 당일 스나이퍼 활동을 짜릿하게 종료합니다."
                                    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                    hunt_success = True
                                    break
                                else:
                                    await asyncio.to_thread(broker.cancel_all_orders_safe, t, side="BUY")
                                    await asyncio.sleep(1.0)
                    
                    await asyncio.sleep(1.5)
                
                if hunt_success:
                    continue

                now_ts = time.time()
                fail_history = app_data.setdefault('sniper_fail_ts', {})
                if now_ts - fail_history.get(t, 0) > 3600:
                    msg = f"🛡️ <b>[{t}] V19.1 하이브리드 덫 기습 실패 (1시간 쿨타임 진입)</b>\n"
                    msg += f"📉 하이브리드 타격선(${target_buy_price:.2f})에 3회 덫을 놓았으나 전량 체결되지 않았습니다.\n"
                    msg += f"🦇 스나이퍼는 1시간 동안 숨을 죽이며, 일반 방어 매수(LOC) 주문을 호가창에 원상 복구합니다."
                    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                    fail_history[t] = now_ts
                
                ma_5day = await asyncio.to_thread(broker.get_5day_ma, t)
                plan = strategy.get_plan(t, curr_p, avg_price, qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t], force_turbo_off=force_turbo_off)
                
                for o in plan.get('core_orders', []) + plan.get('bonus_orders', []):
                    if o['side'] == 'BUY':
                        broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                        await asyncio.sleep(0.2)
                        
                continue

            target_pct_val = cfg.get_target_profit(t)
            target_price = math.ceil(avg_price * (1 + target_pct_val / 100.0) * 100) / 100.0
            
            split = cfg.get_split_count(t)
            t_val, _ = cfg.get_absolute_t_val(t, qty, avg_price)
            
            depreciation_factor = 2.0 / split if split > 0 else 0.1
            star_ratio = (target_pct_val / 100.0) - ((target_pct_val / 100.0) * depreciation_factor * t_val)
            star_price = math.ceil(avg_price * (1 + star_ratio) * 100) / 100.0
            
            if curr_p >= target_price:
                await asyncio.to_thread(broker.cancel_all_orders_safe, t)
                await asyncio.sleep(1.0)
                
                hunt_success = False
                
                for attempt in range(3):
                    bid_price = await asyncio.to_thread(broker.get_bid_price, t)
                    
                    if bid_price > 0 and bid_price >= target_price:
                        res = broker.send_order(t, "SELL", qty, bid_price, "LIMIT")
                        if res.get('rt_cd') == '0':
                            await asyncio.sleep(5.0)
                            unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                            sell_unfilled = [o for o in unfilled if o.get('sll_buy_dvsn_cd') == '01']
                            
                            if not sell_unfilled:
                                cfg.set_lock(t, "SNIPER")
                                
                                today_est_str = datetime.datetime.now(est).strftime('%Y%m%d')
                                execs = await asyncio.to_thread(broker.get_execution_history, t, today_est_str, today_est_str)
                                
                                actual_sell_price = bid_price 
                                if execs:
                                    computed_price = get_actual_execution_price(execs, qty, '01')
                                    if computed_price > 0: actual_sell_price = computed_price

                                msg = f"🔥 <b>[{t}] 스나이퍼 잭팟 터짐! (목표가 돌파)</b>\n"
                                msg += f"🎯 실시간 매수 1호가(${bid_price:.2f})가 목표가(${target_price:.2f})를 돌파하여 <b>실제 단가 ${actual_sell_price:.2f}에 전량 강제 익절</b> 처리했습니다.\n"
                                msg += "🔫 당일 스나이퍼 활동을 완전 종료합니다."
                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                hunt_success = True
                                break
                            else:
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, side="SELL")
                                await asyncio.sleep(1.0)
                                
                    await asyncio.sleep(1.5)
                    
                if hunt_success:
                    continue
                    
                now_ts = time.time()
                fail_history_j = app_data.setdefault('sniper_j_fail_ts', {})
                if now_ts - fail_history_j.get(t, 0) > 3600:
                    msg = f"🛡️ <b>[{t}] 스나이퍼 잭팟 기습 실패 (방어선 복구)</b>\n"
                    msg += f"🎯 3회에 걸쳐 전량 익절을 시도했으나 체결되지 않았습니다.\n"
                    msg += f"🦇 취소했던 원래의 방어 주문을 다시 호가창에 장전합니다."
                    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                    fail_history_j[t] = now_ts
                
                ma_5day = await asyncio.to_thread(broker.get_5day_ma, t)
                plan = strategy.get_plan(t, curr_p, avg_price, qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t], force_turbo_off=force_turbo_off)
                for o in plan.get('core_orders', []) + plan.get('bonus_orders', []):
                    broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                    await asyncio.sleep(0.2)
                    
                continue
            
            is_first_half = t_val < (split / 2)
            trigger_price = star_price if is_first_half else math.ceil(avg_price * 1.0025 * 100) / 100.0
            
            if curr_p >= trigger_price:
                unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                target_odno = None
                
                for o in unfilled:
                    if o.get('sll_buy_dvsn_cd') == '01': 
                        target_odno = o.get('odno')
                        break
                        
                if target_odno:
                    await asyncio.to_thread(broker.cancel_order, t, target_odno)
                    await asyncio.sleep(1.0) 
                    
                    q_qty = math.ceil(qty / 4)
                    hunt_success = False
                    
                    for attempt in range(3):
                        bid_price = await asyncio.to_thread(broker.get_bid_price, t)
                        
                        if bid_price > 0 and bid_price >= trigger_price:
                            res = broker.send_order(t, "SELL", q_qty, bid_price, "LIMIT")
                            if res.get('rt_cd') == '0':
                                await asyncio.sleep(5.0)
                                unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                sell_unfilled = [o for o in unfilled_check if o.get('sll_buy_dvsn_cd') == '01']
                                
                                if not sell_unfilled:
                                    cfg.set_lock(t, "SNIPER")
                                    phase = "전반전(별값 돌파)" if is_first_half else "후반전(본전+수수료 돌파)"
                                    
                                    today_est_str = datetime.datetime.now(est).strftime('%Y%m%d')
                                    execs = await asyncio.to_thread(broker.get_execution_history, t, today_est_str, today_est_str)
                                    
                                    actual_sell_price = bid_price 
                                    if execs:
                                        computed_price = get_actual_execution_price(execs, q_qty, '01')
                                        if computed_price > 0: actual_sell_price = computed_price

                                    msg = f"🔫 <b>[{t}] V17 시크릿 쿼터 익절 발동! ({phase})</b>\n"
                                    msg += f"🎯 실시간 매수 1호가: ${bid_price:.2f} (트리거: ${trigger_price:.2f})\n"
                                    msg += f"🛡️ 기존 방어선을 해제하고 {q_qty}주를 <b>최적의 단가 ${actual_sell_price:.2f}에 즉시 낚아챘습니다!</b>\n"
                                    
                                    await asyncio.to_thread(broker.cancel_all_orders_safe, t, side="BUY")
                                    await asyncio.sleep(1.0)
                                    
                                    ma_5day = await asyncio.to_thread(broker.get_5day_ma, t)
                                    plan = strategy.get_plan(t, curr_p, avg_price, qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t], force_turbo_off=force_turbo_off)
                                    
                                    smart_cores = plan.get('smart_core_orders', [])
                                    smart_bonus = plan.get('smart_bonus_orders', [])
                                    
                                    if len(smart_cores) == 0:
                                        msg += "\n🛑 <b>[스마트 밸런싱 발동]</b>\n└ 전반전 종가 관망 모드로 전환 (오늘 추가 매수 안 함)"
                                    else:
                                        msg += "\n🦇 <b>[스마트 방어 매수 장전] (플랜 B 전환)</b>\n"
                                        for o in smart_cores:
                                            buy_res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                                            err_msg = buy_res.get('msg1')
                                            msg += f"└ {o['desc']} {o['qty']}주: {'✅' if buy_res.get('rt_cd') == '0' else f'❌({err_msg})'}\n"
                                            await asyncio.sleep(0.2)
                                        for o in smart_bonus:
                                            buy_res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                                            msg += f"└ {o['desc']} {o['qty']}주: {'✅' if buy_res.get('rt_cd') == '0' else '❌'}\n"
                                            await asyncio.sleep(0.2)
                                    
                                    msg += "\n🔒 당일 스나이퍼 감시를 안전하게 종료합니다."
                                    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                    hunt_success = True
                                    break
                                else:
                                    await asyncio.to_thread(broker.cancel_all_orders_safe, t, side="SELL")
                                    await asyncio.sleep(1.0)
                                    
                        await asyncio.sleep(1.5)
                        
                    if hunt_success:
                        continue
                        
                    now_ts = time.time()
                    fail_history_q = app_data.setdefault('sniper_q_fail_ts', {})
                    if now_ts - fail_history_q.get(t, 0) > 3600:
                        msg = f"🛡️ <b>[{t}] 스나이퍼 쿼터 기습 실패 (방어선 복구)</b>\n"
                        msg += f"🎯 3회에 걸쳐 쿼터 익절을 시도했으나 체결되지 않았습니다.\n"
                        msg += f"🦇 취소했던 원래의 방어 주문을 다시 호가창에 장전합니다."
                        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                        fail_history_q[t] = now_ts
                    
                    ma_5day = await asyncio.to_thread(broker.get_5day_ma, t)
                    plan = strategy.get_plan(t, curr_p, avg_price, qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t], force_turbo_off=force_turbo_off)
                    for o in plan.get('core_orders', []) + plan.get('bonus_orders', []):
                        broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                        await asyncio.sleep(0.2)
                        
                    continue

async def scheduled_regular_trade(context):
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.datetime.now(kst)
    target_hour, _ = get_target_hour()
    
    if now.hour != target_hour or now.minute != 30: return
    if not is_market_open(): return
    
    chat_id = context.job.chat_id
    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    
    latest_version = cfg.get_latest_version()
    await context.bot.send_message(chat_id=chat_id, text=f"🌃 <b>[{target_hour}:30] 다이내믹 스노우볼 {latest_version} 정규장 주문을 준비합니다.</b>", parse_mode='HTML')
    
    async with tx_lock:
        cash, holdings = broker.get_account_balance()
        if holdings is None:
            await context.bot.send_message(chat_id=chat_id, text="❌ 계좌 정보를 불러오지 못해 정규장 주문을 취소합니다.", parse_mode='HTML')
            return

        sorted_tickers, allocated_cash, force_turbo_off = get_budget_allocation(cash, cfg.get_active_tickers(), cfg)
        
        plans = {}
        msgs = {t: "" for t in sorted_tickers}
        all_success = {t: True for t in sorted_tickers}

        for t in sorted_tickers:
            if cfg.check_lock(t, "REG"): continue
            h = holdings.get(t, {'qty': 0, 'avg': 0})
            
            curr_p = await asyncio.to_thread(broker.get_current_price, t)
            prev_c = await asyncio.to_thread(broker.get_previous_close, t)
            ma_5day = await asyncio.to_thread(broker.get_5day_ma, t)
            
            plan = strategy.get_plan(t, curr_p, float(h['avg']), int(h['qty']), prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t], force_turbo_off=force_turbo_off)
            plans[t] = plan
            
            if plan['orders']:
                is_rev = plan.get('is_reverse', False)
                title = f"🔄 <b>[{t}] 리버스 주문 실행 (교차 전송)</b>\n" if is_rev else f"💎 <b>[{t}] 정규장 주문 실행 (교차 전송)</b>\n"
                msgs[t] += title

        for t in sorted_tickers:
            if t not in plans or not plans[t]['orders']: continue
            for o in plans[t].get('core_orders', []):
                res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                is_success = res.get('rt_cd') == '0'
                if not is_success: all_success[t] = False
                err_msg = res.get('msg1')
                msgs[t] += f"└ 1차 필수: {o['desc']} {o['qty']}주: {'✅' if is_success else f'❌({err_msg})'}\n"
                await asyncio.sleep(0.2) 

        for t in sorted_tickers:
            if t not in plans or not plans[t]['orders']: continue
            for o in plans[t].get('bonus_orders', []):
                res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                is_success = res.get('rt_cd') == '0'
                msgs[t] += f"└ 2차 보너스: {o['desc']} {o['qty']}주: {'✅' if is_success else '❌(잔금패스)'}\n"
                await asyncio.sleep(0.2) 

        for t in sorted_tickers:
            if t not in plans or not plans[t]['orders']: continue
            
            if all_success[t] and len(plans[t].get('core_orders', [])) > 0:
                cfg.set_lock(t, "REG")
                msgs[t] += "\n🔒 <b>필수 주문 정상 전송 완료 (잠금 설정됨)</b>"
            elif not all_success[t]:
                msgs[t] += "\n⚠️ <b>일부 필수 주문 실패 (매매 잠금 보류 - 다음 스케줄 재시도)</b>"
            else:
                cfg.set_lock(t, "REG")
                msgs[t] += "\n🔒 <b>보너스 줍줍 주문만 전송 완료 (잠금 설정됨)</b>"
                
            await context.bot.send_message(chat_id=chat_id, text=msgs[t], parse_mode='HTML')

async def scheduled_auto_sync_summer(context):
    if not is_dst_active(): return 
    await run_auto_sync(context, "08:30")

async def scheduled_auto_sync_winter(context):
    if is_dst_active(): return 
    await run_auto_sync(context, "09:30")

async def run_auto_sync(context, time_str):
    chat_id = context.job.chat_id
    bot = context.job.data['bot']
    status_msg = await context.bot.send_message(chat_id=chat_id, text=f"📝 <b>[{time_str}] 장부 자동 동기화(무결성 검증)를 시작합니다.</b>", parse_mode='HTML')
    
    success_tickers = []
    for t in context.job.data['cfg'].get_active_tickers():
        res = await bot.process_auto_sync(t, chat_id, context, silent_ledger=True)
        if res == "SUCCESS":
            success_tickers.append(t)
            
    if success_tickers:
        await bot._display_ledger(success_tickers[0], chat_id, context, message_obj=status_msg)
    else:
        await status_msg.edit_text(f"📝 <b>[{time_str}] 장부 동기화 완료</b> (표시할 진행 중인 장부가 없습니다)", parse_mode='HTML')

def main():
    TARGET_HOUR, season_msg = get_target_hour()
    
    cfg = ConfigManager()
    latest_version = cfg.get_latest_version() 
    
    print("=" * 50)
    print(f"🚀 방치형 자동매매 {latest_version} (방탄 아키텍처 적용 완료)")
    print(f"📅 날짜 정보: {season_msg}")
    print(f"⏰ 자동 동기화: 08:30(여름) / 09:30(겨울) 자동 변경")
    print("=" * 50)
    
    if ADMIN_CHAT_ID: cfg.set_chat_id(ADMIN_CHAT_ID)
    broker = KoreaInvestmentBroker(APP_KEY, APP_SECRET, CANO, ACNT_PRDT_CD)
    strategy = InfiniteStrategy(cfg)
    tx_lock = asyncio.Lock()
    bot = TelegramController(cfg, broker, strategy, tx_lock)
    
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    for cmd, handler in [
        ("start", bot.cmd_start), 
        ("record", bot.cmd_record), 
        ("history", bot.cmd_history), 
        ("sync", bot.cmd_sync), 
        ("settlement", bot.cmd_settlement), 
        ("seed", bot.cmd_seed), 
        ("ticker", bot.cmd_ticker), 
        ("mode", bot.cmd_mode), 
        ("reset", bot.cmd_reset), 
        ("version", bot.cmd_version),
        ("v17", bot.cmd_v17),
        ("v4", bot.cmd_v4)
    ]:
        app.add_handler(CommandHandler(cmd, handler))
    app.add_handler(CallbackQueryHandler(bot.handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.handle_message))
    
    if cfg.get_chat_id():
        jq = app.job_queue
        app_data = {'cfg': cfg, 'broker': broker, 'strategy': strategy, 'bot': bot, 'tx_lock': tx_lock}
        kst = pytz.timezone('Asia/Seoul')
        
        for tt in [datetime.time(7,0,tzinfo=kst), datetime.time(11,0,tzinfo=kst), datetime.time(16,30,tzinfo=kst), datetime.time(22,0,tzinfo=kst)]:
            jq.run_daily(scheduled_token_check, time=tt, days=tuple(range(7)), data=app_data)
        
        jq.run_daily(scheduled_auto_sync_summer, time=datetime.time(8, 30, tzinfo=kst), days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        jq.run_daily(scheduled_auto_sync_winter, time=datetime.time(9, 30, tzinfo=kst), days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        
        for hour in [17, 18]:
            jq.run_daily(scheduled_force_reset, time=datetime.time(hour, 0, tzinfo=kst), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)
            jq.run_daily(scheduled_regular_trade, time=datetime.time(hour, 30, tzinfo=kst), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)

        jq.run_repeating(scheduled_premarket_monitor, interval=60, chat_id=cfg.get_chat_id(), data=app_data)
        jq.run_repeating(scheduled_sniper_monitor, interval=60, chat_id=cfg.get_chat_id(), data=app_data)
        
    app.run_polling()

if __name__ == "__main__":
    main()

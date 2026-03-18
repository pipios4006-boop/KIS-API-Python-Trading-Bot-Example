import os
import logging
import datetime
import pytz
import time
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
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID")) if os.getenv("ADMIN_CHAT_ID") else None
APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
CANO = os.getenv("CANO")
ACNT_PRDT_CD = os.getenv("ACNT_PRDT_CD", "01")

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

def get_budget_allocation(cash, tickers, cfg):
    sorted_tickers = sorted(tickers, key=lambda x: 0 if x == "SOXL" else (1 if x == "TQQQ" else 2))
    total_req, portions = 0, {}
    
    for tx in sorted_tickers:
        split = cfg.get_split_count(tx)
        portion = cfg.get_seed(tx) / split if split > 0 else 0
        portions[tx] = portion
        total_req += portion
        
    force_turbo_off = cash < total_req
    rem_cash, allocated = cash, {}
    
    for tx in sorted_tickers:
        if rem_cash >= portions[tx]:
            allocated[tx] = rem_cash 
            rem_cash -= portions[tx] 
        else: allocated[tx] = 0 
            
    return sorted_tickers, allocated, force_turbo_off

async def scheduled_token_check(context):
    context.job.data['broker']._get_access_token(force=True)

async def scheduled_force_reset(context):
    if not is_market_open(): return
    app_data = context.job.data
    app_data['cfg'].reset_locks()
    await context.bot.send_message(chat_id=context.job.chat_id, text=f"🔓 <b>[{app_data.get('target_hour')}:00] 시스템 초기화 완료 (매매 잠금 해제)</b>", parse_mode='HTML')

async def scheduled_premarket_monitor(context):
    if not is_market_open(): return
    app_data = context.job.data
    
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.datetime.now(kst)
    target_hour = app_data.get('target_hour', 18)
    
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
                    msg += f"\n└ {o['desc']}: {'✅' if res.get('rt_cd') == '0' else f'❌({res.get('msg1')})'}"
                    await asyncio.sleep(0.2) # 🚀 [V18.0] API Throttle 방어
                await context.bot.send_message(chat_id=context.job.chat_id, text=msg, parse_mode='HTML')

async def scheduled_regular_trade(context):
    if not is_market_open(): return
    chat_id = context.job.chat_id
    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    
    latest_version = cfg.get_latest_version()
    await context.bot.send_message(chat_id=chat_id, text=f"🌃 <b>[{app_data.get('target_hour')}:30] 다이내믹 스노우볼 {latest_version} 정규장 주문을 준비합니다.</b>", parse_mode='HTML')
    
    async with tx_lock:
        cash, holdings = broker.get_account_balance()
        if holdings is None:
            await context.bot.send_message(chat_id=chat_id, text="❌ 계좌 정보를 불러오지 못해 정규장 주문을 취소합니다.", parse_mode='HTML')
            return

        sorted_tickers, allocated_cash, force_turbo_off = get_budget_allocation(cash, cfg.get_active_tickers(), cfg)
        
        plans = {}
        msgs = {t: "" for t in sorted_tickers}
        all_success = {t: True for t in sorted_tickers}

        # 0. 전략 일괄 수립 (Pre-fetch Plans)
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

        # 1. 제 1라운드: 생존 필수 매수 (Core Buy) 교차 전송
        for t in sorted_tickers:
            if t not in plans or not plans[t]['orders']: continue
            for o in plans[t].get('core_orders', []):
                res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                is_success = res.get('rt_cd') == '0'
                if not is_success: all_success[t] = False
                msgs[t] += f"└ 1차 필수: {o['desc']} {o['qty']}주: {'✅' if is_success else f'❌({res.get('msg1')})'}\n"
                await asyncio.sleep(0.2) # 🚀 [V18.0] API Throttle 방어

        # 2. 제 2라운드: 보너스 줍줍 (JubJub Extra) 교차 전송
        for t in sorted_tickers:
            if t not in plans or not plans[t]['orders']: continue
            for o in plans[t].get('bonus_orders', []):
                res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                is_success = res.get('rt_cd') == '0'
                msgs[t] += f"└ 2차 보너스: {o['desc']} {o['qty']}주: {'✅' if is_success else '❌(잔금패스)'}\n"
                await asyncio.sleep(0.2) # 🚀 [V18.0] API Throttle 방어

        # 3. 매매 결과 판정 및 락다운
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
    print(f"⏰ 정규장 주문: {TARGET_HOUR}:30")
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
        app_data = {'cfg': cfg, 'broker': broker, 'strategy': strategy, 'target_hour': TARGET_HOUR, 'bot': bot, 'tx_lock': tx_lock}
        kst = pytz.timezone('Asia/Seoul')
        
        for tt in [datetime.time(7,0,tzinfo=kst), datetime.time(11,0,tzinfo=kst), datetime.time(16,30,tzinfo=kst), datetime.time(22,0,tzinfo=kst)]:
            jq.run_daily(scheduled_token_check, time=tt, days=tuple(range(7)), data=app_data)
        
        jq.run_daily(scheduled_auto_sync_summer, time=datetime.time(8, 30, tzinfo=kst), days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        jq.run_daily(scheduled_auto_sync_winter, time=datetime.time(9, 30, tzinfo=kst), days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        
        jq.run_daily(scheduled_force_reset, time=datetime.time(TARGET_HOUR, 0, tzinfo=kst), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)
        jq.run_repeating(scheduled_premarket_monitor, interval=60, chat_id=cfg.get_chat_id(), data=app_data)
        jq.run_daily(scheduled_regular_trade, time=datetime.time(TARGET_HOUR, 30, tzinfo=kst), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)
        
    app.run_polling()

if __name__ == "__main__":
    main()

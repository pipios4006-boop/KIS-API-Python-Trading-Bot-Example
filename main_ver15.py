#최초 개발자: 승승장군
#V14.1 리버스모드 하이브리드 업데이트: V14 소진 시 자동 리버스 엔진 적용
# [V14.2] 데이터 및 로그 디렉토리 분리 관리 및 버전 정보 누적 관리 기능 추가
# [V16.0] 시스템 최적화: 30개 프리마켓 감시 스케줄러 단일화
# [V16.1] 필수 진입점인 /record 명령어 부활

import os
import logging
import datetime
import pytz
import time
import pandas_market_calendars as mcal
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from dotenv import load_dotenv

from config import ConfigManager
from broker import KoreaInvestmentBroker
from strategy import InfiniteStrategy
from telegram_bot import TelegramController

# [V14.2] 구동 시 data 및 logs 디렉토리 자동 생성 (휴먼 에러 방지)
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

# [V14.2] 로그 설정 변경: 콘솔 출력뿐만 아니라 logs/ 폴더 하위에 날짜별 파일로 누적 저장
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

# 전역 변수로 NYSE 캘린더 객체를 한 번만 생성하여 재사용합니다. (리소스 최적화)
NYSE_CALENDAR = mcal.get_calendar('NYSE')

def is_market_open():
    est = pytz.timezone('US/Eastern')
    today = datetime.datetime.now(est).date()
    # 전역 캘린더 객체 사용 및 스케줄 조회 최적화
    return not NYSE_CALENDAR.schedule(start_date=today, end_date=today).empty

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

# 🚀 [V16.0] 메모리 최적화: 30개로 쪼개졌던 프리마켓 감시기를 1개의 스마트 반복 타이머로 통합
async def scheduled_premarket_monitor(context):
    if not is_market_open(): return
    app_data = context.job.data
    
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.datetime.now(kst)
    target_hour = app_data.get('target_hour', 18)
    
    # 설정된 타겟 시간(17시 또는 18시) 정각부터 29분 사이의 프리마켓 시간대에만 작동하도록 방어
    if not (now.hour == target_hour and 0 <= now.minute < 30):
        return

    cfg, broker, strategy = app_data['cfg'], app_data['broker'], app_data['strategy']

    _, holdings = broker.get_account_balance()
    if holdings is None: return 

    for t in cfg.get_active_tickers():
        h = holdings.get(t, {'qty': 0, 'avg': 0})
        if int(h['qty']) == 0: continue 

        curr_p = broker.get_current_price(t)
        plan = strategy.get_plan(t, curr_p, float(h['avg']), int(h['qty']), broker.get_previous_close(t), market_type="PRE_CHECK")
        
        if plan['orders']:
            msg = f"🌅 <b>[{t}] 대박! 프리마켓 목표 달성 🎉</b>\n⚡ 전량 익절 주문을 실행합니다!"
            broker.cancel_all_orders_safe(t)
            for o in plan['orders']:
                res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                msg += f"\n└ {o['desc']}: {'✅' if res.get('rt_cd') == '0' else f'❌({res.get('msg1')})'}"
            await context.bot.send_message(chat_id=context.job.chat_id, text=msg, parse_mode='HTML')

async def scheduled_regular_trade(context):
    if not is_market_open(): return
    chat_id = context.job.chat_id
    app_data = context.job.data
    cfg, broker, strategy = app_data['cfg'], app_data['broker'], app_data['strategy']
    
    latest_version = cfg.get_latest_version()
    await context.bot.send_message(chat_id=chat_id, text=f"🌃 <b>[{app_data.get('target_hour')}:30] 다이내믹 스노우볼 {latest_version} 정규장 주문을 준비합니다.</b>", parse_mode='HTML')
    
    cash, holdings = broker.get_account_balance()
    if holdings is None:
        await context.bot.send_message(chat_id=chat_id, text="❌ API 통신 오류로 계좌 정보를 불러오지 못해 정규장 주문을 취소합니다.", parse_mode='HTML')
        return

    sorted_tickers, allocated_cash, force_turbo_off = get_budget_allocation(cash, cfg.get_active_tickers(), cfg)
    
    for t in sorted_tickers:
        if cfg.check_lock(t, "REG"): continue
        h = holdings.get(t, {'qty': 0, 'avg': 0})
        
        ma_5day = broker.get_5day_ma(t)
        plan = strategy.get_plan(t, broker.get_current_price(t), float(h['avg']), int(h['qty']), broker.get_previous_close(t), ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t], force_turbo_off=force_turbo_off)
        
        if plan['orders']:
            is_rev = plan.get('is_reverse', False)
            title = f"🔄 <b>[{t}] V14 리버스 주문 실행</b>\n" if is_rev else f"💎 <b>[{t}] 주문 실행</b>\n"
            msg, success_count = title, 0
            
            for o in plan['orders']:
                res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                is_success = res.get('rt_cd') == '0'
                if is_success: success_count += 1
                msg += f"└ {o['desc']} {o['qty']}주: {'✅' if is_success else f'❌({res.get('msg1')})'}\n"
            
            if success_count > 0:
                cfg.set_lock(t, "REG")
                msg += "\n🔒 <b>주문 전송 완료 (매매 잠금 설정됨)</b>"
                
                rev_state = cfg.get_reverse_state(t)
                if rev_state["is_active"]:
                    cfg.set_reverse_state(t, True, rev_state["day_count"] + 1)
                    
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

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
    print(f"🚀 방치형 자동매매 {latest_version} (시스템 전면 리팩토링 최적화)")
    print(f"📅 날짜 정보: {season_msg}")
    print(f"⏰ 자동 동기화: 08:30(여름) / 09:30(겨울) 자동 변경")
    print(f"⏰ 정규장 주문: {TARGET_HOUR}:30")
    print("=" * 50)
    
    if ADMIN_CHAT_ID: cfg.set_chat_id(ADMIN_CHAT_ID)
    broker = KoreaInvestmentBroker(APP_KEY, APP_SECRET, CANO, ACNT_PRDT_CD)
    strategy = InfiniteStrategy(cfg)
    bot = TelegramController(cfg, broker, strategy)
    
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # 🔥 [V16.1] 긴급 부활한 record 명령어를 다시 라우터에 등록
    for cmd, handler in [("start", bot.cmd_start), ("record", bot.cmd_record), ("history", bot.cmd_history), ("sync", bot.cmd_sync), ("settlement", bot.cmd_settlement), ("seed", bot.cmd_seed), ("ticker", bot.cmd_ticker), ("mode", bot.cmd_mode), ("reset", bot.cmd_reset), ("version", bot.cmd_version)]:
        app.add_handler(CommandHandler(cmd, handler))
    app.add_handler(CallbackQueryHandler(bot.handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.handle_message))
    
    if cfg.get_chat_id():
        jq = app.job_queue
        app_data = {'cfg': cfg, 'broker': broker, 'strategy': strategy, 'target_hour': TARGET_HOUR, 'bot': bot}
        kst = pytz.timezone('Asia/Seoul')
        
        for tt in [datetime.time(7,0,tzinfo=kst), datetime.time(11,0,tzinfo=kst), datetime.time(16,30,tzinfo=kst), datetime.time(22,0,tzinfo=kst)]:
            jq.run_daily(scheduled_token_check, time=tt, days=tuple(range(7)), data=app_data)
        
        jq.run_daily(scheduled_auto_sync_summer, time=datetime.time(8, 30, tzinfo=kst), days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        jq.run_daily(scheduled_auto_sync_winter, time=datetime.time(9, 30, tzinfo=kst), days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        
        jq.run_daily(scheduled_force_reset, time=datetime.time(TARGET_HOUR, 0, tzinfo=kst), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)
        
        # 🚀 [V16.0] 메모리 최적화: 프리마켓 감시 로직을 60초 간격 단 1개의 스마트 타이머로 교체!
        jq.run_repeating(scheduled_premarket_monitor, interval=60, chat_id=cfg.get_chat_id(), data=app_data)
            
        jq.run_daily(scheduled_regular_trade, time=datetime.time(TARGET_HOUR, 30, tzinfo=kst), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)
        
    app.run_polling()

if __name__ == "__main__":
    main()

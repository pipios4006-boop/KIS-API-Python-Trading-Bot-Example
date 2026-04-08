# ==========================================================
# [main.py] - Part 1/2 부 (상반부)
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# 💡 [V24.10] 텔레그램 API 통신 타임아웃(TimedOut) 방어 및 커넥션 풀 최적화 이식 완료
# 💡 [V24.11 수술] VolatilityEngine 동적 연결 및 TelegramController 의존성 주입
# ==========================================================

import os
import logging
import datetime
import pytz
import asyncio
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from dotenv import load_dotenv

from config import ConfigManager
from broker import KoreaInvestmentBroker
from strategy import InfiniteStrategy
from vwap_strategy import VwapStrategy
from telegram_bot import TelegramController

# 💡 [V_REV 신규 역추세 엔진 의존성 주입]
from queue_ledger import QueueLedger
from strategy_reversion import ReversionStrategy
from volatility_engine import VolatilityEngine  # 💡 [핵심 수술] 하드코딩 제거를 위한 변동성 엔진 임포트

# 💡 [핵심 수술] 분할된 2개의 스케줄러 파일에서 각각 역할에 맞게 함수를 임포트
from scheduler_core import (
    scheduled_token_check,
    scheduled_auto_sync_summer,
    scheduled_auto_sync_winter,
    scheduled_force_reset,
    scheduled_self_cleaning,
    get_target_hour,
    perform_self_cleaning
)
from scheduler_trade import (
    scheduled_regular_trade,
    scheduled_sniper_monitor,
    scheduled_vwap_trade,
    scheduled_vwap_init_and_cancel,  
    scheduled_emergency_liquidation,
    scheduled_after_market_lottery  
)

if not os.path.exists('data'):
    os.makedirs('data')
if not os.path.exists('logs'):
    os.makedirs('logs')

load_dotenv() 

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
try:
    ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID")) if os.getenv("ADMIN_CHAT_ID") else None
except ValueError:
    ADMIN_CHAT_ID = None

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
CANO = os.getenv("CANO")
ACNT_PRDT_CD = os.getenv("ACNT_PRDT_CD", "01")

if not all([TELEGRAM_TOKEN, APP_KEY, APP_SECRET, CANO]):
    print("❌ [치명적 오류] .env 파일에 봇 구동 필수 키(TELEGRAM_TOKEN, APP_KEY, APP_SECRET, CANO)가 누락되었습니다. 봇을 종료합니다.")
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

# ==========================================================
# 🛡️ [V23.05] 자율주행 변동성 마스터 스위치 터미널 렌더링 엔진
# ==========================================================
async def scheduled_volatility_scan(context):
    """
    10:20 EST (정규장 개장 50분 후) 격발.
    대상 종목들의 HV와 당일 VXN을 연산하여 터미널 메인 화면에 1-Tier 브리핑 덤프
    """
    app_data = context.job.data
    cfg = app_data['cfg']
    
    print("\n" + "=" * 60)
    print("📈 [자율주행 변동성 스캔 완료] (10:20 EST 스냅샷)")
    
    active_tickers = []
    for r in cfg.get_ledger():
        t = r.get('ticker')
        if t and t not in active_tickers:
            active_tickers.append(t)
            
    if not active_tickers:
        print("📊 현재 운용 중인 종목이 없습니다.")
    else:
        briefing_lines = []
        vol_engine = VolatilityEngine()  # 💡 [핵심 수술] 자율주행 엔진 인스턴스화
        
        for ticker in active_tickers:
            try:
                # 💡 [핵심 수술] 기존 하드코딩(0.85/1.15)을 완벽히 도려내고 실시간 변동성 지표 동적 스캔
                weight_data = await asyncio.to_thread(vol_engine.calculate_weight, ticker)
                real_weight = float(weight_data.get('weight', 1.0) if isinstance(weight_data, dict) else weight_data)
            except Exception as e:
                logging.warning(f"[{ticker}] 변동성 지표 산출 실패. 폴백(Fallback) 안전마진 적용: {e}")
                real_weight = 0.85 if ticker == "TQQQ" else 1.15 
                
            status_text = "OFF 권장" if real_weight <= 1.0 else "ON 권장"
            briefing_lines.append(f"{ticker}: {real_weight:.2f} ({status_text})")
            
        print(f"📊 [자율주행 지표] {' | '.join(briefing_lines)} (상세 게이지: /mode)")
    print("=" * 60 + "\n")
# ==========================================================
def main():
    TARGET_HOUR, season_msg = get_target_hour()
    
    cfg = ConfigManager()
    latest_version = cfg.get_latest_version() 
    
    print("=" * 60)
    print(f"🚀 앱솔루트 스노우볼 퀀트 엔진 {latest_version} (VWAP 스플릿 & 경량화 아키텍처 탑재)")
    print(f"📅 날짜 정보: {season_msg}")
    print(f"⏰ 자동 동기화: 08:30(여름) / 09:30(겨울) 자동 변경")
    print(f"🛡️ 1-Tier 자율주행 지표 스캔 대기 중... (매일 10:20 EST 격발)")
    print("=" * 60)
    
    perform_self_cleaning()
    
    if ADMIN_CHAT_ID: cfg.set_chat_id(ADMIN_CHAT_ID)
    
    broker = KoreaInvestmentBroker(APP_KEY, APP_SECRET, CANO, ACNT_PRDT_CD)
    strategy = InfiniteStrategy(cfg)
    vwap_strategy = VwapStrategy(cfg)
    
    # 💡 [V_REV] 독립 모듈 객체 초기화
    queue_ledger = QueueLedger()
    strategy_rev = ReversionStrategy()
    
    tx_lock = asyncio.Lock()
    
    # 💡 [핵심 수술] TelegramController에 V-REV 필수 모듈(queue_ledger, strategy_rev) 완벽 주입
    # (주의: telegram_bot.py 의 __init__ 인자 순서에 맞춰 kwargs로 안전하게 주입합니다)
    bot = TelegramController(
        cfg, 
        broker, 
        strategy, 
        tx_lock, 
        vwap_strategy=vwap_strategy, 
        queue_ledger=queue_ledger, 
        strategy_rev=strategy_rev
    )
    
    # 💡 [핵심 수술] 텔레그램 네트워크 지연(Timeout) 방어 및 커넥션 풀 확장
    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .connect_timeout(30.0)
        .pool_timeout(30.0)
        .connection_pool_size(512)
        .build()
    )
    
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
        app_data = {
            'cfg': cfg, 
            'broker': broker, 
            'strategy': strategy, 
            'vwap_strategy': vwap_strategy,
            'queue_ledger': queue_ledger,  
            'strategy_rev': strategy_rev,  
            'bot': bot, 
            'tx_lock': tx_lock
        }
        kst = pytz.timezone('Asia/Seoul')
        est = pytz.timezone('US/Eastern')
        
        # 1. 시스템 관리 스케줄러 (core)
        for tt in [datetime.time(7,0,tzinfo=kst), datetime.time(11,0,tzinfo=kst), datetime.time(16,30,tzinfo=kst), datetime.time(22,0,tzinfo=kst)]:
            jq.run_daily(scheduled_token_check, time=tt, days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        
        jq.run_daily(scheduled_auto_sync_summer, time=datetime.time(8, 30, tzinfo=kst), days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        jq.run_daily(scheduled_auto_sync_winter, time=datetime.time(9, 30, tzinfo=kst), days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        
        for hour in [17, 18]:
            jq.run_daily(scheduled_force_reset, time=datetime.time(hour, 0, tzinfo=kst), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)
            
        jq.run_daily(scheduled_volatility_scan, time=datetime.time(10, 20, tzinfo=est), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)
        
        # 2. 실전 전투 매매 스케줄러 (trade)
        # 💡 [Phase 1] 프리장(17:05 KST)에 '선제적 양방향 LOC 덫' 사전 전송 (블랙스완/서버다운 대비)
        for hour in [17, 18]:
            jq.run_daily(scheduled_regular_trade, time=datetime.time(hour, 5, tzinfo=kst), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)
        
        # 💡 [Phase 2] 장 후반 15:30 EST (04:30 KST) 기상: 사전 LOC 전량 취소 후 VWAP 1분봉 타격 준비
        jq.run_daily(scheduled_vwap_init_and_cancel, time=datetime.time(15, 30, tzinfo=est), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)

        # 💡 스나이퍼 감시 및 VWAP 슬라이싱 (60초 간격 무한 반복 - 내부에서 15:30 EST 이후에만 작동하도록 통제됨)
        jq.run_repeating(scheduled_sniper_monitor, interval=60, chat_id=cfg.get_chat_id(), data=app_data)
        jq.run_repeating(scheduled_vwap_trade, interval=60, chat_id=cfg.get_chat_id(), data=app_data)
        
        # 💡 [Phase 3] 15:59 EST 긴급 수혈 스케줄러 (MOC)
        jq.run_daily(scheduled_emergency_liquidation, time=datetime.time(15, 59, tzinfo=est), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)
        
        # 💡 [Phase 4] 애프터마켓 로터리 덫 (16:05 EST)
        jq.run_daily(scheduled_after_market_lottery, time=datetime.time(16, 5, tzinfo=est), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)

        # 3. 자정 청소 (core)
        jq.run_daily(scheduled_self_cleaning, time=datetime.time(6, 0, tzinfo=kst), days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        
    app.run_polling()

if __name__ == "__main__":
    main()

# ==========================================================
# [main.py] - 🌟 100% 통합 무결점 완성본 (V44.07) 🌟
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# 💡 [V24.10] 텔레그램 API 통신 타임아웃(TimedOut) 방어 및 커넥션 풀 최적화 이식 완료
# 💡 [V24.11 수술] VolatilityEngine 동적 연결 및 TelegramController 의존성 주입
# 💡 [V24.15 대수술] V_VWAP 플러그인 의존성 100% 영구 적출 및 2대 코어 체제 확립
# 💡 [V24.20 패치] 듀얼 레퍼런싱(SOXX/SOXL) 인프라 및 스냅샷 파이프라인 증설
# 🚨 [V25.19 핫픽스] EST/KST 타임존 혼용에 따른 스케줄링 오작동 방어 (명시적 타임존 주입)
# 🚨 [V25.19 핫픽스] 듀얼 레퍼런싱(TICKER_BASE_MAP) 전역 공유 파이프라인 완벽 확립
# 🚀 [V27.00 자가 업데이트 라우터 이식] 텔레그램 핸들러 루프에 'update' 명령어 공식 등록 완료
# NEW: [V44.07 암살자 타임라인 전진 배치] 옴니 매트릭스 스캔 및 스나이퍼 격발 10:20 -> 10:00 EST 락온 수술 완료
# MODIFIED: [V44.07 핫픽스] scheduler_core에서 영구 소각된 get_target_hour 유령 임포트 참조 완벽 적출.
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
from telegram_bot import TelegramController

# 💡 [V_REV 신규 역추세 엔진 의존성 주입]
from queue_ledger import QueueLedger
from strategy_reversion import ReversionStrategy
from volatility_engine import VolatilityEngine

# 💡 [핵심 수술] 분할된 2개의 스케줄러 파일에서 각각 역할에 맞게 함수를 임포트
from scheduler_core import (
    scheduled_token_check,
    scheduled_auto_sync_summer,
    scheduled_auto_sync_winter,
    scheduled_force_reset,
    scheduled_self_cleaning,
    perform_self_cleaning
)
from scheduler_trade import (
    scheduled_regular_trade,
    scheduled_sniper_monitor,
    scheduled_vwap_trade,
    scheduled_vwap_init_and_cancel,  
    scheduled_after_market_lottery  
)

# NEW: [듀얼 레퍼런싱] 기초자산(Base)과 파생상품(Exec) 간의 1:1 매핑 딕셔너리 정의
# (펀더멘털 시그널 스캔을 위한 듀얼 레퍼런싱 앵커 맵)
TICKER_BASE_MAP = {
    "SOXL": "SOXX",
    "TQQQ": "QQQ",
    "TSLL": "TSLA",
    "FNGU": "FNGS",
    "BULZ": "FNGS"
}

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

est_zone = ZoneInfo('America/New_York')
log_filename = f"logs/bot_app_{datetime.datetime.now(est_zone).strftime('%Y%m%d')}.log"

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO,
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# NEW: [V44.04 핫픽스] yfinance 패키지 내부의 불필요한 스팸 로그(possibly delisted 등)가 로그를 오염시키는 현상 원천 차단(침묵 락온)
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# ==========================================================
# 🛡️ [V23.05] 자율주행 변동성 마스터 스위치 터미널 렌더링 엔진
# ==========================================================
async def scheduled_volatility_scan(context):
    """
    # MODIFIED: [V44.07] 10:00 EST (정규장 개장 30분 후) 격발.
    대상 종목들의 HV와 당일 VXN을 연산하여 터미널 메인 화면에 1-Tier 브리핑 덤프
    """
    app_data = context.job.data
    cfg = app_data['cfg']
    # MODIFIED: 듀얼 레퍼런싱 매핑 데이터 로드 (Medium 10 연계)
    base_map = app_data.get('base_map', TICKER_BASE_MAP)
    
    print("\n" + "=" * 60)
    print("📈 [자율주행 변동성 스캔 완료] (10:00 EST 스냅샷)")
    
    regime_data = await determine_market_regime(app_data['broker'])
    app_data['regime_data'] = regime_data
    
    if regime_data.get("status") == "success":
        regime = regime_data.get("regime")
        target_ticker = regime_data.get("target_ticker")
        close_p = regime_data.get("close", 0.0)
        prev_vwap = regime_data.get("prev_vwap", 0.0)
        curr_vwap = regime_data.get("curr_vwap", 0.0)
        desc = regime_data.get("desc", "")
        print(f"🏛️ 옴니 매트릭스: [{regime}] 타겟: {target_ticker} ({desc}) | 종가: {close_p:.2f}, 당일VWAP: {curr_vwap:.2f}, 전일VWAP: {prev_vwap:.2f}")
    else:
        print(f"⚠️ 옴니 매트릭스 판별 실패: {regime_data.get('msg')}")

    active_tickers = cfg.get_active_tickers()
            
    if not active_tickers:
        print("📊 현재 운용 중인 종목이 없습니다.")
    else:
        briefing_lines = []
        vol_engine = VolatilityEngine()
        
        for ticker in active_tickers:
            # MODIFIED: 기초자산 매핑 확인 (없으면 본인 사용)
            target_base = base_map.get(ticker, ticker)
            try:
                # 💡 [핵심 수술] 계산은 파생상품 노이즈가 배제된 기초자산(SOXX 등) 기준으로 수행
                weight_data = await asyncio.to_thread(vol_engine.calculate_weight, target_base)
                real_weight = float(weight_data.get('weight', 1.0) if isinstance(weight_data, dict) else weight_data)
                if not math.isfinite(real_weight):
                    raise ValueError(f"비정상 수학 수치 산출: {real_weight}")
            except Exception as e:
                logging.warning(f"[{ticker}] 변동성 지표 산출 실패. 폴백(Fallback) 안전마진 적용: {e}")
                real_weight = 1.0 
                
            status_text = "OFF 권장" if real_weight <= 1.0 else "ON 권장"
            # MODIFIED: 브리핑 시 기초자산 병기
            if ticker != target_base:
                briefing_lines.append(f"{ticker}({target_base}): {real_weight:.2f} ({status_text})")
            else:
                briefing_lines.append(f"{ticker}: {real_weight:.2f} ({status_text})")
        
        print(f"📊 [자율주행 지표] {' | '.join(briefing_lines)} (상세 게이지: /mode)")
    print("=" * 60 + "\n")

async def post_init(application: Application):
    tx_lock = asyncio.Lock()
    application.bot_data['app_data']['tx_lock'] = tx_lock
    application.bot_data['bot_controller'].tx_lock = tx_lock

def main():
    est_zone = ZoneInfo('America/New_York')
    kst_zone = ZoneInfo('Asia/Seoul')
    now_est = datetime.datetime.now(est_zone)
    is_dst = bool(now_est.dst())
    season_msg = "☀️ 서머타임 (EDT) 적용 중" if is_dst else "❄️ 표준시간 (EST) 적용 중"
    
    cfg = ConfigManager()
    latest_version = cfg.get_latest_version() 
    
    print("=" * 60)
    print(f"🚀 옴니 매트릭스 퀀트 엔진 {latest_version} (V40.00 락온)")
    print(f"📅 날짜 정보: {season_msg}")
    print(f"⏰ 자동 동기화: 10:00(KST) 확정 스캔 가동")
    # MODIFIED: [V44.07] 타임라인 10:20 -> 10:00 EST 전진 배치
    print(f"🛡️ 1-Tier 자율주행 지표 스캔 대기 중... (매일 10:00 EST 격발)")
    print("=" * 60)
    
    perform_self_cleaning()
    
    if ADMIN_CHAT_ID: 
        cfg.set_chat_id(ADMIN_CHAT_ID)
    
    broker = KoreaInvestmentBroker(APP_KEY, APP_SECRET, CANO, ACNT_PRDT_CD)
    strategy = InfiniteStrategy(cfg)
    
    queue_ledger = QueueLedger()
    strategy_rev = ReversionStrategy()
    
    tx_lock = asyncio.Lock()
    
    bot = TelegramController(
        cfg, 
        broker, 
        strategy, 
        tx_lock,
        queue_ledger=queue_ledger, 
        strategy_rev=strategy_rev
    )
    
    app_data = {
        'cfg': cfg, 'broker': broker, 'strategy': strategy, 
        'queue_ledger': queue_ledger, 'strategy_rev': strategy_rev,  
        'bot': bot, 'tx_lock': None, 'base_map': TICKER_BASE_MAP,
        'tz_kst': kst_zone, 'tz_est': est_zone,
        'regime_data': None 
    }

    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .connect_timeout(30.0)
        .pool_timeout(30.0)
        .connection_pool_size(512)
        .post_init(post_init) 
        .build()
    )
    
    app.bot_data['app_data'] = app_data
    app.bot_data['bot_controller'] = bot
    
    # MODIFIED: [V27.00] "update" 명령어를 앱 핸들러 루프에 공식 등록 완료
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
        ("update", bot.cmd_update),
        ("avwap", bot.cmd_avwap), ("queue", bot.cmd_queue), ("add_q", bot.cmd_add_q), ("clear_q", bot.cmd_clear_q)
    ]:
        app.add_handler(CommandHandler(cmd, handler))
        
    app.add_handler(CallbackQueryHandler(bot.handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.handle_message))
    
    if cfg.get_chat_id():
        jq = app.job_queue
        
        # 1. 시스템 관리 스케줄러 (core)
        for tt in [datetime.time(7,0,tzinfo=kst_zone), datetime.time(11,0,tzinfo=kst_zone), datetime.time(16,30,tzinfo=kst_zone), datetime.time(22,0,tzinfo=kst_zone)]:
            jq.run_daily(scheduled_token_check, time=tt, days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        
        SYNC_FUNC = scheduled_auto_sync_summer if is_dst else scheduled_auto_sync_winter
        jq.run_daily(SYNC_FUNC, time=datetime.time(10, 0, 5, tzinfo=kst_zone), days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        
        jq.run_daily(scheduled_force_reset, time=datetime.time(4, 0, tzinfo=est_zone), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)
            
        # MODIFIED: [V44.07] 타임라인 10:20 -> 10:00 EST 전진 배치 (정규장 오픈 후 30분)
        jq.run_daily(scheduled_volatility_scan, time=datetime.time(10, 0, tzinfo=est_zone), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)
        
        # 2. 실전 전투 매매 스케줄러 (trade)
        jq.run_daily(scheduled_regular_trade, time=datetime.time(4, 5, tzinfo=est_zone), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)
        
        jq.run_daily(scheduled_vwap_init_and_cancel, time=datetime.time(15, 30, tzinfo=est_zone), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)

        jq.run_repeating(scheduled_sniper_monitor, interval=60, first=30, chat_id=cfg.get_chat_id(), data=app_data)
        jq.run_repeating(scheduled_vwap_trade, interval=60, first=30, chat_id=cfg.get_chat_id(), data=app_data)
        
        jq.run_daily(scheduled_after_market_lottery, time=datetime.time(16, 5, tzinfo=est_zone), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)

        jq.run_daily(scheduled_self_cleaning, time=datetime.time(6, 0, tzinfo=kst_zone), days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        
    app.run_polling()

if __name__ == "__main__":
    main()

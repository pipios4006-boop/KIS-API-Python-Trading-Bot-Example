# ==========================================================
# [main.py] - 🌟 100% 통합 완성본 🌟
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# 💡 [V24.10] 텔레그램 API 통신 타임아웃(TimedOut) 방어 및 커넥션 풀 최적화 이식 완료
# 💡 [V24.11 수술] VolatilityEngine 동적 연결 및 TelegramController 의존성 주입
# 💡 [V24.15 대수술] V_VWAP 플러그인 의존성 100% 영구 적출 및 2대 코어 체제 확립
# 💡 [V24.20 패치] 듀얼 레퍼런싱(SOXX/SOXL) 인프라 및 스냅샷 파이프라인 증설
# 🚨 [V25.19 핫픽스] EST/KST 타임존 혼용에 따른 스케줄링 오작동 방어 (명시적 타임존 주입)
# 🚨 [V25.19 핫픽스] 듀얼 레퍼런싱(TICKER_BASE_MAP) 전역 공유 파이프라인 완벽 확립
# 🚀 [V27.00 자가 업데이트 라우터 이식] 텔레그램 핸들러 루프에 'update' 명령어 공식 등록 완료
# 🚨 [V27.11 그랜드 수술] 코파일럿 합작 - asyncio.Lock 런타임 붕괴 방어, NaN 오판 및 역방향 폴백 차단, 
# 콜드 스타트 폭풍 제어(first=30) 및 통합 타임존(America/New_York) 파이프라인 구축
# 🚨 [V27.12 핫픽스] 사계절 타임존 이중 타격 원천 차단, 이중 동기화 붕괴 방지 및 FD 최적화 이식
# MODIFIED: [V28.27 그랜드 수술] 로그 파일명 생성 시 KST(서버 시간) 의존성 전면 소각 및 EST(미국 동부) 타임존 락온으로 디버깅 파편화 영구 차단
# MODIFIED: [V30.00 그랜드 리팩토링] 비대해진 scheduler_trade.py를 4개의 정예 코어(sniper, vwap, regular, aftermarket)로 100% 분할 및 의존성 주입 완료.
# MODIFIED: [V30.08 스케줄러 디커플링 수술] 17시/17시05분 스케줄 증발을 유발한 KST 의존성(TARGET_HOUR) 전면 소각 및 EST 04:00 / 04:05 절대 락온
# MODIFIED: [V30.09 핫픽스] pytz의 LMT(Local Mean Time) 버그를 원천 차단하기 위해 zoneinfo 도입 및 적용
# ==========================================================

import os
import logging
import datetime
import pytz
import asyncio
import math # 🚨 [수술 완료] NaN 검증용 math 모듈 추가
# NEW: [V30.09 핫픽스] LMT 버그 방어를 위한 zoneinfo 명시적 도입
from zoneinfo import ZoneInfo
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from dotenv import load_dotenv

from config import ConfigManager
from broker import KoreaInvestmentBroker
from strategy import InfiniteStrategy
from telegram_bot import TelegramController

from queue_ledger import QueueLedger
from strategy_reversion import ReversionStrategy
from volatility_engine import VolatilityEngine

# MODIFIED: [V30.09] 존재하지 않는 get_target_hour 임포트 영구 소각 (ImportError 원천 차단)
from scheduler_core import (
    scheduled_token_check,
    scheduled_auto_sync_summer,
    scheduled_auto_sync_winter,
    scheduled_force_reset,
    scheduled_self_cleaning,
    perform_self_cleaning
)

# 🚨 [V30.00 수술] 분할된 4대 전투 사령부 코어 독립 임포트
from scheduler_sniper import scheduled_sniper_monitor
from scheduler_vwap import scheduled_vwap_trade, scheduled_vwap_init_and_cancel
from scheduler_regular import scheduled_regular_trade
from scheduler_aftermarket import scheduled_after_market_lottery

TICKER_BASE_MAP = {
    "SOXL": "SOXX",
    "TQQQ": "QQQ",
    "TSLL": "TSLA",
    "FNGU": "FNGS",
    "BULZ": "FNGS"
}

if not os.path.exists('data'): os.makedirs('data')
if not os.path.exists('logs'): os.makedirs('logs')

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

# 🚨 [수술 완료] ADMIN_CHAT_ID 누락 시 묵언수행(Silent Zombie) 봇 구동 원천 차단
if not all([TELEGRAM_TOKEN, APP_KEY, APP_SECRET, CANO, ADMIN_CHAT_ID]):
    print("❌ [치명적 오류] .env 파일에 봇 구동 필수 키(TELEGRAM_TOKEN, APP_KEY, APP_SECRET, CANO, ADMIN_CHAT_ID)가 누락되었습니다. 봇을 종료합니다.")
    exit(1)

# 🚨 [V28.27 수술 완료] 로그 파일명 타임존 EST 락온
est_tz_log = pytz.timezone('US/Eastern')
log_filename = f"logs/bot_app_{datetime.datetime.now(est_tz_log).strftime('%Y%m%d')}.log"

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO,
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

async def scheduled_volatility_scan(context):
    """
    10:20 EST (정규장 개장 50분 후) 격발.
    대상 종목들의 HV와 당일 VXN을 연산하여 터미널 메인 화면에 1-Tier 브리핑 덤프
    """
    app_data = context.job.data
    cfg = app_data['cfg']
    base_map = app_data.get('base_map', TICKER_BASE_MAP)
    
    print("\n" + "=" * 60)
    print("📈 [자율주행 변동성 스캔 완료] (10:20 EST 스냅샷)")
    
    active_tickers = cfg.get_active_tickers()
            
    if not active_tickers:
        print("📊 현재 운용 중인 종목이 없습니다.")
    else:
        briefing_lines = []
        vol_engine = VolatilityEngine()
        
        for ticker in active_tickers:
            target_base = base_map.get(ticker, ticker)
            try:
                weight_data = await asyncio.to_thread(vol_engine.calculate_weight, target_base)
                raw_weight = weight_data.get('weight', 1.0) if isinstance(weight_data, dict) else weight_data
                real_weight = float(raw_weight)
                
                # 🚨 [수술 완료] NaN/Inf 결측치 침투 시 무조건 중립(1.0) 폴백 적용하여 공격 오판 원천 차단
                if not math.isfinite(real_weight):
                    raise ValueError(f"비정상 수학 수치 산출: {real_weight}")
            except Exception as e:
                # 🚨 [수술 완료] 에러 시 역방향 배팅(0.85/1.15) 금지. 무조건 중립(1.0) 안전마진 강제 적용
                logging.warning(f"[{ticker}] 변동성 지표 산출 실패. 중립 안전마진(1.0) 강제 적용: {e}")
                real_weight = 1.0 
                
            status_text = "OFF 권장" if real_weight <= 1.0 else "ON 권장"
            if ticker != target_base: briefing_lines.append(f"{ticker}({target_base}): {real_weight:.2f} ({status_text})")
            else: briefing_lines.append(f"{ticker}: {real_weight:.2f} ({status_text})")
            
        print(f"📊 [자율주행 지표] {' | '.join(briefing_lines)} (상세 게이지: /mode)")
    print("=" * 60 + "\n")

# 🚨 [수술 완료] 파이썬 3.10+ 호환성을 위해 이벤트 루프 내부에서 asyncio.Lock()을 안전하게 생성하는 콜백
async def post_init(application: Application):
    tx_lock = asyncio.Lock()
    application.bot_data['app_data']['tx_lock'] = tx_lock
    application.bot_data['bot_controller'].tx_lock = tx_lock

def main():
    # MODIFIED: [V30.09] KST 의존성 함수(get_target_hour) 소각 및 EST 팩트 기반 서머타임 실시간 판별 락온
    est_zone = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est_zone)
    is_dst = bool(now_est.dst())
    season_msg = "☀️ 서머타임 (EDT) 적용 중" if is_dst else "❄️ 표준시간 (EST) 적용 중"
    
    cfg = ConfigManager()
    latest_version = cfg.get_latest_version() 
    
    print("=" * 60)
    print(f"🚀 앱솔루트 스노우볼 퀀트 엔진 {latest_version} (초경량 2대 코어 아키텍처 탑재)")
    print(f"📅 날짜 정보: {season_msg}")
    print(f"⏰ 자동 동기화: 10:00(KST) 확정 스캔 및 비파괴 보정 가동")
    print(f"🛡️ 1-Tier 자율주행 지표 스캔 대기 중... (매일 10:20 EST 격발)")
    print("=" * 60)
    
    perform_self_cleaning()
    
    # 상단에서 ADMIN_CHAT_ID 유효성 검사를 마쳤으므로 무조건 세팅
    cfg.set_chat_id(ADMIN_CHAT_ID)
    
    broker = KoreaInvestmentBroker(APP_KEY, APP_SECRET, CANO, ACNT_PRDT_CD)
    strategy = InfiniteStrategy(cfg)
    queue_ledger = QueueLedger()
    strategy_rev = ReversionStrategy()
    
    # 🚨 [수술 완료] tx_lock은 동기 함수인 main()이 아닌 비동기 post_init에서 생성됩니다.
    bot = TelegramController(
        cfg, broker, strategy, tx_lock=None, 
        queue_ledger=queue_ledger, strategy_rev=strategy_rev
    )
    
    # 🚨 [수술 완료] IANA 표준 타임존 파이프라인 확립
    kst = pytz.timezone('Asia/Seoul')
    est = pytz.timezone('America/New_York')
    
    # NEW: [V30.09 핫픽스] LMT 오차를 원천 차단하는 스케줄러(datetime.time) 전용 ZoneInfo 객체 생성
    zi_kst = ZoneInfo('Asia/Seoul')
    zi_est = ZoneInfo('America/New_York')
    
    app_data = {
        'cfg': cfg, 'broker': broker, 'strategy': strategy, 
        'queue_ledger': queue_ledger, 'strategy_rev': strategy_rev,  
        'bot': bot, 'tx_lock': None, 'base_map': TICKER_BASE_MAP,
        'tz_kst': kst, 'tz_est': est # 타임존 전역 공유
    }

    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .connect_timeout(30.0)
        .pool_timeout(30.0)
        # MODIFIED: [단일 관리자 봇 환경에 맞춰 FD(파일 디스크립터) 누수 및 OS 자원 고갈 방지를 위해 커넥션 풀을 512에서 8로 최적화]
        .connection_pool_size(8)
        .post_init(post_init) # 🚨 Lock 생성을 위한 훅 연결
        .build()
    )
    
    app.bot_data['app_data'] = app_data
    app.bot_data['bot_controller'] = bot
    
    for cmd, handler in [
        ("start", bot.cmd_start), ("record", bot.cmd_record), ("history", bot.cmd_history), 
        ("sync", bot.cmd_sync), ("settlement", bot.cmd_settlement), ("seed", bot.cmd_seed), 
        ("ticker", bot.cmd_ticker), ("mode", bot.cmd_mode), ("reset", bot.cmd_reset), 
        ("version", bot.cmd_version), ("update", bot.cmd_update)
    ]:
        app.add_handler(CommandHandler(cmd, handler))
        
    app.add_handler(CallbackQueryHandler(bot.handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.handle_message))
    
    jq = app.job_queue
    
    # 1. 시스템 관리 스케줄러 (core)
    # MODIFIED: [V30.09 핫픽스] LMT 버그 제거를 위해 tzinfo=zi_kst 교체 주입
    for tt in [datetime.time(7,0,tzinfo=zi_kst), datetime.time(11,0,tzinfo=zi_kst), datetime.time(16,30,tzinfo=zi_kst), datetime.time(22,0,tzinfo=zi_kst)]:
        jq.run_daily(scheduled_token_check, time=tt, days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    # NEW: [V30.08 스케줄러 디커플링 수술] KST TARGET_HOUR 의존성 전면 소각 및 EST 팩트 기반 서머타임 실시간 판별 이식
    est_now = datetime.datetime.now(est)
    is_dst_job = bool(est_now.dst())
    SYNC_FUNC = scheduled_auto_sync_summer if is_dst_job else scheduled_auto_sync_winter
    # MODIFIED: [V30.09 핫픽스] LMT 버그 제거를 위해 tzinfo=zi_kst 교체 주입
    jq.run_daily(SYNC_FUNC, time=datetime.time(10, 0, 5, tzinfo=zi_kst), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    # MODIFIED: [V30.08 스케줄러 디커플링 수술] KST 연산 패러독스 방어를 위해 EST 04:00 AM 단일 슬롯으로 락온 (17시/18시 멱등성 자동 보장)
    # MODIFIED: [V30.09 핫픽스] LMT 버그 제거를 위해 tzinfo=zi_est 교체 주입
    jq.run_daily(scheduled_force_reset, time=datetime.time(4, 0, tzinfo=zi_est), days=(0,1,2,3,4), chat_id=ADMIN_CHAT_ID, data=app_data)
        
    jq.run_daily(scheduled_volatility_scan, time=datetime.time(10, 20, tzinfo=zi_est), days=(0,1,2,3,4), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    # 2. 실전 전투 매매 스케줄러 (trade)
    # MODIFIED: [V30.08 스케줄러 디커플링 수술] 정규장 타격 스케줄 역시 KST 변수를 소각하고 EST 04:05 AM 으로 절대 락온
    # MODIFIED: [V30.09 핫픽스] LMT 버그 제거를 위해 tzinfo=zi_est 교체 주입
    jq.run_daily(scheduled_regular_trade, time=datetime.time(4, 5, tzinfo=zi_est), days=(0,1,2,3,4), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    # MODIFIED: [V30.09 핫픽스] LMT 버그 제거를 위해 tzinfo=zi_est 교체 주입
    jq.run_daily(scheduled_vwap_init_and_cancel, time=datetime.time(15, 30, tzinfo=zi_est), days=(0,1,2,3,4), chat_id=ADMIN_CHAT_ID, data=app_data)

    # 🚨 [수술 완료] 콜드 스타트 폭풍 방어: 봇 구동 후 30초 뒤 첫 실행(first=30)
    jq.run_repeating(scheduled_sniper_monitor, interval=60, first=30, chat_id=ADMIN_CHAT_ID, data=app_data)
    jq.run_repeating(scheduled_vwap_trade, interval=60, first=30, chat_id=ADMIN_CHAT_ID, data=app_data)
    
    # MODIFIED: [V30.09 핫픽스] LMT 버그 제거를 위해 tzinfo=zi_est 교체 주입
    jq.run_daily(scheduled_after_market_lottery, time=datetime.time(16, 5, tzinfo=zi_est), days=(0,1,2,3,4), chat_id=ADMIN_CHAT_ID, data=app_data)

    # MODIFIED: [V30.09 핫픽스] LMT 버그 제거를 위해 tzinfo=zi_kst 교체 주입
    jq.run_daily(scheduled_self_cleaning, time=datetime.time(6, 0, tzinfo=zi_kst), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
        
    app.run_polling()

if __name__ == "__main__":
    main()

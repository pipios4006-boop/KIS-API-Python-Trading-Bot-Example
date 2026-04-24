# ==========================================================
# [telegram_bot.py] - 🌟 100% 통합 무결점 완성본 (Full Version) 🌟
# MODIFIED: [V28.16 지시서(UI) 스냅샷 디커플링 그랜드 수술]
# 1) V-REV 지시서 렌더링 시 실시간 큐(Queue) 장부 조회를 전면 소각하고, 
#    17:05 스냅샷(cached_snap)에 박제된 매도 주문(SELL)을 직접 파싱하여 
#    출력하도록 완벽한 디커플링(Decoupling) 적용. (장중 타점 변동 원천 차단)
# 2) V14 (오리지널 LOC) 모드 스냅샷 로드 분기 신설.
# 3) V14-VWAP 모드 스냅샷 수량 Key('initial_qty') 불일치 팩트 교정.
# 4) [2차 보완] 하드코딩된 '[1층 단독]' 문자열 의존성 100% 소각 및 
#    유연한 동적 번호 부여(Pop1, Pop2...) 파싱 아키텍처 이식.
# 5) [3차 보완] V-REV 잭팟 타점 실시간 변이 방어를 위해 actual_avg 대신
#    스냅샷에 박제된 avg_price 앵커 최우선 적용.
# MODIFIED: [V28.22 스냅샷 렌더링 디커플링 수술] 졸업 카드 목록에서 
# 과거 내역을 조회할 때, 해당 내역의 고유 식별자(ID)를 뷰 엔진으로 
# 100% 전달하도록 라우팅 배선(history_id=hid) 교정 완료.
# MODIFIED: [V28.28 이벤트 루프 직접 차단(Blocking) 원천 방어]
# /sync 핸들러 내부에서 get_account_balance() 동기 함수가 이벤트 루프를 
# 영구 점유하던 치명적 버그를 asyncio.to_thread() 래핑으로 완벽 교정.
# NEW: [V28.31] 하단 고정 키보드 한글 신호 무응답 맹점 완벽 수술 (라우팅 복구)
# 🚨 [V28.50 NEW] AVWAP 암살자 전용 '조기퇴근/타겟설정' 독립 UI 라우터 개통
# 🚨 [V29.04 MODIFIED] UI 렌더링 파편화 수술: /history 명령어(cmd_history)의 구형 출력을 최신형 콜백 UI(HIST:LIST)와 100% 동일하게 통일화 완료.
# 🚨 [V29.05 핵심 수술] 평단가 하방 오염 디커플링: V-REV 지시서(가이던스) 연산 시 한투 평단가(actual_avg) 개입을 영구 차단하고, 큐(Queue) 지층 기반 순수 역산 로직 100% 이식 완료.
# 🚨 [V29.08 팩트 교정] 장마감(CLOSE) 현재가 출력 수술: 애프터마켓 종료 후에는 실시간 가격($100.25) 대신 '정규장 종가($98.09)'를 현재가(curr)로 강제 고정하여 HTS와 100% 동기화 완료.
# 🚨 [V29.10 팩트 교정] V-REV 0주 새출발 렌더링 시 스냅샷 수량(logic_qty) 의존성 전면 소각 및 실잔고(v_rev_q_qty) 기반 100% 디커플링 완성 (타점 오염 및 줍줍 렌더링 버그 영구 차단)
# 🚨 [V30.01 팩트 수술] AVWAP 암살자 실시간 레이더(Radar) 시각화 엔진의 혈관(Sync Engine) 개통
# 🚨 [V30.02 팩트 교정] 버전 기록(V28.40/V29.10)의 0주 락온 디커플링 환각 사태 완벽 수술
# 🚨 [V30.04 팩트 교정] AVWAP 암살자 타임라인 시간 표기 100% 소각 및 직관적 상태 위주 렌더링
# 🚨 [V30.05 팩트 수술] AVWAP 암살자 모니터링 타임라인 1시간 연장 (15:00 EST 마감) 렌더링 동기화
# 🚨 [V30.06 NEW] 장중 업데이트 레드존(Update Red-Zone) 원천 차단:
# VWAP 타임 슬라이싱 및 장마감 정산의 무결성을 위해 EST 14:55 ~ 16:10 사이의
# /update 명령어를 시스템적으로 차단하고 팩트 메시지를 타전하도록 수술 완료.
# MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo 이식을 통한 타임존 오차 차단. cmd_record 비동기 블로킹 방어막 탑재 완료.
# ==========================================================
import logging
import datetime
# MODIFIED: [V30.09] LMT 오차 방어를 위해 pytz 적출 및 ZoneInfo 도입
from zoneinfo import ZoneInfo
import time
import os
import math 
import asyncio
import html
import yfinance as yf
import pandas_market_calendars as mcal 
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, CommandHandler, CallbackQueryHandler, MessageHandler, filters

from telegram_view import TelegramView 
from telegram_sync_engine import TelegramSyncEngine
from telegram_states import TelegramStates
from telegram_callbacks import TelegramCallbacks

class TelegramController:
    def __init__(self, config, broker, strategy, tx_lock=None, queue_ledger=None, strategy_rev=None):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.view = TelegramView()
        self.user_states = {} 
        self.admin_id = self.cfg.get_chat_id()
        self.sync_locks = {} 
        self.tx_lock = tx_lock or asyncio.Lock()
        
        self.queue_ledger = queue_ledger
        self.strategy_rev = strategy_rev 

        self.sync_engine = TelegramSyncEngine(self.cfg, self.broker, self.strategy, self.queue_ledger, self.view, self.tx_lock, self.sync_locks)
        self.states_handler = TelegramStates(self.cfg, self.broker, self.queue_ledger, self.sync_engine)
        self.callbacks_handler = TelegramCallbacks(self.cfg, self.broker, self.strategy, self.queue_ledger, self.sync_engine, self.view, self.tx_lock)

    def _is_admin(self, update: Update):
        if self.admin_id is None:
            self.admin_id = self.cfg.get_chat_id()
        
        if self.admin_id is None:
            print("⚠️ 보안 경고: ADMIN_CHAT_ID가 설정되지 않아 알 수 없는 사용자의 접근을 차단했습니다.")
            return False
            
        return update.effective_chat.id == int(self.admin_id)

    def _get_dst_info(self):
        # MODIFIED: [V30.09] pytz 소각 및 ZoneInfo 이식
        est = ZoneInfo('America/New_York')
        now_est = datetime.datetime.now(est)
        is_dst = now_est.dst() != datetime.timedelta(0)
        
        if is_dst:
            return (17, "🌞 <b>서머타임 적용 (Summer)</b>")
        else:
            return (18, "❄️ <b>서머타임 해제 (Winter)</b>")

    def _get_market_status(self):
        # MODIFIED: [V30.09] pytz 소각 및 ZoneInfo 이식
        est = ZoneInfo('America/New_York')
        now = datetime.datetime.now(est)
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=now.date(), end_date=now.date())
        
        if schedule.empty:
            return "CLOSE", "⛔ 장휴일"
        
        market_open = schedule.iloc[0]['market_open'].astimezone(est)
        market_close = schedule.iloc[0]['market_close'].astimezone(est)
        pre_start = market_open.replace(hour=4, minute=0)
        after_end = market_close.replace(hour=20, minute=0)

        if pre_start <= now < market_open:
            return "PRE", "🌅 프리마켓"
        elif market_open <= now < market_close:
            return "REG", "🔥 정규장"
        elif market_close <= now < after_end:
            return "AFTER", "🌙 애프터마켓"
        else:
            return "CLOSE", "⛔ 장마감"

    def _calculate_budget_allocation(self, cash, tickers):
        sorted_tickers = sorted(tickers, key=lambda x: 0 if x == "SOXL" else (1 if x == "TQQQ" else 2))
        allocated = {}
        rem_cash = cash
        
        for tx in sorted_tickers:
            rev_state = self.cfg.get_reverse_state(tx)
            is_rev = rev_state.get("is_active", False)
            
            if is_rev:
                allocated[tx] = 0.0 
            else:
                split = self.cfg.get_split_count(tx)
                portion = self.cfg.get_seed(tx) / split if split > 0 else 0
                if rem_cash >= portion:
                    allocated[tx] = portion
                    rem_cash -= portion
                else: 
                    allocated[tx] = 0
                    
        return sorted_tickers, allocated

    def setup_handlers(self, application):
        application.add_handler(CommandHandler("start", self.cmd_start))
        application.add_handler(CommandHandler("sync", self.cmd_sync))
        application.add_handler(CommandHandler("record", self.cmd_record))
        application.add_handler(CommandHandler("history", self.cmd_history))
        application.add_handler(CommandHandler("settlement", self.cmd_settlement))
        application.add_handler(CommandHandler("seed", self.cmd_seed))
        application.add_handler(CommandHandler("ticker", self.cmd_ticker))
        application.add_handler(CommandHandler("mode", self.cmd_mode))
        application.add_handler(CommandHandler("version", self.cmd_version))
        
        application.add_handler(CommandHandler("queue", self.cmd_queue))
        application.add_handler(CommandHandler("add_q", self.cmd_add_q))
        application.add_handler(CommandHandler("clear_q", self.cmd_clear_q))
        
        application.add_handler(CommandHandler("reset", self.cmd_reset))
        application.add_handler(CommandHandler("update", self.cmd_update))
        
        application.add_handler(CommandHandler("avwap", self.cmd_avwap))
        
        application.add_handler(CallbackQueryHandler(self.handle_callback))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.callbacks_handler.handle_callback(update, context, self)

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update):
            return
            
        text = update.message.text
        
        if "장부 조회" in text:
            return await self.cmd_record(update, context)
        elif "시드 변경" in text:
            return await self.cmd_seed(update, context)
        elif "모드 전환" in text:
            return await self.cmd_ticker(update, context)
        elif "분할 변경" in text or "환경 설정" in text or "세팅" in text:
            return await self.cmd_settlement(update, context)
        elif "스나이퍼" in text:
            return await self.cmd_mode(update, context)
        elif "명예의 전당" in text or "졸업" in text:
            return await self.cmd_history(update, context)
        elif "암살자" in text or "조기" in text:
            return await self.cmd_avwap(update, context)
            
        await self.states_handler.handle_message(update, context, self)

    async def cmd_avwap(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update):
            return
            
        t = "SOXL"
        is_hybrid_on = self.cfg.get_avwap_hybrid_mode(t)
        
        if not is_hybrid_on:
            msg = f"⚠️ <b>[AVWAP 암살자 오프라인]</b>\n"
            msg += f"▫️ 현재 {t}의 AVWAP 하이브리드 모드가 꺼져있습니다.\n"
            msg += f"▫️ <code>/settlement</code> 메뉴에서 먼저 활성화해주세요."
            return await update.message.reply_text(msg, parse_mode='HTML')

        early_mode = self.cfg.get_avwap_early_exit_mode(t)
        early_target = self.cfg.get_avwap_early_target(t)
        
        msg = f"🔫 <b>[ {t} AVWAP 암살자 제어 콘솔 ]</b>\n\n"
        
        if early_mode:
            msg += "🏃‍♂️ <b>현재 모드: [조기 퇴근 (사용자 맞춤형)]</b>\n"
            msg += f"▫️ 장중 시간에 구애받지 않고 <b>+{early_target}%</b> 수익 도달 시 즉각 전량 익절하고 퇴근합니다.\n"
            msg += "▫️ 장막판 변동성 리스크를 회피하고 일일 수익을 확정 짓는 데 유리합니다.\n"
        else:
            msg += "🦅 <b>현재 모드: [오리지널 스퀴즈 타겟팅]</b>\n"
            msg += "▫️ 수익이 나도 기다렸다가 <b>오후 2시 30분(EST)</b> 이후 발생하는 기관 숏커버링 스퀴즈(+3% 이상)를 노립니다.\n"
            msg += "▫️ 휩소 장세에서는 종가에 수익을 반납할 리스크가 있습니다.\n"

        keyboard = [
            [
                InlineKeyboardButton(f"⚪ 오리지널 모드로 전환" if early_mode else "🎯 오리지널 모드 (현재 적용)", callback_data=f"AVWAP:EARLY:OFF:{t}"),
                InlineKeyboardButton(f"🎯 조기 퇴근 모드 (현재 적용)" if early_mode else "🏃‍♂️ 조기 퇴근 모드로 전환", callback_data=f"AVWAP:EARLY:ON:{t}")
            ],
            [
                InlineKeyboardButton(f"⚙️ 목표 수익률 설정 (현재: {early_target}%)", callback_data=f"AVWAP:TARGET_SET:{t}")
            ]
        ]
        
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    async def cmd_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update):
            return
            
        # 🚨 [V30.06 NEW] 장중 업데이트 레드존 체크 (14:55 ~ 16:10 EST)
        from plugin_updater import SystemUpdater
        updater = SystemUpdater()
        
        allowed, fail_msg = updater.is_update_allowed()
        if not allowed:
            return await update.message.reply_text(f"🛑 <b>[작전 중 업데이트 거부]</b>\n\n{fail_msg}", parse_mode='HTML')
        
        status_msg = await update.message.reply_text("⏳ <b>[시스템 업데이트]</b> 깃허브 원격 서버와 통신을 시작합니다...", parse_mode='HTML')
        
        try:
            success, msg = await updater.pull_latest_code()
            
            safe_msg = html.escape(msg)
            
            if success:
                await status_msg.edit_text(f"✅ <b>[동기화 완료]</b> {safe_msg}\n\n🔄 시스템 데몬(pipiosbot)을 OS 단에서 재가동합니다. 다운타임 후 봇이 다시 깨어납니다.", parse_mode='HTML')
                updater.restart_daemon()
            else:
                await status_msg.edit_text(f"❌ <b>[동기화 실패]</b>\n▫️ 사유: {safe_msg}", parse_mode='HTML')
        except Exception as e:
            safe_err = html.escape(str(e))
            await status_msg.edit_text(f"🚨 <b>[치명적 오류]</b> 플러그인 호출 및 프로세스 예외 발생: {safe_err}", parse_mode='HTML')

    async def cmd_queue(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update):
            return
            
        args = context.args
        if not args:
            return await update.message.reply_text("❌ 종목명을 입력하세요. 예: /queue SOXL")
            
        ticker = args[0].upper()
        
        if not getattr(self, 'queue_ledger', None):
            from queue_ledger import QueueLedger
            self.queue_ledger = QueueLedger()
            
        q_data = self.queue_ledger.get_queue(ticker)
            
        msg, reply_markup = self.view.get_queue_management_menu(ticker, q_data)
        await update.message.reply_text(text=msg, reply_markup=reply_markup, parse_mode='HTML')

    async def cmd_add_q(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update):
            return
        
        try:
            args = context.args
            if len(args) < 4:
                return await update.message.reply_text("❌ 정확한 양식: <code>/add_q SOXL 2026-04-06 20 52.16</code>", parse_mode='HTML')
                
            ticker = args[0].upper()
            date_str = args[1]
            try:
                qty = int(args[2])
                price = float(args[3])
            except ValueError:
                return await update.message.reply_text("❌ 수량은 정수, 평단가는 숫자로 입력하세요.")
                
            try:
                curr_p = await asyncio.wait_for(
                    asyncio.to_thread(self.broker.get_current_price, ticker), 
                    timeout=3.0
                )
                if curr_p and curr_p > 0:
                    if price < curr_p * 0.7 or price > curr_p * 1.3:
                        return await update.message.reply_text(f"🚨 <b>오입력 차단:</b> 입력하신 평단가(<b>${price:.2f}</b>)가 현재가 대비 ±30%를 벗어납니다. 오타를 확인하세요!", parse_mode='HTML')
            except asyncio.TimeoutError:
                pass 
            except Exception:
                pass
                
            q_file = "data/queue_ledger.json"
            all_q = {}
            if os.path.exists(q_file):
                try:
                    import json
                    with open(q_file, 'r', encoding='utf-8') as f:
                        all_q = json.load(f)
                except Exception:
                    pass
                    
            ticker_q = all_q.get(ticker, [])
            ticker_q.append({
                "qty": qty,
                "price": price,
                "date": f"{date_str} 23:59:59", 
                "type": "MANUAL_OVERRIDE"
            })
            
            ticker_q.sort(key=lambda x: x.get('date', ''), reverse=True)
            
            chat_id = update.effective_chat.id
            await self.sync_engine._verify_and_update_queue(ticker, ticker_q, context, chat_id)
            await update.message.reply_text(f"✅ <b>[{ticker}] 수동 지층 삽입 완료!</b>\n▫️ {date_str} | {qty}주 | ${price:.2f}", parse_mode='HTML')
                
        except Exception as e:
            await update.message.reply_text(f"❌ 알 수 없는 에러 발생: {e}")

    async def cmd_clear_q(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update):
            return
            
        args = context.args
        if not args:
            return await update.message.reply_text("❌ 종목명을 입력하세요. 예: /clear_q SOXL")
            
        ticker = args[0].upper()
        try:
            chat_id = update.effective_chat.id
            await self.sync_engine._verify_and_update_queue(ticker, [], context, chat_id)
            await update.message.reply_text(f"🗑️ <b>[{ticker}] 장부가 완전히 소각되었습니다.</b>\n새로운 지층을 구축할 준비가 완료되었습니다.", parse_mode='HTML')
        except Exception as e:
            await update.message.reply_text(f"❌ 소각 중 에러 발생: {e}")

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update):
            return
            
        target_hour, season_icon = self._get_dst_info()
        latest_version = self.cfg.get_latest_version() 
        msg = self.view.get_start_message(target_hour, season_icon, latest_version) 
        await update.message.reply_text(msg, parse_mode='HTML')

    async def cmd_sync(self, update, context):
        if not self._is_admin(update):
            return
            
        await update.message.reply_text("🔄 시장 분석 및 지시서 작성 중...")
        
        async with self.tx_lock:
            cash, holdings = await asyncio.to_thread(self.broker.get_account_balance)
            
        if holdings is None:
            await update.message.reply_text("❌ KIS API 통신 오류로 계좌 정보를 불러올 수 없습니다. 잠시 후 다시 시도해주세요.")
            return

        target_hour, _ = self._get_dst_info() 
        dst_txt = "🌞 서머타임 (17:30)" if target_hour == 17 else "❄️ 겨울 (18:30)"
        status_code, status_text = self._get_market_status()
        
        tickers = self.cfg.get_active_tickers()
        sorted_tickers, allocated_cash = self._calculate_budget_allocation(cash, tickers)
        
        ticker_data_list = []
        total_buy_needed = 0.0

        tracking_cache = {}
        try:
            jobs = context.job_queue.jobs() if context.job_queue else []
            job_data = jobs[0].data if jobs and jobs[0].data is not None else {}
            tracking_cache = job_data.get('sniper_tracking', {})
        except (IndexError, AttributeError):
            tracking_cache = {}

        # MODIFIED: [V30.09] pytz 소각 및 ZoneInfo 이식
        est = ZoneInfo('America/New_York')
        now_est = datetime.datetime.now(est)
        
        is_sniper_active_time = False
        try:
            nyse = mcal.get_calendar('NYSE')
            schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
            if not schedule.empty:
                market_open = schedule.iloc[0]['market_open'].astimezone(est)
                switch_time = market_open + datetime.timedelta(minutes=50)
                if now_est >= switch_time:
                    is_sniper_active_time = True
        except Exception:
            if now_est.weekday() < 5 and now_est.time() >= datetime.time(10, 20):
                is_sniper_active_time = True

        for t in sorted_tickers:
            h = holdings.get(t, {'qty':0, 'avg':0})
            curr = await asyncio.to_thread(self.broker.get_current_price, t, is_market_closed=(status_code == "CLOSE"))
            prev_close = await asyncio.to_thread(self.broker.get_previous_close, t)
            ma_5day = await asyncio.to_thread(self.broker.get_5day_ma, t)
            day_high, day_low = await asyncio.to_thread(self.broker.get_day_high_low, t)
            
            actual_avg = float(h['avg']) if h['avg'] else 0.0
            actual_qty = int(h['qty'])
            
            safe_prev_close = prev_close if prev_close else 0.0
            
            if status_code in ["AFTER", "CLOSE", "PRE"]:
                try:
                    def get_yf_close():
                        df = yf.Ticker(t).history(period="5d", interval="1d")
                        return float(df['Close'].iloc[-1]) if not df.empty else None
                    yf_close = await asyncio.wait_for(asyncio.to_thread(get_yf_close), timeout=3.0)
                    if yf_close and yf_close > 0:
                        safe_prev_close = yf_close
                except Exception as e:
                    logging.debug(f"YF 정규장 종가 롤오버 스캔 실패 ({t}): {e}")

            if status_code == "CLOSE":
                curr = safe_prev_close

            idx_ticker = "SOXX" if t == "SOXL" else "QQQ"
            dynamic_pct_obj = await asyncio.to_thread(self.broker.get_dynamic_sniper_target, idx_ticker)
            dynamic_pct = float(dynamic_pct_obj) if dynamic_pct_obj is not None else (8.79 if t == "SOXL" else 4.95)
            
            tracking_status = tracking_cache.get(t, {})
            current_day_high = tracking_status.get('day_high', day_high) 
            hybrid_target_price = current_day_high * (1 - (abs(dynamic_pct) / 100.0))
            trigger_reason = f"-{abs(dynamic_pct)}%"
            is_already_ordered = self.cfg.check_lock(t, "REG") or self.cfg.check_lock(t, "SNIPER")
            
            ver = self.cfg.get_version(t)
            is_manual_vwap = getattr(self.cfg, 'get_manual_vwap_mode', lambda x: False)(t)
            
            cached_snap = None
            if ver == "V_REV":
                cached_snap = self.strategy.v_rev_plugin.load_daily_snapshot(t)
            elif ver == "V14":
                if is_manual_vwap:
                    cached_snap = self.strategy.v14_vwap_plugin.load_daily_snapshot(t)
                else:
                    if hasattr(self.strategy, 'v14_plugin') and hasattr(self.strategy.v14_plugin, 'load_daily_snapshot'):
                        cached_snap = self.strategy.v14_plugin.load_daily_snapshot(t)
            
            logic_qty = actual_qty
            is_zero_start_fact = (actual_qty == 0)
            if cached_snap:
                if "total_q" in cached_snap:
                    logic_qty = cached_snap["total_q"]
                elif "initial_qty" in cached_snap:
                    logic_qty = cached_snap["initial_qty"]
                is_zero_start_fact = cached_snap.get("is_zero_start", logic_qty == 0)

            plan = self.strategy.get_plan(
                t, curr, actual_avg, logic_qty, safe_prev_close, ma_5day=ma_5day,
                market_type="REG", available_cash=allocated_cash[t],
                is_simulation=True 
            )
            
            split = self.cfg.get_split_count(t)
            seed = self.cfg.get_seed(t)
            t_val = plan.get('t_val', 0.0)
            is_rev = plan.get('is_reverse', False)
            
            if dynamic_pct_obj and hasattr(dynamic_pct_obj, 'metric_val'):
                real_val = float(dynamic_pct_obj.metric_val)
            else:
                real_val = 0.0
                
            vol_status = "ON" if real_val >= 20.0 else "OFF"
            v_rev_q_qty = 0
            v_rev_q_lots = 0
            v_rev_guidance = ""
            
            is_avwap_active = False
            avwap_budget = 0.0
            avwap_qty = 0
            avwap_avg = 0.0
            avwap_status_txt = ""
            
            avwap_base_ticker = 'N/A'
            avwap_base_price = 0.0
            avwap_base_vwap = 0.0
            avwap_gap_pct = 0.0

            if ver == "V_REV":
                if not getattr(self, 'queue_ledger', None):
                    from queue_ledger import QueueLedger
                    self.queue_ledger = QueueLedger()
                    
                q_list = self.queue_ledger.get_queue(t)
                v_rev_q_lots = len(q_list)
                v_rev_q_qty = sum(item.get('qty', 0) for item in q_list)
   
                one_portion_cash = seed * 0.15
                plan['one_portion'] = one_portion_cash
                half_portion_cash = one_portion_cash * 0.5
                
                tag = "VWAP" if is_manual_vwap else "LOC"
                
                if cached_snap and "orders" in cached_snap and v_rev_q_qty > 0:
                    sell_idx = 1
                    for o in cached_snap["orders"]:
                        if o.get('side') == 'SELL':
                            v_rev_guidance += f" 🔵 매도{sell_idx}(Pop{sell_idx}) ${o['price']:.2f} <b>{o['qty']}주</b> ({tag})\n"
                            sell_idx += 1
                            
                    if not is_manual_vwap:
                        if 'avg_price' in cached_snap:
                            snap_avg = cached_snap['avg_price']
                        else:
                            total_q = sum(item.get('qty', 0) for item in q_list)
                            total_inv = sum(item.get('qty', 0) * item.get('price', 0.0) for item in q_list)
                            snap_avg = total_inv / total_q if total_q > 0 else 0.0
                            
                        target_jackpot = round(snap_avg * 1.01, 2) if snap_avg > 0 else 0.0
                        v_rev_guidance += f" 🎯 [전체 잭팟] ${target_jackpot:.2f} <b>{logic_qty}주</b> (옵션)\n"
                
                elif q_list and logic_qty > 0:
                    l1_qty = q_list[-1].get('qty', 0)
                    l1_price = q_list[-1].get('price', safe_prev_close)
                    
                    target_l1 = round(l1_price * 1.006, 2)
                    v_rev_guidance += f" 🔵 매도1(Pop1) ${target_l1:.2f} <b>{l1_qty}주</b> ({tag})\n"
                    
                    upper_qty = sum(item.get('qty', 0) for item in q_list[:-1])
                    if upper_qty > 0:
                        upper_invested = sum(item.get('qty', 0) * item.get('price', 0.0) for item in q_list[:-1])
                        upper_avg = upper_invested / upper_qty
                        
                        target_upper = round(upper_avg * 1.005, 2)
                        v_rev_guidance += f" 🔵 매도2(Pop2) ${target_upper:.2f} <b>{upper_qty}주</b> ({tag})\n"
                        
                    if not is_manual_vwap:
                        total_q = sum(item.get('qty', 0) for item in q_list)
                        total_inv = sum(item.get('qty', 0) * item.get('price', 0.0) for item in q_list)
                        pure_queue_avg = total_inv / total_q if total_q > 0 else 0.0
                        
                        target_jackpot = round(pure_queue_avg * 1.01, 2) if pure_queue_avg > 0 else 0.0
                        v_rev_guidance += f" 🎯 [전체 잭팟] ${target_jackpot:.2f} <b>{logic_qty}주</b> (옵션)\n"
                else:
                    v_rev_guidance += " 🔵 매도: 대기 물량 없음 (관망)\n"
                
                if safe_prev_close > 0:
                    b1_price = round(safe_prev_close / 0.935 if is_zero_start_fact else safe_prev_close * 0.995, 2)
                    b2_price = round(safe_prev_close * 0.999 if is_zero_start_fact else safe_prev_close * 0.9725, 2)
                    
                    b1_qty = math.floor(half_portion_cash / b1_price) if b1_price > 0 else 0
                    b2_qty = math.floor(half_portion_cash / b2_price) if b2_price > 0 else 0
                    
                    if b1_qty > 0:
                        v_rev_guidance += f" 🔴 매수1(Buy1) ${b1_price:.2f} <b>{b1_qty}주</b> ({tag})\n"
                    if b2_qty > 0:
                        v_rev_guidance += f" 🔴 매수2(Buy2) ${b2_price:.2f} <b>{b2_qty}주</b> ({tag})\n"
                        
                    if is_zero_start_fact:
                        v_rev_guidance += " 🚫 <code>[0주 새출발] 기준 평단가 부재로 줍줍 생략 (1층 확보에 예산 100% 집중)</code>"
                    elif b2_qty > 0 and b2_price > 0:
                        if not is_manual_vwap:
                            grid_start = round(half_portion_cash / (b2_qty + 1), 2)
                            grid_end = round(half_portion_cash / (b2_qty + 5), 2)
                            if grid_start >= 0.01 and grid_start < b2_price:
                                grid_end = max(grid_end, 0.01)
                                v_rev_guidance += f" 🧹 줍줍(5개): ${grid_start:.2f} ~ ${grid_end:.2f} ({tag})"
                else:
                    v_rev_guidance += " 🔴 매수 대기: 타점 연산 대기 중"

                if is_manual_vwap:
                    v_rev_guidance += "\n\n🚨 <b>[ ⛔ 치명적 경고: 수동 VWAP 설정 ]</b> 🚨\n"
                    v_rev_guidance += "한투 앱(V앱)에서 수동 주문을 거실 때, <b>절대로 '하루 종일'로 설정하지 마십시오!</b>\n"
                    v_rev_guidance += "작동 시간은 반드시 \n<b>[장 마감 30분 전 ~ 장 마감]</b>\n으로만 세팅하셔야 창출됩니다.\n"
                    v_rev_guidance += "장중 내내 작동하게 둘 경우 V-REV 코어 전략의 수익률이 심각하게 파괴됩니다."

                if hasattr(self.cfg, 'get_avwap_hybrid_mode') and self.cfg.get_avwap_hybrid_mode(t):
                    is_avwap_active = True
                    avwap_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
                    avwap_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
                    avwap_budget = cash
                    
                    if tracking_cache.get(f"AVWAP_SHUTDOWN_{t}"):
                        avwap_status_txt = "🛑 영구동결 (SHUTDOWN)"
                    elif tracking_cache.get(f"AVWAP_BOUGHT_{t}"):
                        avwap_status_txt = "🎯 딥매수 완료 (익절/손절 감시중)"
                    else:
                        avwap_status_txt = "👀 장초반 필터 스캔 및 타점 대기"

                    avwap_base_ticker = 'SOXX' if t == 'SOXL' else ('QQQ' if t == 'TQQQ' else t)

                    if status_code in ["PRE", "REG"] and not tracking_cache.get(f"AVWAP_SHUTDOWN_{t}"):
                        try:
                            df_1min_base = await asyncio.wait_for(asyncio.to_thread(self.broker.get_1min_candles_df, avwap_base_ticker), timeout=3.0)
                            base_curr_p = float(await asyncio.wait_for(asyncio.to_thread(self.broker.get_current_price, avwap_base_ticker), timeout=3.0) or 0.0)
                            
                            if hasattr(self.strategy, 'v_avwap_plugin'):
                                decision = self.strategy.v_avwap_plugin.get_decision(
                                    base_ticker=avwap_base_ticker,
                                    exec_ticker=t,
                                    base_curr_p=base_curr_p,
                                    exec_curr_p=curr,
                                    df_1min_base=df_1min_base,
                                    avwap_qty=avwap_qty,
                                    now_est=now_est
                                )
                                avwap_base_price = decision.get('base_curr_p', base_curr_p)
                                avwap_base_vwap = decision.get('vwap', 0.0)
                                avwap_gap_pct = decision.get('gap_pct', 0.0)
                                
                                if "대기" in avwap_status_txt:
                                    reason = decision.get('reason', '타점 계산중')
                                    avwap_status_txt = f"⏳ 대기 ({reason})"
                        except Exception as e:
                            logging.error(f"🚨 [{t}] AVWAP 실시간 레이더 스캔 타임아웃/에러: {e}")

                    if not tracking_cache.get(f"AVWAP_BOUGHT_{t}") and not tracking_cache.get(f"AVWAP_SHUTDOWN_{t}"):
                        curr_time = now_est.time()
                        time_1000 = datetime.time(10, 0)
                        time_1500 = datetime.time(15, 0)
                        
                        if curr_time < time_1000:
                            avwap_status_txt = "⏳ 금일 감시 대기"
                        elif curr_time >= time_1500:
                            avwap_status_txt = "⛔ 금일 감시 종료"

            ticker_data_list.append({
                'ticker': t, 'version': ver, 't_val': t_val, 'split': split, 'curr': curr, 'avg': actual_avg, 'qty': actual_qty,
                'profit_amt': (curr - actual_avg) * actual_qty if actual_qty > 0 else 0, 
                'profit_pct': (curr - actual_avg) / actual_avg * 100 if actual_avg > 0 else 0,
                'upward_sniper': "ON" if self.cfg.get_upward_sniper_mode(t) else "OFF",
                'target': self.cfg.get_target_profit(t), 'star_pct': round(plan.get('star_ratio', 0) * 100, 2) if 'star_ratio' in plan else 0.0,
                'seed': seed, 'one_portion': plan.get('one_portion', 0.0), 'plan': plan,
                'is_locked': is_already_ordered, 'mode': "REG",
                'is_reverse': is_rev, 'star_price': plan.get('star_price', 0.0),
                'escrow': self.cfg.get_escrow_cash(t),
                'hybrid_target': hybrid_target_price,
                'trigger_reason': trigger_reason,
                'sniper_trigger': abs(float(dynamic_pct)), 
                'day_high': day_high,
                'day_low': day_low,
                'prev_close': safe_prev_close,
                'tracking_info': tracking_status,
                'dynamic_obj': dynamic_pct_obj,
                'is_sniper_active_time': is_sniper_active_time,
                'vol_weight': round(real_val, 2), 
                'vol_status': vol_status,
                'v_rev_q_lots': v_rev_q_lots,
                'v_rev_q_qty': v_rev_q_qty,
                'v_rev_guidance': v_rev_guidance,
                'avwap_active': is_avwap_active,
                'avwap_budget': avwap_budget,
                'avwap_qty': avwap_qty,
                'avwap_avg': avwap_avg,
                'avwap_status': avwap_status_txt,
                'avwap_base_ticker': avwap_base_ticker if is_avwap_active else 'N/A',
                'avwap_base_price': avwap_base_price if is_avwap_active else 0.0,
                'avwap_base_vwap': avwap_base_vwap if is_avwap_active else 0.0,
                'avwap_gap_pct': avwap_gap_pct if is_avwap_active else 0.0,
                'is_manual_vwap': is_manual_vwap,
                'is_zero_start': is_zero_start_fact
            })
            
            total_buy_needed += sum(o['price']*o['qty'] for o in plan.get('orders', []) if o.get('side')=='BUY')

        surplus = cash - total_buy_needed
        rp_amount = surplus * 0.95 if surplus > 0 else 0
        
        final_msg, markup = self.view.create_sync_report(status_text, dst_txt, cash, rp_amount, ticker_data_list, status_code in ["PRE", "REG"], p_trade_data={})
        
        await update.message.reply_text(final_msg, reply_markup=markup, parse_mode='HTML')

    async def cmd_record(self, update, context):
        if not self._is_admin(update):
            return
            
        chat_id = update.message.chat_id
        status_msg = await context.bot.send_message(chat_id, "🛡️ <b>장부 무결성 검증 및 동기화 중...</b>", parse_mode='HTML')
        
        success_tickers = []
        for t in self.cfg.get_active_tickers():
            res = await self.sync_engine.process_auto_sync(t, chat_id, context, silent_ledger=True)
            if res == "SUCCESS":
                success_tickers.append(t)
        
        if success_tickers: 
            async with self.tx_lock:
                # MODIFIED: [V30.09] 이벤트 루프 교착 방어를 위한 비동기 래핑
                _, holdings = await asyncio.to_thread(self.broker.get_account_balance)
            await self.sync_engine._display_ledger(success_tickers[0], chat_id, context, message_obj=status_msg, pre_fetched_holdings=holdings)
        else:
            await status_msg.edit_text("✅ <b>동기화 완료</b> (표시할 진행 중인 장부가 없거나 에러 대기 중입니다)", parse_mode='HTML')

    async def cmd_history(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update):
            return
            
        try:
            history_data = self.cfg.get_history()
        except Exception:
            history_data = []
            
        if not history_data:
            await update.message.reply_text("📭 <b>명예의 전당 (졸업 기록)이 비어있습니다.</b>", parse_mode='HTML')
            return
            
        sorted_hist = sorted(history_data, key=lambda x: x.get('end_date', ''), reverse=True)
        
        msg = "🏆 <b>[ 명예의 전당 (과거 졸업 기록) ]</b>\n\n"
        msg += "상세 내역을 조회할 기록을 선택하세요.\n"
        
        keyboard = []
        
        for h in sorted_hist[:15]: 
            t = h.get('ticker', 'UNK')
            p = h.get('profit', 0.0)
            date_str = h.get('end_date', '')[:10].replace("-", ".")
            sign = "+" if p >= 0 else "-"
            
            btn_text = f"🏅 {date_str} [{t}] {sign}${abs(p):.2f}"
            keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"HIST:VIEW:{h['id']}")])
            
        keyboard.append([InlineKeyboardButton("❌ 닫기", callback_data="RESET:CANCEL")])
        
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    async def cmd_mode(self, update, context):
        if not self._is_admin(update):
            return
            
        active_tickers = self.cfg.get_active_tickers()

        report = "📊 <b>[ 자율주행 변동성 마스터 지표 상세 분석 ]</b>\n\n"
        
        report += "<b>[ 🧭 지수 범위 범례 (ON/OFF 권장) ]</b>\n"
        report += "🧊 <code>~ 15.00</code> : 극저변동성 (OFF)\n"
        report += "🟩 <code>15.00 ~ 20.00</code> : 정상 궤도 (OFF)\n"
        report += "🟨 <code>20.00 ~ 25.00</code> : 변동성 확대 (ON)\n"
        report += "🟥 <code>25.00 이상 </code> : 패닉 셀링 (ON)\n\n"
        
        for t in active_tickers:
            idx_ticker = "SOXX" if t == "SOXL" else "QQQ"
            dynamic_pct_obj = await asyncio.to_thread(self.broker.get_dynamic_sniper_target, idx_ticker)
            
            if dynamic_pct_obj and hasattr(dynamic_pct_obj, 'metric_val'):
                real_val = float(dynamic_pct_obj.metric_val)
                real_name = dynamic_pct_obj.metric_name
            else:
                real_val = 0.0
                real_name = "지표"
            
            if real_val <= 15.0:
                diag_text = "극저변동성 (우측 꼬리 절단 방지를 위해 스나이퍼 OFF)"
                status_icon = "🧊"
            elif real_val <= 20.0:
                diag_text = "정상 궤도 안착 (스나이퍼 OFF)"
                status_icon = "🟩"
            elif real_val <= 25.0:
                diag_text = "변동성 확대 장세 (계좌 방어를 위해 스나이퍼 ON)"
                status_icon = "🟨"
            else:
                diag_text = "패닉 셀링 및 시스템 충격 (스나이퍼 필수 가동)"
                status_icon = "🟥"
            
            report += f"💠 <b>[ {t} 국면 분석 ]</b>\n"
            report += f"▫️ 당일 절대 지수({real_name}): {real_val:.2f}\n"
            report += f"▫️ 진단 : {status_icon} {diag_text}\n\n"

        report += "🎯 <b>[ 수동 상방 스나이퍼 독립 제어 ]</b>\n"
        keyboard = []
        for t in active_tickers:
            is_sniper = self.cfg.get_upward_sniper_mode(t)
            status_txt = 'ON (가동중)' if is_sniper else 'OFF (대기중)'
            report += f"▫️ {t} 현재 상태 : {status_txt}\n"
            
            keyboard.append([
                InlineKeyboardButton(f"{t} ⚪ OFF", callback_data=f"MODE:OFF:{t}"), 
                InlineKeyboardButton(f"{t} 🎯 ON", callback_data=f"MODE:ON:{t}")
            ])
            
        await update.message.reply_text(report, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    async def cmd_reset(self, update, context):
        if not self._is_admin(update):
            return
            
        active_tickers = self.cfg.get_active_tickers()
        msg, markup = self.view.get_reset_menu(active_tickers)
        await update.message.reply_text(msg, reply_markup=markup, parse_mode='HTML')

    async def cmd_seed(self, update, context):
        if not self._is_admin(update):
            return
            
        msg = "💵 <b>[ 종목별 시드머니 관리 ]</b>\n\n"
        keyboard = []
        for t in self.cfg.get_active_tickers():
            current_seed = self.cfg.get_seed(t)
            msg += f"💎 <b>{t}</b>: ${current_seed:,.0f}\n"
            keyboard.append([
                InlineKeyboardButton(f"➕ {t} 추가", callback_data=f"SEED:ADD:{t}"), 
                InlineKeyboardButton(f"➖ {t} 감소", callback_data=f"SEED:SUB:{t}"),
                InlineKeyboardButton(f"🔢 {t} 고정", callback_data=f"SEED:SET:{t}")
            ])
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    async def cmd_ticker(self, update, context):
        if not self._is_admin(update):
            return
            
        msg, markup = self.view.get_ticker_menu(self.cfg.get_active_tickers())
        await update.message.reply_text(msg, reply_markup=markup, parse_mode='HTML')

    async def cmd_settlement(self, update, context):
        if not self._is_admin(update):
            return
        
        active_tickers = self.cfg.get_active_tickers()
        atr_data = {}
        dynamic_target_data = {} 
        
        status_msg = await update.message.reply_text("⏳ <b>실시간 시장 지표(HV/VXN) 연산 중...</b>", parse_mode='HTML')
        
        # MODIFIED: [V30.09] pytz 소각 및 ZoneInfo 이식
        est = ZoneInfo('America/New_York')
        now_est = datetime.datetime.now(est)

        for t in active_tickers:
            atr_data[t] = (0.0, 0.0)
            dynamic_target_data[t] = None
                
        msg, markup = self.view.get_settlement_message(active_tickers, self.cfg, atr_data, dynamic_target_data)
        
        await status_msg.edit_text(msg, reply_markup=markup, parse_mode='HTML')

    async def cmd_version(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update):
            return
            
        history_data = self.cfg.get_full_version_history()
        msg, markup = self.view.get_version_message(history_data, page_index=None)
        await update.message.reply_text(msg, reply_markup=markup, parse_mode='HTML')

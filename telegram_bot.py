# ==========================================================
# [telegram_bot.py]
# 🌟 100% 통합 완성본 (V27.02 완결판) 🌟
# ⚠️ 수술 내역: 
# 1. /reset 시 삼위일체(본장부, 에스크로, 백업장부, 큐장부) 100% 소각 엔진 완벽 보존
# 2. 0주 도달 시 마이너스 수익이라도 장부를 비우는(강제 손절 리셋) 로직 개방 보존
# 💡 [V25.13 디커플링 스왑] Buy1(/0.935)과 Buy2(*0.999)의 고가->저가 배열 원칙 엄수
# 🚀 [V27.01 지시서 스냅샷] /sync 조회 시 17:05 KST 박제된 스냅샷을 우선 로드하여 지시서 변이 원천 차단
# 🚨 [V27.01 렌더링 방어] 텍스트 살균(html.escape) 주입으로 특수문자(< >)에 의한 UI 붕괴 원천 차단
# 🚨 [V27.01 라우터 격리] 위험 명령어(/reset, /update) 핸들러 최하단 격리 배치로 휴먼 에러 방어
# 🚨 [V27.02 최후 배선 연결] 비파괴 보정(CALIB) 발동 시 평단가 0.0 붕괴를 막기 위해 actual_avg 주입 파이프라인 완성
# 🚨 [V27.15 그랜드 수술] 코파일럿 합작 - tx_lock 병목(Starvation) 해체, 0.0달러 매도 폭탄 제거, 
# 런타임 즉사(AttributeError/KeyError) 방어막 이식, Atomic Write 도입 및 Dangling 코드 복구 완료
# ==========================================================
import logging
import datetime
import pytz
import time
import os
import math 
import asyncio
import json
import html
import tempfile # 🚨 Atomic Write를 위한 추가 모듈
import yfinance as yf
import pandas_market_calendars as mcal 
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from telegram_view import TelegramView 

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

    def _is_admin(self, update: Update):
        if self.admin_id is None:
            self.admin_id = self.cfg.get_chat_id()
        
        if self.admin_id is None:
            print("⚠️ 보안 경고: ADMIN_CHAT_ID가 설정되지 않아 알 수 없는 사용자의 접근을 차단했습니다.")
            return False
            
        return update.effective_chat.id == int(self.admin_id)

    def _get_dst_info(self):
        est = pytz.timezone('US/Eastern')
        now_est = datetime.datetime.now(est)
        is_dst = now_est.dst() != datetime.timedelta(0)
        
        if is_dst:
            return (17, "🌞 <b>서머타임 적용 (Summer)</b>")
        else:
            return (18, "❄️ <b>서머타임 해제 (Winter)</b>")

    def _get_market_status(self):
        est = pytz.timezone('US/Eastern')
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
        
        application.add_handler(CallbackQueryHandler(self.handle_callback))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

    async def cmd_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update):
            return
        
        status_msg = await update.message.reply_text("⏳ <b>[시스템 업데이트]</b> 깃허브 원격 서버와 통신을 시작합니다...", parse_mode='HTML')
        
        try:
            from plugin_updater import SystemUpdater
            updater = SystemUpdater()
            
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

    def _update_queue_file(self, ticker, new_q):
        q_file = "data/queue_ledger.json"
        os.makedirs("data", exist_ok=True)
        all_q = {}
        if os.path.exists(q_file):
            try:
                with open(q_file, 'r', encoding='utf-8') as f:
                    all_q = json.load(f)
            except Exception:
                pass
            
        all_q[ticker] = new_q
        
        # 🚨 [수술 완료] Atomic Write (원자적 쓰기) 도입으로 중간에 서버가 꺼져도 파일 파손(0바이트) 방지
        dir_name = os.path.dirname(os.path.abspath(q_file))
        try:
            fd, tmp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(all_q, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, q_file)
        except Exception as e:
            logging.error(f"🚨 수동 큐 파일 저장 에러 (데이터 보존됨): {e}")

        if self.queue_ledger:
            self.queue_ledger.queues = all_q

    async def _verify_and_update_queue(self, ticker, new_q, context, chat_id):
        self._update_queue_file(ticker, new_q)
        
        try:
            _, holdings = await asyncio.wait_for(
                asyncio.to_thread(self.broker.get_account_balance), 
                timeout=5.0
            )
            
            if holdings:
                # 🚨 [수술 완료] API가 None이나 빈칸 반환 시 TypeError 방어 (Safe Casting)
                actual_qty = int(float(holdings.get(ticker, {'qty': 0}).get('qty') or 0))
                new_q_total = sum(int(float(item.get('qty', 0))) for item in new_q)

                if actual_qty != new_q_total:
                    await context.bot.send_message(
                        chat_id, 
                        f"⚠️ <b>[무결성 경고]</b> 큐 총합(<b>{new_q_total}주</b>) 🆚 실제 계좌 잔고(<b>{actual_qty}주</b>)\n"
                        "▫️ <i>수량이 일치하지 않습니다. 분할 입력 중이시라면 나머지 물량도 마저 입력해 맞춰주세요.</i>", 
                        parse_mode='HTML'
                    )
        except asyncio.TimeoutError:
            await context.bot.send_message(chat_id, "⚠️ KIS 서버 통신 지연으로 실잔고 검증을 생략했습니다. (저장 완료)", parse_mode='HTML')
        except Exception as e:
            logging.error(f"잔고 검증 중 에러 (스킵됨): {e}")
            
        return True

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
            await self._verify_and_update_queue(ticker, ticker_q, context, chat_id)
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
            await self._verify_and_update_queue(ticker, [], context, chat_id)
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
        
        # 🚨 [수술 완료] tx_lock 범위를 최소화하여 스케줄러 병목(Starvation) 원천 해제
        async with self.tx_lock:
            cash, holdings = self.broker.get_account_balance()
            
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

        # 🚨 [수술 완료] jobs()[0].data 객체 참조 런타임 즉사 버그 방어
        tracking_cache = {}
        try:
            jobs = context.job_queue.jobs() if context.job_queue else []
            job_data = jobs[0].data if jobs and jobs[0].data is not None else {}
            tracking_cache = job_data.get('sniper_tracking', {})
        except (IndexError, AttributeError):
            tracking_cache = {}

        est = pytz.timezone('US/Eastern')
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
            # 🚨 Lock 외부에서 비동기로 안전하게 I/O 호출
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
            elif ver == "V14" and is_manual_vwap:
                cached_snap = self.strategy.v14_vwap_plugin.load_daily_snapshot(t)
            
            logic_qty = actual_qty
            if cached_snap and "total_q" in cached_snap:
                logic_qty = cached_snap["total_q"]

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
                
                if q_list and logic_qty > 0:
                    l1_qty = q_list[-1].get('qty', 0)
                    l1_price = q_list[-1].get('price', safe_prev_close)
                    
                    target_l1 = round(l1_price * 1.006, 2)
                    v_rev_guidance += f" 🔵 매도1(Pop1) ${target_l1:.2f} <b>{l1_qty}주</b> ({tag})\n"
                    
                    upper_qty = logic_qty - l1_qty
                    if upper_qty > 0:
                        upper_invested = (logic_qty * actual_avg) - (l1_qty * l1_price)
                        # 🚨 [수술 완료] 상위 레이어 평단가 0.0달러 및 손실 매도 폭탄 교정
                        safe_fallback = curr if curr and curr > 0 else actual_avg
                        upper_avg = upper_invested / upper_qty if upper_invested > 0 and upper_qty > 0 else safe_fallback
                        
                        target_upper = round(upper_avg * 1.005, 2)
                        v_rev_guidance += f" 🔵 매도2(Pop2) ${target_upper:.2f} <b>{upper_qty}주</b> ({tag})\n"
                        
                        target_jackpot = round(actual_avg * 1.01, 2)
                        if not is_manual_vwap:
                            v_rev_guidance += f" 🎯 [전체 잭팟] ${target_jackpot:.2f} 돌파 시 <b>{logic_qty}주</b> (옵션)\n"
                else:
                    v_rev_guidance += " 🔵 매도: 대기 물량 없음 (관망)\n"
                
                if safe_prev_close > 0:
                    b1_price = round(safe_prev_close / 0.935 if logic_qty == 0 else safe_prev_close * 0.995, 2)
                    b2_price = round(safe_prev_close * 0.999 if logic_qty == 0 else safe_prev_close * 0.9725, 2)
                    
                    b1_qty = math.floor(half_portion_cash / b1_price) if b1_price > 0 else 0
                    b2_qty = math.floor(half_portion_cash / b2_price) if b2_price > 0 else 0
                    
                    if b1_qty > 0:
                        v_rev_guidance += f" 🔴 매수1(Buy1) ${b1_price:.2f} <b>{b1_qty}주</b> ({tag})\n"
                    if b2_qty > 0:
                        v_rev_guidance += f" 🔴 매수2(Buy2) ${b2_price:.2f} <b>{b2_qty}주</b> ({tag})\n"
                        
                    if logic_qty == 0:
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
                    v_rev_guidance += "작동 시간은 반드시 \n<b>[장 마감 30분 전 ~ 장 마감]</b>\n으로만 세팅하셔야 합니다.\n"
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
                'is_manual_vwap': is_manual_vwap
            })
            
            # 🚨 [수술 완료] plan에 'orders' 키가 없을 경우 KeyError로 뻗어버리는 취약점 차단
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
            res = await self.process_auto_sync(t, chat_id, context, silent_ledger=True)
            if res == "SUCCESS":
                success_tickers.append(t)
        
        if success_tickers: 
            async with self.tx_lock:
                _, holdings = self.broker.get_account_balance()
            await self._display_ledger(success_tickers[0], chat_id, context, message_obj=status_msg, pre_fetched_holdings=holdings)
        else:
            await status_msg.edit_text("✅ <b>동기화 완료</b> (표시할 진행 중인 장부가 없거나 에러 대기 중입니다)", parse_mode='HTML')

    def _sync_escrow_cash(self, ticker):
        is_rev = self.cfg.get_reverse_state(ticker).get("is_active", False)
        if not is_rev:
            self.cfg.clear_escrow_cash(ticker)
            return

        ledger = self.cfg.get_ledger()
        
        target_recs = []
        for r in reversed(ledger):
            if r.get('ticker') == ticker:
                if r.get('is_reverse', False):
                    target_recs.append(r)
                else:
                    break
        
        escrow = 0.0
        for r in target_recs:
            amt = r['qty'] * r['price']
            if r['side'] == 'SELL':
                escrow += amt
            elif r['side'] == 'BUY':
                escrow -= amt
                
        self.cfg.set_escrow_cash(ticker, max(0.0, escrow))

    async def process_auto_sync(self, ticker, chat_id, context, silent_ledger=False):
        if ticker not in self.sync_locks:
            self.sync_locks[ticker] = asyncio.Lock()
            
        if self.sync_locks[ticker].locked(): 
            return "LOCKED"
            
        async with self.sync_locks[ticker]:
            async with self.tx_lock:
                
                last_split_date = self.cfg.get_last_split_date(ticker)
                split_ratio, split_date = await asyncio.to_thread(self.broker.get_recent_stock_split, ticker, last_split_date)
                
                if split_ratio > 0.0 and split_date != "":
                    self.cfg.apply_stock_split(ticker, split_ratio)
                    self.cfg.set_last_split_date(ticker, split_date)
                    split_type = "액면분할" if split_ratio > 1.0 else "액면병합(역분할)"
                    await context.bot.send_message(chat_id, f"✂️ <b>[{ticker}] 야후 파이낸스 {split_type} 자동 감지!</b>\n▫️ 감지된 비율: <b>{split_ratio}배</b> (발생일: {split_date})\n▫️ 봇이 기존 장부의 수량과 평단가를 100% 무인 자동 소급 조정 완료했습니다.", parse_mode='HTML')
                
                kst = pytz.timezone('Asia/Seoul')
                now_kst = datetime.datetime.now(kst)
                
                est = pytz.timezone('US/Eastern')
                now_est = datetime.datetime.now(est)
                nyse = mcal.get_calendar('NYSE')
                schedule = nyse.schedule(start_date=(now_est - datetime.timedelta(days=10)).date(), end_date=now_est.date())
                
                if not schedule.empty:
                    last_trade_date = schedule.index[-1]
                    target_kis_str = last_trade_date.strftime('%Y%m%d')
                    target_ledger_str = last_trade_date.strftime('%Y-%m-%d')
                else:
                    target_kis_str = now_kst.strftime('%Y%m%d')
                    target_ledger_str = now_kst.strftime('%Y-%m-%d')

                _, holdings = self.broker.get_account_balance()
                if holdings is None:
                    await context.bot.send_message(chat_id, f"❌ <b>[{ticker}] API 오류</b>\n잔고를 불러오지 못했습니다.", parse_mode='HTML')
                    return "ERROR"

                actual_qty = int(float(holdings.get(ticker, {'qty': 0}).get('qty') or 0))
                actual_avg = float(holdings.get(ticker, {'avg': 0}).get('avg') or 0.0)

                if self.cfg.get_version(ticker) == "V_REV":
                    if not getattr(self, 'queue_ledger', None):
                        from queue_ledger import QueueLedger
                        self.queue_ledger = QueueLedger()
                    
                    q_data_before = self.queue_ledger.get_queue(ticker)
                    ledger_qty = sum(int(float(item.get("qty") or 0)) for item in q_data_before)
                    
                    if actual_qty == 0 and ledger_qty > 0:
                        added_seed = 0.0
                        try:
                            total_invested = sum(float(item.get("qty", 0)) * float(item.get("price", 0)) for item in q_data_before)
                            q_avg_price = total_invested / ledger_qty if ledger_qty > 0 else 0.0
                            
                            curr_p = await asyncio.to_thread(self.broker.get_current_price, ticker)
                            clear_price = curr_p if curr_p and curr_p > 0 else q_avg_price * 1.006 
                            
                            snapshot = self.strategy.capture_vrev_snapshot(ticker, clear_price, q_avg_price, ledger_qty)
                            
                            if snapshot:
                                realized_pnl = snapshot['realized_pnl']
                                yield_pct = snapshot['realized_pnl_pct']
                                
                                compound_rate = float(self.cfg.get_compound_rate(ticker)) / 100.0
                                if realized_pnl > 0 and compound_rate > 0:
                                    added_seed = realized_pnl * compound_rate
                                    current_seed = self.cfg.get_seed(ticker)
                                    self.cfg.set_seed(ticker, current_seed + added_seed)
                                
                                hist_data = self.cfg._load_json(self.cfg.FILES["HISTORY"], [])
                                new_hist = {
                                    "id": int(time.time()),
                                    "ticker": ticker,
                                    "start_date": q_data_before[-1]['date'][:10] if q_data_before else snapshot['captured_at'].strftime('%Y-%m-%d'),
                                    "end_date": snapshot['captured_at'].strftime('%Y-%m-%d'),
                                    "invested": total_invested,
                                    "revenue": total_invested + realized_pnl,
                                    "profit": realized_pnl,
                                    "yield": yield_pct,
                                    "trades": q_data_before 
                                }
                                hist_data.append(new_hist)
                                self.cfg._save_json(self.cfg.FILES["HISTORY"], hist_data)
                                
                        except Exception as e:
                            logging.error(f"스냅샷 캡처 및 복리 정산 중 오류: {e}")
                            snapshot = None
                            
                        self.queue_ledger.sync_with_broker(ticker, 0)
                        
                        msg = f"🎉 <b>[{ticker} V-REV 잭팟 스윕(전량 익절) 감지!]</b>\n▫️ 잔고가 0주가 되어 LIFO 큐 지층을 100% 소각(초기화)했습니다."
                        if added_seed > 0:
                            msg += f"\n💸 <b>자동 복리 +${added_seed:,.0f}</b> 이 다음 운용 시드에 완벽하게 추가되었습니다!"
                        await context.bot.send_message(chat_id, msg, parse_mode='HTML')
                        
                        if snapshot:
                            try:
                                import shutil
                                current_dir = os.getcwd()
                                bg_path = os.path.join(current_dir, "background.png")
                                
                                if not os.path.exists("background.png") and os.path.exists(bg_path):
                                    shutil.copy(bg_path, "background.png")
                                    
                                img_path = self.view.create_profit_image(
                                    ticker=ticker, 
                                    profit=snapshot['realized_pnl'], 
                                    yield_pct=snapshot['realized_pnl_pct'],
                                    invested=snapshot['avg_price'] * snapshot['cleared_qty'], 
                                    revenue=snapshot['clear_price'] * snapshot['cleared_qty'], 
                                    end_date=snapshot['captured_at'].strftime('%Y-%m-%d')
                                )
                                if img_path and os.path.exists(img_path):
                                    with open(img_path, 'rb') as photo:
                                        await context.bot.send_photo(chat_id=chat_id, photo=photo)
                            except Exception as e:
                                logging.error(f"📸 V-REV 스냅샷 이미지 렌더링/발송 실패: {e}")
                                
                        self._sync_escrow_cash(ticker)
                        return "SUCCESS"
                        
                    calibrated = self.queue_ledger.sync_with_broker(ticker, actual_qty, actual_avg)
                    if calibrated:
                        await context.bot.send_message(chat_id, f"🔧 <b>[{ticker}] V-REV 큐(Queue) 비파괴 보정(CALIB) 완료!</b>\n▫️ KIS 실제 잔고(<b>{actual_qty}주</b>)에 맞춰 LIFO 지층을 정밀 차감/추가했습니다.", parse_mode='HTML')
                    
                    self._sync_escrow_cash(ticker)
                    return "SUCCESS"

                target_execs = await asyncio.to_thread(self.broker.get_execution_history, ticker, target_kis_str, target_kis_str)
                if target_execs:
                    calibrated_count = self.cfg.calibrate_ledger_prices(ticker, target_ledger_str, target_execs)
                    if calibrated_count > 0:
                        logging.info(f"🔧 [{ticker}] LOC/MOC 주문 {calibrated_count}건에 대해 실제 체결 단가 소급 업데이트를 완료했습니다.")

                recs = [r for r in self.cfg.get_ledger() if r['ticker'] == ticker]
                ledger_qty, avg_price, _, _ = self.cfg.calculate_holdings(ticker, recs)
                
                diff = actual_qty - ledger_qty
                price_diff = abs(actual_avg - avg_price)

                if actual_qty == 0:
                    if ledger_qty > 0:
                        kst = pytz.timezone('Asia/Seoul')
                        today_str = datetime.datetime.now(kst).strftime('%Y-%m-%d')
                        prev_c = await asyncio.to_thread(self.broker.get_previous_close, ticker)
                        
                        try:
                            new_hist, added_seed = self.cfg.archive_graduation(ticker, today_str, prev_c)
                            
                            if new_hist:
                                msg = f"🎉 <b>[{ticker} 졸업 확인!]</b>\n장부를 명예의 전당에 저장하고 새 사이클을 준비합니다."
                                if added_seed > 0:
                                    msg += f"\n💸 <b>자동 복리 +${added_seed:,.0f}</b> 이 다음 운용 시드에 완벽하게 추가되었습니다!"
                                await context.bot.send_message(chat_id, msg, parse_mode='HTML')
                                try:
                                    import shutil
                                    current_dir = os.getcwd()
                                    bg_path = os.path.join(current_dir, "background.png")
                                    
                                    if not os.path.exists("background.png") and os.path.exists(bg_path):
                                        shutil.copy(bg_path, "background.png")
                                        
                                    img_path = self.view.create_profit_image(
                                        ticker=ticker, profit=new_hist['profit'], yield_pct=new_hist['yield'],
                                        invested=new_hist['invested'], revenue=new_hist['revenue'], end_date=new_hist['end_date']
                                    )
                                    if img_path and os.path.exists(img_path):
                                        with open(img_path, 'rb') as photo:
                                            await context.bot.send_photo(chat_id=chat_id, photo=photo)
                                except Exception as e:
                                    logging.error(f"📸 졸업 이미지 발송 실패: {e}")
                            else:
                                all_recs = [r for r in self.cfg.get_ledger() if r['ticker'] != ticker]
                                self.cfg._save_json(self.cfg.FILES["LEDGER"], all_recs)
                                await context.bot.send_message(chat_id, f"⚠️ <b>[{ticker} 강제 정산 완료]</b>\n잔고가 0주이나 마이너스 수익 상태이므로 명예의 전당 박제 없이 장부를 비우고 새출발 타점을 장전합니다.", parse_mode='HTML')
                        except Exception as e:
                            all_recs = [r for r in self.cfg.get_ledger() if r['ticker'] != ticker]
                            self.cfg._save_json(self.cfg.FILES["LEDGER"], all_recs)
                            logging.error(f"강제 졸업 처리 중 에러: {e}")

                    self._sync_escrow_cash(ticker) 
                    return "SUCCESS"

                if diff == 0 and price_diff < 0.01:
                    pass 
                elif diff == 0 and price_diff >= 0.01:
                    self.cfg.calibrate_avg_price(ticker, actual_avg)
                    await context.bot.send_message(chat_id, f"🔧 <b>[{ticker}] 장부 평단가 미세 오차({price_diff:.4f}) 교정 완료!</b>", parse_mode='HTML')
                elif diff != 0:
                    temp_recs = [r for r in recs if r['date'] != target_ledger_str or 'INIT' in str(r.get('exec_id', ''))]
                    temp_qty, temp_avg, _, _ = self.cfg.calculate_holdings(ticker, temp_recs)
                    
                    temp_sim_qty = temp_qty
                    temp_sim_avg = temp_avg
                    new_target_records = []
                    
                    if target_execs:
                        target_execs.sort(key=lambda x: x.get('ord_tmd', '000000')) 
                        for ex in target_execs:
                            side_cd = ex.get('sll_buy_dvsn_cd')
                            exec_qty = int(float(ex.get('ft_ccld_qty', '0')))
                            exec_price = float(ex.get('ft_ccld_unpr3', '0'))
                            
                            if side_cd == "02": 
                                new_avg = ((temp_sim_qty * temp_sim_avg) + (exec_qty * exec_price)) / (temp_sim_qty + exec_qty) if (temp_sim_qty + exec_qty) > 0 else exec_price
                                temp_sim_qty += exec_qty
                                temp_sim_avg = new_avg
                            else:
                                temp_sim_qty -= exec_qty
                                
                            new_target_records.append({
                                'date': target_ledger_str, 'side': "BUY" if side_cd == "02" else "SELL",
                                'qty': exec_qty, 'price': exec_price, 'avg_price': temp_sim_avg
                            })
                            
                    gap_qty = actual_qty - temp_sim_qty
                    if gap_qty != 0:
                        calib_side = "BUY" if gap_qty > 0 else "SELL"
                        new_target_records.append({
                            'date': target_ledger_str, 
                            'side': calib_side,
                            'qty': abs(gap_qty), 
                            'price': actual_avg, 
                            'avg_price': actual_avg,
                            'exec_id': f"CALIB_{int(time.time())}",
                            'desc': "비파괴 보정"
                        })
                        
                    if new_target_records:
                        for r in new_target_records:
                            r['avg_price'] = actual_avg
                    elif temp_recs: 
                        temp_recs[-1]['avg_price'] = actual_avg
                        
                    self.cfg.overwrite_incremental_ledger(ticker, temp_recs, new_target_records)
                    
                    if gap_qty != 0:
                        await context.bot.send_message(chat_id, f"🔧 <b>[{ticker}] 비파괴 장부 보정 완료!</b>\n▫️ 오차 수량({gap_qty}주)을 기존 역사 보존 상태로 안전하게 교정했습니다.", parse_mode='HTML')

                self._sync_escrow_cash(ticker)
                return "SUCCESS"

    async def _display_ledger(self, ticker, chat_id, context, query=None, message_obj=None, pre_fetched_holdings=None):
        recs = [r for r in self.cfg.get_ledger() if r['ticker'] == ticker]
        
        if not recs:
            msg = f"📭 <b>[{ticker}]</b> 현재 진행 중인 사이클이 없습니다 (보유량 0주)."
        else:
            from collections import OrderedDict
            agg_dict = OrderedDict()
            total_buy = 0.0
            total_sell = 0.0
            
            for rec in recs:
                parts = rec['date'].split('-')
                if len(parts) == 3:
                    date_short = f"{parts[1]}.{parts[2]}"
                else:
                    date_short = rec['date']
                    
                side_str = "🔴매수" if rec['side'] == 'BUY' else "🔵매도"
                key = (date_short, side_str)
                
                if key not in agg_dict:
                    agg_dict[key] = {'qty': 0, 'amt': 0.0}
                    
                agg_dict[key]['qty'] += rec['qty']
                agg_dict[key]['amt'] += (rec['qty'] * rec['price'])
                
                if rec['side'] == 'BUY':
                    total_buy += (rec['qty'] * rec['price'])
                elif rec['side'] == 'SELL':
                    total_sell += (rec['qty'] * rec['price'])
            
            report = f"📜 <b>[ {ticker} 일자별 매매 (통합 변동분) (총 {len(agg_dict)}일) ]</b>\n\n<code>No. 일자   구분  평균단가  수량\n"
            report += "-"*30 + "\n"
            
            idx = 1
            for (date, side), data in agg_dict.items():
                tot_qty = data['qty']
                avg_prc = data['amt'] / tot_qty if tot_qty > 0 else 0.0
                report += f"{idx:<3} {date} {side} ${avg_prc:<6.2f} {tot_qty}주\n"
                idx += 1
                
            report += "-"*30 + "</code>\n"
            
            actual_qty = int(float(pre_fetched_holdings.get(ticker, {'qty': 0})['qty'] or 0)) if pre_fetched_holdings else 0
            actual_avg = float(pre_fetched_holdings.get(ticker, {'avg': 0})['avg'] or 0.0) if pre_fetched_holdings else 0.0
            
            split = self.cfg.get_split_count(ticker)
            t_val, _ = self.cfg.get_absolute_t_val(ticker, actual_qty, actual_avg)
            
            report += "📊 <b>[ 현재 진행 상황 요약 ]</b>\n"
            report += f"▪️ 현재 T값 : {t_val:.4f} T ({int(split)}분할)\n"
            report += f"▪️ 보유 수량 : {actual_qty} 주 (평단 ${actual_avg:,.2f})\n"
            report += f"▪️ 총 매수액 : ${total_buy:,.2f}\n"
            report += f"▪️ 총 매도액 : ${total_sell:,.2f}"
            
            msg = report

        tickers = self.cfg.get_active_tickers()
        keyboard = []
        
        if self.cfg.get_version(ticker) == "V_REV":
            keyboard.append([InlineKeyboardButton(f"🗄️ {ticker} V-REV 큐(Queue) 정밀 관리", callback_data=f"QUEUE:VIEW:{ticker}")])
            
        row = [InlineKeyboardButton(f"🔄 {t} 장부 업데이트", callback_data=f"REC:SYNC:{t}") for t in tickers]
        keyboard.append(row)
        markup = InlineKeyboardMarkup(keyboard)

        if query:
            await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
        elif message_obj:
            await message_obj.edit_text(msg, reply_markup=markup, parse_mode='HTML')
        else:
            await context.bot.send_message(chat_id, msg, reply_markup=markup, parse_mode='HTML')

    async def cmd_history(self, update, context):
        if not self._is_admin(update):
            return
            
        history = self.cfg.get_history()
        if not history:
            await update.message.reply_text("📜 저장된 역사가 없습니다.")
            return
            
        msg = "🏆 <b>[ 졸업 명예의 전당 ]</b>\n"
        keyboard = [[InlineKeyboardButton(f"{h['end_date']} | {h['ticker']} (+${h['profit']:.0f})", callback_data=f"HIST:VIEW:{h['id']}")] for h in history]
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
        
        est = pytz.timezone('US/Eastern')
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

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data.split(":")
        action, sub = data[0], data[1] if len(data) > 1 else ""

        if action == "QUEUE":
            if sub == "VIEW":
                ticker = data[2]
                if getattr(self, 'queue_ledger', None):
                    q_data = self.queue_ledger.get_queue(ticker)
                else:
                    q_data = []
                    try:
                        if os.path.exists("data/queue_ledger.json"):
                            with open("data/queue_ledger.json", "r", encoding='utf-8') as f:
                                q_data = json.load(f).get(ticker, [])
                    except Exception:
                        pass
                        
                msg, markup = self.view.get_queue_management_menu(ticker, q_data)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')

        elif action == "EMERGENCY_REQ":
            ticker = sub
            
            status_code, _ = self._get_market_status()
            if status_code not in ["PRE", "REG"]:
                await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                return
                
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                self.queue_ledger = QueueLedger()
                
            q_data = self.queue_ledger.get_queue(ticker)
            total_q = sum(item.get("qty", 0) for item in q_data)
            
            if total_q == 0:
                await query.answer("⚠️ 큐(Queue)가 텅 비어있어 수혈할 잔여 물량이 없습니다.", show_alert=True)
                return
            
            emergency_qty = q_data[-1].get('qty', 0)
            emergency_price = q_data[-1].get('price', 0.0)
            
            msg, markup = self.view.get_emergency_moc_confirm_menu(ticker, emergency_qty, emergency_price)
            await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')

        elif action == "EMERGENCY_EXEC":
            ticker = sub
            status_code, _ = self._get_market_status()
            
            if status_code not in ["PRE", "REG"]:
                await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                return
                
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                self.queue_ledger = QueueLedger()
                
            q_data = self.queue_ledger.get_queue(ticker)
            if not q_data:
                await query.answer("⚠️ 큐(Queue)가 텅 비어있어 수혈할 잔여 물량이 없습니다.", show_alert=True)
                return
                
            await query.answer("⏳ KIS 서버에 수동 긴급 수혈(MOC) 명령을 격발합니다...", show_alert=False)
            
            emergency_qty = q_data[-1].get('qty', 0)
            
            if emergency_qty > 0:
                async with self.tx_lock:
                    res = self.broker.send_order(ticker, "SELL", emergency_qty, 0.0, "MOC")
                    
                    if res.get('rt_cd') == '0':
                        self.queue_ledger.pop_lots(ticker, emergency_qty)
                        
                        msg = f"🚨 <b>[{ticker}] 수동 긴급 수혈 (Emergency MOC) 격발 완료!</b>\n"
                        msg += f"▫️ 포트폴리오 매니저의 승인 하에 최근 로트 <b>{emergency_qty}주</b>를 시장가(MOC)로 강제 청산했습니다.\n"
                        await context.bot.send_message(chat_id=update.effective_chat.id, text=msg, parse_mode='HTML')
                        
                        new_q_data = self.queue_ledger.get_queue(ticker)
                        new_msg, markup = self.view.get_queue_management_menu(ticker, new_q_data)
                        await query.edit_message_text(new_msg, reply_markup=markup, parse_mode='HTML')
                    else:
                        err_msg = res.get('msg1', '알 수 없는 에러')
                        await query.edit_message_text(f"❌ <b>[{ticker}] 수동 긴급 수혈 실패:</b> {err_msg}", parse_mode='HTML')

        elif action == "DEL_REQ":
            ticker = sub
            target_date = ":".join(data[2:])
            
            q_data = self.queue_ledger.get_queue(ticker) if getattr(self, 'queue_ledger', None) else []
            if not q_data:
                try:
                    with open("data/queue_ledger.json", "r") as f:
                        q_data = json.load(f).get(ticker, [])
                except Exception:
                    pass
            
            qty, price = 0, 0.0
            for item in q_data:
                if item.get('date') == target_date:
                    qty = item.get('qty', 0)
                    price = item.get('price', 0.0)
                    break
                    
            msg, markup = self.view.get_queue_action_confirm_menu(ticker, target_date, qty, price)
            await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')

        elif action in ["DEL_Q", "EDIT_Q"]:
            ticker = sub
            target_date = ":".join(data[2:])
            
            try:
                q_file = "data/queue_ledger.json"
                all_q = {}
                if os.path.exists(q_file):
                    with open(q_file, 'r', encoding='utf-8') as f:
                        all_q = json.load(f)
                
                ticker_q = all_q.get(ticker, [])
                
                if action == "DEL_Q":
                    new_q = [item for item in ticker_q if item.get('date') != target_date]
                    await self._verify_and_update_queue(ticker, new_q, context, query.message.chat_id)
                    await query.answer("✅ 삭제 완료.", show_alert=False)
                    
                    msg, markup = self.view.get_queue_management_menu(ticker, new_q)
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                    
                elif action == "EDIT_Q":
                    await query.answer("✏️ 수정 모드 진입", show_alert=False)
                    short_date = target_date[:10]
                    self.user_states[update.effective_chat.id] = f"EDITQ_{ticker}_{target_date}"
                    
                    prompt = f"✏️ <b>[{ticker} 지층 수정 모드]</b>\n"
                    prompt += f"선택하신 <b>[{short_date}]</b> 지층을 재설정합니다.\n\n"
                    prompt += "새로운 <b>[수량]</b>과 <b>[평단가]</b>를 띄어쓰기로 입력하세요.\n"
                    prompt += "(예: <code>229 52.16</code>)\n\n"
                    prompt += "<i>(입력을 취소하려면 숫자 이외의 문자를 보내주세요)</i>"
                    await query.edit_message_text(prompt, parse_mode='HTML')
            except Exception as e:
                await query.answer(f"❌ 처리 중 에러 발생: {e}", show_alert=True)

        elif action == "VERSION":
            history_data = self.cfg.get_full_version_history()
            if sub == "LATEST":
                msg, markup = self.view.get_version_message(history_data, page_index=None)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            elif sub == "PAGE":
                page_idx = int(data[2])
                msg, markup = self.view.get_version_message(history_data, page_index=page_idx)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')

        elif action == "RESET":
            if sub == "MENU":
                active_tickers = self.cfg.get_active_tickers()
                msg, markup = self.view.get_reset_menu(active_tickers)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            elif sub == "LOCK": 
                ticker = data[2]
                self.cfg.reset_lock_for_ticker(ticker)
                await query.edit_message_text(f"✅ <b>[{ticker}] 금일 매매 잠금이 해제되었습니다.</b>", parse_mode='HTML')
            elif sub == "REV":
                ticker = data[2]
                msg, markup = self.view.get_reset_confirm_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            elif sub == "CONFIRM":
                ticker = data[2]
                
                self.cfg.set_reverse_state(ticker, False, 0)
                self.cfg.clear_escrow_cash(ticker) 
                
                ledger_data = [r for r in self.cfg.get_ledger() if r.get('ticker') != ticker]
                self.cfg._save_json(self.cfg.FILES["LEDGER"], ledger_data)
                
                backup_file = self.cfg.FILES["LEDGER"].replace(".json", "_backup.json")
                if os.path.exists(backup_file):
                    try:
                        with open(backup_file, 'r', encoding='utf-8') as f:
                            b_data = json.load(f)
                        b_data = [r for r in b_data if r.get('ticker') != ticker]
                        with open(backup_file, 'w', encoding='utf-8') as f:
                            json.dump(b_data, f, ensure_ascii=False, indent=4)
                    except Exception:
                        pass
                
                q_file = "data/queue_ledger.json"
                if os.path.exists(q_file):
                    try:
                        with open(q_file, 'r', encoding='utf-8') as f:
                            q_data = json.load(f)
                        if ticker in q_data:
                            del q_data[ticker]
                        with open(q_file, 'w', encoding='utf-8') as f:
                            json.dump(q_data, f, ensure_ascii=False, indent=4)
                    except Exception:
                        pass
                    
                if getattr(self, 'queue_ledger', None) and hasattr(self.queue_ledger, 'queues') and ticker in self.queue_ledger.queues:
                    del self.queue_ledger.queues[ticker]
                    
                await query.edit_message_text(f"✅ <b>[{ticker}] 삼위일체 소각(Nuke) 및 초기화 완료!</b>\n▫️ 본장부, 백업장부, 큐(Queue), 에스크로의 찌꺼기 데이터가 100% 영구 삭제되었습니다.\n▫️ 다음 매수 진입 시 0주 새출발 디커플링 타점 모드로 완벽히 재시작합니다.", parse_mode='HTML')
            
            elif sub == "CANCEL":
                await query.edit_message_text("❌ 안전 통제실 메뉴를 닫습니다.", parse_mode='HTML')

        elif action == "REC":
            if sub == "VIEW": 
                async with self.tx_lock:
                    _, holdings = self.broker.get_account_balance()
                await self._display_ledger(data[2], update.effective_chat.id, context, query=query, pre_fetched_holdings=holdings)
            elif sub == "SYNC": 
                ticker = data[2]
                
                if ticker not in self.sync_locks:
                    self.sync_locks[ticker] = asyncio.Lock()
                    
                if not self.sync_locks[ticker].locked():
                    await query.edit_message_text(f"🔄 <b>[{ticker}] 잔고 기반 대시보드 업데이트 중...</b>", parse_mode='HTML')
                    res = await self.process_auto_sync(ticker, update.effective_chat.id, context, silent_ledger=True)
                    if res == "SUCCESS": 
                        async with self.tx_lock:
                            _, holdings = self.broker.get_account_balance()
                        await self._display_ledger(ticker, update.effective_chat.id, context, message_obj=query.message, pre_fetched_holdings=holdings)

        elif action == "HIST":
            if sub == "VIEW":
                hid = int(data[2])
                target = next((h for h in self.cfg.get_history() if h['id'] == hid), None)
                if target:
                    safe_trades = target.get('trades', [])
                    for t_rec in safe_trades:
                        if 'ticker' not in t_rec:
                            t_rec['ticker'] = target['ticker']
                        if 'side' not in t_rec:
                            t_rec['side'] = 'BUY'
                            
                    qty, avg, invested, sold = self.cfg.calculate_holdings(target['ticker'], safe_trades)
                    msg, markup = self.view.create_ledger_dashboard(target['ticker'], qty, avg, invested, sold, safe_trades, 0, 0, is_history=True)
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            elif sub == "LIST":
                await self.cmd_history(update, context)
            elif sub == "IMG":
                ticker = data[2]
                hist_list = [h for h in self.cfg.get_history() if h['ticker'] == ticker]
                
                if not hist_list:
                    await context.bot.send_message(update.effective_chat.id, f"📭 <b>[{ticker}]</b> 발급 가능한 졸업 기록이 존재하지 않습니다.", parse_mode='HTML')
                    return
                
                latest_hist = sorted(hist_list, key=lambda x: x.get('end_date', ''), reverse=True)[0]
                
                try:
                    import shutil
                    current_dir = os.getcwd()
                    bg_path = os.path.join(current_dir, "background.png")
                    
                    if not os.path.exists("background.png") and os.path.exists(bg_path):
                        shutil.copy(bg_path, "background.png")
                        
                    img_path = self.view.create_profit_image(
                        ticker=latest_hist['ticker'],
                        profit=latest_hist['profit'],
                        yield_pct=latest_hist['yield'],
                        invested=latest_hist['invested'],
                        revenue=latest_hist['revenue'],
                        end_date=latest_hist['end_date']
                    )
                    if os.path.exists(img_path):
                        with open(img_path, 'rb') as photo:
                            await context.bot.send_photo(chat_id=update.effective_chat.id, photo=photo)
                except Exception as e:
                    logging.error(f"📸 👑 졸업 이미지 생성/발송 실패: {e}")
                    await context.bot.send_message(update.effective_chat.id, "❌ 이미지 렌더링 모듈 장애 발생.", parse_mode='HTML')
            
        elif action == "EXEC":
            t = sub
            ver = self.cfg.get_version(t)
            
            if ver == "V_REV" and getattr(self.cfg, 'get_manual_vwap_mode', lambda x: False)(t):
                await query.answer("🚨 [격발 차단] 수동 VWAP 모드가 가동 중입니다. 지시서를 참고하여 한투 앱(V앱)에서 직접 매매를 걸어주십시오.", show_alert=True)
                return
            
            await query.edit_message_text(f"🚀 {t} 수동 강제 전송 시작 (교차 분리)...")
            async with self.tx_lock:
                cash, holdings = self.broker.get_account_balance()
                
            if holdings is None:
                return await query.edit_message_text("❌ API 통신 오류로 주문을 실행할 수문을 실행할 수 없습니다.")
                
            _, allocated_cash = self._calculate_budget_allocation(cash, self.cfg.get_active_tickers())
            h = holdings.get(t, {'qty':0, 'avg':0})
            
            curr_p = float(await asyncio.to_thread(self.broker.get_current_price, t) or 0.0)
            prev_c = float(await asyncio.to_thread(self.broker.get_previous_close, t) or 0.0)
            safe_avg = float(h.get('avg') or 0.0)
            safe_qty = int(float(h.get('qty') or 0))

            status_code, _ = self._get_market_status()
            
            if status_code in ["AFTER", "CLOSE", "PRE"]:
                try:
                    def get_yf_close():
                        df = yf.Ticker(t).history(period="5d", interval="1d")
                        return float(df['Close'].iloc[-1]) if not df.empty else None
                    yf_close = await asyncio.wait_for(asyncio.to_thread(get_yf_close), timeout=3.0)
                    if yf_close and yf_close > 0:
                        prev_c = yf_close
                except Exception as e:
                    logging.debug(f"YF 정규장 종가 롤오버 스캔 실패 ({t}): {e}")
                    if curr_p > 0 and prev_c == 0.0:
                        prev_c = curr_p

            if ver == "V_REV":
                if not getattr(self, 'queue_ledger', None):
                    from queue_ledger import QueueLedger
                    self.queue_ledger = QueueLedger()
                    
                q_data = self.queue_ledger.get_queue(t)
                
                cached_snap = self.strategy.v_rev_plugin.load_daily_snapshot(t)
                logic_qty = safe_qty
                if cached_snap and "total_q" in cached_snap:
                    logic_qty = cached_snap["total_q"]

                rev_budget = float(self.cfg.get_seed(t) or 0.0) * 0.15
                half_portion_cash = rev_budget * 0.5
                
                loc_orders = []
                
                if q_data and logic_qty > 0:
                    dates_in_queue = sorted(list(set(item.get('date') for item in q_data if item.get('date'))), reverse=True)
                    l1_qty = 0
                    l1_price = 0.0
                    if dates_in_queue:
                        lots_1 = [item for item in q_data if item.get('date') == dates_in_queue[0]]
                        l1_qty = sum(item.get('qty', 0) for item in lots_1)
                        if l1_qty > 0:
                            l1_price = sum(item.get('qty', 0) * item.get('price', 0.0) for item in lots_1) / l1_qty
                    
                    target_l1 = round(l1_price * 1.006, 2)
                    
                    if l1_qty > 0:
                        loc_orders.append({'side': 'SELL', 'qty': l1_qty, 'price': target_l1, 'type': 'LOC', 'desc': '[1층 단독]'})
                        
                    upper_qty = logic_qty - l1_qty
                    if upper_qty > 0:
                        # 🚨 [수술 완료] API가 유효한 평단가를 주지 않았을 경우 0.0달러 매도 폭탄 발사 차단
                        if safe_avg <= 0.0:
                            msg = f"🚨 <b>[{t}] 수동 장전 차단:</b> KIS API가 유효한 평단가를 반환하지 않았습니다 (avg=0). 주문을 취소합니다."
                            await context.bot.send_message(update.effective_chat.id, msg, parse_mode='HTML')
                            return

                        upper_invested = (logic_qty * safe_avg) - (l1_qty * l1_price)
                        # 🚨 [수술 완료] 상위 레이어 평단가가 0 이하일 때 안전한 현재가(혹은 L1가)로 폴백 처리
                        if upper_invested > 0 and upper_qty > 0:
                            upper_avg = upper_invested / upper_qty
                        else:
                            upper_avg = l1_price
                            logging.warning(f"[{t}] upper_invested={upper_invested:.2f} <= 0. Falling back to l1_price={l1_price:.2f} for upper_avg.")
                            
                        target_upper = round(upper_avg * 1.005, 2)
                        loc_orders.append({'side': 'SELL', 'qty': upper_qty, 'price': target_upper, 'type': 'LOC', 'desc': '[상위 재고]'})
                
                if prev_c > 0:
                    b1_price = round(prev_c / 0.935 if logic_qty == 0 else prev_c * 0.995, 2)
                    b2_price = round(prev_c * 0.999 if logic_qty == 0 else prev_c * 0.9725, 2)
                    
                    b1_qty = math.floor(half_portion_cash / b1_price) if b1_price > 0 else 0
                    b2_qty = math.floor(half_portion_cash / b2_price) if b2_price > 0 else 0
                    
                    if b1_qty > 0:
                        loc_orders.append({'side': 'BUY', 'qty': b1_qty, 'price': b1_price, 'type': 'LOC', 'desc': '예방적 매수(Buy1)'})
                    if b2_qty > 0:
                        loc_orders.append({'side': 'BUY', 'qty': b2_qty, 'price': b2_price, 'type': 'LOC', 'desc': '예방적 매수(Buy2)'})
                        
                    if logic_qty == 0:
                        pass 
                    elif b2_qty > 0 and b2_price > 0:
                        for n in range(1, 6):
                            grid_p = round(half_portion_cash / (b2_qty + n), 2)
                            if grid_p >= 0.01 and grid_p < b2_price:
                                loc_orders.append({'side': 'BUY', 'qty': 1, 'price': grid_p, 'type': 'LOC', 'desc': f'예방적 줍줍({n})'})

                msg = f"🛡️ <b>[{t}] V-REV 예방적 양방향 LOC 방어선 수동 장전 완료</b>\n"
                
                if logic_qty == 0:
                    msg += "🚫 <code>[0주 새출발] 기준 평단가 부재로 줍줍 생략 (1층 확보에 예산 100% 집중)</code>\n"
                    
                all_success = True
                for o in loc_orders:
                    res = self.broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                    is_success = res.get('rt_cd') == '0'
                    if not is_success:
                        all_success = False
                        
                    err_msg = res.get('msg1', '오류')
                    status_icon = '✅' if is_success else f'❌({err_msg})'
                    msg += f"└ {o['desc']} {o['qty']}주 (${o['price']}): {status_icon}\n"
                    await asyncio.sleep(0.2)
                    
                if all_success and len(loc_orders) > 0:
                    self.cfg.set_lock(t, "REG")
                    msg += "\n🔒 <b>방어선 전송 완료 (매매 잠금 설정됨)</b>"
                elif len(loc_orders) == 0:
                    msg += "\n⚠️ <b>전송할 방어선(예산/수량)이 없습니다.</b>"
                else:
                    msg += "\n⚠️ <b>일부 방어선 구축 실패 (잠금 보류)</b>"
                    
                await context.bot.send_message(update.effective_chat.id, msg, parse_mode='HTML')
                return
            
            ma_5day = await asyncio.to_thread(self.broker.get_5day_ma, t)
            
            logic_qty_v14 = safe_qty
            is_manual_vwap = getattr(self.cfg, 'get_manual_vwap_mode', lambda x: False)(t)
            if is_manual_vwap:
                cached_snap_v14 = self.strategy.v14_vwap_plugin.load_daily_snapshot(t)
                if cached_snap_v14 and "total_q" in cached_snap_v14:
                    logic_qty_v14 = cached_snap_v14["total_q"]

            plan = self.strategy.get_plan(t, curr_p, safe_avg, logic_qty_v14, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t], is_simulation=True)
            
            title = f"💎 <b>[{t}] 무매4 정규장 주문 수동 실행</b>\n"
            msg = title
            
            all_success = True
            
            for o in plan.get('core_orders', []):
                res = self.broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                is_success = res.get('rt_cd') == '0'
                if not is_success:
                    all_success = False
                    
                err_msg = res.get('msg1', '오류')
                status_icon = '✅' if is_success else f'❌({err_msg})'
                msg += f"└ 1차 필수: {o['desc']} {o['qty']}주: {status_icon}\n"
                await asyncio.sleep(0.2) 
                
            for o in plan.get('bonus_orders', []):
                res = self.broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                is_success = res.get('rt_cd') == '0'
                err_msg = res.get('msg1', '잔금패스')
                status_icon = '✅' if is_success else f'❌({err_msg})'
                msg += f"└ 2차 보너스: {o['desc']} {o['qty']}주: {status_icon}\n"
                await asyncio.sleep(0.2) 
            
            if all_success and len(plan.get('core_orders', [])) > 0:
                self.cfg.set_lock(t, "REG")
                msg += "\n🔒 <b>필수 주문 전송 완료 (잠금 설정됨)</b>"
            else:
                msg += "\n⚠️ <b>일부 필수 주문 실패 (매매 잠금 보류)</b>"

            await context.bot.send_message(update.effective_chat.id, msg, parse_mode='HTML')

        elif action == "SET_VER":
            new_ver = sub
            ticker = data[2]
            
            if new_ver == "V_REV":
                if not (os.path.exists("strategy_reversion.py") and os.path.exists("queue_ledger.py")):
                    await query.answer("🚨 [개봉박두] V-REV 엔진 모듈 파일이 존재하지 않아 전환할 수 없습니다! (업데이트 필요)", show_alert=True)
                    return
                msg, markup = self.view.get_vrev_mode_selection_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                return
            
            elif new_ver == "V14":
                msg, markup = self.view.get_v14_mode_selection_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                return
                
            self.cfg.set_version(ticker, new_ver)
            self.cfg.set_upward_sniper_mode(ticker, False)
            if hasattr(self.cfg, 'set_avwap_hybrid_mode'):
                self.cfg.set_avwap_hybrid_mode(ticker, False)
            if hasattr(self.cfg, 'set_manual_vwap_mode'):
                self.cfg.set_manual_vwap_mode(ticker, False)
                
            await query.edit_message_text(f"✅ <b>[{ticker}]</b> 퀀트 엔진이 <b>V14 무매4</b> 모드로 전환되었습니다.\n▫️ /sync 명령어에서 변경된 지시서를 확인하세요.", parse_mode='HTML')

        elif action == "SET_VER_CONFIRM":
            mode_type = sub 
            ticker = data[2]
            
            if mode_type in ["AUTO", "MANUAL"]:
                self.cfg.set_version(ticker, "V_REV")
                self.cfg.set_upward_sniper_mode(ticker, False)
                if hasattr(self.cfg, 'set_avwap_hybrid_mode'):
                    self.cfg.set_avwap_hybrid_mode(ticker, False)
                    
                if mode_type == "MANUAL":
                    self.cfg.set_manual_vwap_mode(ticker, True)
                    mode_txt = "🖐️ 수동 VWAP 모드 (수수료 회피)"
                else:
                    self.cfg.set_manual_vwap_mode(ticker, False)
                    mode_txt = "🤖 API 자동매매 모드 (1분 정밀타격)"
                    
                await query.edit_message_text(f"✅ <b>[{ticker}]</b> 퀀트 엔진이 <b>V_REV 역추세 하이브리드</b>로 전환되었습니다.\n▫️ <b>운용 방식:</b> {mode_txt}\n▫️ /sync 지시서를 확인해 주십시오.", parse_mode='HTML')
            
            elif mode_type in ["V14_LOC", "V14_VWAP"]:
                self.cfg.set_version(ticker, "V14")
                self.cfg.set_upward_sniper_mode(ticker, False)
                if hasattr(self.cfg, 'set_avwap_hybrid_mode'):
                    self.cfg.set_avwap_hybrid_mode(ticker, False)
                    
                if mode_type == "V14_VWAP":
                    self.cfg.set_manual_vwap_mode(ticker, True)
                    mode_txt = "🕒 VWAP 타임 슬라이싱 (유동성 추적)"
                else:
                    self.cfg.set_manual_vwap_mode(ticker, False)
                    mode_txt = "📉 LOC 단일 타격 (초안정성)"
                    
                await query.edit_message_text(f"✅ <b>[{ticker}]</b> 퀀트 엔진이 <b>V14 무매4</b> 모드로 전환되었습니다.\n▫️ <b>집행 방식:</b> {mode_txt}\n▫️ /sync 명령어에서 변경된 지시서를 확인하세요.", parse_mode='HTML')

        elif action == "MODE":
            mode_val = sub
            ticker = data[2] if len(data) > 2 else "SOXL"
            
            if mode_val == "AVWAP_WARN":
                msg, markup = self.view.get_avwap_warning_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                return
            elif mode_val == "AVWAP_ON":
                if hasattr(self.cfg, 'set_avwap_hybrid_mode'):
                    self.cfg.set_avwap_hybrid_mode(ticker, True)
                self.cfg.set_upward_sniper_mode(ticker, False) 
                await query.edit_message_text(f"🔥 <b>[{ticker}] 차세대 AVWAP 하이브리드 암살자 모드가 락온(Lock-on) 되었습니다!</b>\n▫️ 남은 가용 예산 100%를 활용하여 장중 -2% 타점을 정밀 사냥합니다.", parse_mode='HTML')
                return
            elif mode_val == "AVWAP_OFF":
                if hasattr(self.cfg, 'set_avwap_hybrid_mode'):
                    self.cfg.set_avwap_hybrid_mode(ticker, False)
                await query.edit_message_text(f"🛑 <b>[{ticker}] 차세대 AVWAP 하이브리드 전술이 즉시 해제되었습니다.</b>", parse_mode='HTML')
                return

            current_ver = self.cfg.get_version(ticker)
            if current_ver == "V_REV" and mode_val == "ON":
                await query.answer(f"🚨 {current_ver} 모드에서는 로직 충돌 방지를 위해 상방 스나이퍼를 켤 수 없습니다!", show_alert=True)
                return
                
            self.cfg.set_upward_sniper_mode(ticker, mode_val == "ON")
            await query.edit_message_text(f"✅ <b>[{ticker}]</b> 상방 스나이퍼 모드 변경 완료: {'🎯 ON (가동중)' if mode_val == 'ON' else '⚪ OFF (대기중)'}", parse_mode='HTML')
            
        elif action == "SET_INIT":
            ticker = data[2]
            if sub == "V_REV":
                msg, markup = self.view.get_init_v_rev_confirm_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                return

            elif sub == "EXEC_CONFIRM":
                await query.answer("⏳ 장부 재구성 중...")
                async with self.tx_lock:
                    _, holdings = self.broker.get_account_balance()
                h = holdings.get(ticker, {'qty': 0, 'avg': 0})
                qty = int(h['qty'])
                avg = float(h['avg'])
                
                if qty > 0:
                    new_q = [{
                        "qty": qty,
                        "price": avg,
                        "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "type": "INIT_TRANSFERRED" 
                    }]
                    try:
                        await self._verify_and_update_queue(ticker, new_q, context, query.message.chat_id)
                        await query.edit_message_text(f"✅ <b>[{ticker}] 자동 물량 이관 및 초기화 완료!</b>\n\n<b>{qty}주</b>(평단 <b>${avg:.2f}</b>)의 단일 기초 블록으로 완벽히 재구성되었습니다.", parse_mode='HTML')
                    except Exception as e:
                        await query.edit_message_text(f"❌ 쓰기 오류 발생: {e}", parse_mode='HTML')
                else:
                    await query.edit_message_text(f"⚠️ <b>[{ticker}] 보유 물량이 없어 이관할 대상이 없습니다.</b>", parse_mode='HTML')

        elif action == "TICKER":
            self.cfg.set_active_tickers([sub] if sub != "ALL" else ["SOXL", "TQQQ"])
            await query.edit_message_text(f"✅ 운용 종목 변경: {sub}")
            
        elif action == "SEED":
            ticker = data[2]
            self.user_states[update.effective_chat.id] = f"SEED_{sub}_{ticker}"
            await context.bot.send_message(update.effective_chat.id, f"💵 [{ticker}] 시드머니 금액 입력:")
            
        elif action == "INPUT":
            ticker = data[2]
            self.user_states[update.effective_chat.id] = f"CONF_{sub}_{ticker}"
            
            if sub == "SPLIT":
                ko_name = "분할 횟수"
            elif sub == "TARGET":
                ko_name = "목표 수익률(%)"
            elif sub == "COMPOUND":
                ko_name = "자동 복리율(%)"
            elif sub == "STOCK_SPLIT":
                ko_name = "액면 분할/병합 비율 (예: 10분할은 10, 10병합은 0.1)"
            else:
                ko_name = "값"
            
            await context.bot.send_message(update.effective_chat.id, f"⚙️ [{ticker}] {ko_name} 입력 (숫자만):")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update):
            return
            
        chat_id = update.effective_chat.id
        text = update.message.text.strip() if update.message.text else ""
        
        if "통합 지시서" in text or "지시서 조회" in text:
            return await self.cmd_sync(update, context)
        elif "장부 동기화" in text or "장부 조회" in text:
            return await self.cmd_record(update, context)
        elif "명예의 전당" in text:
            return await self.cmd_history(update, context)
        elif "코어 스위칭" in text or "전술 설정" in text:
            return await self.cmd_settlement(update, context)
        elif "시드머니" in text or "시드 변경" in text or "시드 관리" in text:
            return await self.cmd_seed(update, context)
        elif "종목 선택" in text:
            return await self.cmd_ticker(update, context)
        elif "스나이퍼" in text:
            return await self.cmd_mode(update, context)
        elif "버전" in text or "업데이트 내역" in text:
            return await self.cmd_version(update, context)
        elif "비상 해제" in text:
            return await self.cmd_reset(update, context)
        elif "시스템 업데이트" in text or "엔진 업데이트" in text:
            return await self.cmd_update(update, context)

        state = self.user_states.get(chat_id)
        
        if not state:
            return

        try:
            if state.startswith("EDITQ_"):
                parts = state.split("_", 2)
                ticker = parts[1]
                target_date = parts[2]
                
                input_parts = text.split()
                if len(input_parts) != 2:
                    del self.user_states[chat_id]
                    return await update.message.reply_text("❌ 입력 형식 오류입니다. 띄어쓰기로 수량과 평단가를 입력해주세요. (수정 취소됨)")
                
                try:
                    qty = int(input_parts[0])
                    price = float(input_parts[1])
                except ValueError:
                    del self.user_states[chat_id]
                    return await update.message.reply_text("❌ 수량/평단가는 숫자로 입력하세요. (수정 취소됨)")
                
                try:
                    curr_p = await asyncio.wait_for(
                        asyncio.to_thread(self.broker.get_current_price, ticker), 
                        timeout=3.0
                    )
                    if curr_p and curr_p > 0 and (price < curr_p * 0.7 or price > curr_p * 1.3):
                        del self.user_states[chat_id]
                        return await update.message.reply_text(f"🚨 <b>팻핑거 방어 가동:</b> 입력가(${price:.2f})가 현재가(${curr_p:.2f}) 대비 ±30%를 초과합니다. 다시 시도해주세요.", parse_mode='HTML')
                except Exception:
                    pass

                q_file = "data/queue_ledger.json"
                all_q = {}
                if os.path.exists(q_file):
                    with open(q_file, 'r', encoding='utf-8') as f:
                        all_q = json.load(f)
                        
                ticker_q = all_q.get(ticker, [])
                for item in ticker_q:
                    if item.get('date') == target_date:
                        item['qty'] = qty
                        item['price'] = price
                        break
                
                await self._verify_and_update_queue(ticker, ticker_q, context, chat_id)
                del self.user_states[chat_id]
                short_date = target_date[:10]
                await update.message.reply_text(f"✅ <b>[{ticker}] 지층 정밀 수정 완료!</b>\n▫️ {short_date} | {qty}주 | ${price:.2f}\n▫️ 확인: 장부 하단 🗄️ 버튼", parse_mode='HTML')
                return

            val = float(text)
            parts = state.split("_")
            
            if state.startswith("SEED"):
                if val < 0:
                    return await update.message.reply_text("❌ 오류: 시드머니는 0 이상이어야 합니다.")
                    
                action, ticker = parts[1], parts[2]
                curr = self.cfg.get_seed(ticker)
                new_v = curr + val if action == "ADD" else (max(0, curr - val) if action == "SUB" else val)
                self.cfg.set_seed(ticker, new_v)
                await update.message.reply_text(f"✅ [{ticker}] 시드 변경: ${new_v:,.0f}")
                
            elif state.startswith("CONF_SPLIT"):
                if val < 1:
                    return await update.message.reply_text("❌ 오류: 분할 횟수는 1 이상이어야 합니다.")
                    
                ticker = parts[2]
                d = self.cfg._load_json(self.cfg.FILES["SPLIT"], self.cfg.DEFAULT_SPLIT)
                d[ticker] = val
                self.cfg._save_json(self.cfg.FILES["SPLIT"], d)
                await update.message.reply_text(f"✅ [{ticker}] 분할: {int(val)}회")
                
            # 🚨 [수술 완료] 누락되어 허공에 붕 떠있던 (Dangling) 목표 수익률 설정 로직 완벽 복구
            elif state.startswith("CONF_TARGET"):
                ticker = parts[2]
                d = self.cfg._load_json(self.cfg.FILES["PROFIT_CFG"], self.cfg.DEFAULT_TARGET)
                d[ticker] = val
                self.cfg._save_json(self.cfg.FILES["PROFIT_CFG"], d)
                await update.message.reply_text(f"✅ [{ticker}] 목표 수익률: {val}%")

            elif state.startswith("CONF_COMPOUND"):
                if val < 0:
                    return await update.message.reply_text("❌ 오류: 복리율은 0 이상이어야 합니다.")
                    
                ticker = parts[2]
                self.cfg.set_compound_rate(ticker, val)
                await update.message.reply_text(f"✅ [{ticker}] 졸업 시 자동 복리율: {val}%")
                
            elif state.startswith("CONF_STOCK_SPLIT"):
                if val <= 0:
                    return await update.message.reply_text("❌ 오류: 액면 보정 비율은 0보다 커야 합니다.")
                    
                ticker = parts[2]
                self.cfg.apply_stock_split(ticker, val)
                
                est = pytz.timezone('US/Eastern')
                today_str = datetime.datetime.now(est).strftime('%Y-%m-%d')
                self.cfg.set_last_split_date(ticker, today_str)
                
                await update.message.reply_text(f"✅ [{ticker}] 수동 액면 보정 완료\n▫️ 모든 장부 기록이 {val}배 비율로 정밀하게 소급 조정되었습니다.")
                
        except ValueError:
            await update.message.reply_text("❌ 오류: 유효한 숫자를 입력하세요. (입력 대기 상태가 강제 해제되었습니다.)")
        except Exception as e:
            await update.message.reply_text(f"❌ 알 수 없는 오류 발생: {str(e)}")
        finally:
            if chat_id in self.user_states:
                del self.user_states[chat_id]

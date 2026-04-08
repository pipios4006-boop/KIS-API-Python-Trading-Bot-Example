# ==========================================================
# [telegram_bot.py] - Part 1/2 부 (상반부)
# ⚠️ 수술 내역: 
# 1. /reset 시 삼위일체(본장부, 에스크로, 백업장부, 큐장부) 100% 소각 엔진 탑재
# 2. 0주 도달 시 마이너스 수익이라도 장부를 비우는(강제 손절 리셋) 로직 개방
# 💡 [핵심 수술] __init__ 확장을 통한 V-REV 및 VWAP 의존성 주입(DI) 연결 완비
# ==========================================================
import logging
import datetime
import pytz
import time
import os
import math 
import asyncio
import json
import pandas_market_calendars as mcal 
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from telegram_view import TelegramView 

class TelegramController:
    # 💡 [핵심 수술] main.py로부터 신규 엔진(vwap_strategy, queue_ledger, strategy_rev)을 주입받도록 파라미터 확장
    def __init__(self, config, broker, strategy, tx_lock=None, vwap_strategy=None, queue_ledger=None, strategy_rev=None):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.view = TelegramView()
        self.user_states = {} 
        self.admin_id = self.cfg.get_chat_id()
        self.sync_locks = {} 
        self.tx_lock = tx_lock or asyncio.Lock()
        
        # 💡 [핵심 수술] 주입받은 의존성 객체를 컨트롤러 메모리에 완벽히 바인딩
        self.vwap_strategy = vwap_strategy
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
        return (17, "🌞 <b>서머타임 적용 (Summer)</b>") if is_dst else (18, "❄️ <b>서머타임 해제 (Winter)</b>")

    def _get_market_status(self):
        est = pytz.timezone('US/Eastern')
        now = datetime.datetime.now(est)
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=now.date(), end_date=now.date())
        if schedule.empty: return "CLOSE", "⛔ 장휴일"
        
        market_open = schedule.iloc[0]['market_open'].astimezone(est)
        market_close = schedule.iloc[0]['market_close'].astimezone(est)
        pre_start = market_open.replace(hour=4, minute=0)
        after_end = market_close.replace(hour=20, minute=0)

        if pre_start <= now < market_open: return "PRE", "🌅 프리마켓"
        elif market_open <= now < market_close: return "REG", "🔥 정규장"
        elif market_close <= now < after_end: return "AFTER", "🌙 애프터마켓"
        else: return "CLOSE", "⛔ 장마감"

    def _calculate_budget_allocation(self, cash, tickers):
        sorted_tickers = sorted(tickers, key=lambda x: 0 if x == "SOXL" else (1 if x == "TQQQ" else 2))
        allocated = {}
        rem_cash = cash
        
        for tx in sorted_tickers:
            rev_state = self.cfg.get_reverse_state(tx)
            is_rev = rev_state.get("is_active", False)
            
            if is_rev:
                portion = 0.0
            else:
                split = self.cfg.get_split_count(tx)
                portion = self.cfg.get_seed(tx) / split if split > 0 else 0
                
            if rem_cash >= portion:
                allocated[tx] = rem_cash
                rem_cash -= portion
            else: 
                allocated[tx] = 0
                    
        return sorted_tickers, allocated

    def setup_handlers(self, application):
        application.add_handler(CommandHandler("start", self.cmd_start))
        application.add_handler(CommandHandler("v17", self.cmd_v17))
        application.add_handler(CommandHandler("v4", self.cmd_v4))
        application.add_handler(CommandHandler("sync", self.cmd_sync))
        application.add_handler(CommandHandler("record", self.cmd_record))
        application.add_handler(CommandHandler("history", self.cmd_history))
        application.add_handler(CommandHandler("mode", self.cmd_mode))
        application.add_handler(CommandHandler("reset", self.cmd_reset))
        application.add_handler(CommandHandler("seed", self.cmd_seed))
        application.add_handler(CommandHandler("ticker", self.cmd_ticker))
        application.add_handler(CommandHandler("settlement", self.cmd_settlement))
        application.add_handler(CommandHandler("version", self.cmd_version))
        
        application.add_handler(CommandHandler("queue", self.cmd_queue))
        application.add_handler(CommandHandler("add_q", self.cmd_add_q))
        application.add_handler(CommandHandler("clear_q", self.cmd_clear_q))
        
        application.add_handler(CallbackQueryHandler(self.handle_callback))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

    def _update_queue_file(self, ticker, new_q):
        q_file = "data/queue_ledger.json"
        os.makedirs("data", exist_ok=True)
        all_q = {}
        if os.path.exists(q_file):
            try:
                with open(q_file, 'r', encoding='utf-8') as f:
                    all_q = json.load(f)
            except: pass
            
        all_q[ticker] = new_q
        
        with open(q_file, 'w', encoding='utf-8') as f:
            json.dump(all_q, f, ensure_ascii=False, indent=4)
            
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
                actual_qty = int(holdings.get(ticker, {'qty': 0})['qty'])
                new_q_total = sum(int(float(item.get('qty', 0))) for item in new_q)

                if actual_qty != new_q_total:
                    await context.bot.send_message(
                        chat_id, 
                        f"⚠️ <b>[무결성 경고]</b> 큐 총합(<b>{new_q_total}주</b>) 🆚 실제 계좌 잔고(<b>{actual_qty}주</b>)\n"
                        f"▫️ <i>수량이 일치하지 않습니다. 분할 입력 중이시라면 나머지 물량도 마저 입력해 맞춰주세요.</i>", 
                        parse_mode='HTML'
                    )
        except asyncio.TimeoutError:
            await context.bot.send_message(chat_id, "⚠️ KIS 서버 통신 지연으로 실잔고 검증을 생략했습니다. (저장 완료)", parse_mode='HTML')
        except Exception as e:
            logging.error(f"잔고 검증 중 에러 (스킵됨): {e}")
            
        return True

    async def cmd_queue(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update): return
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
        if not self._is_admin(update): return
        
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
                except: pass
                    
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
        if not self._is_admin(update): return
        args = context.args
        if not args:
            return await update.message.reply_text("❌ 종목명을 입력하세요. 예: /clear_q SOXL")
            
        ticker = args[0].upper()
        try:
            chat_id = update.effective_chat.id
            await self._verify_and_update_queue(ticker, [], context, chat_id)
            await update.message.reply_text(f"🗑️ <b>[{ticker}] 장부가 완전히 소각되었습니다.</b>\n새로운 지층을 구축할 준비가 되었습니다.", parse_mode='HTML')
        except Exception as e:
            await update.message.reply_text(f"❌ 소각 중 에러 발생: {e}")

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update): return
        target_hour, season_icon = self._get_dst_info()
        latest_version = self.cfg.get_latest_version() 
        msg = self.view.get_start_message(target_hour, season_icon, latest_version) 
        await update.message.reply_text(msg, parse_mode='HTML')

    async def cmd_v17(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update): return
        if os.getenv("SECRET_MODE") != "ON": return 

        args = context.args
        if not args:
            await update.message.reply_text("⚠️ 종목명을 함께 입력하세요. 예) /v17 TQQQ")
            return
            
        ticker = args[0].upper()
        active_tickers = self.cfg.get_active_tickers()
        
        if ticker in active_tickers:
            self.cfg.set_version(ticker, "V17")
            await update.message.reply_text(f"🦇 쉿! <b>[{ticker}] 나만의 시크릿 V17 모드(스나이퍼 어쌔신)</b>가 은밀하게 활성화되었습니다.", parse_mode='HTML')
        else:
            await update.message.reply_text(f"❌ 현재 운용 중인 종목이 아닙니다. (운용 중: {', '.join(active_tickers)})")

    async def cmd_v4(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update): return
        if os.getenv("SECRET_MODE") != "ON": return 

        for t in self.cfg.get_active_tickers():
            self.cfg.set_version(t, "V14")
        await update.message.reply_text("✅ <b>모든 종목이 오리지널 V4(무매4) 모드로 복귀했습니다.</b>", parse_mode='HTML')
    async def cmd_sync(self, update, context):
        if not self._is_admin(update): return
        await update.message.reply_text("🔄 시장 분석 및 지시서 작성 중...")
        
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

            tracking_cache = context.job_queue.jobs()[0].data.get('sniper_tracking', {}) if context.job_queue and context.job_queue.jobs() else {}

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
                curr = await asyncio.to_thread(self.broker.get_current_price, t, is_market_closed=(status_code == "CLOSE"))
                prev_close = await asyncio.to_thread(self.broker.get_previous_close, t)
                ma_5day = await asyncio.to_thread(self.broker.get_5day_ma, t)
                day_high, day_low = await asyncio.to_thread(self.broker.get_day_high_low, t)
                
                actual_avg = float(h['avg']) if h['avg'] else 0.0
                actual_qty = int(h['qty'])
                
                safe_prev_close = prev_close if prev_close else 0.0
                
                idx_ticker = "SOXX" if t == "SOXL" else "QQQ"
                
                dynamic_pct_obj = await asyncio.to_thread(self.broker.get_dynamic_sniper_target, idx_ticker)
                
                dynamic_pct = float(dynamic_pct_obj) if dynamic_pct_obj is not None else (8.79 if t == "SOXL" else 4.95)
                
                tracking_status = tracking_cache.get(t, {})
                current_day_high = tracking_status.get('day_high', day_high) 
                
                hybrid_target_price = current_day_high * (1 - (abs(dynamic_pct) / 100.0))
                trigger_reason = f"-{abs(dynamic_pct)}%"
                is_already_ordered = self.cfg.check_lock(t, "REG") or self.cfg.check_lock(t, "SNIPER")
                
                plan = self.strategy.get_plan(
                    t, curr, actual_avg, actual_qty, safe_prev_close, ma_5day=ma_5day,
                    market_type="REG", available_cash=allocated_cash[t],
                    is_simulation=True 
                )
                
                split = self.cfg.get_split_count(t)
                seed = self.cfg.get_seed(t)
                ver = self.cfg.get_version(t)
                
                t_val = plan.get('t_val', 0.0)
                is_rev = plan.get('is_reverse', False)
                secret_quarter_target = 0.0
                
                if ver == "V17" and actual_qty > 0:
                    if is_rev:
                        secret_quarter_target = plan.get('star_price', 0.0)
                    else:
                        is_first_half = t_val < (split / 2)
                        secret_quarter_target = plan.get('star_price', 0.0) if is_first_half else math.ceil(actual_avg * 1.005 * 100) / 100.0

                if dynamic_pct_obj and hasattr(dynamic_pct_obj, 'metric_val'):
                    real_val = float(dynamic_pct_obj.metric_val)
                    real_name = dynamic_pct_obj.metric_name
                else:
                    real_val = 0.0
                    real_name = "지표"
                    
                vol_status = "ON" if real_val >= 20.0 else "OFF"
                
                v_rev_q_qty = 0
                v_rev_q_lots = 0
                v_rev_guidance = ""
                
                if ver == "V_REV":
                    if not getattr(self, 'queue_ledger', None):
                        from queue_ledger import QueueLedger
                        self.queue_ledger = QueueLedger()
                        
                    q_list = self.queue_ledger.get_queue(t)
                    v_rev_q_lots = len(q_list)
                    v_rev_q_qty = sum(item.get('qty', 0) for item in q_list)
       
                    one_portion_cash = seed * 0.15
                    plan['one_portion'] = one_portion_cash
                    one_portion_qty = math.floor(one_portion_cash / curr) if curr > 0 else 0
                    
                    half_portion_cash = one_portion_cash * 0.5
                    
                    if q_list:
                        recent_lots = list(reversed(q_list))[:3]
                        for idx, lot in enumerate(recent_lots):
                            if idx == 0:
                                target_sell_price = round(safe_prev_close * 1.006, 2)
                            else:
                                target_sell_price = round(actual_avg * 1.005, 2)
                                
                            sell_qty = min(lot['qty'], one_portion_qty) if one_portion_qty > 0 else lot['qty']
                            v_rev_guidance += f" 🔵 매도{idx+1}(Pop{idx+1}): ${target_sell_price:.2f} 돌파 시 <b>{sell_qty}주</b>\n"
                        
                        if len(q_list) > 3:
                            v_rev_guidance += f"  <i>... (이하 {len(q_list)-3}개 로트 대기 중)</i>\n"
                    else:
                        v_rev_guidance += f" 🔵 매도(Pop): 대기 물량 없음 (관망)\n"
                    
                    b1_price = round(safe_prev_close * 1.10 if v_rev_q_qty == 0 else safe_prev_close * 0.995, 2)
                    b2_price = round(safe_prev_close * 1.10 if v_rev_q_qty == 0 else safe_prev_close * 0.975, 2)
                    
                    b1_qty = math.floor(half_portion_cash / b1_price) if b1_price > 0 else 0
                    b2_qty = math.floor(half_portion_cash / b2_price) if b2_price > 0 else 0
                    
                    v_rev_guidance += f" 🔴 매수1(Buy1): ${b1_price:.2f} 진입 시 <b>{b1_qty}주</b>\n"
                    v_rev_guidance += f" 🔴 매수2(Buy2): ${b2_price:.2f} 진입 시 <b>{b2_qty}주</b>"

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
                    'bb_lower': 0.0,  
                    'hybrid_base': 0.0, 
                    'hybrid_target': hybrid_target_price,
                    'trigger_reason': trigger_reason,
                    'sniper_trigger': abs(float(dynamic_pct)), 
                    'secret_quarter_target': secret_quarter_target,
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
                    'v_rev_guidance': v_rev_guidance
                })
                total_buy_needed += sum(o['price']*o['qty'] for o in plan['orders'] if o['side']=='BUY')

        surplus = cash - total_buy_needed
        rp_amount = surplus * 0.95 if surplus > 0 else 0
        
        final_msg, markup = self.view.create_sync_report(status_text, dst_txt, cash, rp_amount, ticker_data_list, status_code in ["PRE", "REG"], p_trade_data={})
        
        await update.message.reply_text(final_msg, reply_markup=markup, parse_mode='HTML')

    async def cmd_record(self, update, context):
        if not self._is_admin(update): return
        chat_id = update.message.chat_id
        status_msg = await context.bot.send_message(chat_id, "🛡️ <b>장부 무결성 검증 및 동기화 중...</b>", parse_mode='HTML')
        
        success_tickers = []
        for t in self.cfg.get_active_tickers():
            res = await self.process_auto_sync(t, chat_id, context, silent_ledger=True)
            if res == "SUCCESS": success_tickers.append(t)
        
        if success_tickers: 
            async with self.tx_lock:
                _, holdings = self.broker.get_account_balance()
            await self._display_ledger(success_tickers[0], chat_id, context, message_obj=status_msg, pre_fetched_holdings=holdings)
        else: await status_msg.edit_text("✅ <b>동기화 완료</b> (표시할 진행 중인 장부가 없거나 에러 대기 중입니다)", parse_mode='HTML')

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
            if r['side'] == 'SELL': escrow += amt
            elif r['side'] == 'BUY': escrow -= amt
                
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
                    await context.bot.send_message(chat_id, f"✂️ <b>[{ticker}] 야 파이낸스 {split_type} 자동 감지!</b>\n▫️ 감지된 비율: <b>{split_ratio}배</b> (발생일: {split_date})\n▫️ 봇이 기존 장부의 수량과 평단가를 100% 무인 자동 소급 조정 완료했습니다.", parse_mode='HTML')
                
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
                        self.queue_ledger.sync_with_broker(ticker, 0)
                        msg = f"🎉 <b>[{ticker} V-REV 잭팟 스윕(전량 익절) 감지!]</b>\n▫️ 잔고가 0주가 되어 LIFO 큐 지층을 100% 소각(초기화)했습니다."
                        await context.bot.send_message(chat_id, msg, parse_mode='HTML')
                        self._sync_escrow_cash(ticker)
                        return "SUCCESS"
                        
                    calibrated = self.queue_ledger.sync_with_broker(ticker, actual_qty)
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
                                if added_seed > 0: msg += f"\n💸 <b>자동 복리 +${added_seed:,.0f}</b> 이 다음 운용 시드에 완벽하게 추가되었습니다!"
                                await context.bot.send_message(chat_id, msg, parse_mode='HTML')
                                try:
                                    img_path = self.view.create_profit_image(
                                        ticker=ticker, profit=new_hist['profit'], yield_pct=new_hist['yield'],
                                        invested=new_hist['invested'], revenue=new_hist['revenue'], end_date=new_hist['end_date']
                                    )
                                    if os.path.exists(img_path):
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
                            else: temp_sim_qty -= exec_qty
                                
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
                        for r in new_target_records: r['avg_price'] = actual_avg
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
                if len(parts) == 3: date_short = f"{parts[1]}.{parts[2]}"
                else: date_short = rec['date']
                    
                side_str = "🔴매수" if rec['side'] == 'BUY' else "🔵매도"
                key = (date_short, side_str)
                
                if key not in agg_dict: agg_dict[key] = {'qty': 0, 'amt': 0.0}
                agg_dict[key]['qty'] += rec['qty']
                agg_dict[key]['amt'] += (rec['qty'] * rec['price'])
                
                if rec['side'] == 'BUY': total_buy += (rec['qty'] * rec['price'])
                elif rec['side'] == 'SELL': total_sell += (rec['qty'] * rec['price'])
            
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
            
            report += f"📊 <b>[ 현재 진행 상황 요약 ]</b>\n"
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

        if query: await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
        elif message_obj: await message_obj.edit_text(msg, reply_markup=markup, parse_mode='HTML')
        else: await context.bot.send_message(chat_id, msg, reply_markup=markup, parse_mode='HTML')

    async def cmd_history(self, update, context):
        if not self._is_admin(update): return
        history = self.cfg.get_history()
        if not history:
            await update.message.reply_text("📜 저장된 역사가 없습니다.")
            return
        msg = "🏆 <b>[ 졸업 명예의 전당 ]</b>\n"
        keyboard = [[InlineKeyboardButton(f"{h['end_date']} | {h['ticker']} (+${h['profit']:.0f})", callback_data=f"HIST:VIEW:{h['id']}")] for h in history]
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    async def cmd_mode(self, update, context):
        if not self._is_admin(update): return
        
        active_tickers = self.cfg.get_active_tickers()
        is_all_v17 = all(self.cfg.get_version(t) == "V17" for t in active_tickers)
        
        if is_all_v17:
            msg = (
                "🦇 <b>[ V17 시크릿 네이티브 가동 중 ]</b>\n\n"
                "현재 운용 중인 모든 종목이 <b>V17 시크릿 모드</b>로 설정되어 있습니다.\n"
                "V17 아키텍처는 상방 및 하방 스나이퍼가 코어 엔진에 100% 내장되어 상시 자동 격발되므로, "
                "별도의 스나이퍼 ON/OFF 제어가 필요하지 않습니다. 🎯"
            )
            await update.message.reply_text(msg, parse_mode='HTML')
            return

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

        report += "⚠️ <b>[매도 엔진 충돌 경고]</b> 스나이퍼 수동 가동 시 VWAP 매도 엔진과 충돌합니다. 스나이퍼가 명중하여 KIS 원장에 체결 이력이 기록될 경우, 다중 매도 방지 락온 로직에 의해 당일 VWAP 매도 스케줄러는 즉각 가동 중단(Lock-down) 처리됩니다.\n\n"
        
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
        if not self._is_admin(update): return
        active_tickers = self.cfg.get_active_tickers()
        msg, markup = self.view.get_reset_menu(active_tickers)
        await update.message.reply_text(msg, reply_markup=markup, parse_mode='HTML')

    async def cmd_seed(self, update, context):
        if not self._is_admin(update): return
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
        if not self._is_admin(update): return
        msg, markup = self.view.get_ticker_menu(self.cfg.get_active_tickers())
        await update.message.reply_text(msg, reply_markup=markup, parse_mode='HTML')

    async def cmd_settlement(self, update, context):
        if not self._is_admin(update): return
        
        active_tickers = self.cfg.get_active_tickers()
        atr_data = {}
        dynamic_target_data = {} 
        
        status_msg = await update.message.reply_text("⏳ <b>실시간 시장 지표(HV/VXN) 연산 중...</b>", parse_mode='HTML')
        
        est = pytz.timezone('US/Eastern')
        now_est = datetime.datetime.now(est)
        
        is_sniper_active_time = False
        try:
            nyse = mcal.get_calendar('NYSE')
            schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
            if not schedule.empty:
                market_open = schedule.iloc[0]['market_open'].astimezone(est)
                switch_time = market_open + datetime.timedelta(minutes=50) # 10:20 EST
                if now_est >= switch_time:
                    is_sniper_active_time = True
        except Exception:
            if now_est.weekday() < 5 and now_est.time() >= datetime.time(10, 20):
                is_sniper_active_time = True

        for t in active_tickers:
            if self.cfg.get_version(t) == "V17":
                atr_data[t] = await asyncio.to_thread(self.broker.get_atr_data, t)
                idx_ticker = "SOXX" if t == "SOXL" else "QQQ"
                dynamic_target_data[t] = await asyncio.to_thread(self.broker.get_dynamic_sniper_target, idx_ticker)
                
                if dynamic_target_data[t] is not None:
                    dynamic_target_data[t].is_sniper_active_time = is_sniper_active_time
            else:
                atr_data[t] = (0.0, 0.0)
                dynamic_target_data[t] = None
                
        msg, markup = self.view.get_settlement_message(active_tickers, self.cfg, atr_data, dynamic_target_data)
        
        keyboard = list(markup.inline_keyboard) if markup else []
        for t in active_tickers:
            keyboard.append([InlineKeyboardButton(f"🔄 [{t}] V_REV (역추세 하이브리드) 전환", callback_data=f"SET_VER:V_REV:{t}")])
            keyboard.append([InlineKeyboardButton(f"▶️ [{t}] V_VWAP (기존 자율주행) 복귀", callback_data=f"SET_VER:V_VWAP:{t}")])
        
        new_markup = InlineKeyboardMarkup(keyboard)
        await status_msg.edit_text(msg, reply_markup=new_markup, parse_mode='HTML')

    async def cmd_version(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update): return
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
                if self.queue_ledger:
                    q_data = self.queue_ledger.get_queue(ticker)
                else:
                    q_data = []
                    try:
                        if os.path.exists("data/queue_ledger.json"):
                            with open("data/queue_ledger.json", "r", encoding='utf-8') as f:
                                q_data = json.load(f).get(ticker, [])
                    except: pass
                msg, markup = self.view.get_queue_management_menu(ticker, q_data)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')

        elif action == "DEL_REQ":
            ticker = sub
            target_date = ":".join(data[2:])
            
            q_data = self.queue_ledger.get_queue(ticker) if self.queue_ledger else []
            if not q_data:
                try:
                    with open("data/queue_ledger.json", "r") as f:
                        q_data = json.load(f).get(ticker, [])
                except: pass
            
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
                    await query.answer(f"✅ 삭제 완료.", show_alert=False)
                    
                    msg, markup = self.view.get_queue_management_menu(ticker, new_q)
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                    
                elif action == "EDIT_Q":
                    await query.answer("✏️ 수정 모드 진입", show_alert=False)
                    short_date = target_date[:10]
                    self.user_states[update.effective_chat.id] = f"EDITQ_{ticker}_{target_date}"
                    
                    prompt = f"✏️ <b>[{ticker} 지층 수정 모드]</b>\n"
                    prompt += f"선택하신 <b>[{short_date}]</b> 지층을 재설정합니다.\n\n"
                    prompt += f"새로운 <b>[수량]</b>과 <b>[평단가]</b>를 띄어쓰기로 입력하세요.\n"
                    prompt += f"(예: <code>229 52.16</code>)\n\n"
                    prompt += f"<i>(입력을 취소하려면 숫자 이외의 문자를 보내주세요)</i>"
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
                    except: pass
                
                q_file = "data/queue_ledger.json"
                if os.path.exists(q_file):
                    try:
                        with open(q_file, 'r', encoding='utf-8') as f:
                            q_data = json.load(f)
                        if ticker in q_data:
                            del q_data[ticker]
                        with open(q_file, 'w', encoding='utf-8') as f:
                            json.dump(q_data, f, ensure_ascii=False, indent=4)
                    except: pass
                    
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
                    qty, avg, invested, sold = self.cfg.calculate_holdings(target['ticker'], target['trades'])
                    msg, markup = self.view.create_ledger_dashboard(target['ticker'], qty, avg, invested, sold, target['trades'], 0, 0, is_history=True)
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
                    await context.bot.send_message(update.effective_chat.id, f"❌ 이미지 렌더링 모듈 장애 발생.", parse_mode='HTML')
            
        elif action == "EXEC":
            t = sub
            ver = self.cfg.get_version(t)
            
            await query.edit_message_text(f"🚀 {t} 수동 강제 전송 시작 (교차 분리)...")
            async with self.tx_lock:
                cash, holdings = self.broker.get_account_balance()
                if holdings is None: return await query.edit_message_text("❌ API 통신 오류로 주문을 실행할 수 없습니다.")
                    
                _, allocated_cash = self._calculate_budget_allocation(cash, self.cfg.get_active_tickers())
                h = holdings.get(t, {'qty':0, 'avg':0})
                
                curr_p = await asyncio.to_thread(self.broker.get_current_price, t)
                prev_c = await asyncio.to_thread(self.broker.get_previous_close, t)
                ma_5day = await asyncio.to_thread(self.broker.get_5day_ma, t)
                
                plan = self.strategy.get_plan(t, curr_p, float(h['avg']), int(h['qty']), prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t])
                
                is_rev = plan.get('is_reverse', False)
                
                if ver == "V17": ver_display = "V17 시크릿"
                elif ver == "V_VWAP": ver_display = "VWAP 자율주행 (페일세이프 장전)"
                elif ver == "V_REV": ver_display = "V_REV 역추세 하이브리드"
                elif ver == "V14": ver_display = "무매4"
                else: ver_display = "무매3"
                
                title = f"🔄 <b>[{t}] {ver_display} 리버스 주문 수동 실행</b>\n" if is_rev else f"💎 <b>[{t}] 정규장 주문 수동 실행</b>\n"
                msg = title
                
                all_success = True
                
                for o in plan.get('core_orders', []):
                    res = self.broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                    is_success = res.get('rt_cd') == '0'
                    if not is_success: all_success = False
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
                self.cfg.set_upward_sniper_mode(ticker, False)
            
            if new_ver == "V13": new_ver_display = "무매3"
            elif new_ver == "V14": new_ver_display = "무매4"
            elif new_ver == "V_VWAP": new_ver_display = "VWAP 자율주행"
            elif new_ver == "V_REV": new_ver_display = "V_REV 역추세 하이브리드"
            else: new_ver_display = new_ver
            
            self.cfg.set_version(ticker, new_ver)
            await query.edit_message_text(f"✅ <b>[{ticker}]</b> 퀀트 엔진이 <b>{new_ver_display}</b> 모드로 직접 전환되었습니다.\n/sync 명령어에서 변경된 지시서를 확인하세요.", parse_mode='HTML')

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
            
        elif action == "MODE":
            mode_val = sub
            ticker = data[2] if len(data) > 2 else "SOXL"
            
            if self.cfg.get_version(ticker) == "V_REV" and mode_val == "ON":
                await query.answer("🚨 V-REV 역추세 모드에서는 로직 충돌 방지를 위해 상방 스나이퍼를 켤 수 없습니다!", show_alert=True)
                return
                
            self.cfg.set_upward_sniper_mode(ticker, mode_val == "ON")
            await query.edit_message_text(f"✅ <b>[{ticker}]</b> 상방 스나이퍼 모드 변경 완료: {'🎯 ON (가동중)' if mode_val == 'ON' else '⚪ OFF (대기중)'}", parse_mode='HTML')
            
        elif action == "SEED":
            ticker = data[2]
            self.user_states[update.effective_chat.id] = f"SEED_{sub}_{ticker}"
            await context.bot.send_message(update.effective_chat.id, f"💵 [{ticker}] 시드머니 금액 입력:")
            
        elif action == "INPUT":
            ticker = data[2]
            self.user_states[update.effective_chat.id] = f"CONF_{sub}_{ticker}"
            
            if sub == "SPLIT": ko_name = "분할 횟수"
            elif sub == "TARGET": ko_name = "목표 수익률(%)"
            elif sub == "COMPOUND": ko_name = "자동 복리율(%)"
            elif sub == "STOCK_SPLIT": ko_name = "액면 분할/병합 비율 (예: 10분할은 10, 10병합은 0.1)"
            else: ko_name = "값"
            
            await context.bot.send_message(update.effective_chat.id, f"⚙️ [{ticker}] {ko_name} 입력 (숫자만):")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update): return
        chat_id = update.effective_chat.id
        state = self.user_states.get(chat_id)
        if not state: return

        try:
            if state.startswith("EDITQ_"):
                parts = state.split("_", 2)
                ticker = parts[1]
                target_date = parts[2]
                
                input_parts = update.message.text.strip().split()
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

            val = float(update.message.text.strip())
            parts = state.split("_")
            
            if state.startswith("SEED"):
                if val < 0: return await update.message.reply_text("❌ 오류: 시드머니는 0 이상이어야 합니다.")
                action, ticker = parts[1], parts[2]
                curr = self.cfg.get_seed(ticker)
                new_v = curr + val if action == "ADD" else (max(0, curr - val) if action == "SUB" else val)
                self.cfg.set_seed(ticker, new_v)
                await update.message.reply_text(f"✅ [{ticker}] 시드 변경: ${new_v:,.0f}")
                
            elif state.startswith("CONF_SPLIT"):
                if val < 1: return await update.message.reply_text("❌ 오류: 분할 횟수는 1 이상이어야 합니다.")
                ticker = parts[2]
                d = self.cfg._load_json(self.cfg.FILES["SPLIT"], self.cfg.DEFAULT_SPLIT)
                d[ticker] = val; self.cfg._save_json(self.cfg.FILES["SPLIT"], d)
                await update.message.reply_text(f"✅ [{ticker}] 분할: {int(val)}회")
                
            elif state.startswith("CONF_TARGET"):
                ticker = parts[2]
                d = self.cfg._load_json(self.cfg.FILES["PROFIT_CFG"], self.cfg.DEFAULT_TARGET)
                d[ticker] = val; self.cfg._save_json(self.cfg.FILES["PROFIT_CFG"], d)
                await update.message.reply_text(f"✅ [{ticker}] 목표: {val}%")
                
            elif state.startswith("CONF_COMPOUND"):
                if val < 0: return await update.message.reply_text("❌ 오류: 복리율은 0 이상이어야 합니다.")
                ticker = parts[2]
                self.cfg.set_compound_rate(ticker, val)
                await update.message.reply_text(f"✅ [{ticker}] 졸업 시 자동 복리율: {val}%")
                
            elif state.startswith("CONF_STOCK_SPLIT"):
                if val <= 0: return await update.message.reply_text("❌ 오류: 액면 보정 비율은 0보다 커야 합니다.")
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

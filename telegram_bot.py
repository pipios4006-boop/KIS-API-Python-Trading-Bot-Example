# ==========================================================
# [telegram_bot.py] - Part 1
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# ==========================================================
import logging
import datetime
import pytz
import time
import os
import math 
import asyncio
import pandas_market_calendars as mcal 
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram_view import TelegramView 

class TelegramController:
    def __init__(self, config, broker, strategy, tx_lock=None):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.view = TelegramView()
        self.user_states = {} 
        self.admin_id = self.cfg.get_chat_id()
        self.sync_locks = {} 
        self.tx_lock = tx_lock or asyncio.Lock()

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

    # ==========================================================
    # 💡 [핵심 수술] P매매 타임 윈도우 (Time-Lock) 검증 엔진
    # ==========================================================
    def _is_p_trade_window_open(self):
        est = pytz.timezone('US/Eastern')
        now_est = datetime.datetime.now(est)
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
        
        if not schedule.empty:
            market_close = schedule.iloc[0]['market_close'].astimezone(est)
            vwap_start = market_close - datetime.timedelta(minutes=30)
            after_end = market_close + datetime.timedelta(hours=4)
            
            # 정규장 마감 30분 전 ~ 애프터마켓 종료까지는 락다운 (데드존)
            if vwap_start <= now_est <= after_end:
                return False
        return True

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

    # ==========================================================
    # 💡 [핵심 수술] P매매 시크릿 진입점 (다크 트리거)
    # ==========================================================
    async def cmd_p4006(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update): return
        
        if not self._is_p_trade_window_open():
            msg = self.view.get_p_trade_locked_message()
            await update.message.reply_text(msg, parse_mode='HTML')
            return

        active_tickers = self.cfg.get_active_tickers()
        if not active_tickers:
            await update.message.reply_text("❌ 운용 중인 종목이 설정되지 않았습니다.")
            return
            
        target_ticker = active_tickers[0] 
        seed = self.cfg.get_seed(target_ticker)
        multiplier = seed / 10000.0

        self.user_states[update.effective_chat.id] = f"P_TRADE_{target_ticker}_{multiplier}"
        
        msg = self.view.get_p_trade_unlocked_message(target_ticker, seed, multiplier)
        await update.message.reply_text(msg, parse_mode='HTML')

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
                    # 💡 [핵심 수술] 리버스 모드일 경우 안전 마진(1.005) 전면 배제 및 5MA(star_price) 다이렉트 매핑
                    if is_rev:
                        secret_quarter_target = plan.get('star_price', 0.0)
                    else:
                        is_first_half = t_val < (split / 2)
                        secret_quarter_target = plan.get('star_price', 0.0) if is_first_half else math.ceil(actual_avg * 1.005 * 100) / 100.0

                ticker_data_list.append({
                    'ticker': t, 'version': ver, 't_val': t_val, 'split': split, 'curr': curr, 'avg': actual_avg, 'qty': actual_qty,
                    'profit_amt': (curr - actual_avg) * actual_qty if actual_qty > 0 else 0, 
                    'profit_pct': (curr - actual_avg) / actual_avg * 100 if actual_avg > 0 else 0,
                    'upward_sniper': "ON" if self.cfg.get_upward_sniper_mode() else "OFF",
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
                    'is_sniper_active_time': is_sniper_active_time 
                })
                total_buy_needed += sum(o['price']*o['qty'] for o in plan['orders'] if o['side']=='BUY')

        surplus = cash - total_buy_needed
        rp_amount = surplus * 0.95 if surplus > 0 else 0
        
        p_trade_data = self.cfg.get_p_trade_data() 
        final_msg, markup = self.view.create_sync_report(status_text, dst_txt, cash, rp_amount, ticker_data_list, status_code in ["PRE", "REG"], p_trade_data=p_trade_data)
        
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
                    await context.bot.send_message(chat_id, f"✂️ <b>[{ticker}] 야후 파이낸스 {split_type} 자동 감지!</b>\n▫️ 감지된 비율: <b>{split_ratio}배</b> (발생일: {split_date})\n▫️ 봇이 기존 장부의 수량과 평단가를 100% 무인 자동 소급 조정 완료했습니다.", parse_mode='HTML')
                
                # ==========================================================
                # 💡 [핵심 수술] 단가 소급 업데이트 (Retrospective Unit Price Update) 
                # ==========================================================
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

                # 1. KIS API 전일 체결 영수증 스캔
                target_execs = await asyncio.to_thread(self.broker.get_execution_history, ticker, target_kis_str, target_kis_str)
                
                # 2. 장부(Ledger) 단가 핀셋 교정 위임 (config.py로 패스)
                if target_execs:
                    calibrated_count = self.cfg.calibrate_ledger_prices(ticker, target_ledger_str, target_execs)
                    if calibrated_count > 0:
                        logging.info(f"🔧 [{ticker}] LOC/MOC 주문 {calibrated_count}건에 대해 실제 체결 단가 소급 업데이트를 완료했습니다.")

                # ==========================================================
                    
                _, holdings = self.broker.get_account_balance()
                if holdings is None:
                    await context.bot.send_message(chat_id, f"❌ <b>[{ticker}] API 오류</b>\n잔고를 불러오지 못했습니다.", parse_mode='HTML')
                    return "ERROR"

                actual_qty = int(holdings.get(ticker, {'qty': 0})['qty'])
                actual_avg = float(holdings.get(ticker, {'avg': 0})['avg'])
                
                recs = [r for r in self.cfg.get_ledger() if r['ticker'] == ticker]
                ledger_qty, avg_price, _, _ = self.cfg.calculate_holdings(ticker, recs)
                
                diff = actual_qty - ledger_qty
                price_diff = abs(actual_avg - avg_price)

                if actual_qty == 0:
                    if ledger_qty > 0:
                        kst = pytz.timezone('Asia/Seoul')
                        today_str = datetime.datetime.now(kst).strftime('%Y-%m-%d')
                        prev_c = await asyncio.to_thread(self.broker.get_previous_close, ticker)
                        new_hist, added_seed = self.cfg.archive_graduation(ticker, today_str, prev_c)
                        msg = f"🎉 <b>[{ticker} 졸업 확인!]</b>\n장부를 명예의 전당에 저장하고 새 사이클을 준비합니다."
                        if added_seed > 0: msg += f"\n💸 <b>자동 복리 +${added_seed:,.0f}</b> 이 다음 운용 시드에 완벽하게 추가되었습니다!"
                        await context.bot.send_message(chat_id, msg, parse_mode='HTML')

                        if new_hist:
                            try:
                                img_path = self.view.create_profit_image(
                                    ticker=ticker,
                                    profit=new_hist['profit'],
                                    yield_pct=new_hist['yield'],
                                    invested=new_hist['invested'],
                                    revenue=new_hist['revenue'],
                                    end_date=new_hist['end_date']
                                )
                                if os.path.exists(img_path):
                                    with open(img_path, 'rb') as photo:
                                        await context.bot.send_photo(chat_id=chat_id, photo=photo)
                            except Exception as e:
                                logging.error(f"📸 졸업 이미지 생성/발송 실패: {e}")

                    self._sync_escrow_cash(ticker) 
                    return "SUCCESS"

                if diff == 0 and price_diff < 0.01:
                    pass 
                elif diff == 0 and price_diff >= 0.01:
                    self.cfg.calibrate_avg_price(ticker, actual_avg)
                    await context.bot.send_message(chat_id, f"🔧 <b>[{ticker}] 장부 평단가 미세 오차({price_diff:.4f}) 교정 완료!</b>", parse_mode='HTML')
                elif diff != 0:
                    # (중략된 기존 TrueSync 오차 교정(CALIB) 로직 - V20.4 버전)
                    # 위에서 이미 날짜(target_kis_str) 연산을 수행했으므로 재활용
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
            
            actual_qty = int(pre_fetched_holdings.get(ticker, {'qty': 0})['qty']) if pre_fetched_holdings else 0
            actual_avg = float(pre_fetched_holdings.get(ticker, {'avg': 0})['avg']) if pre_fetched_holdings else 0.0
            
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
        row = [InlineKeyboardButton(t, callback_data=f"REC:SYNC:{t}") for t in tickers]
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

        is_sniper = self.cfg.get_upward_sniper_mode()
        msg = f"🎯 <b>[ 상방 쿼터 스나이퍼 모드 (일반/무매4 전용) ]</b>\n현재 상태: {'ON (가동중)' if is_sniper else 'OFF (대기중)'}\n\n💡 <i>상방 스나이퍼를 켜면 장중 최고가 대비 1.5% 하락 시 25%의 물량을 선제적으로 익절하여 현금을 쟁취합니다.</i>"
        keyboard = [[InlineKeyboardButton("⚪ OFF", callback_data="MODE:OFF"), InlineKeyboardButton("🎯 ON", callback_data="MODE:ON")]]
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

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
        await status_msg.edit_text(msg, reply_markup=markup, parse_mode='HTML')

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

        if action == "VERSION":
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
                
                ledger_data = self.cfg.get_ledger()
                changed = False
                for lr in ledger_data:
                    if lr.get('ticker') == ticker and lr.get('is_reverse', False):
                        lr['is_reverse'] = False
                        changed = True
                if changed:
                    self.cfg._save_json(self.cfg.FILES["LEDGER"], ledger_data)
                    
                await query.edit_message_text(f"✅ <b>[{ticker}] 리버스 모드 및 가상장부(Escrow)가 모두 강제 초기화되었습니다.</b>\n(과거 리버스 꼬리표 소각 완료. 다음 주문부터 일반 모드로 복귀합니다.)", parse_mode='HTML')
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
            elif sub == "LIST": await self.cmd_history(update, context)
            
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
            
            if new_ver == "V13": new_ver_display = "무매3"
            elif new_ver == "V14": new_ver_display = "무매4"
            elif new_ver == "V_VWAP": new_ver_display = "VWAP 자율주행"
            else: new_ver_display = new_ver
            
            self.cfg.set_version(ticker, new_ver)
            await query.edit_message_text(f"✅ <b>[{ticker}]</b> 퀀트 엔진이 <b>{new_ver_display}</b> 모드로 직접 전환되었습니다.\n/sync 명령어에서 변경된 지시서를 확인하세요.", parse_mode='HTML')

        elif action == "TICKER":
            self.cfg.set_active_tickers([sub] if sub != "ALL" else ["SOXL", "TQQQ"])
            await query.edit_message_text(f"✅ 운용 종목 변경: {sub}")
        elif action == "MODE":
            self.cfg.set_upward_sniper_mode(sub == "ON")
            await query.edit_message_text(f"✅ 상방 스나이퍼 모드 변경 완료: {'🎯 ON (가동중)' if sub == 'ON' else '⚪ OFF (대기중)'}")
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
        
        # ==========================================================
        # 💡 [핵심 수술] 다중 P매매 지시서 파싱 및 P-VWAP 대기열 소각(OFF) 인터셉트 스위치
        # ==========================================================
        if state.startswith("P_TRADE_"):
            parts = state.split("_")
            ticker = parts[2]
            multiplier = float(parts[3])
            
            raw_text = update.message.text.strip()
            
            # 💡 [OFF 스위치 엔진] 특수 명령어 감지 시 즉시 대기열 강제 철거 및 엔진 무효화
            if raw_text.upper() in ["OFF", "취소", "종료", "끄기", "CANCEL", "STOP"]:
                self.cfg.clear_p_trade_data()
                await update.message.reply_text("🗑️ <b>[ P-VWAP 대기열 소각 (OFF) 완료 ]</b>\n당일 P매매 타격 대기열이 완벽히 비워졌으며 15:30 스케줄러 타격이 무효화되었습니다.", parse_mode='HTML')
                del self.user_states[chat_id]
                return
                
            raw_items = raw_text.replace('\n', ',').split(',')
            
            parsed_list = []
            for item in raw_items:
                item = item.strip()
                if not item: continue
                tokens = item.split()
                if len(tokens) >= 3:
                    side_kr = tokens[0]
                    price_str = tokens[1]
                    qty_str = tokens[2]
                    
                    try:
                        side = "SELL" if "매도" in side_kr else "BUY"
                        target_price = float(price_str)
                        base_qty = float(qty_str)
                        
                        # KIS API 정수 규격 준수 (Floor 함수로 내림 처리)
                        final_qty = math.floor(base_qty * multiplier) 
                        
                        if final_qty > 0:
                            parsed_list.append({
                                'side': side,
                                'target_price': target_price,
                                'qty': final_qty,
                                'rem_qty': final_qty  # VWAP 분할 타격 시 차감될 잔여 수량
                            })
                    except ValueError:
                        continue 
            
            if parsed_list:
                p_data = self.cfg.get_p_trade_data()
                p_data[ticker] = parsed_list
                self.cfg.set_p_trade_data(p_data)
                
                msg = self.view.get_p_trade_parsed_message(multiplier, parsed_list)
                await update.message.reply_text(msg, parse_mode='HTML')
            else:
                await update.message.reply_text("❌ <b>[입력 양식 오류]</b> 파싱에 실패했습니다.\n예시: <code>매도 45.50 10, 매수 42.00 15</code>\n취소하려면 <code>OFF</code> 또는 <code>취소</code>를 입력하세요.", parse_mode='HTML')
            
            del self.user_states[chat_id]
            return

        try:
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

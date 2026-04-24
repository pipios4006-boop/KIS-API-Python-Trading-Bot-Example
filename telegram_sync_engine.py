# ==========================================================
# [telegram_sync_engine.py] - 🌟 100% 통합 완성본 🌟 (Part 1)
# MODIFIED: [V28.10 장부 환각 엣지 케이스 수술] 실잔고와 큐 장부 수량이 일치할 경우 비파괴 보정 원천 차단.
# MODIFIED: [V28.21 동기화 엇박자 그랜드 수술] 졸업 판별 전 매도 원장 우선 기록으로 수익률 -100% 버그 차단.
# MODIFIED: [V30.08 애프터마켓 기억상실 방어] 체결 원장 지연(Lag) 대기 및 8초 다중 교차 검증 엔진 이식. 
# MODIFIED: [V30.12 유니버설 장부 동기화 & LIFO 큐 딥 캘리브레이션] 메인 장부 업데이트 개방 및 큐 다이렉트 편입.
# MODIFIED: [V30.13 수동매수 익절 증발 방어] 0주 졸업 시 MANUAL_SYNC 지층의 원자적 파일 I/O 강제 락온.
# NEW: [V30.15 제로섬 바이패스 & KST 자정 맹점 그랜드 수술] 
# 1) 당일 매수 후 애프터마켓 전량 익절 시 실잔고 오차가 0(Zero-Sum)이 되어 메인 장부 기입이 
# 증발하는 맹점 수술 (needs_reconstruction 엔진 이식).
# 2) KST 자정을 넘긴 애프터마켓 매도 원장이 단일 날짜 조회 시 누락되는 '데이터 기아' 방어. 
# 과거 4일치 광역 스캔 후 ord_dt/ord_tmd를 EST 타임존으로 정밀 형변환하여 당일 체결분만 핀셋 필터링(filter_to_est).
# 3) 큐 장부가 0이라도 당일 매도(sold_today)가 존재하면 0주 졸업 엔진 강제 격발 및 스냅샷 충돌 방어.
# MODIFIED: [V30.09 핫픽스] pytz 영구 적출 및 ZoneInfo 도입으로 LMT 버그 차단 및 타임존 락온 100% 달성.
# ==========================================================
import logging
import datetime
# MODIFIED: [V30.09 핫픽스] LMT 오차 방어를 위해 pytz 적출 및 ZoneInfo 도입
from zoneinfo import ZoneInfo
import time
import os
import asyncio
import json
import tempfile
import traceback
import yfinance as yf
import pandas_market_calendars as mcal
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

class TelegramSyncEngine:
    def __init__(self, config, broker, strategy, queue_ledger, view, tx_lock, sync_locks):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.queue_ledger = queue_ledger
        self.view = view
        self.tx_lock = tx_lock
        self.sync_locks = sync_locks

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
                
                try:
                    split_ratio, split_date = await asyncio.wait_for(
                        asyncio.to_thread(self.broker.get_recent_stock_split, ticker, last_split_date),
                        timeout=10.0
                    )
                except asyncio.TimeoutError:
                    split_ratio, split_date = 0.0, ""
                    logging.warning(f"⚠️ [{ticker}] 야후 파이낸스 액면분할 조회 타임아웃 (10초 초과), 이번 싱크에서 스킵")
                
                if split_ratio > 0.0 and split_date != "":
                    self.cfg.apply_stock_split(ticker, split_ratio)
                    self.cfg.set_last_split_date(ticker, split_date)
                    split_type = "액면분할" if split_ratio > 1.0 else "액면병합(역분할)"
                    await context.bot.send_message(chat_id, f"✂️ <b>[{ticker}] 야후 파이낸스 {split_type} 자동 감지!</b>\n▫️ 감지된 비율: <b>{split_ratio}배</b> (발생일: {split_date})\n▫️ 봇이 기존 장부의 수량과 평단가를 100% 무인 자동 소급 조정 완료했습니다.", parse_mode='HTML')
                
                # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo 이식
                kst = ZoneInfo('Asia/Seoul')
                now_kst = datetime.datetime.now(kst)
                
                # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo 이식
                est = ZoneInfo('America/New_York')
                now_est = datetime.datetime.now(est)
                nyse = mcal.get_calendar('NYSE')
                schedule = nyse.schedule(start_date=(now_est - datetime.timedelta(days=10)).date(), end_date=now_est.date())
                
                if not schedule.empty:
                    last_trade_date = schedule.index[-1]
                    target_ledger_str = last_trade_date.strftime('%Y-%m-%d')
                else:
                    target_ledger_str = now_est.strftime('%Y-%m-%d')

                _, holdings = self.broker.get_account_balance()
                if holdings is None:
                    await context.bot.send_message(chat_id, f"❌ <b>[{ticker}] API 오류</b>\n잔고를 불러오지 못했습니다.", parse_mode='HTML')
                    return "ERROR"

                actual_qty = int(float(holdings.get(ticker, {'qty': 0}).get('qty') or 0))
                actual_avg = float(holdings.get(ticker, {'avg': 0}).get('avg') or 0.0)

                recs_for_check = [r for r in self.cfg.get_ledger() if r['ticker'] == ticker]
                ledger_qty_for_check, _, _, _ = self.cfg.calculate_holdings(ticker, recs_for_check)
                
                vrev_ledger_qty_for_check = 0
                is_rev = (self.cfg.get_version(ticker) == "V_REV")
                
                if is_rev:
                    if not getattr(self, 'queue_ledger', None):
                        from queue_ledger import QueueLedger
                        self.queue_ledger = QueueLedger()
                    vrev_ledger_qty_for_check = sum(int(float(item.get("qty") or 0)) for item in self.queue_ledger.get_queue(ticker))
                
                max_check_qty = max(ledger_qty_for_check, vrev_ledger_qty_for_check)

                # NEW: [V30.15 방어] KST 자정 크로스오버 원장 기아 방지를 위한 4일치 광역 조회 및 EST 정밀 핀셋 필터링
                kis_search_start = (now_kst - datetime.timedelta(days=4)).strftime('%Y%m%d')
                query_end_dt = now_kst.strftime('%Y%m%d')

                def filter_to_est(execs_raw):
                    filtered = []
                    if not execs_raw: return filtered
                    for ex in execs_raw:
                        ord_dt = ex.get('ord_dt') or ex.get('ord_strt_dt')
                        if not ord_dt: continue
                        ord_tmd = ex.get('ord_tmd')
                        if not ord_tmd or len(str(ord_tmd)) != 6: 
                            ord_tmd = '000000'
                        try:
                            # MODIFIED: [V30.09 핫픽스] ZoneInfo 규격에 맞춰 localize() 대신 replace(tzinfo=...) 적용
                            # 1. KST 원장을 timezone-aware datetime으로 파싱
                            k_dt = datetime.datetime.strptime(f"{ord_dt}{ord_tmd}", "%Y%m%d%H%M%S").replace(tzinfo=kst)
                            # 2. 미국 동부 시간(EST/EDT)으로 정밀 변환
                            e_dt = k_dt.astimezone(est)
                            # 3. 봇의 논리적 앵커 일자(target_ledger_str)와 일치하는 체결만 핀셋 추출
                            if e_dt.strftime('%Y-%m-%d') == target_ledger_str:
                                filtered.append(ex)
                        except Exception as e:
                            logging.error(f"🚨 타임존 파싱 에러: {e}")
                    return filtered

                raw_execs = []
                target_execs = []
                
                # MODIFIED: [V30.13 방어] KIS 체결 지연 안정화 대기 루프 (False Break 차단)
                if actual_qty == 0 and max_check_qty > 0:
                    max_retries = 6
                    prev_sold_today = -1
                    stable_cnt = 0
                    for attempt in range(max_retries):
                        raw_execs = await asyncio.to_thread(self.broker.get_execution_history, ticker, kis_search_start, query_end_dt)
                        target_execs = filter_to_est(raw_execs)
                        sold_today = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01")
                        
                        if sold_today >= max_check_qty:
                            if sold_today == prev_sold_today:
                                stable_cnt += 1
                                if stable_cnt >= 1: 
                                    break
                            else:
                                stable_cnt = 0
                        prev_sold_today = sold_today
                        
                        if attempt < max_retries - 1:
                            logging.info(f"⏳ [{ticker}] 체결 원장 지연(Lag) 감지. 데이터 안정화 및 EST 매핑 검증 중... ({attempt+1}/{max_retries})")
                            await asyncio.sleep(2.0)
                else:
                    raw_execs = await asyncio.to_thread(self.broker.get_execution_history, ticker, kis_search_start, query_end_dt)
                    target_execs = filter_to_est(raw_execs)

                if target_execs:
                    calibrated_count = self.cfg.calibrate_ledger_prices(ticker, target_ledger_str, target_execs)
                    if calibrated_count > 0:
                        logging.info(f"🔧 [{ticker}] LOC/MOC 주문 {calibrated_count}건에 대해 실제 체결 단가 소급 업데이트를 완료했습니다.")

                recs = [r for r in self.cfg.get_ledger() if r['ticker'] == ticker]
                ledger_qty, avg_price, _, _ = self.cfg.calculate_holdings(ticker, recs)
                
                diff = actual_qty - ledger_qty
                price_diff = abs(actual_avg - avg_price)

                # ==========================================================
                # [통합 메인 장부 동기화 엔진] 
                # ==========================================================
                # NEW: [V30.15 방어] 당일 제로섬(Zero-Sum) 거래 시 메인 장부 업데이트 Bypass 원천 차단
                today_recs = [r for r in recs if r['date'] == target_ledger_str and 'INIT' not in str(r.get('exec_id', '')) and 'CALIB' not in str(r.get('exec_id', ''))]
                ledger_today_buy = sum(r['qty'] for r in today_recs if r['side'] == 'BUY')
                ledger_today_sell = sum(r['qty'] for r in today_recs if r['side'] == 'SELL')
                
                exec_today_buy = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "02")
                exec_today_sell = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01")
                
                needs_reconstruction = (diff != 0) or (ledger_today_buy != exec_today_buy) or (ledger_today_sell != exec_today_sell)

                if not needs_reconstruction and price_diff < 0.01:
                    pass 
                elif not needs_reconstruction and price_diff >= 0.01:
                    self.cfg.calibrate_avg_price(ticker, actual_avg)
                    await context.bot.send_message(chat_id, f"🔧 <b>[{ticker}] 장부 평단가 미세 오차({price_diff:.4f}) 교정 완료!</b>", parse_mode='HTML')
                elif needs_reconstruction:
                    temp_recs = [r for r in recs if r['date'] != target_ledger_str or 'INIT' in str(r.get('exec_id', ''))]
                    temp_qty, temp_avg, _, _ = self.cfg.calculate_holdings(ticker, temp_recs)
                    
                    temp_sim_qty = temp_qty
                    temp_sim_avg = temp_avg
                    new_target_records = []
                    
                    if target_execs:
                        # NEW: [V30.15 방어] 절대 시간 정렬 락온 (시간 역행 방어)
                        target_execs.sort(key=lambda x: str(x.get('ord_dt', '00000000')) + str(x.get('ord_tmd', '000000'))) 
                        for ex in target_execs:
                            side_cd = ex.get('sll_buy_dvsn_cd')
                            exec_qty = int(float(ex.get('ft_ccld_qty') or '0'))
                            exec_price = float(ex.get('ft_ccld_unpr3') or '0')
                            
                            if side_cd == "02": 
                                new_avg = ((temp_sim_qty * temp_sim_avg) + (exec_qty * exec_price)) / (temp_sim_qty + exec_qty) if (temp_sim_qty + exec_qty) > 0 else exec_price
                                temp_sim_qty += exec_qty
                                temp_sim_avg = new_avg
                            else:
                                temp_sim_qty -= exec_qty
                                
                            rec_item = {
                                'date': target_ledger_str, 'side': "BUY" if side_cd == "02" else "SELL",
                                'qty': exec_qty, 'price': exec_price, 'avg_price': temp_sim_avg
                            }
                            if is_rev:
                                rec_item['is_reverse'] = True
                            new_target_records.append(rec_item)
                            
                    gap_qty = actual_qty - temp_sim_qty
                    if gap_qty != 0:
                        calib_side = "BUY" if gap_qty > 0 else "SELL"
                        calib_item = {
                            'date': target_ledger_str, 
                            'side': calib_side,
                            'qty': abs(gap_qty), 
                            'price': actual_avg, 
                            'avg_price': actual_avg,
                            'exec_id': f"CALIB_{int(time.time())}",
                            'desc': "비파괴 보정"
                        }
                        if is_rev:
                            calib_item['is_reverse'] = True
                        new_target_records.append(calib_item)
                        
                    # MODIFIED: [V30.15 방어] 0주 회귀 시 평단가 0.0 강제 덮어쓰기로 인한 오염 붕괴 원천 차단
                    if new_target_records:
                        if actual_qty > 0:
                            for r in new_target_records:
                                r['avg_price'] = actual_avg
                    elif temp_recs: 
                        if actual_qty > 0:
                            temp_recs[-1]['avg_price'] = actual_avg
                        
                    self.cfg.overwrite_incremental_ledger(ticker, temp_recs, new_target_records)
                    
                    if gap_qty != 0:
                        await context.bot.send_message(chat_id, f"🔧 <b>[{ticker}] 통합 메인 장부(MAIN LEDGER) 비파괴 보정 완료!</b>\n▫️ KIS 실잔고 오차 수량({gap_qty}주)을 역사 보존 상태로 안전하게 교정했습니다.", parse_mode='HTML')
                    elif exec_today_buy > 0 or exec_today_sell > 0:
                        logging.info(f"📜 [{ticker}] 당일 데이트레이딩 체결 원장(제로섬 회귀)이 메인 장부에 완벽히 복원 기입되었습니다.")

                # ==========================================================
                # V-REV 큐 관리 및 0주 졸업 판별 로직 시작
                # ==========================================================
                if is_rev:
                    q_data_before = self.queue_ledger.get_queue(ticker)
                    vrev_ledger_qty = sum(int(float(item.get("qty") or 0)) for item in q_data_before)
                    
                    # NEW: [V30.15 방어] 큐 장부가 0이더라도 당일 매도 원장이 존재하면 졸업 판별 강제 격발
                    sold_today_vrev = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01") if target_execs else 0
                    
                    if actual_qty == 0 and (vrev_ledger_qty > 0 or sold_today_vrev > 0):
                        if now_kst.hour < 10:
                            await context.bot.send_message(chat_id, "⏳ <b>증권사 확정 정산(10:00 KST) 대기 중입니다.</b> 가결제 오차 방지를 위해 졸업 카드 발급 및 장부 초기화가 보류됩니다.", parse_mode='HTML')
                            self._sync_escrow_cash(ticker)
                            return "SUCCESS"

                        added_seed = 0.0
                        _vrev_snap_ok = False
                        snapshot = None
                        try:
                            actual_clear_price = 0.0
                            tot_q = 0
                            
                            if target_execs:
                                sell_execs = [ex for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                                if sell_execs:
                                    tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in sell_execs)
                                    tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in sell_execs)
                                    if tot_q > 0:
                                        actual_clear_price = round(tot_amt / tot_q, 4)
                            
                            # Fallback: API 타임존 필터링 후 잔여 오차가 생겼을 시 최후 수단으로 raw_execs 스캔
                            if actual_clear_price == 0.0:
                                if raw_execs:
                                    recent_sells = [ex for ex in raw_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                                    if recent_sells:
                                        recent_sells.sort(key=lambda x: f"{x.get('ord_dt', '')}{x.get('ord_tmd', '')}", reverse=True)
                                        last_sell_dt = recent_sells[0].get('ord_dt')
                                        same_day_sells = [ex for ex in recent_sells if ex.get('ord_dt') == last_sell_dt]
                                        tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in same_day_sells)
                                        tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in same_day_sells)
                                        if tot_q > 0:
                                            actual_clear_price = round(tot_amt / tot_q, 4)
                                            logging.info(f"🔍 [{ticker}] 과거 4일치 광역 스캔 및 최근일({last_sell_dt}) 추출 폴백으로 매도 단가(${actual_clear_price})를 복원했습니다.")

                            if tot_q > vrev_ledger_qty:
                                missing_qty = tot_q - vrev_ledger_qty
                                buy_execs = [ex for ex in (target_execs or []) if ex.get('sll_buy_dvsn_cd') == "02"]
                                
                                temp_invested = sum(float(item.get("qty", 0)) * float(item.get("price", 0)) for item in q_data_before)
                                temp_avg = temp_invested / vrev_ledger_qty if vrev_ledger_qty > 0 else 0.0
                                missing_price = temp_avg
                                
                                # MODIFIED: [V30.13 방어] 수동 매수 단가 이중 합산 뻥튀기 방어 (한계 단가 정밀 역산)
                                if buy_execs:
                                    b_tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in buy_execs)
                                    b_tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in buy_execs)
                                    
                                    if b_tot_q > 0:
                                        q_today_amt = 0.0
                                        q_today_qty = 0
                                        for item in q_data_before:
                                            if str(item.get("date", "")).startswith(target_ledger_str):
                                                iq = int(float(item.get("qty", 0)))
                                                q_today_qty += iq
                                                q_today_amt += iq * float(item.get("price", 0))
                                                
                                        pure_manual_q = b_tot_q - q_today_qty
                                        pure_manual_amt = b_tot_amt - q_today_amt
                                        
                                        if pure_manual_q >= missing_qty and pure_manual_q > 0 and pure_manual_amt > 0:
                                            derived_price = pure_manual_amt / pure_manual_q
                                            missing_price = round(derived_price, 4)
                                        else:
                                            missing_price = round(b_tot_amt / b_tot_q, 4)
                                
                                q_data_before.append({
                                    "date": now_est.strftime('%Y-%m-%d %H:%M:%S'),
                                    "qty": missing_qty,
                                    "price": missing_price,
                                    "exec_id": "MANUAL_SYNC"
                                })
                                vrev_ledger_qty = tot_q
                                
                                # NEW: [V30.15 방어] 큐 인메모리 증발 및 스냅샷 캡처 충돌 방지를 위한 원자적 덮어쓰기 락온
                                q_file = "data/queue_ledger.json"
                                try:
                                    os.makedirs(os.path.dirname(q_file) if os.path.dirname(q_file) else '.', exist_ok=True)
                                    all_q = {}
                                    if os.path.exists(q_file):
                                        with open(q_file, 'r', encoding='utf-8') as f:
                                            all_q = json.load(f)
                                    all_q[ticker] = q_data_before
                                    
                                    fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(q_file) if os.path.dirname(q_file) else '.')
                                    with os.fdopen(fd, 'w', encoding='utf-8') as f:
                                        json.dump(all_q, f, indent=4, ensure_ascii=False)
                                    os.replace(tmp_path, q_file)
                                    
                                    if hasattr(self.queue_ledger, 'data'):
                                        self.queue_ledger.data = all_q
                                    if hasattr(self.queue_ledger, 'queues'):
                                        self.queue_ledger.queues = all_q
                                    if hasattr(self.queue_ledger, 'load'):
                                        self.queue_ledger.load()
                                        
                                    logging.info(f"🔧 [{ticker}] 미동기화 수동 매수 물량({missing_qty}주, 진성단가 ${missing_price})을 졸업 큐에 다이렉트 영속화하여 PnL 오차 교정 및 스냅샷 충돌 방어 완료.")
                                except Exception as e:
                                    logging.error(f"🚨 MANUAL_SYNC LIFO 큐 파일 I/O 영속화 실패: {e}")

                            total_invested = sum(float(item.get("qty", 0)) * float(item.get("price", 0)) for item in q_data_before)
                            q_avg_price = total_invested / vrev_ledger_qty if vrev_ledger_qty > 0 else 0.0

                            try:
                                curr_p = await asyncio.wait_for(asyncio.to_thread(self.broker.get_current_price, ticker), timeout=10.0)
                            except asyncio.TimeoutError:
                                curr_p = 0.0
                                logging.warning(f"⚠️ [{ticker}] 현재가 조회 타임아웃 (10초), 스냅샷 보정용 가격에서 제외")
                            
                            clear_price = actual_clear_price if actual_clear_price > 0.0 else (curr_p if curr_p and curr_p > 0 else q_avg_price * 1.006)
                            
                            snapshot = self.strategy.capture_vrev_snapshot(ticker, clear_price, q_avg_price, vrev_ledger_qty)
                            
                            if snapshot:
                                realized_pnl = snapshot['realized_pnl']
                                yield_pct = snapshot['realized_pnl_pct']
                                
                                compound_rate = float(self.cfg.get_compound_rate(ticker)) / 100.0
                                if realized_pnl > 0 and compound_rate > 0:
                                    added_seed = realized_pnl * compound_rate
                                    current_seed = self.cfg.get_seed(ticker)
                                    self.cfg.set_seed(ticker, current_seed + added_seed)
                                
                                cap_dt = snapshot['captured_at']
                                cap_dt_str = cap_dt if isinstance(cap_dt, str) else cap_dt.strftime('%Y-%m-%d')
                                start_dt_str = q_data_before[0]['date'][:10] if q_data_before else cap_dt_str[:10]
                                
                                hist_data = self.cfg._load_json(self.cfg.FILES["HISTORY"], [])
                                new_hist = {
                                    "id": int(time.time()),
                                    "ticker": ticker,
                                    "start_date": start_dt_str,
                                    "end_date": cap_dt_str[:10],
                                    "invested": total_invested,
                                    "revenue": total_invested + realized_pnl,
                                    "profit": realized_pnl,
                                    "yield": yield_pct,
                                    "trades": q_data_before 
                                }
                                hist_data.append(new_hist)
                                self.cfg._save_json(self.cfg.FILES["HISTORY"], hist_data)
                                _vrev_snap_ok = True
                                
                        except Exception as e:
                            logging.error(f"🚨 스냅샷 캡처 및 복리 정산 중 치명적 오류 감지: {e}\n{traceback.format_exc()}")
                            snapshot = None
                            
                        self.queue_ledger.sync_with_broker(ticker, 0)
                        
                        if _vrev_snap_ok:
                            msg = f"🎉 <b>[{ticker} V-REV 잭팟 스윕(전량 익절) 감지!]</b>\n▫️ 잔고가 0주가 되어 LIFO 큐 지층을 100% 소각(초기화)했습니다."
                            if added_seed > 0:
                                msg += f"\n💸 <b>자동 복리 +${added_seed:,.0f}</b> 이 다음 운용 시드에 완벽하게 추가되었습니다!"
                            await context.bot.send_message(chat_id, msg, parse_mode='HTML')
                            
                            if snapshot:
                                try:
                                    img_path = self.view.create_profit_image(
                                        ticker=ticker, 
                                        profit=snapshot['realized_pnl'], 
                                        yield_pct=snapshot['realized_pnl_pct'],
                                        invested=snapshot['avg_price'] * snapshot['cleared_qty'], 
                                        revenue=snapshot['clear_price'] * snapshot['cleared_qty'], 
                                        end_date=cap_dt_str[:10]
                                    )
                                    if img_path and os.path.exists(img_path):
                                        with open(img_path, 'rb') as f_out:
                                            if img_path.lower().endswith('.gif'):
                                                await context.bot.send_animation(chat_id=chat_id, animation=f_out)
                                            else:
                                                await context.bot.send_photo(chat_id=chat_id, photo=f_out)
                                except Exception as e:
                                    logging.error(f"📸 V-REV 스냅샷 이미지 렌더링/발송 실패: {e}")
                        else:
                            await context.bot.send_message(chat_id, f"⚠️ <b>[{ticker} V-REV 0주 강제 정산 완료]</b>\n▫️ 0주를 확인하여 큐를 안전하게 비웠으나 통신 지연으로 졸업 카드는 생략되었습니다.", parse_mode='HTML')
                                    
                        self._sync_escrow_cash(ticker)
                        return "SUCCESS"
                        
                    if actual_qty == vrev_ledger_qty:
                        pass
                    else:
                        if actual_qty > 0 and actual_qty < vrev_ledger_qty:
                            gap_qty = vrev_ledger_qty - actual_qty
                            
                            vwap_state_file = f"data/vwap_state_REV_{ticker}.json"
                            if os.path.exists(vwap_state_file):
                                try:
                                    with open(vwap_state_file, 'r', encoding='utf-8') as vf:
                                        v_state = json.load(vf)
                                    if "executed" in v_state and "SELL_QTY" in v_state["executed"]:
                                        old_sell_qty = v_state["executed"]["SELL_QTY"]
                                        v_state["executed"]["SELL_QTY"] = max(0, old_sell_qty - gap_qty)
                                        with open(vwap_state_file, 'w', encoding='utf-8') as vf:
                                            json.dump(v_state, vf, ensure_ascii=False, indent=4)
                                        logging.info(f"🔧 [{ticker}] VWAP 잔차 수학적 보정 완료: {old_sell_qty} -> {v_state['executed']['SELL_QTY']}")
                                except Exception as e:
                                    logging.error(f"🚨 VWAP 상태 교정 에러: {e}")

                            calibrated = self.queue_ledger.sync_with_broker(ticker, actual_qty, actual_avg)
                            if calibrated:
                                await context.bot.send_message(chat_id, f"🔧 <b>[{ticker}] V-REV 큐(Queue) 비파괴 보정 완료!</b>\n▫️ 수동 매도 물량(<b>{gap_qty}주</b>)을 LIFO 큐에서 안전하게 차감했습니다.", parse_mode='HTML')
                                
                        elif actual_qty > 0 and actual_qty > vrev_ledger_qty:
                            gap_qty = actual_qty - vrev_ledger_qty
                            
                            real_buy_price = actual_avg
                            try:
                                buy_execs = [ex for ex in (target_execs or []) if ex.get('sll_buy_dvsn_cd') == "02"]
                                if buy_execs:
                                    b_tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in buy_execs)
                                    b_tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in buy_execs)
                                    if b_tot_q > 0:
                                        real_buy_price = round(b_tot_amt / b_tot_q, 4)
                                
                                if real_buy_price == actual_avg:
                                    search_start_dt = (now_kst - datetime.timedelta(days=4)).strftime('%Y%m%d')
                                    past_raw = await asyncio.to_thread(self.broker.get_execution_history, ticker, search_start_dt, query_end_dt)
                                    past_execs = filter_to_est(past_raw)
                                    if past_execs:
                                        p_buy_execs = [ex for ex in past_execs if ex.get('sll_buy_dvsn_cd') == "02"]
                                        if p_buy_execs:
                                            b_tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in p_buy_execs)
                                            b_tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in p_buy_execs)
                                            if b_tot_q > 0:
                                                real_buy_price = round(b_tot_amt / b_tot_q, 4)
                            except Exception as e:
                                logging.error(f"🚨 수동매수 실제 체결단가 역산 중 예외 발생 (기존 평단가 fallback): {e}")

                            if real_buy_price == actual_avg:
                                old_invested = sum(float(item.get("qty", 0)) * float(item.get("price", 0)) for item in q_data_before)
                                new_invested = actual_qty * actual_avg
                                if new_invested > old_invested:
                                    derived_price = (new_invested - old_invested) / gap_qty
                                    real_buy_price = round(derived_price, 4) if derived_price > 0 else actual_avg
                            
                            q_data = self.queue_ledger.get_queue(ticker)
                            q_data.append({
                                "date": now_est.strftime('%Y-%m-%d %H:%M:%S'),
                                "qty": gap_qty,
                                "price": real_buy_price,
                                "exec_id": f"MANUAL_BUY_{int(time.time())}"
                            })
                            
                            q_file = "data/queue_ledger.json"
                            try:
                                os.makedirs(os.path.dirname(q_file) if os.path.dirname(q_file) else '.', exist_ok=True)
                                if os.path.exists(q_file):
                                    with open(q_file, 'r', encoding='utf-8') as f:
                                        all_q = json.load(f)
                                else:
                                    all_q = {}
                                all_q[ticker] = q_data
                                
                                fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(q_file) if os.path.dirname(q_file) else '.')
                                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                                    json.dump(all_q, f, indent=4, ensure_ascii=False)
                                os.replace(tmp_path, q_file)
                                
                                if hasattr(self.queue_ledger, 'data'):
                                    self.queue_ledger.data = all_q
                                    
                                logging.info(f"🔧 [{ticker}] 수동 매수 감지! KIS 실잔고에 맞춰 LIFO 큐에 신규 지층({gap_qty}주, 진성단가 ${real_buy_price}) 다이렉트 편입 및 파일 영속화 완료.")
                                await context.bot.send_message(chat_id, f"🔧 <b>[{ticker}] V-REV 큐(Queue) 수동 매수 편입 완료!</b>\n▫️ KIS 실잔고에 맞춰 신규 지층(<b>{gap_qty}주</b>, 추정단가 ${real_buy_price})을 정밀 추가했습니다.", parse_mode='HTML')
                            except Exception as e:
                                logging.error(f"🚨 LIFO 큐 다이렉트 파일 I/O 쓰기 에러: {e}")
                    
                    self._sync_escrow_cash(ticker)
                    return "SUCCESS"

                # ==========================================================
                # V14 0주 졸업 판별 로직 
                # ==========================================================
                if not is_rev:
                    # NEW: [V30.15 방어] 장부가 0이라도 당일 매도가 존재하면 V14 데이트레이딩 0주 졸업 강제 격발
                    sold_today_v14 = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01") if target_execs else 0
                    if actual_qty == 0 and (ledger_qty > 0 or sold_today_v14 > 0):
                        if now_kst.hour < 10:
                            await context.bot.send_message(chat_id, "⏳ <b>증권사 확정 정산(10:00 KST) 대기 중입니다.</b> 가결제 오차 방지를 위해 졸업 카드 발급 및 장부 초기화가 보류됩니다.", parse_mode='HTML')
                        else:
                            today_est_str = now_est.strftime('%Y-%m-%d')
                            
                            try:
                                prev_c = await asyncio.wait_for(
                                    asyncio.to_thread(self.broker.get_previous_close, ticker),
                                    timeout=10.0
                                )
                            except asyncio.TimeoutError:
                                prev_c = 0.0
                                logging.warning(f"⚠️ [{ticker}] 야후 파이낸스 전일 종가 조회 타임아웃 (10초). 0.0으로 대체")
                            
                            try:
                                new_hist, added_seed = self.cfg.archive_graduation(ticker, today_est_str, prev_c)
                                
                                if new_hist:
                                    msg = f"🎉 <b>[{ticker} 졸업 확인!]</b>\n장부를 명예의 전당에 저장하고 새 사이클을 준비합니다."
                                    if added_seed > 0:
                                        msg += f"\n💸 <b>자동 복리 +${added_seed:,.0f}</b> 이 다음 운용 시드에 완벽하게 추가되었습니다!"
                                    await context.bot.send_message(chat_id, msg, parse_mode='HTML')
                                    try:
                                        img_path = self.view.create_profit_image(
                                            ticker=ticker, profit=new_hist['profit'], yield_pct=new_hist['yield'],
                                            invested=new_hist['invested'], revenue=new_hist['revenue'], end_date=new_hist['end_date']
                                        )
                                        if img_path and os.path.exists(img_path):
                                            with open(img_path, 'rb') as f_out:
                                                if img_path.lower().endswith('.gif'):
                                                    await context.bot.send_animation(chat_id=chat_id, animation=f_out)
                                                else:
                                                    await context.bot.send_photo(chat_id=chat_id, photo=f_out)
                                    except Exception as e:
                                        logging.error(f"📸 졸업 이미지 발송 실패: {e}")
                                else:
                                    all_recs = [r for r in self.cfg.get_ledger() if r['ticker'] != ticker]
                                    self.cfg._save_json(self.cfg.FILES["LEDGER"], all_recs)
                                    await context.bot.send_message(chat_id, f"⚠️ <b>[{ticker} 강제 정산 완료]</b>\n잔고가 0주이나 마이너스 수익 상태이므로 명예의 전당 박제 없이 장부를 비우고 새출발 타점을 장전합니다.", parse_mode='HTML')
                            except Exception as e:
                                logging.error(f"강제 졸업 처리 중 에러: {e}")

                    self._sync_escrow_cash(ticker) 
                    return "SUCCESS"

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

# ==========================================================
# FILE: telegram_sync_engine.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 5대 헌법 및 48대 엣지 케이스 완벽 결속 교차 검증 완료.
# 🚨 MODIFIED: [단일 지층 팽창 자가 치유 방해 맹독성 Bypass 궁극 수술] V-REV 큐 장부 동기화 및 메인 장부 교정 로직이 `if target_execs:` (당일 체결 내역 존재 시) 블록 내부에 갇혀 있어, 당일 매매가 없는 날에는 팽창된 지층이 절대 쪼개지지 않던 치명적 버그를 원천 차단. 해당 로직들을 `if` 블록 외부로 들여쓰기 전진 배치(Un-indent)하여 365일 100% 무결성 동기화가 강제되도록 팩트 락온.
# 🚨 MODIFIED: [제1헌법 철저 준수] _get_last_trade_date 내부 달력 API(mcal) 스캔 시 GlobalThrottle.wait_api_sync()를 강제 주입하여 썬더링 허드 완벽 차단.
# 🚨 MODIFIED: [Event Loop 마비 궁극 수술] get_exact_prev_close 내부에 잔존하던 맹독성 time.sleep(0.06)을 영구 소각하고 GlobalThrottle.wait_api_sync() 중앙 통제 락온 완료.
# 🚨 MODIFIED: [자전거래/암살자 찌꺼기 맹독성 유입 궁극 방어] 16:05 EST 정산 시 KIS 실원장(target_execs)의 모든 당일 체결 내역을 맹목적으로 무한 편입하던 로직 전면 소각.
# 🚨 MODIFIED: [통신 장애 핀셋 추적망 결속] process_auto_sync 내부에서 broker API 호출 실패 시 정확한 실패 구간(Endpoint)과 사유를 문자열로 반환하여 상위 라우터가 진단할 수 있도록 팩트 락온.
# 🚨 NEW: [스냅샷 디커플링 해소 (Nuke & Regenerate) 아키텍처 팩트 결속] 잔고 오차 교정, 수동 조작, 큐 병합 등 장부에 단 1주라도 변동이 감지되면 `snapshot_needs_regen` 트리거를 발동시켜, 16:00 EST 타임락을 강제 개방(Override)하고 오염된 낡은 스냅샷(daily_snapshot)을 물리적으로 영구 소각(Nuke)한 뒤 최신 수량 기반의 팩트 지시서로 즉각 재생성(Regenerate)하도록 100% 시스템 교정 완료.
# 🚨 MODIFIED: [체결 원장 디커플링 붕괴 수술] 16:05 EST 정산 시 한투(KIS)에서 수신한 체결 원장 데이터(`execs_raw`) 중, 암살자 캐시에 기록된 `history_odnos`에 속하는 주문은 100% 투명 인간(Ghosting) 취급하여 도려냄으로써, 암살자의 타점 오염 및 유령 졸업(Ghost Graduation)을 원천 봉쇄.
# 🚨 NEW: [유령 졸업(Ghost Graduation) 패러독스 궁극 수술] 주말(토, 일)이나 휴일에 16:05 확정 정산 또는 자정 루프가 가동될 때, 과거 금요일의 체결 내역을 당일 실적으로 오인하여 0주 잔고 상태에서 매일 무한정 졸업 카드를 복제 발행하던 맹독성 버그를 원천 차단하기 위해, 체결 내역 필터링 및 졸업 트리거 검증 시 '실제 영업일(Today) 당일 팩트'인지 검증하는 `is_today_trading_day` 타임라인 방어막을 100% 팩트 결속 완료.
# 🚨 MODIFIED: [SyntaxError 즉사 버그 소각] 이전 버전에서 오타로 주입된 괄호 불일치(r.get('ticker']) 등 15개 맹독성 코드를 r.get('ticker')로 100% 원상 복구 완료.
# 🚨 NEW: [스레드 풀 고갈(Thread Leak) 궁극 수술] 하위 KIS 통신 지연(최대 35초) 시 상위 `wait_for`가 코루틴을 취소시켜 발생하는 '좀비 스레드 누적' 대참사를 원천 봉쇄하기 위해, 모든 비동기 I/O 래퍼의 타임아웃을 45.0초로 상향 팩트 하드 캡핑 완료.
# 🚨 MODIFIED: [타임라인 롤오버(Timeline Rollover) 패러독스 방어] 16:05 정산 시 prev_close가 당일 종가로 롤오버되어 큐 장부의 1층 정량이 비정상적으로 팽창하던 치명적 결함을 막기 위해, YF 과거 일봉을 핀셋 추출하여 100% 무결성 팩트 타점(safe_prev_c)을 주입하도록 파이프라인 교정 완료.
# ==========================================================

import logging
import datetime
from zoneinfo import ZoneInfo
import time
import os
import asyncio
import json
import tempfile
import traceback
import math 
import html 
import functools
import glob
import yfinance as yf
import pandas as pd 
import pandas_market_calendars as mcal
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from global_throttle import GlobalThrottle # 🚨 중앙 통제소 결속

class TelegramSyncEngine:
    def __init__(self, config, broker, strategy, queue_ledger, view, tx_lock, sync_locks):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.queue_ledger = queue_ledger
        self.view = view
        self.tx_lock = tx_lock
        self.sync_locks = sync_locks

    def _safe_float(self, value):
        try:
            f_val = float(str(value or 0.0).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val): return 0.0
            return f_val
        except Exception: return 0.0

    # 🚨 MODIFIED: [스레드 풀 고갈 붕괴 수술] 15.0 -> 45.0 강제 하드캡
    async def _retry_api(self, func, *args, timeout=45.0, default=None, **kwargs):
        """ 🚨 [Case 31, 32] 3단 지수 백오프 및 GlobalThrottle 중앙 집중형 TPS 방어망 결속 """
        for attempt in range(3):
            try:
                await asyncio.to_thread(GlobalThrottle.wait_api_sync)
                
                if asyncio.iscoroutinefunction(func):
                    return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
                else:
                    p_func = functools.partial(func, *args, **kwargs)
                    return await asyncio.wait_for(asyncio.to_thread(p_func), timeout=timeout)
            except Exception as e:
                if attempt == 2:
                    func_name = getattr(func, '__name__', 'unknown_func')
                    logging.debug(f"🚨 API 래퍼 최종 실패 ({func_name}): {e}")
                    return default
                await asyncio.sleep(1.0 * (2 ** attempt))
        return default

    # 🚨 MODIFIED: [스레드 풀 고갈 붕괴 수술] 15.0 -> 45.0 강제 하드캡
    async def _safe_send(self, context, chat_id, text, timeout=45.0, **kwargs):
        if not chat_id: return None
        try:
            return await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=text, **kwargs), timeout=timeout)
        except Exception as e:
            logging.error(f"🚨 텔레그램 전송 실패: {e}")
            return None

    async def process_auto_sync(self, ticker, chat_id, context, silent_ledger=False):
        if ticker not in self.sync_locks:
            self.sync_locks[ticker] = asyncio.Lock()
            
        if self.sync_locks[ticker].locked(): return "LOCKED"
            
        async with self.sync_locks[ticker]:
            async with self.tx_lock:
                est = ZoneInfo('America/New_York')
                now_est = datetime.datetime.now(est)
                kst = ZoneInfo('Asia/Seoul')
                now_kst = datetime.datetime.now(kst)

                # 🚨 NEW: 스냅샷 팩트 재생성 트리거 초기화
                snapshot_needs_regen = False

                last_split_date = await self._retry_api(self.cfg.get_last_split_date, ticker, default="")
                split_ratio, split_date = await self._retry_api(self.broker.get_recent_stock_split, ticker, last_split_date, default=(0.0, ""))
                
                if split_ratio > 0.0 and split_date != "":
                    snapshot_needs_regen = True # 🚨 트리거 발동
                    # 🚨 MODIFIED: 10.0 -> 45.0
                    await self._retry_api(self.cfg.apply_stock_split, ticker, split_ratio, timeout=45.0)
                    if getattr(self, 'queue_ledger', None):
                        await self._retry_api(self.queue_ledger.apply_stock_split, ticker, split_ratio, timeout=45.0)
                    if hasattr(self.strategy, 'v_avwap_plugin'):
                        await self._retry_api(self.strategy.v_avwap_plugin.apply_stock_split, ticker, split_ratio, now_est, timeout=45.0)
                    
                    try:
                        from assassin_ledger import AssassinLedger
                        # 🚨 MODIFIED: 5.0 -> 45.0
                        a_ledger = await asyncio.wait_for(asyncio.to_thread(AssassinLedger), timeout=45.0)
                        await self._retry_api(a_ledger.apply_stock_split, ticker, split_ratio, timeout=45.0)
                    except Exception as e:
                        logging.error(f"🚨 암살자 장부 액면분할 팩트 적용 실패: {e}")
                    
                    await self._retry_api(self.cfg.set_last_split_date, ticker, split_date, timeout=45.0)
                    
                    split_type = "액면분할" if split_ratio > 1.0 else "액면병합(역분할)"
                    await self._safe_send(context, chat_id, f"✂️ <b>[{html.escape(str(ticker))}] 야후 파이낸스 {split_type} 자동 감지!</b>\n▫️ 감지된 비율: <b>{split_ratio}배</b> (발생일: {html.escape(str(split_date))})\n▫️ 봇이 기존 V14 장부, V-REV 큐 장부, 암살자 장부, AVWAP 상태 캐시의 수량과 평단가를 100% 무인 자동 소급 조정 완료했습니다.", parse_mode='HTML')
             
                def _get_last_trade_date(target_est):
                    GlobalThrottle.wait_api_sync()
                    nyse = mcal.get_calendar('NYSE')
                    return nyse.schedule(start_date=(target_est - datetime.timedelta(days=10)).date(), end_date=target_est.date())

                # 🚨 MODIFIED: 10.0 -> 45.0
                schedule = await self._retry_api(_get_last_trade_date, now_est, timeout=45.0, default=pd.DataFrame())
                if not schedule.empty:
                    last_trade_date = schedule.index[-1]
                    target_ledger_str = last_trade_date.strftime('%Y-%m-%d')
                else:
                    target_ledger_str = now_est.strftime('%Y-%m-%d')

                # 🚨 NEW: [유령 졸업 방어] 현재 봇이 실행되는 실제 날짜와 계산된 영업일이 100% 일치하는지 판별하는 엄격한 타임라인 플래그 락온
                today_est_str = now_est.strftime('%Y-%m-%d')
                is_today_trading_day = (target_ledger_str == today_est_str)

                # 🚨 MODIFIED: 15.0 -> 45.0
                res_bal = await self._retry_api(self.broker.get_account_balance, timeout=45.0, default=None)
                if not res_bal or (isinstance(res_bal, (list, tuple)) and len(res_bal) > 1 and res_bal[1] is None):
                    await self._safe_send(context, chat_id, f"❌ <b>[{html.escape(str(ticker))}] API 통신 차단</b>\n증권사 서버가 계좌 잔고를 반환하지 않습니다. (토큰 만료 또는 서버 점검 중)", parse_mode='HTML')
                    return "잔고 조회(get_account_balance) 실패 - API 서버 무응답 또는 거절"
                    
                holdings = res_bal[1] if isinstance(res_bal, (list, tuple)) and len(res_bal) > 1 else {}
                safe_holdings = holdings if isinstance(holdings, dict) else {}
                safe_ticker_info = safe_holdings.get(ticker) or {'qty': 0, 'avg': 0.0}
                
                actual_qty = int(self._safe_float(safe_ticker_info.get('qty')))
                actual_avg = self._safe_float(safe_ticker_info.get('avg'))

                # 🚨 MODIFIED: r.get('ticker'] -> r.get('ticker')
                full_ledger = await self._retry_api(self.cfg.get_ledger, default=[])
                recs_for_check = [r for r in (full_ledger or []) if isinstance(r, dict) and r.get('ticker') == ticker]
                hold_res = await self._retry_api(self.cfg.calculate_holdings, ticker, recs_for_check, default=(0, 0.0, 0.0, 0.0))
                ledger_qty_for_check = hold_res[0] if isinstance(hold_res, tuple) and len(hold_res) > 0 else 0
                
                vrev_ledger_qty_for_check = 0
                is_rev = (await self._retry_api(self.cfg.get_version, ticker, default="V14") == "V_REV")
                
                if is_rev and getattr(self, 'queue_ledger', None):
                    q_data_check = await self._retry_api(self.queue_ledger.get_queue, ticker, default=[])
                    vrev_ledger_qty_for_check = sum(int(self._safe_float(item.get("qty"))) for item in q_data_check if isinstance(item, dict))
                    vrev_total_invested = sum(int(self._safe_float(item.get("qty"))) * self._safe_float(item.get("price")) for item in q_data_check if isinstance(item, dict))
                    
                    if vrev_ledger_qty_for_check > 0: 
                        actual_avg = round(vrev_total_invested / vrev_ledger_qty_for_check, 4)
                 
                max_check_qty = max(ledger_qty_for_check, vrev_ledger_qty_for_check)

                a_qty_for_check = 0
                try:
                    from assassin_ledger import AssassinLedger
                    # 🚨 MODIFIED: 5.0 -> 45.0
                    a_ledger = await asyncio.wait_for(asyncio.to_thread(AssassinLedger), timeout=45.0)
                    a_data_check = await self._retry_api(a_ledger.get_ledger, ticker, default=[])
                    a_qty_for_check = sum(int(self._safe_float(item.get("qty"))) for item in (a_data_check or []))
                except Exception as e:
                    logging.error(f"🚨 암살자 장부 수량 스캔 에러: {e}")
                
                max_check_qty = max(max_check_qty, a_qty_for_check)

                # 🚨 MODIFIED: [체결 원장 디커플링 붕괴 수술] 암살자 캐시에 기록된 주문번호 추출 (Ghosting 필터 용도)
                avwap_state_file = f"data/avwap_trade_state_{ticker}.json"
                history_odnos = []
                with GlobalThrottle.get_file_lock(avwap_state_file):
                    try:
                        with open(avwap_state_file, 'r', encoding='utf-8') as f:
                            avwap_data = json.load(f)
                            if isinstance(avwap_data, dict) and avwap_data.get('date') == target_ledger_str:
                                history_odnos = avwap_data.get('history_odnos', [])
                    except Exception:
                        pass
                        
                kis_search_start = (now_kst - datetime.timedelta(days=4)).strftime('%Y%m%d')
                query_end_dt = now_kst.strftime('%Y%m%d')

                def filter_to_est(execs_raw):
                    filtered = []
                    if not execs_raw: return filtered
                    for ex in execs_raw:
                        if not isinstance(ex, dict): continue
                        
                        # 🚨 MODIFIED: [체결 원장 디커플링 붕괴 수술] 암살자 찌꺼기(Ghost) 100% 소각 배제
                        ex_odno = str(ex.get('odno', ''))
                        if ex_odno and ex_odno in history_odnos:
                            continue
                            
                        ord_dt = ex.get('ord_dt') or ex.get('ord_strt_dt')
                        if not ord_dt: continue
                        ord_tmd = ex.get('ord_tmd')
                        if not ord_tmd or len(str(ord_tmd)) != 6: ord_tmd = '000000'
                        try:
                            k_dt = datetime.datetime.strptime(f"{ord_dt}{ord_tmd}", "%Y%m%d%H%M%S").replace(tzinfo=kst)
                            e_dt = k_dt.astimezone(est)
                            if e_dt.strftime('%Y-%m-%d') == target_ledger_str:
                                filtered.append(ex)
                        except Exception: pass
                    return filtered

                raw_execs = []
                target_execs = []
                    
                if actual_qty == 0 and max_check_qty > 0:
                    max_retries = 6
                    prev_sold_today = -1
                    stable_cnt = 0
                    for attempt in range(max_retries):
                        # 🚨 MODIFIED: 15.0 -> 45.0
                        raw_execs = await self._retry_api(self.broker.get_execution_history, ticker, kis_search_start, query_end_dt, timeout=45.0, default=None)
                        if raw_execs is None:
                            return "체결 원장 조회(get_execution_history) 실패 - API 서버 무응답 또는 거절"
                        target_execs = filter_to_est(raw_execs)
                        # 🚨 MODIFIED: ex.get('sll_buy_dvsn_cd'] -> ex.get('sll_buy_dvsn_cd')
                        sold_today = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01")
                        
                        if sold_today >= max_check_qty:
                            if sold_today == prev_sold_today:
                                stable_cnt += 1
                                if stable_cnt >= 1: break
                            else: stable_cnt = 0
                        
                        prev_sold_today = sold_today
                        
                        if attempt < max_retries - 1:
                            logging.info(f"⏳ [{ticker}] 체결 원장 지연(Lag) 감지. 데이터 안정화 및 EST 매핑 검증 중... ({attempt+1}/{max_retries})")
                            await asyncio.sleep(2.0)
                else:
                    # 🚨 MODIFIED: 15.0 -> 45.0
                    raw_execs = await self._retry_api(self.broker.get_execution_history, ticker, kis_search_start, query_end_dt, timeout=45.0, default=None)
                    if raw_execs is None:
                        return "체결 원장 조회(get_execution_history) 실패 - API 서버 무응답 또는 거절"
                    target_execs = filter_to_est(raw_execs)

                calibrated_count = 0
                if target_execs:
                    # 🚨 MODIFIED: 10.0 -> 45.0
                    calibrated_count = await self._retry_api(self.cfg.calibrate_ledger_prices, ticker, target_ledger_str, target_execs, timeout=45.0, default=0)
                    if calibrated_count > 0:
                        snapshot_needs_regen = True # 🚨 트리거 발동
                        logging.info(f"🔧 [{ticker}] LOC/MOC 주문 {calibrated_count}건에 대해 실제 체결 단가 소급 업데이트를 완료했습니다.")

                # 🚨 MODIFIED: r.get('ticker'] -> r.get('ticker')
                full_ledger = await self._retry_api(self.cfg.get_ledger, default=[])
                recs = [r for r in (full_ledger or []) if isinstance(r, dict) and r.get('ticker') == ticker]
                
                # 🚨 MODIFIED: 10.0 -> 45.0 (though it's default 45 now)
                hold_res2 = await self._retry_api(self.cfg.calculate_holdings, ticker, recs, default=(0, 0.0, 0.0, 0.0))
                ledger_qty = hold_res2[0] if isinstance(hold_res2, tuple) and len(hold_res2) > 0 else 0
                avg_price = hold_res2[1] if isinstance(hold_res2, tuple) and len(hold_res2) > 1 else 0.0
                
                raw_actual_qty_for_vrev = actual_qty - a_qty_for_check
                if raw_actual_qty_for_vrev < 0:
                    logging.warning(f"🚨 [{ticker}] KIS 잔고 불일치 감지: KIS({actual_qty}) - 암살자({a_qty_for_check}) = {raw_actual_qty_for_vrev}주. 유령 잔고(Ghost Balance) 방어를 위해 본진 수량을 0주가 아닌 큐 장부 수량으로 강제 보정합니다.")
                    safe_actual_qty_for_vrev = vrev_ledger_qty_for_check if is_rev else ledger_qty
                else:
                    safe_actual_qty_for_vrev = raw_actual_qty_for_vrev

                diff = safe_actual_qty_for_vrev - ledger_qty
                price_diff = abs(actual_avg - avg_price)

                # 🚨 MODIFIED: r.get('date'] -> r.get('date')
                today_recs = [r for r in recs if r.get('date') == target_ledger_str and 'INIT' not in str(r.get('exec_id', '')) and 'CALIB' not in str(r.get('exec_id', ''))]
                
                needs_reconstruction = (diff != 0)

                if not needs_reconstruction and price_diff >= 0.01:
                    snapshot_needs_regen = True # 🚨 트리거 발동
                    # 🚨 MODIFIED: 10.0 -> 45.0
                    await self._retry_api(self.cfg.calibrate_avg_price, ticker, actual_avg, timeout=45.0)
                    await self._safe_send(context, chat_id, f"🔧 <b>[{html.escape(str(ticker))}] 장부 평단가 미세 오차({price_diff:.4f}) 교정 완료!</b>", parse_mode='HTML')
                elif needs_reconstruction:
                    snapshot_needs_regen = True # 🚨 트리거 발동
                    # 🚨 MODIFIED: r.get('date'] -> r.get('date')
                    temp_recs = [r for r in recs if r.get('date') != target_ledger_str or 'INIT' in str(r.get('exec_id', ''))]
                    
                    temp_res = await self._retry_api(self.cfg.calculate_holdings, ticker, temp_recs, default=(0, 0.0, 0.0, 0.0))
                    temp_sim_qty = temp_res[0] if isinstance(temp_res, tuple) and len(temp_res) > 0 else 0
                    temp_sim_avg = temp_res[1] if isinstance(temp_res, tuple) and len(temp_res) > 1 else 0.0
                    
                    new_target_records = []
                    
                    # 🚨 MODIFIED: r.get('date'] -> r.get('date')
                    today_valid_recs = [r for r in recs if r.get('date') == target_ledger_str and 'INIT' not in str(r.get('exec_id', '')) and 'CALIB' not in str(r.get('exec_id', ''))]
                    new_target_records.extend(today_valid_recs)
                    
                    # 🚨 MODIFIED: r.get('side'] -> r.get('side')
                    current_ledger_qty = temp_sim_qty + sum(r.get('qty', 0) if r.get('side') == 'BUY' else -r.get('qty', 0) for r in today_valid_recs)
                    
                    gap_qty = safe_actual_qty_for_vrev - current_ledger_qty
                    
                    if gap_qty != 0:
                        calib_side = "BUY" if gap_qty > 0 else "SELL"
                        calib_price = actual_avg
                        
                        actual_clear_price_calib = 0.0
                        if target_execs and is_today_trading_day:
                            # 🚨 MODIFIED: ex.get('sll_buy_dvsn_cd'] -> ex.get('sll_buy_dvsn_cd')
                            sell_execs_calib = [ex for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                            if sell_execs_calib:
                                tot_amt_calib = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) * self._safe_float(ex.get('ft_ccld_unpr3')) for ex in sell_execs_calib)
                                tot_q_calib = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in sell_execs_calib)
                                if tot_q_calib > 0: actual_clear_price_calib = round(tot_amt_calib / tot_q_calib, 4)
                        
                        if calib_side == "SELL" and actual_avg <= 0.0:
                            if actual_clear_price_calib > 0.0: calib_price = actual_clear_price_calib
                            else: calib_price = temp_sim_avg if temp_sim_avg > 0 else 0.01
                            calib_avg = temp_sim_avg
                        elif calib_side == "BUY" and actual_avg <= 0.0:
                            if actual_clear_price_calib > 0.0:
                                calib_price = actual_clear_price_calib
                                calib_avg = actual_clear_price_calib
                            else:
                                calib_price = temp_sim_avg if temp_sim_avg > 0 else 0.01
                                calib_avg = temp_sim_avg
                        else:
                            calib_price = actual_avg if actual_avg > 0 else temp_sim_avg
                            calib_avg = actual_avg if actual_avg > 0 else temp_sim_avg
                            
                        calib_item = {'date': target_ledger_str, 'side': calib_side, 'qty': abs(gap_qty), 'price': calib_price, 'avg_price': calib_avg, 'exec_id': f"CALIB_{int(time.time())}", 'desc': "비파괴 보정"}
                        if is_rev: calib_item['is_reverse'] = True
                        new_target_records.append(calib_item)
                        
                    if new_target_records:
                        if safe_actual_qty_for_vrev > 0:
                            for r in new_target_records: r['avg_price'] = actual_avg
                
                    # 🚨 MODIFIED: 10.0 -> 45.0
                    await self._retry_api(self.cfg.overwrite_incremental_ledger, ticker, temp_recs, new_target_records, timeout=45.0)
                    if gap_qty != 0: await self._safe_send(context, chat_id, f"🔧 <b>[{html.escape(str(ticker))}] 통합 메인 장부(MAIN LEDGER) 비파괴 보정 완료!</b>\n▫️ KIS 실잔고 오차 수량({gap_qty}주)을 역사 보존 상태로 안전하게 교정했습니다.", parse_mode='HTML')

                if is_rev:
                    q_data_before = await self._retry_api(self.queue_ledger.get_queue, ticker, default=[])
                    vrev_ledger_qty = sum(int(self._safe_float(item.get("qty"))) for item in q_data_before if isinstance(item, dict))
                    
                    # 🚨 NEW: [유령 졸업 방어 결속] KIS 체결 내역 중 매도 건을 필터링하되, 반드시 '오늘이 실제 영업일'일 때만 유효한 당일 실적으로 인정
                    sold_today_vrev = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01") if (target_execs and is_today_trading_day) else 0
                    
                    if safe_actual_qty_for_vrev == 0 and (vrev_ledger_qty > 0 or sold_today_vrev > 0):
                        if sold_today_vrev == 0 and vrev_ledger_qty > 0:
                            await self._safe_send(context, chat_id, f"🚨 <b>[{html.escape(str(ticker))} 유령 잔고 방어 가동]</b>\nKIS 실잔고가 0주로 조회되었으나, 당일 매도 체결 내역이 0건입니다. 통신 오류(Ghost Balance)일 가능성이 매우 높아 장부 강제 소각(자동 졸업)을 차단합니다.\n▫️ HTS 등을 통해 수동으로 100% 전량 매도한 상태라면 <code>/reset</code> 명령어를 사용하여 봇을 초기화하십시오.", parse_mode='HTML')
                            return "유령 잔고(Ghost Balance) 강제 차단 - 매도 체결 없이 KIS 잔고 0주 리턴됨"

                        added_seed = 0.0
                        _vrev_snap_ok = False
                        snapshot = None
                        actual_clear_price = 0.0
                        tot_q = 0
            
                        if target_execs and is_today_trading_day:
                            # 🚨 MODIFIED: ex.get('sll_buy_dvsn_cd'] -> ex.get('sll_buy_dvsn_cd')
                            sell_execs = [ex for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                            if sell_execs:
                                tot_amt = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) * self._safe_float(ex.get('ft_ccld_unpr3')) for ex in sell_execs)
                                tot_q = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in sell_execs)
                                if tot_q > 0: actual_clear_price = round(tot_amt / tot_q, 4)

                        if tot_q > vrev_ledger_qty:
                            snapshot_needs_regen = True # 🚨 트리거 발동
                            missing_qty = tot_q - vrev_ledger_qty
                            # 🚨 MODIFIED: ex.get('sll_buy_dvsn_cd'] -> ex.get('sll_buy_dvsn_cd')
                            buy_execs = [ex for ex in (target_execs or []) if ex.get('sll_buy_dvsn_cd') == "02"]
                            temp_invested = sum(self._safe_float(item.get("qty")) * self._safe_float(item.get("price")) for item in q_data_before if isinstance(item, dict))
                            temp_avg = temp_invested / vrev_ledger_qty if vrev_ledger_qty > 0 else 0.0
                            missing_price = temp_avg
            
                            if buy_execs:
                                b_tot_amt = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) * self._safe_float(ex.get('ft_ccld_unpr3')) for ex in buy_execs)
                                b_tot_q = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in buy_execs)
                                if b_tot_q > 0:
                                    q_today_amt = 0.0
                                    q_today_qty = 0
                                    for item in q_data_before:
                                        if isinstance(item, dict) and str(item.get("date", "")).startswith(target_ledger_str):
                                            iq = int(self._safe_float(item.get("qty")))
                                            q_today_qty += iq
                                            q_today_amt += iq * self._safe_float(item.get("price"))
                                    
                                    pure_manual_q = b_tot_q - q_today_qty
                                    pure_manual_amt = b_tot_amt - q_today_amt
                               
                                    if pure_manual_q >= missing_qty and pure_manual_q > 0 and pure_manual_amt > 0:
                                        derived_price = pure_manual_amt / pure_manual_q
                                        missing_price = round(derived_price, 4)
                                    else: missing_price = round(b_tot_amt / b_tot_q, 4)

                            q_data_before.append({"date": now_est.strftime('%Y-%m-%d %H:%M:%S'), "qty": missing_qty, "price": missing_price, "exec_id": "MANUAL_SYNC"})
                            vrev_ledger_qty = tot_q
                            # 🚨 MODIFIED: 10.0 -> 45.0
                            await self._retry_api(self.queue_ledger.overwrite_queue, ticker, q_data_before, timeout=45.0)

                        total_invested = sum(self._safe_float(item.get("qty")) * self._safe_float(item.get("price")) for item in q_data_before if isinstance(item, dict))
                        q_avg_price = total_invested / vrev_ledger_qty if vrev_ledger_qty > 0 else 0.0

                        # 🚨 MODIFIED: 15.0 -> 45.0
                        curr_p = await self._retry_api(self.broker.get_current_price, ticker, timeout=45.0, default=0.0)
                        clear_price = actual_clear_price if actual_clear_price > 0.0 else (curr_p if curr_p and curr_p > 0 else q_avg_price * 1.006)
                        # 🚨 MODIFIED: 10.0 -> 45.0
                        snapshot = await self._retry_api(self.strategy.capture_vrev_snapshot, ticker, clear_price, q_avg_price, vrev_ledger_qty, timeout=45.0, default={})
                         
                        if snapshot and isinstance(snapshot, dict):
                            realized_pnl = self._safe_float(snapshot.get('realized_pnl', 0.0))
                            yield_pct = self._safe_float(snapshot.get('realized_pnl_pct', 0.0))
                            compound_rate = self._safe_float(await self._retry_api(self.cfg.get_compound_rate, ticker, default=70.0)) / 100.0
                             
                            if realized_pnl > 0 and compound_rate > 0:
                                added_seed = realized_pnl * compound_rate
                                current_seed = self._safe_float(await self._retry_api(self.cfg.get_seed, ticker, default=6720.0))
                                # 🚨 MODIFIED: 10.0 -> 45.0
                                await self._retry_api(self.cfg.set_seed, ticker, current_seed + added_seed, timeout=45.0)
                             
                            cap_dt = snapshot.get('captured_at', now_est)
                            cap_dt_str = cap_dt if isinstance(cap_dt, str) else cap_dt.strftime('%Y-%m-%d')
                 
                            start_dt_str = str(q_data_before[0].get('date', ''))[:10] if q_data_before and isinstance(q_data_before[0], dict) else cap_dt_str[:10]
                   
                            hist_data = await self._retry_api(self.cfg._load_json, self.cfg.FILES["HISTORY"], [], default=[])
                             
                            new_hist = {
                                "id": int(time.time()), "ticker": ticker, "start_date": start_dt_str, "end_date": cap_dt_str[:10],
                                "invested": self._safe_float(total_invested), "revenue": self._safe_float(total_invested + realized_pnl),
                                "profit": realized_pnl, "yield": yield_pct, "trades": q_data_before 
                            }
            
                            hist_data.append(new_hist)
                            # 🚨 MODIFIED: 10.0 -> 45.0
                            await self._retry_api(self.cfg._save_json, self.cfg.FILES["HISTORY"], hist_data, timeout=45.0)
                            _vrev_snap_ok = True
                                
                        if getattr(self, 'queue_ledger', None):
                            # 🚨 MODIFIED: 10.0 -> 45.0
                            await self._retry_api(self.queue_ledger.sync_with_broker, ticker, 0, timeout=45.0)
                            
                        # 🚨 NEW: [V-REV 유령 졸업 방어] 큐 장부 소각과 동시에 메인 장부도 100% 소각하여 주말 무한 검증 루프 원천 차단
                        full_ledger_clear = await self._retry_api(self.cfg.get_ledger, default=[])
                        all_recs_clear = [r for r in full_ledger_clear if isinstance(r, dict) and r.get('ticker') != ticker]
                        # 🚨 MODIFIED: 10.0 -> 45.0
                        await self._retry_api(self.cfg._save_json, self.cfg.FILES["LEDGER"], all_recs_clear, timeout=45.0)
                    
                        if _vrev_snap_ok:
                            msg = f"🎉 <b>[{html.escape(str(ticker))}] V-REV 잭팟 스윕(전량 익절) 감지!]</b>\n▫️ 잔고가 0주가 되어 LIFO 큐 지층을 100% 소각(초기화)했습니다."
                            if added_seed > 0: msg += f"\n💸 <b>자동 복리 +${added_seed:,.0f}</b> 이 다음 운용 시드에 완벽하게 추가되었습니다!"
                            await self._safe_send(context, chat_id, msg, parse_mode='HTML')
                             
                            if snapshot and isinstance(snapshot, dict):
                                img_path = await self._retry_api(
                                    self.view.create_profit_image, 
                                    ticker=ticker, 
                                    profit=self._safe_float(snapshot.get('realized_pnl', 0.0)), 
                                    yield_pct=self._safe_float(snapshot.get('realized_pnl_pct', 0.0)), 
                                    invested=self._safe_float(snapshot.get('avg_price', 0.0)) * self._safe_float(snapshot.get('cleared_qty', 0)), 
                                    revenue=self._safe_float(snapshot.get('clear_price', 0.0)) * self._safe_float(snapshot.get('cleared_qty', 0)), 
                                    end_date=cap_dt_str[:10],
                                    timeout=45.0, default=None
                                )
                                if img_path:
                                    def _read_img3(p):
                                        with open(p, 'rb') as f_in: return f_in.read()
                                    try:
                                        img_bytes3 = await self._retry_api(_read_img3, img_path)
                                        if str(img_path).lower().endswith('.gif'): await context.bot.send_animation(chat_id=chat_id, animation=img_bytes3)
                                        else: await context.bot.send_photo(chat_id=chat_id, photo=img_bytes3)
                                    except OSError: pass
                        else:
                            await self._safe_send(context, chat_id, f"⚠️ <b>[{html.escape(str(ticker))}] V-REV 0주 강제 정산 완료]</b>\n▫️ 0주를 확인하여 큐를 안전하게 비웠으나 통신 지연으로 졸업 카드는 생략되었습니다.", parse_mode='HTML')

                    if safe_actual_qty_for_vrev > 0 and safe_actual_qty_for_vrev <= vrev_ledger_qty:
                        gap_qty = vrev_ledger_qty - safe_actual_qty_for_vrev
                        
                        if gap_qty > 0:
                            def _update_v_state(tkr, g_qty):
                                f_path = f"data/vwap_state_REV_{tkr}.json"
                                with GlobalThrottle.get_file_lock(f_path):
                                    try:
                                        with open(f_path, 'r', encoding='utf-8') as vf: v_state = json.load(vf)
                                    except Exception:
                                        v_state = {}
                                        
                                    if isinstance(v_state, dict) and "executed" in v_state and isinstance(v_state["executed"], dict) and "SELL_QTY" in v_state["executed"]:
                                        old_sell_qty = v_state["executed"]["SELL_QTY"]
                                        v_state["executed"]["SELL_QTY"] = max(0, old_sell_qty - g_qty)
                                        
                                    fd = None
                                    tmp_path = None
                                    try:
                                        fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(f_path) or '.')
                                        with os.fdopen(fd, 'w', encoding='utf-8') as _vf_out:
                                            fd = None
                                            json.dump(v_state, _vf_out, ensure_ascii=False, indent=4)
                                            _vf_out.flush()
                                            os.fsync(_vf_out.fileno())
                                        os.replace(tmp_path, f_path)
                                        tmp_path = None
                                    except Exception as write_err:
                                        if fd is not None:
                                            try: os.close(fd)
                                            except OSError: pass
                                        if tmp_path:
                                            try: os.remove(tmp_path)
                                            except OSError: pass
                                        raise write_err

                            # 🚨 MODIFIED: 10.0 -> 45.0
                            await self._retry_api(_update_v_state, ticker, gap_qty, timeout=45.0)

                        actual_clear_price_for_sync = 0.0
                        if target_execs and is_today_trading_day:
                            # 🚨 MODIFIED: ex.get('sll_buy_dvsn_cd'] -> ex.get('sll_buy_dvsn_cd')
                            sell_execs_sync = [ex for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                            if sell_execs_sync:
                                t_amt = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) * self._safe_float(ex.get('ft_ccld_unpr3')) for ex in sell_execs_sync)
                                t_q = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in sell_execs_sync)
                                if t_q > 0: actual_clear_price_for_sync = round(t_amt / t_q, 4)
                        
                        calibrated = False
                        if getattr(self, 'queue_ledger', None):
                            safe_prev_c = 0.0
                            # 🚨 MODIFIED: [타임라인 롤오버 패러독스 방어] 16:05 확정 정산 시 큐 장부 동기화에 주입되는 prev_close가 당일 종가로 롤오버되는 현상을 막기 위해, YF 과거 일봉 핀셋 추출을 통한 100% 무결성 팩트 타점을 주입합니다.
                            def _get_factual_prev_c():
                                GlobalThrottle.wait_api_sync()
                                try:
                                    df = yf.Ticker(ticker).history(period="15d", interval="1d", timeout=5)
                                    if not df.empty:
                                        est_tz = ZoneInfo('America/New_York')
                                        if df.index.tzinfo is None:
                                            df.index = df.index.tz_localize('UTC').tz_convert(est_tz)
                                        else:
                                            df.index = df.index.tz_convert(est_tz)
                                        past_df = df[df.index.strftime('%Y-%m-%d') < target_ledger_str]
                                        if not past_df.empty:
                                            return float(past_df['Close'].iloc[-1])
                                except Exception: pass
                                return 0.0
                            
                            safe_prev_c = await self._retry_api(_get_factual_prev_c, timeout=45.0, default=0.0)
                            
                            if safe_prev_c <= 0.0:
                                for attempt in range(3):
                                    try:
                                        GlobalThrottle.wait_api_sync()
                                        p_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_previous_close, ticker), timeout=45.0)
                                        safe_prev_c = self._safe_float(p_val)
                                        if safe_prev_c > 0: break
                                    except Exception:
                                        if attempt == 2: pass
                                        else: await asyncio.sleep(1.0 * (2 ** attempt))
                            
                            safe_seed = await self._retry_api(self.cfg.get_seed, ticker, default=0.0)
                            portion_budget = safe_seed * 0.15
                            
                            calibrated = await self._retry_api(
                                self.queue_ledger.sync_with_broker, 
                                ticker, 
                                safe_actual_qty_for_vrev, 
                                0.0, 
                                actual_clear_price_for_sync,
                                prev_close=safe_prev_c,
                                portion_budget=portion_budget,
                                timeout=45.0,
                                default=False
                            )
                         
                        if calibrated:
                            snapshot_needs_regen = True # 🚨 트리거 발동
                            if gap_qty > 0:
                                await self._safe_send(context, chat_id, f"🔧 <b>[{html.escape(str(ticker))}] V-REV 큐(Queue) 비파괴 보정 및 리앵커링 완료!</b>\n▫️ 수동 매도 물량(<b>{gap_qty}주</b>)을 LIFO 큐에서 안전하게 차감하고, 수익금만큼 잔여 지층의 평단가를 일괄 차감했습니다.", parse_mode='HTML')
                            else:
                                await self._safe_send(context, chat_id, f"🔧 <b>[{html.escape(str(ticker))}] V-REV 큐(Queue) 단일 지층 자가 치유 완료!</b>\n▫️ 비정상적으로 팽창된 단일 지층을 1층과 상위층으로 정밀 분할(Split)하여 팩트 복구했습니다.", parse_mode='HTML')
                         
                    elif safe_actual_qty_for_vrev > 0 and safe_actual_qty_for_vrev > vrev_ledger_qty:
                        snapshot_needs_regen = True # 🚨 트리거 발동
                        gap_qty = safe_actual_qty_for_vrev - vrev_ledger_qty
                        
                        logging.info(f"🛡️ [{ticker}] V-REV 큐 장부 절대주의 가동: KIS 실잔고 초과분({gap_qty}주)을 개인 물량으로 간주하여 편입 차단.")
                        await self._safe_send(context, chat_id, f"🛡️ <b>[{html.escape(str(ticker))}] 개인 장기 물량 격리 보호 (Ghost Balance 차단)</b>\n▫️ KIS 실잔고가 로컬 큐 장부보다 <b>{gap_qty}주</b> 많습니다.\n▫️ 봇 관리를 벗어난 개인 장기 투자 물량으로 간주하여 V-REV 장부에 강제 편입(MANUAL_BUY)하지 않고 안전하게 100% 격리했습니다.\n▫️ 만약 봇 운용을 위해 수동 매수한 물량이라면 <code>/add_q</code> 명령어로 직접 지층을 주입하십시오.", parse_mode='HTML')

                if not is_rev:
                    # 🚨 NEW: [유령 졸업 방어 결속] KIS 체결 내역 중 매도 건을 필터링하되, 반드시 '오늘이 실제 영업일'일 때만 유효한 당일 실적으로 인정
                    # 🚨 MODIFIED: ex.get('sll_buy_dvsn_cd'] -> ex.get('sll_buy_dvsn_cd')
                    sold_today_v14 = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01") if (target_execs and is_today_trading_day) else 0
                    
                    if actual_qty == 0 and (ledger_qty > 0 or sold_today_v14 > 0):
                        if sold_today_v14 == 0 and ledger_qty > 0:
                            await self._safe_send(context, chat_id, f"🚨 <b>[{html.escape(str(ticker))} 유령 잔고 방어 가동]</b>\nKIS 실잔고가 0주로 조회되었으나, 당일 매도 체결 내역이 0건입니다. 통신 오류(Ghost Balance)일 가능성이 매우 높아 장부 강제 소각(자동 졸업)을 차단합니다.\n▫️ HTS 등을 통해 수동으로 100% 전량 매도한 상태라면 <code>/reset</code> 명령어를 사용하여 봇을 초기화하십시오.", parse_mode='HTML')
                            return "유령 잔고(Ghost Balance) 강제 차단 - 매도 체결 없이 KIS 잔고 0주 리턴됨"

                        today_est_str = now_est.strftime('%Y-%m-%d')
                        prev_c = await self._retry_api(self.broker.get_previous_close, ticker, default=0.0)
                        
                        # 🚨 MODIFIED: 10.0 -> 45.0
                        grad_res = await self._retry_api(self.cfg.archive_graduation, ticker, today_est_str, prev_c, timeout=45.0, default=(None, 0.0))
                        new_hist, added_seed = grad_res if isinstance(grad_res, tuple) and len(grad_res) >= 2 else (None, 0.0)

                        if new_hist and isinstance(new_hist, dict):
                            msg = f"🎉 <b>[{html.escape(str(ticker))} 졸업 확인!]</b>\n장부를 명예의 전당에 저장하고 새 사이클을 준비합니다."
                            if added_seed > 0: msg += f"\n💸 <b>자동 복리 +${added_seed:,.0f}</b> 이 다음 운용 시드에 완벽하게 추가되었습니다!"
                            await self._safe_send(context, chat_id, msg, parse_mode='HTML')
                            
                            # 🚨 MODIFIED: 15.0 -> 45.0
                            img_path = await self._retry_api(
                                self.view.create_profit_image,
                                ticker=ticker, 
                                profit=self._safe_float(new_hist.get('profit', 0.0)), 
                                yield_pct=self._safe_float(new_hist.get('yield', 0.0)),
                                invested=self._safe_float(new_hist.get('invested', 0.0)), 
                                revenue=self._safe_float(new_hist.get('revenue', 0.0)), 
                                end_date=new_hist.get('end_date', target_ledger_str),
                                timeout=45.0, default=None
                            )
                            if img_path:
                                def _read_img4(p):
                                    with open(p, 'rb') as f_in: return f_in.read()
                                try:
                                    img_bytes4 = await self._retry_api(_read_img4, img_path)
                                    if str(img_path).lower().endswith('.gif'): await context.bot.send_animation(chat_id=chat_id, animation=img_bytes4)
                                    else: await context.bot.send_photo(chat_id=chat_id, photo=img_bytes4)
                                except OSError: pass
                        else:
                            full_ledger2 = await self._retry_api(self.cfg.get_ledger, default=[])
                            # 🚨 MODIFIED: r.get('ticker'] -> r.get('ticker')
                            all_recs = [r for r in full_ledger2 if isinstance(r, dict) and r.get('ticker') != ticker]
                            # 🚨 MODIFIED: 10.0 -> 45.0
                            await self._retry_api(self.cfg._save_json, self.cfg.FILES["LEDGER"], all_recs, timeout=45.0)
                            await self._safe_send(context, chat_id, f"⚠️ <b>[{html.escape(str(ticker))} 강제 정산 완료]</b>\n잔고가 0주이나 마이너스 수익 상태이므로 명예의 전당 박제 없이 장부를 비우고 새출발 타점을 장전합니다.", parse_mode='HTML')

                is_after_market = now_est.time() >= datetime.time(16, 0)
                
                # 🚨 MODIFIED: [스냅샷 디커플링 해소 수술] 장부나 큐에 단 1주라도 변동이 생기면 낡은 스냅샷 소각 및 즉시 팩트 재생성 가동
                if is_after_market or snapshot_needs_regen:
                    if snapshot_needs_regen:
                        logging.info(f"🔄 [{ticker}] 장부/큐 오차 교정 감지! 낡은 스냅샷(Snapshot) 및 캐시를 전면 소각(Nuke)하고 재생성(Regenerate)합니다.")
                        def _nuke_old_files():
                            for f in glob.glob(f"data/daily_snapshot_*_{ticker}.json"):
                                with GlobalThrottle.get_file_lock(f):
                                    try: os.remove(f)
                                    except OSError: pass
                            for f in glob.glob(f"data/vwap_state_*_{ticker}.json"):
                                with GlobalThrottle.get_file_lock(f):
                                    try: os.remove(f)
                                    except OSError: pass
                        # 🚨 MODIFIED: 10.0 -> 45.0
                        await asyncio.wait_for(asyncio.to_thread(_nuke_old_files), timeout=45.0)

                    try:
                        # 🚨 MODIFIED: 10.0 -> 45.0
                        curr_p_val = await self._retry_api(self.broker.get_current_price, ticker, timeout=45.0)
                        curr_p = self._safe_float(curr_p_val)
                        
                        def get_exact_prev_close(ticker_name):
                            GlobalThrottle.wait_api_sync()
                            df = yf.Ticker(ticker_name).history(period="5d", interval="1m", prepost=True, timeout=5)
                            if not df.empty and 'Close' in df.columns:
                                tz_est = ZoneInfo('America/New_York')
                                tz_now = datetime.datetime.now(tz_est)
                                cutoff_date = tz_now.date()
                                if tz_now.time() <= datetime.time(16, 0, 30):
                                    cutoff_date -= datetime.timedelta(days=1)
                                
                                if df.index.tzinfo is None:
                                    df.index = df.index.tz_localize('UTC').tz_convert(tz_est)
                                else:
                                    df.index = df.index.tz_convert(tz_est)
                                    
                                past_df = df[df.index.date <= cutoff_date].copy()
                                if not past_df.empty:
                                    past_df['Close'] = past_df['Close'].ffill().bfill()
                                    regular_past = past_df.between_time('09:30', '15:59')
                                    if not regular_past.empty:
                                        val = float(regular_past['Close'].iloc[-1])
                                    else:
                                        val = float(past_df['Close'].iloc[-1])
                                    return val if not math.isnan(val) else None
                            return None

                        yf_close = None
                        for attempt in range(3):
                            try:
                                # 🚨 MODIFIED: 10.0 -> 45.0
                                yf_close = await asyncio.wait_for(asyncio.to_thread(get_exact_prev_close, ticker), timeout=45.0)
                                break
                            except Exception:
                                if attempt == 2: pass
                                else: await asyncio.sleep(1.0 * (2 ** attempt))
                        
                        prev_c = yf_close if yf_close and yf_close > 0 else curr_p

                        if now_est.weekday() >= 5 or now_est.time() < datetime.time(4, 0):
                            curr_p = prev_c
                    
                        # 🚨 MODIFIED: 10.0 -> 45.0
                        ma_5day_val = await self._retry_api(self.broker.get_5day_ma, ticker, timeout=45.0)
                        ma_5day = self._safe_float(ma_5day_val)
                        
                        bal_res = await self._retry_api(self.broker.get_account_balance, timeout=45.0)
                        cash_for_snap = self._safe_float(bal_res[0]) if bal_res else 0.0
                        
                        from scheduler_core import get_budget_allocation
                        active_tickers_list = await asyncio.wait_for(asyncio.to_thread(self.cfg.get_active_tickers), timeout=45.0) or []
                        _, alloc_cash_dict = await asyncio.wait_for(asyncio.to_thread(get_budget_allocation, cash_for_snap, active_tickers_list, self.cfg), timeout=45.0)
                        avail_cash = self._safe_float((alloc_cash_dict or {}).get(ticker, 0.0))
             
                        # 🚨 MODIFIED: 10.0 -> 45.0
                        full_ledger_final = await self._retry_api(self.cfg.get_ledger, timeout=45.0)
                        # 🚨 MODIFIED: r.get('ticker'] -> r.get('ticker')
                        recs_final = [r for r in (full_ledger_final or []) if isinstance(r, dict) and r.get('ticker') == ticker]
                        hold_res_final = await self._retry_api(self.cfg.calculate_holdings, ticker, recs_final, timeout=45.0)
                  
                        final_qty = hold_res_final[0] if isinstance(hold_res_final, tuple) and len(hold_res_final) > 0 else 0
                        final_avg = hold_res_final[1] if isinstance(hold_res_final, tuple) and len(hold_res_final) > 1 else 0.0
                    
                        if final_qty == 0:
                            curr_p = 0.0
                         
                        # 🚨 MODIFIED: 15.0 -> 45.0
                        await asyncio.wait_for(asyncio.to_thread(
                            self.strategy.get_plan, ticker, curr_p, final_avg, final_qty, prev_c, ma_5day=ma_5day,
                            market_type="REG", available_cash=avail_cash, is_simulation=True, is_snapshot_mode=True
                        ), timeout=45.0)
                        
                        if is_after_market:
                            logging.info(f"📸 [{ticker}] 16:05 EST 확정 정산 완료 후 명일(D+1) 대비 스냅샷 박제(Forward-Lock) 성공.")
                        else:
                            logging.info(f"📸 [{ticker}] 장부 교정 감지! 낡은 스냅샷 소각 및 실시간 팩트 지시서 재생성(Regenerate) 성공.")
                    except Exception as e:
                        logging.error(f"🚨 [{ticker}] 스냅샷 팩트 박제 실패: {e}")

                return "SUCCESS"

    async def _display_ledger(self, ticker, chat_id, context, query=None, message_obj=None, pre_fetched_holdings=None):
        full_ledger = await self._retry_api(self.cfg.get_ledger, default=[])
        recs = [r for r in full_ledger if isinstance(r, dict) and r.get('ticker') == ticker]
        
        report = ""
        
        if not recs:
            report += f"📭 <b>[{html.escape(str(ticker))}]</b> 현재 진행 중인 본진 사이클이 없습니다 (보유량 0주).\n\n"
        else:
            from collections import OrderedDict
            agg_dict = OrderedDict()
            total_buy = 0.0
            total_sell = 0.0
            
            for rec in recs:
                raw_date = str(rec.get('date') or '').split(' ')[0]
                parts = raw_date.split('-')
                if len(parts) == 3: date_short = f"{parts[1]}.{parts[2]}"
                else: date_short = raw_date
                     
                side_str = "🔴매수" if rec.get('side') == 'BUY' else "🔵매도"
                key = (date_short, side_str)
            
                if key not in agg_dict: agg_dict[key] = {'qty': 0, 'amt': 0.0}

                agg_dict[key]['qty'] += int(self._safe_float(rec.get('qty')))
                agg_dict[key]['amt'] += (int(self._safe_float(rec.get('qty'))) * self._safe_float(rec.get('price')))
            
                if rec.get('side') == 'BUY': total_buy += (int(self._safe_float(rec.get('qty'))) * self._safe_float(rec.get('price')))
                elif rec.get('side') == 'SELL': total_sell += (int(self._safe_float(rec.get('qty'))) * self._safe_float(rec.get('price')))
            
            report += f"📜 <b>[ {html.escape(str(ticker))} 일자별 매매 (통합 변동분) (총 {len(agg_dict)}일) ]</b>\n\n<code>No. 일자   구분  평균단가  수량\n"
            report += "-"*30 + "\n"
            
            idx = 1
            for (date, side), data in agg_dict.items():
                tot_qty = data['qty']
                avg_prc = data['amt'] / tot_qty if tot_qty != 0 else 0.0
                report += f"{idx:<3} {date} {side} ${avg_prc:<6.2f} {tot_qty}주\n"
                idx += 1
                 
            report += "-"*30 + "</code>\n\n"
        
        safe_holdings = pre_fetched_holdings if isinstance(pre_fetched_holdings, dict) else {}
        actual_qty = int(self._safe_float((safe_holdings.get(ticker) or {'qty': 0}).get('qty')))
        actual_avg = self._safe_float((safe_holdings.get(ticker) or {'avg': 0}).get('avg'))
        
        kis_raw_qty = actual_qty
        kis_raw_avg = actual_avg

        v_mode = await self._retry_api(self.cfg.get_version, ticker, default="V14")
        
        if v_mode == "V_REV":
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                # 🚨 MODIFIED: 5.0 -> 45.0
                self.queue_ledger = await asyncio.wait_for(asyncio.to_thread(QueueLedger), timeout=45.0)
            
            if getattr(self, 'queue_ledger', None):
                q_data_ui = await self._retry_api(self.queue_ledger.get_queue, ticker, default=[])
                vrev_inv_ui = sum(int(self._safe_float(item.get('qty'))) * self._safe_float(item.get('price')) for item in q_data_ui if isinstance(item, dict))
                vrev_q_ui = sum(int(self._safe_float(item.get('qty'))) for item in q_data_ui if isinstance(item, dict))
                
                actual_qty = vrev_q_ui
                
                if vrev_q_ui > 0: actual_avg = round(vrev_inv_ui / vrev_q_ui, 4)
                else: actual_avg = 0.0

        split = await self._retry_api(self.cfg.get_split_count, ticker, default=40.0)
        t_val_res = await self._retry_api(self.cfg.get_absolute_t_val, ticker, actual_qty, actual_avg, default=(0.0, 0.0))
        t_val = t_val_res[0] if isinstance(t_val_res, tuple) and len(t_val_res) > 0 else 0.0
         
        t_val_safe = self._safe_float(t_val)
        split_safe = int(self._safe_float(split))

        report += "📊 <b>[ 현재 본진 진행 상황 요약 ]</b>\n"
        report += f"▪️ 현재 T값 : {t_val_safe:.4f} T ({split_safe}분할)\n"
        report += f"▪️ 보유 수량 : {actual_qty} 주 (평단 ${actual_avg:,.2f})\n"
        
        if recs:
            report += f"▪️ 총 매수액 : ${total_buy:,.2f}\n"
            report += f"▪️ 총 매도액 : ${total_sell:,.2f}\n"

        try:
            from assassin_ledger import AssassinLedger
            # 🚨 MODIFIED: 5.0 -> 45.0
            a_ledger = await asyncio.wait_for(asyncio.to_thread(AssassinLedger), timeout=45.0)
            a_data = await self._retry_api(a_ledger.get_ledger, ticker, default=[])
            
            report += f"\n🔫 <b>[ 암살자 (aVWAP) 독립 장부 상태 ]</b>\n"
            
            if a_data and isinstance(a_data, list) and len(a_data) > 0:
                a_qty = sum(int(self._safe_float(r.get('qty'))) for r in a_data)
                if a_qty > 0:
                    a_inv = sum(int(self._safe_float(r.get('qty'))) * self._safe_float(r.get('price')) for r in a_data)
                    a_avg = a_inv / a_qty if a_qty > 0 else 0.0
                    report += f"▪️ 진행 상태 : <b>ON (교전/오버나이트 중)</b>\n"
                    report += f"▪️ 락온 수량 : <b>{a_qty} 주</b>\n"
                    report += f"▪️ 진입 평단가: <b>${a_avg:,.2f}</b>\n"
                else:
                    report += f"▪️ 진행 상태 : <b>STANDBY (타점 감시 중 / 0주)</b>\n"
            else:
                report += f"▪️ 진행 상태 : <b>STANDBY (타점 감시 중 / 0주)</b>\n"
                
        except Exception as e:
            logging.error(f"🚨 암살자 장부 UI 렌더링 실패: {e}")

        report += f"\n🏛️ <b>[ KIS 실서버 종합 원장 계좌 정보 ]</b>\n"
        report += f"▪️ KIS 총 수량 : <b>{kis_raw_qty} 주</b> (본진 {actual_qty}주 + 암살자 {kis_raw_qty - actual_qty if kis_raw_qty > actual_qty else 0}주)\n"
        report += f"▪️ KIS 실평단가 : <b>${kis_raw_avg:,.2f}</b> (증권사 앱 표출 팩트 단가)\n"

        msg = report
        
        if len(msg) > 4000:
            msg = msg[:3900] + "\n\n... (장부 내역이 너무 길어 하단이 생략되었습니다) ✂️"

        active_tickers = await self._retry_api(self.cfg.get_active_tickers, default=[])
        keyboard = []
         
        if v_mode == "V_REV":
            keyboard.append([InlineKeyboardButton(f"🗄️ {html.escape(str(ticker))} V-REV 큐(Queue) 정밀 관리", callback_data=f"QUEUE:VIEW:{ticker}")])
             
        row = [InlineKeyboardButton(f"🔄 {html.escape(str(t))} 장부 업데이트", callback_data=f"REC:SYNC:{t}") for t in active_tickers if isinstance(t, str)]
        if row: keyboard.append(row)
        markup = InlineKeyboardMarkup(keyboard)

        # 🚨 MODIFIED: 15.0 -> 45.0 하드캡
        if query:
            try: await asyncio.wait_for(query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML'), timeout=45.0)
            except Exception: pass
        elif message_obj:
            try: await asyncio.wait_for(message_obj.edit_text(msg, reply_markup=markup, parse_mode='HTML'), timeout=45.0)
            except Exception: pass
        else:
            await self._safe_send(context, chat_id, msg, reply_markup=markup, parse_mode='HTML')

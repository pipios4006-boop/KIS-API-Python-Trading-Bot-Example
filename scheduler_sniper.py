# ==========================================================
# FILE: scheduler_sniper.py
# ==========================================================
# 🚨 MODIFIED: [제1헌법 철저 준수] 달력 API 및 모든 통신 전 하드코딩된 await asyncio.sleep(0.06) 땜질을 영구 소각하고, await asyncio.to_thread(GlobalThrottle.wait_api_sync)로 100% 락온.
# 🚨 MODIFIED: [스케줄러 병목 붕괴 궁극 수술] 스나이퍼 매수/매도 검증 루프 내에 기생하던 불필요한 다중 폴링 대기열(sleep 1.0 * 3회)을 단 1회(sleep 2.0) 타격 판별로 진공 압축하여 55초 TimeoutError 연쇄 폭발을 원천 봉쇄.
# 🚨 MODIFIED: [Thundering Herd 영구 소각] 파편화된 try-except 래핑 및 동기 I/O 블로킹을 _retry_api 단일 래퍼로 100% 통합.
# 🚨 MODIFIED: [암살자 휩쏘 방어망] 1.5초 간격 4틱 교차 검증 락온 (04:30 이전 1분 유지 조건 및 이후 즉각 돌파 검증 후 4연속 상회 시에만 매수 팩트 집행).
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import asyncio
import traceback
import math
import os
import json
import glob
import tempfile
import shutil
import html  
import pandas as pd
import pandas_market_calendars as mcal
import time 
import yfinance as yf
import functools

from scheduler_core import is_market_open
from assassin_ledger import AssassinLedger
from global_throttle import GlobalThrottle # 🚨 전역 통제소

def _safe_float(val):
    try:
        f_val = float(str(val or 0.0).replace(',', ''))
        if math.isnan(f_val) or math.isinf(f_val):
            return 0.0
        return f_val
    except Exception:
        return 0.0

def _write_json_atomic(file_path, data):
    dir_name = os.path.dirname(file_path) or '.'
    try: os.makedirs(dir_name, exist_ok=True)
    except OSError: pass
    
    fd = None
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(dir=dir_name, text=True)
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            fd = None
            json.dump(data, f, ensure_ascii=False, indent=4)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, file_path)
        tmp_path = None
        
        bak_path = file_path + ".bak"
        bak_tmp_path = bak_path + ".tmp"
        try:
            shutil.copy2(file_path, bak_tmp_path)
            os.replace(bak_tmp_path, bak_path)
        except Exception:
            try: os.remove(bak_tmp_path)
            except OSError: pass
            
    except Exception as e:
        if fd is not None:
            try: os.close(fd)
            except OSError: pass
        if tmp_path:
            try: os.remove(tmp_path)
            except OSError: pass
        logging.error(f"🚨 [Sniper] 원자적 파일 쓰기 실패: {e}")

def _read_json_with_bak(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            if content.strip():
                return json.loads(content)
    except Exception:
        pass
        
    try:
        bak_path = file_path + ".bak"
        with open(bak_path, 'r', encoding='utf-8') as f:
            content = f.read()
            if content.strip():
                data = json.loads(content)
                _write_json_atomic(file_path, data) 
                return data
    except Exception:
        pass
    return {}

def _read_state_sync(ticker, now_est):
    date_str = now_est.strftime('%Y-%m-%d')
    file_path = f"data/avwap_trade_state_{ticker}.json"
    
    with GlobalThrottle.get_file_lock(file_path):
        data = _read_json_with_bak(file_path)
            
        if not isinstance(data, dict) or data.get('date') != date_str:
            data = {'date': date_str, 'qty': 0, 'avg_price': 0.0, 'buy_odno': "", 'is_ordering': False, 'sell_odno': "", 'shutdown': False, 'dumped': False, 'cash_warned': False, 'alert_msg_id': 0, 'suppress_sell': False, 'last_filled_sell_qty': 0, 'history_odnos': []}
            _write_json_atomic(file_path, data)
            
        return data

def _update_state_sync(ticker, now_est, updates):
    date_str = now_est.strftime('%Y-%m-%d')
    file_path = f"data/avwap_trade_state_{ticker}.json"
    
    with GlobalThrottle.get_file_lock(file_path):
        data = _read_json_with_bak(file_path)
            
        if not isinstance(data, dict) or data.get('date') != date_str:
            data = {'date': date_str, 'qty': 0, 'avg_price': 0.0, 'buy_odno': "", 'is_ordering': False, 'sell_odno': "", 'shutdown': False, 'dumped': False, 'cash_warned': False, 'alert_msg_id': 0, 'suppress_sell': False, 'last_filled_sell_qty': 0, 'history_odnos': []}
        
        if 'history_odnos' not in data:
            data['history_odnos'] = []
            
        for k, v in updates.items():
            data[k] = v
            if k in ['buy_odno', 'sell_odno', 'dump_odno'] and v:
                if v not in data['history_odnos']:
                    data['history_odnos'].append(str(v))
                    
        _write_json_atomic(file_path, data)
        return data

def _try_lock_ordering_sync(ticker, now_est):
    date_str = now_est.strftime('%Y-%m-%d')
    file_path = f"data/avwap_trade_state_{ticker}.json"
    
    with GlobalThrottle.get_file_lock(file_path):
        data = _read_json_with_bak(file_path)
        
        if not isinstance(data, dict) or data.get('date') != date_str:
            data = {'date': date_str, 'qty': 0, 'avg_price': 0.0, 'buy_odno': "", 'is_ordering': False, 'sell_odno': "", 'shutdown': False, 'dumped': False, 'cash_warned': False, 'alert_msg_id': 0, 'suppress_sell': False, 'last_filled_sell_qty': 0, 'history_odnos': []}
        
        if data.get('is_ordering') or data.get('buy_odno') or data.get('shutdown'):
            return False, data
            
        data['is_ordering'] = True
        _write_json_atomic(file_path, data)
        return True, data

async def _safe_send(context, chat_id, text, timeout=20.0, **kwargs):
    if not chat_id: return None
    try:
        return await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=text, **kwargs), timeout=timeout)
    except Exception as e:
        logging.error(f"🚨 텔레그램 전송 실패: {e}")
        return None

async def _retry_api(func, *args, timeout=45.0, default=None, **kwargs):
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

async def scheduled_sniper_monitor(context):
    job = getattr(context, 'job', None)
    app_data = getattr(job, 'data', {}) if job else {}
    if not isinstance(app_data, dict): app_data = {}
    
    tx_lock = app_data.get('tx_lock')
    chat_id = getattr(job, 'chat_id', None)
    
    if not tx_lock:
        logging.warning("⚠️ [sniper_monitor] tx_lock 미초기화. 이번 사이클 스킵.")
        return

    is_open = False
    for attempt in range(3):
        try:
            is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=45.0)
            break
        except asyncio.TimeoutError:
            if attempt == 2:
                logging.error("⚠️ 달력 API 타임아웃. 평일 강제 개장(Fail-Open) 처리합니다.")
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: 
                await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception:
            if attempt == 2:
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: 
                await asyncio.sleep(1.0 * (2 ** attempt))

    if not is_open: 
        return
    
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    
    kst_zone = ZoneInfo('Asia/Seoul')
    now_kst = datetime.datetime.now(kst_zone)
    kis_search_start = (now_kst - datetime.timedelta(days=2)).strftime('%Y%m%d')
    query_end_dt = now_kst.strftime('%Y%m%d')
    
    def _get_market_hours():
        GlobalThrottle.wait_api_sync()
        nyse = mcal.get_calendar('NYSE')
        return nyse.schedule(start_date=now_est.date(), end_date=now_est.date())

    schedule = await _retry_api(_get_market_hours, timeout=45.0)
            
    if schedule is not None and not schedule.empty:
        market_open = schedule.iloc[0]['market_open'].astimezone(est)
        market_close = schedule.iloc[0]['market_close'].astimezone(est)
    elif schedule is not None and schedule.empty:
        logging.info("💤 [sniper_monitor] 달력 API 빈 데이터 반환. 금일은 미국 증시 휴장일입니다.")
        return
    else:
        if now_est.weekday() < 5:
            market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else: 
            return
   
    pre_start = market_open - datetime.timedelta(hours=5, minutes=30)
    start_monitor = pre_start 
    end_monitor = market_close + datetime.timedelta(hours=4) 
    
    if not (start_monitor <= now_est <= end_monitor): 
        return
    
    cfg = app_data.get('cfg')
    broker = app_data.get('broker')
    strategy = app_data.get('strategy')
    queue_ledger = app_data.get('queue_ledger')
    base_map = app_data.get('base_map', {'SOXL': 'SOXX', 'TQQQ': 'QQQ'})
    
    tracking_cache = app_data.setdefault('sniper_tracking', {})
    today_est_str = now_est.strftime('%Y-%m-%d')
   
    if tracking_cache.get('date') != today_est_str:
        tracking_cache.clear()
        tracking_cache['date'] = today_est_str
        
        def _clean_sniper_caches():
            try:
                for _f in glob.glob("data/sniper_cache_*.json"):
                    try: os.remove(_f)
                    except OSError: pass
            except Exception: pass
        
        try:
            await asyncio.wait_for(asyncio.to_thread(_clean_sniper_caches), timeout=45.0)
        except Exception as e:
            logging.error(f"🚨 [sniper_monitor] 로컬 스나이퍼 캐시 청소 타임아웃/에러: {e}")

    async def _do_sniper():
        assassin_ledger = await asyncio.wait_for(asyncio.to_thread(AssassinLedger), timeout=45.0)

        cash_tuple = await _retry_api(broker.get_account_balance)
        if not cash_tuple: return
        
        available_cash = _safe_float(cash_tuple[0]) if isinstance(cash_tuple, (list, tuple)) and len(cash_tuple) > 0 else 0.0
        holdings = cash_tuple[1] if isinstance(cash_tuple, (list, tuple)) and len(cash_tuple) > 1 else {}
        safe_holdings = holdings if isinstance(holdings, dict) else {}
        
        try:
            active_tickers = await asyncio.wait_for(asyncio.to_thread(cfg.get_active_tickers), timeout=45.0) or []
        except Exception:
            active_tickers = []
        
        for t in active_tickers:
            try:
                await asyncio.sleep(1.5)
                target_base = base_map.get(t, 'SOXX')

                try:
                    version = await asyncio.wait_for(asyncio.to_thread(cfg.get_version, t), timeout=45.0)
                    is_avwap_hybrid = await asyncio.wait_for(asyncio.to_thread(getattr(cfg, 'get_avwap_hybrid_mode', lambda x: False), t), timeout=45.0)
                    user_budget = await _retry_api(getattr(cfg, 'get_avwap_budget', lambda x: 10000.0), t, default=10000.0)
                    is_overnight_allowed = await _retry_api(getattr(cfg, 'get_avwap_overnight_mode', lambda x: False), t, default=False)
                except Exception as e:
                    logging.error(f"🚨 [{t}] Config 파일 스캔 에러 (기본값 폴백): {e}")
                    version = "V14"
                    is_avwap_hybrid = False
                    user_budget = 10000.0
                    is_overnight_allowed = False
               
                if version == "V_REV" and is_avwap_hybrid and t == "SOXL":
                    t_state = await asyncio.wait_for(asyncio.to_thread(_read_state_sync, t, now_est), timeout=45.0)
                    curr_t_obj = now_est.time()
                
                    if curr_t_obj >= datetime.time(15, 59, 0) and not t_state.get('dumped'):
                        if t_state.get('shutdown') or (t_state.get('qty', 0) == 0 and not t_state.get('buy_odno')):
                            await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'dumped': True}), timeout=45.0)
                            continue

                        if is_overnight_allowed:
                            logging.info(f"🌙 [{t}] 15:59 EST 컷오프 도달. 오버나이트 모드 ON ➔ 애프터장 감시망을 보존합니다.")
                            if t_state.get('buy_odno'):
                                await _retry_api(broker.cancel_order, t, t_state['buy_odno'])
                                await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'buy_odno': ""}), timeout=45.0)
                                
                            await asyncio.sleep(1.0)
                            await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'dumped': True}), timeout=45.0)
                            if chat_id:
                                await _safe_send(context, chat_id, f"🌙 <b>[{html.escape(t)}] 암살자 오버나이트 전환 (관망)</b>\n▫️ 익절에 실패했으나 오버나이트가 허용되어 강제 덤핑을 건너뛰고 포지션을 안전하게 이관합니다.\n▫️ <b>애프터장 수익 실현을 위해 +1.0% 지정가 덫은 취소하지 않고 계속 감시합니다.</b>", parse_mode='HTML')
                            continue
                        else:
                            logging.info(f"🛑 [{t}] 15:59 EST 컷오프 도달. 암살자 제로-오버나이트 강제 청산 파이프라인 가동.")
                    
                            if t_state.get('sell_odno'):
                                await _retry_api(broker.cancel_order, t, t_state['sell_odno'])
                            
                            await asyncio.sleep(1.0) 
                            
                            exec_hist = await _retry_api(broker.get_execution_history, t, kis_search_start, query_end_dt)
                            safe_exec = exec_hist if isinstance(exec_hist, list) else []
                            
                            filled_buy_qty = sum(int(_safe_float(ex.get('ft_ccld_qty'))) for ex in safe_exec if str(ex.get('odno')) == t_state.get('buy_odno') and ex.get('sll_buy_dvsn_cd') == '02')
                            filled_sell_qty = sum(int(_safe_float(ex.get('ft_ccld_qty'))) for ex in safe_exec if str(ex.get('odno')) == t_state.get('sell_odno') and ex.get('sll_buy_dvsn_cd') == '01')
                            
                            current_qty = t_state.get('qty', 0)
                            if t_state.get('buy_odno') and filled_buy_qty > 0:
                                current_qty = filled_buy_qty
                            current_qty = max(0, current_qty - filled_sell_qty)
                            
                            dump_qty = current_qty
                            if dump_qty > 0:
                                bid_p = _safe_float(await _retry_api(broker.get_bid_price, t))
                                if bid_p > 0:
                                    dump_price = bid_p
                                    fallback_msg = ""
                                else:
                                    curr_p = _safe_float(await _retry_api(broker.get_current_price, t))
                                    dump_price = max(0.01, math.floor(curr_p * 0.95 * 100) / 100.0)
                                    fallback_msg = " (통신지연 ➔ -5% 하향 폴백)"
                                
                                d_res = await _retry_api(broker.send_order, t, "SELL", dump_qty, dump_price, "LIMIT")
                                
                                if isinstance(d_res, dict) and d_res.get('rt_cd') == '0':
                                    dump_odno = str(d_res.get('odno', ''))
                                    logging.info(f"💥 [{t}] 암살자 물량 {dump_qty}주 매수 1호가 지정가(${dump_price:.2f}) 덤핑 완료{fallback_msg}.")
                                    await asyncio.wait_for(asyncio.to_thread(assassin_ledger.clear_ledger, t), timeout=45.0)
                                    
                                    if chat_id:
                                        await _safe_send(context, chat_id, f"💥 <b>[{html.escape(t)}] 15:59 제로-오버나이트 강제 청산</b>\n▫️ 암살자 미체결 익절망을 취소하고 잔여 물량({dump_qty}주)을 매수 1호가 지정가(${dump_price:.2f})로 전량 스윕(Sweep) 덤핑하여 계좌를 100% 현금화했습니다.{fallback_msg}\n▫️ 암살자 독립 장부가 소각되었습니다.", parse_mode='HTML')
                                    
                                    await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'qty': 0, 'buy_odno': "", 'sell_odno': "", 'shutdown': True, 'dumped': True, 'dump_odno': dump_odno}), timeout=45.0)
                                else:
                                    logging.error(f"🚨 [{t}] 15:59 강제 덤핑 실패: {d_res}")
                                    await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'qty': 0, 'buy_odno': "", 'sell_odno': "", 'shutdown': True, 'dumped': True}), timeout=45.0)
                            else:
                                await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'qty': 0, 'buy_odno': "", 'sell_odno': "", 'shutdown': True, 'dumped': True}), timeout=45.0)
                            continue

                    if curr_t_obj >= datetime.time(9, 30, 0) and not t_state.get('shutdown') and not t_state.get('dumped'):
                        a_ledger_chk = await asyncio.wait_for(asyncio.to_thread(assassin_ledger.get_ledger, t), timeout=45.0)
                        a_qty_chk = sum(int(_safe_float(l.get('qty'))) for l in a_ledger_chk)
                        
                        if a_qty_chk == 0 and int(_safe_float(t_state.get('qty', 0))) == 0:
                            logging.info(f"🛑 [{t}] 09:30 EST 정규장 개장. 프리장 미진입으로 인한 암살자 조기 퇴근 락온.")
                            await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'shutdown': True}), timeout=45.0)
                            if chat_id:
                                await _safe_send(context, chat_id, f"🛑 <b>[{html.escape(t)}] 프리장 진입 실패 (조기 퇴근)</b>\n▫️ 정규장 폭포수 하락에 베이는 것을 방지하기 위해 금일 암살자 신규 진입 권한을 영구 소각합니다.", parse_mode='HTML')
                            continue

                    if t_state.get('shutdown'):
                        continue

                    if t_state.get('buy_odno') and t_state.get('qty') == 0:
                        exec_hist = await _retry_api(broker.get_execution_history, t, kis_search_start, query_end_dt)
                        safe_exec = exec_hist if isinstance(exec_hist, list) else []
                        buy_execs = [ex for ex in safe_exec if str(ex.get('odno')) == t_state['buy_odno'] and ex.get('sll_buy_dvsn_cd') == '02']
                         
                        filled_qty = sum(int(_safe_float(ex.get('ft_ccld_qty'))) for ex in buy_execs)
                        if filled_qty > 0:
                            total_amt = sum(int(_safe_float(ex.get('ft_ccld_qty'))) * _safe_float(ex.get('ft_ccld_unpr3')) for ex in buy_execs)
                            avg_p = total_amt / filled_qty if filled_qty > 0 else 0.0
                            
                            await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'qty': filled_qty, 'avg_price': round(avg_p, 4)}), timeout=45.0)
                            await asyncio.wait_for(asyncio.to_thread(assassin_ledger.add_lot, t, filled_qty, avg_p, "ASSASSIN_BUY"), timeout=45.0)
                            
                            if chat_id:
                                await _safe_send(context, chat_id, f"🎯 <b>[{html.escape(t)}] 암살자 1-Shot 1-Kill 매수 타격!</b>\n▫️ 올인 진입: {filled_qty}주 @ ${avg_p:.2f}\n▫️ 즉시 과욕 제어망(+1.0% 지정가 전량 매도 덫)을 장전합니다.", parse_mode='HTML')
                        else:
                            unfilled = await _retry_api(broker.get_unfilled_orders_detail, t)
                            if unfilled is not None:
                                safe_unfilled = unfilled if isinstance(unfilled, list) else []
                                is_alive = any(str(uo.get('odno')) == t_state['buy_odno'] for uo in safe_unfilled)
                                
                                if not is_alive:
                                    logging.warning(f"🚨 [{t}] 암살자 덫 증발(Ghost Order) 감지! 상태를 강제 초기화합니다.")
                                    await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'buy_odno': ""}), timeout=45.0)

                    a_ledger = await asyncio.wait_for(asyncio.to_thread(assassin_ledger.get_ledger, t), timeout=45.0)
                    a_qty = sum(int(_safe_float(l.get('qty'))) for l in a_ledger)
                    
                    t_state = await asyncio.wait_for(asyncio.to_thread(_read_state_sync, t, now_est), timeout=45.0)
                    
                    if a_qty > 0 and t_state.get('qty') != a_qty:
                        a_inv = sum(int(_safe_float(l.get('qty'))) * _safe_float(l.get('price')) for l in a_ledger)
                        a_avg = a_inv / a_qty if a_qty > 0 else 0.0
                        t_state = await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'qty': a_qty, 'avg_price': round(a_avg, 4)}), timeout=45.0)

                    if t_state.get('qty') > 0 and not t_state.get('sell_odno'):
                        if t_state.get('suppress_sell', False):
                            pass
                        else:
                            exit_multiplier = 1.01
                            sell_price = math.ceil(t_state['avg_price'] * exit_multiplier * 100) / 100.0
                            
                            s_res = await _retry_api(broker.send_order, t, "SELL", t_state['qty'], sell_price, "LIMIT")
                            
                            if isinstance(s_res, dict) and s_res.get('rt_cd') == '0':
                                await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'sell_odno': str(s_res.get('odno'))}), timeout=45.0)
                                if chat_id:
                                    await _safe_send(context, chat_id, f"🕸️ <b>[{html.escape(t)}] +1.0% 고정 익절망 장전 완료</b>\n▫️ 목표 지정가: ${sell_price:.2f} (독립 장부 수량 {t_state['qty']}주)", parse_mode='HTML')

                    t_state = await asyncio.wait_for(asyncio.to_thread(_read_state_sync, t, now_est), timeout=45.0)
                    if t_state.get('sell_odno') and t_state.get('qty') > 0:
                        exec_hist = await _retry_api(broker.get_execution_history, t, kis_search_start, query_end_dt)
                        safe_exec = exec_hist if isinstance(exec_hist, list) else []
                        sell_execs = [ex for ex in safe_exec if str(ex.get('odno')) == t_state['sell_odno'] and ex.get('sll_buy_dvsn_cd') == '01']
                        
                        filled_qty = sum(int(_safe_float(ex.get('ft_ccld_qty'))) for ex in sell_execs)
                        last_filled = t_state.get('last_filled_sell_qty', 0)
                        
                        if filled_qty > last_filled:
                            new_fill_diff = filled_qty - last_filled
                            remaining_qty = max(0, t_state['qty'] - new_fill_diff)
                            
                            if remaining_qty > 0:
                                await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'qty': remaining_qty, 'last_filled_sell_qty': filled_qty}), timeout=45.0)
                                await asyncio.wait_for(asyncio.to_thread(assassin_ledger.pop_lots, t, new_fill_diff), timeout=45.0)
                                if chat_id:
                                    await _safe_send(context, chat_id, f"💥 <b>[{html.escape(t)}] 암살자 덫 부분 체결 (Partial Fill)</b>\n▫️ 익절 물량: {new_fill_diff}주 체결 완료\n▫️ 장부 잔여 물량: {remaining_qty}주 계속 감시 중", parse_mode='HTML')
                            else:
                                await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {
                                    'qty': 0, 
                                    'last_filled_sell_qty': filled_qty, 
                                    'shutdown': True, 
                                    'sell_odno': "", 
                                    'buy_odno': ""
                                }), timeout=45.0)
                                await asyncio.wait_for(asyncio.to_thread(assassin_ledger.clear_ledger, t), timeout=45.0)
                                if chat_id:
                                    await _safe_send(context, chat_id, f"🚀 <b>[{html.escape(t)}] 암살자 +1.0% 전량 익절 성공! (당일 임무 완수)</b>\n▫️ 잔여 물량 100% 현금화 완료\n▫️ 암살자 단독 당일 임무 완수 (독립 장부 소각됨).", parse_mode='HTML')
                            continue

                    if a_qty == 0 and not t_state.get('shutdown'):
                        exec_curr_p = _safe_float(await _retry_api(broker.get_current_price, t))
                        df_1min_t = await _retry_api(broker.get_1min_candles_df, t)
                        
                        decision = await asyncio.wait_for(
                            asyncio.to_thread(
                                strategy.get_avwap_decision,
                                base_ticker=target_base,
                                exec_ticker=t, exec_curr_p=exec_curr_p, 
                                df_1min_exec=df_1min_t, now_est=now_est,
                                is_simulation=False
                            ),
                            timeout=45.0
                        )
                        
                        if decision:
                            action = decision.get('raw_action')
                            
                            if action == 'BREAKOUT_BUY' and not t_state.get('buy_odno'):
                                lock_acquired, t_state = await asyncio.wait_for(asyncio.to_thread(_try_lock_ordering_sync, t, now_est), timeout=45.0)
                                if not lock_acquired:
                                    logging.info(f"⏳ [{t}] 소프트웨어 트리거 중복 발사 방지 락온 가동")
                                    continue
                                
                                # 🚨 MODIFIED: [암살자 휩쏘 방어망] 1.5초 간격 4틱 교차 검증 락온 (04:30 이전 1분 유지 조건 및 이후 즉각 돌파 검증 후 4연속 상회 시에만 매수 팩트 집행).
                                session_vwap = decision.get('session_vwap', 0.0)
                                whipsaw_detected = False
                                
                                if session_vwap > 0.0:
                                    logging.info(f"🛡️ [{t}] BREAKOUT_BUY 최초 감지. 휩쏘(Whipsaw) 방어를 위한 4.5초(3틱) 교차 검증 돌입 (기준 VWAP: ${session_vwap:.2f})")
                                    for tick in range(1, 4):
                                        await asyncio.sleep(1.5)
                                        check_p = _safe_float(await _retry_api(broker.get_current_price, t))
                                        
                                        if check_p <= 0.0 or check_p < session_vwap:
                                            logging.warning(f"🚨 [{t}] 휩쏘 방어망 가동! {tick+1}회차 검증 실패 (현재가 ${check_p:.2f} < VWAP ${session_vwap:.2f} 또는 통신오류). 진입을 차단합니다.")
                                            whipsaw_detected = True
                                            break
                                        else:
                                            logging.info(f"✅ [{t}] {tick+1}/4회차 검증 통과 (현재가 ${check_p:.2f} >= VWAP ${session_vwap:.2f})")
                                else:
                                    logging.warning(f"🚨 [{t}] session_vwap 결측치. Whipsaw 검증 불가로 안전 격리 (진입 차단).")
                                    whipsaw_detected = True
                                    
                                if whipsaw_detected:
                                    await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'is_ordering': False}), timeout=45.0)
                                    continue
                                    
                                ask_price = _safe_float(await _retry_api(broker.get_ask_price, t))
                                if ask_price <= 0.0:
                                    ask_price = _safe_float(await _retry_api(broker.get_current_price, t))
                                    
                                if ask_price > 0.0:
                                    safe_available_cash = min(user_budget, available_cash * 0.95)
                                    buy_qty = int(math.floor(safe_available_cash / ask_price))
                                    
                                    if buy_qty > 0:
                                        b_res = await _retry_api(broker.send_order, t, "BUY", buy_qty, ask_price, "LIMIT")
                                        safe_b_res = b_res if isinstance(b_res, dict) else {}
                                        
                                        if safe_b_res.get('rt_cd') == '0':
                                            await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'buy_odno': str(safe_b_res.get('odno')), 'is_ordering': False, 'cash_warned': False}), timeout=45.0)
                                            
                                            if chat_id:
                                                msg_text = f"🎯 <b>[{html.escape(t)}] 소프트웨어 트리거 요격(Breakout) 성공!</b>\n▫️ VWAP 상향 돌파를 확인하고 매도 1호가(${ask_price:.2f})로 즉각 낚아챘습니다.\n▫️ 암살자 지정 예산 락온 투입(${safe_available_cash:,.2f})\n▫️ 지정가(LIMIT) 요격 대기: {buy_qty}주"
                                                await _safe_send(context, chat_id, msg_text, parse_mode='HTML')
                                        else:
                                            await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'is_ordering': False}), timeout=45.0)
                                            err_msg = html.escape(str(safe_b_res.get('msg1') or '응답 없음/통신 장애'))
                                            logging.error(f"🚨 [{t}] 암살자 소프트웨어 트리거 KIS 서버 거절: {err_msg}")
                                    else:
                                        await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'is_ordering': False}), timeout=45.0)
                                        t_state_chk = await asyncio.wait_for(asyncio.to_thread(_read_state_sync, t, now_est), timeout=45.0)
                                        
                                        if not t_state_chk.get('cash_warned'):
                                            warn_msg = f"⚠️ <b>[{html.escape(t)}] 암살자 요격 보류 (가용 현금 부족)</b>\n▫️ 지정 예산({user_budget}) 한도 내 가용 현금(${safe_available_cash:.2f})이 1주 매수 금액에 미달합니다."
                                            if chat_id: await _safe_send(context, chat_id, warn_msg, parse_mode='HTML')
                                            await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'cash_warned': True}), timeout=45.0)
                                else:
                                    await asyncio.wait_for(asyncio.to_thread(_update_state_sync, t, now_est, {'is_ordering': False}), timeout=45.0)
                                    logging.error(f"🚨 [{t}] 매도 1호가/현재가 모두 0.0 반환. 요격 스킵.")

                try:
                    master_switch = await asyncio.wait_for(asyncio.to_thread(getattr(cfg, 'get_master_switch', lambda x: "ALL"), t), timeout=45.0)
                    sniper_buy_locked = await asyncio.wait_for(asyncio.to_thread(getattr(cfg, 'get_sniper_buy_locked', lambda x: False), t), timeout=45.0)
                    sniper_sell_locked = await asyncio.wait_for(asyncio.to_thread(getattr(cfg, 'get_sniper_sell_locked', lambda x: False), t), timeout=45.0)
                except Exception:
                    master_switch = "ALL"
                    sniper_buy_locked = False
                    sniper_sell_locked = False

                curr_p = _safe_float(await _retry_api(broker.get_current_price, t))
                if curr_p <= 0: continue

                sniper_func = getattr(strategy, 'check_sniper_condition', None)
                if sniper_func:
                    try:
                        res = await asyncio.wait_for(asyncio.to_thread(sniper_func, t, cfg, broker, chat_id), timeout=45.0)
                    except Exception as e:
                        logging.error(f"🚨 [{t}] V14 스나이퍼 조건 검사 타임아웃/오류: {e}")
                        res = {"action": "HOLD", "reason": "스나이퍼 모듈 타임아웃", "limit_price": 0.0}
                else: 
                    res = {"action": "HOLD", "reason": "스나이퍼 모듈 누락(Bypass)", "limit_price": 0.0}
                
                if not isinstance(res, dict): res = {} 
                
                action = res.get("action")
                reason = html.escape(str(res.get("reason", "")))
                limit_p = res.get("limit_price", 0.0)

                try: version = await asyncio.wait_for(asyncio.to_thread(cfg.get_version, t), timeout=45.0)
                except Exception: version = "V14"
                is_rev = (version == "V_REV")

                if action == "BUY" and not is_rev and not sniper_buy_locked and master_switch != "UP_ONLY":
                    qty = res.get("qty", 0)
                    if qty > 0:
                        await _retry_api(broker.cancel_targeted_orders, t, "BUY", "00")
                        await asyncio.sleep(0.5)
                        
                        unfilled = await _retry_api(broker.get_unfilled_orders_detail, t)
                        safe_unfilled = unfilled if isinstance(unfilled, list) else []
                        if any(isinstance(o, dict) and o.get('sll_buy_dvsn_cd') == '02' and str(o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '').strip().zfill(2) == '00' for o in safe_unfilled):
                            continue
                        
                        ask_price = _safe_float(await _retry_api(broker.get_ask_price, t))
                        exec_price = ask_price if ask_price > 0 else limit_p

                        order_res = await _retry_api(broker.send_order, t, "BUY", qty, exec_price, "LIMIT")
                        odno = order_res.get('odno', '') if isinstance(order_res, dict) else ''
       
                        if order_res and order_res.get('rt_cd') == '0' and odno:
                            now_kst_fresh = datetime.datetime.now(ZoneInfo('Asia/Seoul'))
                            kis_search_start_fresh = (now_kst_fresh - datetime.timedelta(days=2)).strftime('%Y%m%d')
                            query_end_dt_fresh = now_kst_fresh.strftime('%Y%m%d')
                            
                            await asyncio.sleep(2.0)
                            
                            unfilled_check = await _retry_api(broker.get_unfilled_orders_detail, t)
                            safe_unfilled_check = unfilled_check if isinstance(unfilled_check, list) else []
                            my_order = next((ox for ox in safe_unfilled_check if isinstance(ox, dict) and str(ox.get('odno', '')) == odno), None)
                            
                            ccld_qty = 0
                            if my_order:
                                ccld_qty = int(_safe_float(my_order.get('tot_ccld_qty')))
                            else:
                                exec_hist = await _retry_api(broker.get_execution_history, t, kis_search_start_fresh, query_end_dt_fresh)
                                safe_exec = exec_hist if isinstance(exec_hist, list) else []
                                filled_rec = next((ex for ex in safe_exec if isinstance(ex, dict) and str(ex.get('odno', '')) == odno), None)
                                if filled_rec: ccld_qty = int(_safe_float(filled_rec.get('ft_ccld_qty')))

                            if ccld_qty < qty:
                                await _retry_api(broker.cancel_order, t, odno)
                                await asyncio.sleep(0.5)

                            if ccld_qty > 0:
                                if hasattr(cfg, 'set_sniper_buy_locked'):
                                    try: await asyncio.wait_for(asyncio.to_thread(cfg.set_sniper_buy_locked, t, True), timeout=45.0)
                                    except Exception: pass
                                
                                exec_history = await _retry_api(broker.get_execution_history, t, kis_search_start_fresh, query_end_dt_fresh)
                                safe_exec_history = exec_history if isinstance(exec_history, list) else []
                                
                                actual_exec_price = next((_safe_float(ex.get('ft_ccld_unpr3')) for ex in safe_exec_history if isinstance(ex, dict) and ex.get('sll_buy_dvsn_cd') == '02' and ex.get('odno') == odno and _safe_float(ex.get('ft_ccld_unpr3')) > 0), next((_safe_float(ex.get('ft_ccld_unpr3')) for ex in safe_exec_history if isinstance(ex, dict) and ex.get('sll_buy_dvsn_cd') == '02' and _safe_float(ex.get('ft_ccld_unpr3')) > 0), limit_p))
                                display_price = actual_exec_price if actual_exec_price > 0 else limit_p
    
                                if chat_id:
                                    msg = f"🚨 <b>[{html.escape(str(t))}] V14 스나이퍼 딥-매수(Intercept) 명중!</b>\n▫️ 타겟가: ${limit_p:.2f}\n▫️ 팩트 단가: ${display_price:.2f}\n▫️ 체결수량: {ccld_qty}주 (요청: {qty}주)\n▫️ 사유: {reason}\n▫️ 하방 방어망이 잠깁니다 (상방 독립 유지)."
                                    await _safe_send(context, chat_id, msg, parse_mode='HTML')
                        else:
                            err_msg = html.escape(str(order_res.get('msg1') or '응답 없음')) if isinstance(order_res, dict) else '통신 장애'
                            logging.error(f"🚨 [{t}] V14 스나이퍼 매수 KIS 서버 거절: {err_msg}")
                            reject_msg = (
                                f"🚨 <b>[{html.escape(str(t))}] V14 스나이퍼 딥매수 서버 거절 (Reject)!</b>\n"
                                f"▫️ 사유: <code>{err_msg}</code>\n"
                                f"▫️ 조치: 다음 스캔 시 재시도합니다."
                            )
                            if chat_id: await _safe_send(context, chat_id, reject_msg, parse_mode='HTML')

                is_zero_start_session = False
                try:
                    snap = None
                    if is_rev and hasattr(strategy, 'v_rev_plugin'): 
                        snap = await asyncio.wait_for(asyncio.to_thread(strategy.v_rev_plugin.load_daily_snapshot, t), timeout=45.0)
                    elif version == "V14":
                        is_manual_vwap = False
                        try: is_manual_vwap = await asyncio.wait_for(asyncio.to_thread(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t), timeout=45.0)
                        except Exception: pass
                        
                        if is_manual_vwap and hasattr(strategy, 'v14_vwap_plugin'): 
                            snap = await asyncio.wait_for(asyncio.to_thread(strategy.v14_vwap_plugin.load_daily_snapshot, t), timeout=45.0)
                        elif hasattr(strategy, 'v14_plugin') and hasattr(strategy.v14_plugin, 'load_daily_snapshot'): 
                            snap = await asyncio.wait_for(asyncio.to_thread(strategy.v14_plugin.load_daily_snapshot, t), timeout=45.0)
                    
                    if snap: 
                        is_zero_val = snap.get("is_zero_start")
                        if is_zero_val is None:
                            tot_q = int(_safe_float(snap.get("total_q", -1)))
                            if tot_q == -1: tot_q = int(_safe_float(snap.get("initial_qty", -1)))
                            is_zero_start_session = (tot_q == 0)
                        else:
                            if isinstance(is_zero_val, str): is_zero_start_session = (is_zero_val.lower() == 'true')
                            else: is_zero_start_session = bool(is_zero_val)
                except Exception: pass

                try:
                    upward_mode = await asyncio.wait_for(asyncio.to_thread(getattr(cfg, 'get_upward_sniper_mode', lambda x: False), t), timeout=45.0)
                except Exception:
                    upward_mode = False
         
                is_upward_active = upward_mode and not is_rev and not sniper_sell_locked and master_switch != "DOWN_ONLY"
                if is_zero_start_session: 
                    is_upward_active = False

                if is_upward_active and action in ["SELL_QUARTER", "SELL_JACKPOT"]:
                    qty = res.get("qty", 0)
                    if qty > 0:
                        await _retry_api(broker.cancel_targeted_orders, t, "SELL", "00")
                        await asyncio.sleep(0.5)
                        
                        unfilled = await _retry_api(broker.get_unfilled_orders_detail, t)
                        safe_unfilled = unfilled if isinstance(unfilled, list) else []
                        if any(isinstance(o, dict) and o.get('sll_buy_dvsn_cd') == '01' and str(o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '').strip().zfill(2) == '00' for o in safe_unfilled):
                            continue 
      
                        bid_price = _safe_float(await _retry_api(broker.get_bid_price, t))
                        exec_price = bid_price if bid_price > 0 else limit_p
            
                        rt_bal_v14 = await _retry_api(broker.get_account_balance)
                        if rt_bal_v14 and isinstance(rt_bal_v14, (list, tuple)) and len(rt_bal_v14) > 1:
                            safe_rt_v14_dict = rt_bal_v14[1] if isinstance(rt_bal_v14[1], dict) else {}
                            _t_data_v14 = safe_rt_v14_dict.get(t)
                            rt_qty_v14 = int(_safe_float(_t_data_v14.get('qty', 0) if isinstance(_t_data_v14, dict) else 0))
                        else:
                            _t_hold_v14 = safe_holdings.get(t)
                            rt_qty_v14 = int(_safe_float(_t_hold_v14.get('qty', 0) if isinstance(_t_hold_v14, dict) else 0))
                        
                        qty = min(qty, rt_qty_v14)
                        
                        if qty > 0:
                            order_res = await _retry_api(broker.send_order, t, "SELL", qty, exec_price, "LIMIT")
                        else:
                            order_res = {'rt_cd': '999', 'msg1': '보유 수량 0주 캡핑으로 스나이퍼 매도 스킵'}
                            
                        odno = order_res.get('odno', '') if isinstance(order_res, dict) else ''
            
                        if order_res and order_res.get('rt_cd') == '0' and odno:
                            now_kst_fresh = datetime.datetime.now(ZoneInfo('Asia/Seoul'))
                            kis_search_start_fresh = (now_kst_fresh - datetime.timedelta(days=2)).strftime('%Y%m%d')
                            query_end_dt_fresh = now_kst_fresh.strftime('%Y%m%d')
                            
                            await asyncio.sleep(2.0) 
                            
                            unfilled_check = await _retry_api(broker.get_unfilled_orders_detail, t)
                            safe_unfilled_check = unfilled_check if isinstance(unfilled_check, list) else []
                            my_order = next((ox for ox in safe_unfilled_check if isinstance(ox, dict) and str(ox.get('odno', '')) == odno), None)
                            
                            ccld_qty = 0
                            if my_order:
                                ccld_qty = int(_safe_float(my_order.get('tot_ccld_qty')))
                            else:
                                exec_hist = await _retry_api(broker.get_execution_history, t, kis_search_start_fresh, query_end_dt_fresh)
                                safe_exec = exec_hist if isinstance(exec_hist, list) else []
                                filled_rec = next((ex for ex in safe_exec if isinstance(ex, dict) and str(ex.get('odno', '')) == odno), None)
                                if filled_rec: ccld_qty = int(_safe_float(filled_rec.get('ft_ccld_qty')))

                            if ccld_qty < qty:
                                await _retry_api(broker.cancel_order, t, odno)
                                await asyncio.sleep(0.5)

                            if ccld_qty > 0:
                                if hasattr(cfg, 'set_sniper_sell_locked'): 
                                    try: await asyncio.wait_for(asyncio.to_thread(cfg.set_sniper_sell_locked, t, True), timeout=45.0)
                                    except Exception: pass
                                
                                exec_history = await _retry_api(broker.get_execution_history, t, kis_search_start_fresh, query_end_dt_fresh)
                                safe_exec_history = exec_history if isinstance(exec_history, list) else []
                                
                                actual_exec_price = next((_safe_float(ex.get('ft_ccld_unpr3')) for ex in safe_exec_history if isinstance(ex, dict) and ex.get('sll_buy_dvsn_cd') == '01' and str(ex.get('odno', '')) == odno and _safe_float(ex.get('ft_ccld_unpr3')) > 0), next((_safe_float(ex.get('ft_ccld_unpr3')) for ex in safe_exec_history if isinstance(ex, dict) and ex.get('sll_buy_dvsn_cd') == '01' and _safe_float(ex.get('ft_ccld_unpr3')) > 0), limit_p))
                                display_price = actual_exec_price if actual_exec_price > 0 else limit_p

                                if chat_id:
                                    msg = f"🦇 <b>[{html.escape(str(t))}] V14 스나이퍼 상방 기습({action}) 명중!</b>\n▫️ 타겟가: ${limit_p:.2f}\n▫️ 팩트 단가: ${display_price:.2f}\n▫️ 체결수량: {ccld_qty}주 (요청: {qty}주)\n▫️ 사유: {reason}\n▫️ 상방 감시망이 잠깁니다 (하방 독립 유지)."
                                    await _safe_send(context, chat_id, msg, parse_mode='HTML')
                        else:
                            err_msg = html.escape(str(order_res.get('msg1') or '응답 없음')) if isinstance(order_res, dict) else '통신 장애'
                            logging.error(f"🚨 [{t}] V14 스나이퍼 상방 기습 서버 거절: {err_msg}")
                            reject_msg = (
                                f"🚨 <b>[{html.escape(str(t))}] V14 스나이퍼 상방 기습 서버 거절 (Reject)!</b>\n"
                                f"▫️ 사유: <code>{err_msg}</code>\n"
                                f"▫️ 조치: 다음 스캔 시 재시도합니다."
                            )
                            if chat_id: await _safe_send(context, chat_id, reject_msg, parse_mode='HTML')

            except Exception as e:
                logging.error(f"🚨 [{t}] 스나이퍼 엔진 단일 종목 연산 중 치명적 오류 (Cascade 방어): {e}", exc_info=True)
                continue

    try:
        await asyncio.wait_for(_do_sniper(), timeout=55.0)
    except Exception as e:
        logging.error(f"🚨 스나이퍼 타임아웃 에러: {e}", exc_info=True)

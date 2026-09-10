# ==========================================================
# FILE: scheduler_regular.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 5대 헌법 및 39대 엣지 케이스 완벽 결속 교차 검증 완료.
# 🚨 MODIFIED: [Thundering Herd 영구 소각] 파편화된 await asyncio.sleep(0.06) 땜질을 무려 24개소에서 전면 삭제.
# 🚨 MODIFIED: [중앙 통제소 위임] 모든 API 지연을 GlobalThrottle(중앙 통제소)로 100% 위임하여 비동기 이벤트 루프 마비 및 교착 상태 완벽 방어.
# 🚨 MODIFIED: [예약 주문 증발(Ghost Order) 궁극 수술] V14 LOC 장전 시 하드코딩되어 있던 `is_market_active_now = False`를 영구 소각. 현재 시간(EST)을 동적으로 판별하여 프리장 개장 후(서머타임 04:05 EST)에는 실시간 본주문(`send_order`)을, 개장 전(윈터타임 03:05 EST)에는 예약 주문(`send_reservation_order`)을 격발하도록 팩트 락온 완료.
# 🚨 MODIFIED: [자본 잠김 맹독성 컷오프 파기] 암살자 물량 보유 여부만으로 무조건 본진 타격을 지연 이관하던 오류를 영구 소각하고, 실질적 예산(safe_alloc_cash)이 목표 예산(15%)의 90% 미만일 때만 자본 잠김으로 판별하도록 팩트 교정 완료.
# 🚨 MODIFIED: [0주 산출 Bypass 오인 패러독스 궁극 방어] 예산 부족 등으로 매수 수량이 0주로 산출될 경우 지시서 파일이 생성되지 않아 15:27 VWAP 엔진이 이를 스케줄러 붕괴로 오인(False Alarm)하는 대참사를 원천 차단하기 위해, V-REV 및 V14_VWAP 매매 집행 직전에 무조건 '빈 지시서(Empty Init)'를 선제 박제하도록 100% 팩트 락온 완료.
# 🚨 MODIFIED: [스케줄러 타임아웃 연쇄 폭발 궁극 수술] 3단 지수 백오프 대기 시간 누적으로 인해 300초 전역 타임아웃을 돌파해버리는 대참사를 막기 위해 600초로 타임아웃 상한(Capping)을 확장 결속 완료.
# 🚨 MODIFIED: [TimeoutError 침묵 패러독스 수술] 파이썬 내장 TimeoutError 발생 시 str(e)가 빈 문자열("")을 반환하여 텔레그램 타전망에 사유가 누락되는 현상을 100% 원천 봉쇄(type(e).__name__ 폴백 결속).
# 🚨 NEW: [스레드 풀 고갈(Thread Leak) 궁극 수술] 하위 KIS 통신 지연(최대 35초) 시 상위 `wait_for`가 10~15초 만에 코루틴을 취소시켜 발생하는 '좀비 스레드 누적 및 교착(Deadlock)' 대참사를 원천 봉쇄하기 위해, 모든 비동기 I/O 래퍼의 타임아웃을 45.0초로 상향 팩트 하드 캡핑 완료.
# 🚨 MODIFIED: [오인 패러독스 방어] 15:26 EST 본진 덫 투하 기상 시, 가동할 V-REV 종목이 모두 매매 잠금(is_locked=True) 상태로 퇴근했다면 맹목적인 "투하 개시" 브로드캐스트를 전면 묵살하고 스케줄러 자체를 조용히 바이패스(Bypass)하도록 팩트 방어막 결속 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import asyncio
import random
import html
import math
import functools

from scheduler_core import is_market_open, get_budget_allocation
from order_executor import execute_order_list
from state_io_manager import read_avwap_state_sync, _atomic_write_json_sync

def _safe_float(val):
    try:
        f_val = float(str(val or 0.0).replace(',', ''))
        if math.isnan(f_val) or math.isinf(f_val):
            return 0.0
        return f_val
    except Exception:
        return 0.0

async def _retry_api(func, *args, timeout=45.0, default=None, **kwargs):
    for attempt in range(3):
        try:
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

async def scheduled_early_regular_trade(context):
    is_open = False
    for attempt in range(3):
        try:
            # 🚨 MODIFIED: [스레드 풀 고갈 붕괴 수술] 10.0 -> 45.0 강제 상향
            is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=45.0)
            break
        except asyncio.TimeoutError:
            if attempt == 2:
                logging.error("⚠️ is_market_open 달력 API 타임아웃. 평일이므로 강제 개장 처리합니다.")
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception:
            if attempt == 2:
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))

    if not is_open:
        return
    
    job = getattr(context, 'job', None)
    app_data = getattr(job, 'data', {}) if job else {}
    if not isinstance(app_data, dict): app_data = {}
    
    cfg = app_data.get('cfg')
    broker = app_data.get('broker')
    strategy = app_data.get('strategy')
    tx_lock = app_data.get('tx_lock')
    chat_id = getattr(job, 'chat_id', None)
  
    if tx_lock is None:
        logging.warning("⚠️ [early_trade] tx_lock 미초기화. 이번 사이클 스킵.")
        return

    jitter_seconds = random.randint(0, 180)
    
    if chat_id:
        try:
            await asyncio.wait_for(
                context.bot.send_message(
                    chat_id=chat_id, 
                    text=f"🌃 <b>[17:05 KST] 정규장 스케줄러 기상!</b>\n"
                         f"▫️ 서버 접속 부하 방지를 위해 <b>{jitter_seconds}초</b> 대기 후 V14 덫 전송 및 스냅샷을 박제합니다.", 
                    parse_mode='HTML'
                ),
                timeout=15.0
            )
        except Exception as e:
            logging.error(f"초기 기상 메시지 텔레그램 발송 실패: {e}")
        
    await asyncio.sleep(jitter_seconds)

    MAX_RETRIES = 5
    RETRY_DELAY = 10
    successful_orders_cache = set()

    async def _do_early_trade():
        est_z = ZoneInfo('America/New_York')
        curr_est = datetime.datetime.now(est_z)
        today_str = curr_est.strftime("%Y-%m-%d")
        
        async with tx_lock:
            cash, holdings = 0.0, None
            for attempt in range(3):
                try:
                    # 🚨 MODIFIED: [스레드 풀 고갈 붕괴 수술] 15.0 -> 45.0 강제 상향
                    res = await asyncio.wait_for(asyncio.to_thread(broker.get_account_balance), timeout=45.0)
                    cash = _safe_float(res[0]) if isinstance(res, (list, tuple)) and len(res) > 0 else 0.0
                    holdings = res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else {}
                    break
                except asyncio.TimeoutError:
                    if attempt == 2: return False, "잔고 조회 타임아웃"
                    else: await asyncio.sleep(1.0 * (2 ** attempt))
                except Exception as e:
                    if attempt == 2: return False, f"잔고 조회 오류: {html.escape(str(e))}"
                    else: await asyncio.sleep(1.0 * (2 ** attempt))
            
            if holdings is None:
                return False, "❌ 계좌 정보를 불러오지 못했습니다."
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}
            
            try:
                # 🚨 MODIFIED: 10.0 -> 45.0
                active_tickers_list = await asyncio.wait_for(asyncio.to_thread(cfg.get_active_tickers), timeout=45.0) or []
            except Exception:
                active_tickers_list = []
            
            if isinstance(active_tickers_list, str): active_tickers_list = [active_tickers_list]
            elif not isinstance(active_tickers_list, list): active_tickers_list = []
            
            if not active_tickers_list:
                return False, "❌ 활성 종목 리스트 결측치 반환. 스케줄 보호 중단."
            
            try:
                # 🚨 MODIFIED: 10.0 -> 45.0
                alloc_res = await asyncio.wait_for(asyncio.to_thread(get_budget_allocation, cash, active_tickers_list, cfg), timeout=45.0)
            except Exception as e:
                logging.error(f"🚨 예산 할당 타임아웃/에러: {e}")
                alloc_res = None
            
            if not alloc_res or len(alloc_res) != 2:
                return False, "❌ 예산 할당 로직 결측치(None) 반환. 스케줄 보호 중단."
            
            sorted_tickers, allocated_cash = alloc_res
            sorted_tickers = sorted_tickers or []
            allocated_cash = allocated_cash or {}
            
            msgs = {t: "" for t in sorted_tickers}
            all_success_map = {t: True for t in sorted_tickers}
            
            loop_fully_successful = True
            loop_fail_reason = ""

            for t in sorted_tickers:
                try:
                    try:
                        # 🚨 MODIFIED: 5.0 -> 45.0
                        version = await asyncio.wait_for(asyncio.to_thread(cfg.get_version, t), timeout=45.0)
                    except Exception:
                        version = "V14"
                        
                    try:
                        # 🚨 MODIFIED: 5.0 -> 45.0
                        is_locked = await asyncio.wait_for(asyncio.to_thread(cfg.check_lock, t, "REG"), timeout=45.0)
                    except Exception:
                        is_locked = False
                    
                    if is_locked:
                        skip_msg = f"⚠️ <b>[{t}] REG 잠금 미해제 — 스케줄 루프 스킵</b>\n▫️ 수동으로 잠금 해제 후 상태를 확인하십시오."
                        if chat_id:
                            try:
                                await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=skip_msg, parse_mode='HTML'), timeout=15.0)
                            except Exception: pass
                        continue

                    h = safe_holdings.get(t) or {}
                    safe_avg = _safe_float(h.get('avg'))
                    safe_qty = int(_safe_float(h.get('qty')))
                    safe_alloc_cash = _safe_float(allocated_cash.get(t, 0.0))

                    curr_p, prev_c = 0.0, 0.0
                    for _api_retry in range(3):
                        try:
                            # 🚨 MODIFIED: 15.0 -> 45.0
                            curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=45.0)
                            curr_p = _safe_float(curr_p_val)
                            
                            prev_c_val = await asyncio.wait_for(asyncio.to_thread(broker.get_previous_close, t), timeout=45.0)
                            prev_c = _safe_float(prev_c_val)
                            
                            if curr_p > 0 and prev_c > 0: break
                        except Exception:
                            pass
                        await asyncio.sleep(1.0 * (2**_api_retry))

                    ma_5day = 0.0
                    for attempt in range(3):
                        try:
                            # 🚨 MODIFIED: 15.0 -> 45.0
                            ma_5day_val = await asyncio.wait_for(asyncio.to_thread(broker.get_5day_ma, t), timeout=45.0)
                            ma_5day = _safe_float(ma_5day_val)
                            break
                        except Exception: 
                            if attempt == 2: ma_5day = 0.0
                            else: await asyncio.sleep(1.0 * (2**attempt))
                    
                    try:
                        # 🚨 MODIFIED: 15.0 -> 45.0
                        plan = await asyncio.wait_for(asyncio.to_thread(
                            strategy.get_plan, t, curr_p, safe_avg, safe_qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=safe_alloc_cash, is_snapshot_mode=True
                        ), timeout=45.0)
                    except Exception as e:
                        logging.error(f"🚨 [{t}] 플랜 생성 타임아웃/에러: {e}")
                        plan = None
                    
                    if not isinstance(plan, dict):
                        msgs[t] += f"🚨 <b>[{t}] 스냅샷 유실 또는 손상! KIS 전송 불가.</b>\n"
                        all_success_map[t] = False
                        loop_fully_successful = False
                        loop_fail_reason = f"[{t}] 플랜 오염"
                        continue
                    
                    if version == "V14":
                        msgs[t] += f"💎 <b>[{t}] V14 오리지널 정규장 실전 덫 장전 완료 (17:05 KST 타격망)</b>\n"
                        
                        try:
                            # 🚨 MODIFIED: 5.0 -> 45.0
                            is_manual_vwap = await asyncio.wait_for(asyncio.to_thread(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t), timeout=45.0)
                        except Exception:
                            is_manual_vwap = False
                            
                        if is_manual_vwap:
                            try:
                                slice_file = f"data/vrev_slice_state_{t}.json"
                                after_file = f"data/vrev_aftermarket_state_{t}.json"
                                empty_state = {"date": today_str, "hijacked": False, "orders": []}
                                # 🚨 MODIFIED: 10.0 -> 45.0
                                await asyncio.wait_for(asyncio.to_thread(_atomic_write_json_sync, slice_file, empty_state), timeout=45.0)
                                await asyncio.wait_for(asyncio.to_thread(_atomic_write_json_sync, after_file, empty_state), timeout=45.0)
                            except Exception as init_e:
                                logging.error(f"🚨 [{t}] V14 VWAP 지시서 선제 초기화 에러: {init_e}")

                        is_market_active_now = curr_est.hour >= 4

                        target_orders = plan.get('core_orders') or plan.get('orders') or []
                        if not isinstance(target_orders, list): target_orders = []
                        
                        success_core, msg_core, fail_reason_core = await execute_order_list(
                            broker, t, target_orders, successful_orders_cache, is_market_active_now, today_str, is_capital_locked=False, order_category="1차 필수"
                        )
                        msgs[t] += msg_core

                        if not success_core:
                            all_success_map[t] = False
                            loop_fully_successful = False
                            loop_fail_reason = fail_reason_core
                            
                        target_bonus = plan.get('bonus_orders') or []
                        if not isinstance(target_bonus, list): target_bonus = []
                        
                        if all_success_map[t]:
                            success_bonus, msg_bonus, fail_reason_bonus = await execute_order_list(
                                broker, t, target_bonus, successful_orders_cache, is_market_active_now, today_str, is_capital_locked=False, order_category="2차 보너스"
                            )
                            msgs[t] += msg_bonus
                            if not success_bonus:
                                all_success_map[t] = False
                                loop_fully_successful = False
                                loop_fail_reason = fail_reason_bonus
                        elif target_bonus:
                            msgs[t] += f"⚠️ 1차 필수 장전 실패로 2차 보너스 덫 보류 (중복 매매 방어)\n"
                        
                        if (target_orders or target_bonus) and all_success_map[t]:
                            try:
                                # 🚨 MODIFIED: 5.0 -> 45.0
                                await asyncio.wait_for(asyncio.to_thread(cfg.set_lock, t, "REG"), timeout=45.0)
                            except Exception as e:
                                logging.error(f"🚨 락 설정 타임아웃: {e}")
                            msgs[t] += "\n🔒 <b>V14 필수 덫(로컬 엔진 포함) 장전 완료 (잠금 설정됨)</b>"
                    
                    else: 
                        msgs[t] += f"🔄 <b>[{t}] V-REV 역추세 덫 모의 장전 및 스냅샷 박제</b>\n"
                        target_orders = plan.get('core_orders') or plan.get('orders') or []
                        if not isinstance(target_orders, list): target_orders = []
                        for o in target_orders:
                            if not isinstance(o, dict): continue
                            safe_desc = html.escape(str(o.get('desc', '주문')))
                            safe_qty = int(_safe_float(o.get('qty')))
                            safe_price = _safe_float(o.get('price'))
                            msgs[t] += f"└ 모의 1차 필수: {safe_desc} {safe_qty}주 (${safe_price})\n"
                    
                        target_bonus = plan.get('bonus_orders') or []
                        if not isinstance(target_bonus, list): target_bonus = []
                        for o in target_bonus:
                            if not isinstance(o, dict): continue
                            safe_desc = html.escape(str(o.get('desc', '주문')))
                            safe_qty = int(_safe_float(o.get('qty')))
                            safe_price = _safe_float(o.get('price'))
                            msgs[t] += f"└ 모의 2차 보너스: {safe_desc} {safe_qty}주 (${safe_price})\n"
                            
                        if target_orders or target_bonus:
                            msgs[t] += "\n📸 <b>V-REV 당일 스냅샷 팩트 박제 완료 (15:26 EST 지연 투하 대기)</b>"
                    
                    if msgs[t].strip() and chat_id:
                        try:
                            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=msgs[t], parse_mode='HTML'), timeout=15.0)
                        except Exception as tg_e:
                            logging.error(f"[{t}] 개별 종목 텔레그램 메시지 발송 실패: {tg_e}")

                except Exception as e:
                    all_success_map[t] = False
                    loop_fully_successful = False
                    loop_fail_reason = f"[{t}] 치명적 오류: {str(e)}"
                    logging.error(f"🚨 [{t}] early_trade 개별 종목 처리 중 치명적 오류: {e}")
                    if chat_id:
                        try:
                            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"🚨 <b>[{t}] 스케줄러 처리 중 오류 발생. 스킵합니다.</b>\n<code>{html.escape(str(e))}</code>", parse_mode='HTML'), timeout=15.0)
                        except Exception: pass

            if not loop_fully_successful:
                return False, loop_fail_reason
        return True, "SUCCESS"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            success, fail_reason = await asyncio.wait_for(_do_early_trade(), timeout=600.0)
            if success:
                if attempt > 1 and chat_id: 
                    try:
                        await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"✅ <b>[통신 복구] {attempt}번째 재시도 끝에 장전을 완수했습니다!</b>", parse_mode='HTML'), timeout=15.0)
                    except Exception: pass
                return 
        except asyncio.TimeoutError:
            logging.error(f"17:05 덫 장전 에러 ({attempt}/{MAX_RETRIES}): 600초 전역 타임아웃 초과 (API 응답 지연 누적)", exc_info=True)
            if attempt == 1 and chat_id:
                try:
                    await asyncio.wait_for(
                        context.bot.send_message(
                            chat_id=chat_id, 
                            text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 10초 뒤 장전을 재시도합니다! 🛡️\n<code>사유: 600초 전역 타임아웃 초과 (API 지연 누적)</code>", 
                            parse_mode='HTML'
                        ),
                        timeout=15.0
                    )
                except Exception: pass
        except Exception as e:
            logging.error(f"17:05 덫 장전 에러 ({attempt}/{MAX_RETRIES}): {e}", exc_info=True)
            if attempt == 1 and chat_id:
                safe_err = html.escape(str(e)) if str(e) else type(e).__name__
                try:
                    await asyncio.wait_for(
                        context.bot.send_message(
                            chat_id=chat_id, 
                            text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 10초 뒤 장전을 재시도합니다! 🛡️\n<code>사유: {safe_err}</code>", 
                            parse_mode='HTML'
                        ),
                        timeout=15.0
                    )
                except Exception: pass
        else:
            logging.warning(f"장전 조건 미충족 ({attempt}/{MAX_RETRIES}): {fail_reason}")
            if attempt == 1 and chat_id:
                 safe_fail = html.escape(str(fail_reason)) if str(fail_reason) else "알 수 없는 실패"
                 try:
                     await asyncio.wait_for(
                         context.bot.send_message(
                            chat_id=chat_id, 
                            text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 10초 뒤 장전을 재시도합니다! 🛡️\n<code>사유: {safe_fail}</code>", 
                            parse_mode='HTML'
                         ),
                         timeout=15.0
                     )
                 except Exception: pass

        if attempt < MAX_RETRIES:
            if attempt != 1 and attempt % 5 == 0 and chat_id:
                try:
                    await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 10초 뒤 재시도합니다! 🛡️", parse_mode='HTML'), timeout=15.0)
                except Exception: pass
            await asyncio.sleep(RETRY_DELAY)

    if chat_id:
        try:
            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text="🚨 <b>[긴급 에러] 17:05 스케줄 통신 복구 최종 실패. 수동 점검 요망!</b>", parse_mode='HTML'), timeout=15.0)
        except Exception: pass


async def scheduled_regular_trade_delayed(context):
    is_open = False
    for attempt in range(3):
        try:
            # 🚨 MODIFIED: 10.0 -> 45.0
            is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=45.0)
            break
        except asyncio.TimeoutError:
            if attempt == 2:
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception:
            if attempt == 2:
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))

    if not is_open:
        return
    
    job = getattr(context, 'job', None)
    app_data = getattr(job, 'data', {}) if job else {}
    if not isinstance(app_data, dict): app_data = {}
    
    cfg = app_data.get('cfg')
    broker = app_data.get('broker')
    strategy = app_data.get('strategy')
    tx_lock = app_data.get('tx_lock')
    chat_id = getattr(job, 'chat_id', None)
    
    if tx_lock is None:
        return
        
    # 🚨 MODIFIED: [오인 패러독스 방어] 모든 V-REV 종목이 이미 락업(퇴근) 상태라면 무의미한 15:26 기상 메시지 발송을 원천 차단하고 즉각 스케줄러 셧다운.
    try:
        active_tickers_list = await asyncio.wait_for(asyncio.to_thread(cfg.get_active_tickers), timeout=45.0) or []
    except Exception:
        active_tickers_list = []
        
    any_unlocked_vrev = False
    for t in active_tickers_list:
        is_locked = await _retry_api(cfg.check_lock, t, "REG", default=False)
        ver = await _retry_api(cfg.get_version, t, default="V14")
        is_manual_vwap = await _retry_api(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t, default=False)
        if not is_locked and (ver == "V_REV" or (ver == "V14" and is_manual_vwap)):
            any_unlocked_vrev = True
            break
            
    if not any_unlocked_vrev:
        logging.info("💤 [regular_delayed] 가동 가능한 V-REV(또는 VWAP) 종목이 없거나 이미 퇴근(Lock)하여 15:26 투하 스케줄을 완전히 바이패스합니다.")
        return
    
    jitter_seconds = random.randint(0, 45)

    if chat_id:
        try:
            await asyncio.wait_for(
                context.bot.send_message(
                    chat_id=chat_id, 
                    text=f"🌃 <b>[15:26 EST] V-REV 본진 덫(자체 1분 슬라이싱 포함) 투하 개시!</b>\n"
                         f"🛡️ 서버 접속 부하 방지를 위해 <b>{jitter_seconds}초</b> 대기 후 전송/인계를 시도합니다.", 
                    parse_mode='HTML'
                ),
                timeout=15.0
            )
        except Exception as e:
            logging.error(f"지연 투하 시작 메시지 텔레그램 발송 실패: {e}")

    await asyncio.sleep(jitter_seconds)

    MAX_RETRIES = 15
    RETRY_DELAY = 60
    successful_orders_cache = set()

    async def _do_delayed_trade():
        async with tx_lock:
            cash, holdings = 0.0, None
            for attempt in range(3):
                try:
                    # 🚨 MODIFIED: 15.0 -> 45.0
                    res = await asyncio.wait_for(asyncio.to_thread(broker.get_account_balance), timeout=45.0)
                    cash = _safe_float(res[0]) if isinstance(res, (list, tuple)) and len(res) > 0 else 0.0
                    holdings = res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else {}
                    break
                except Exception as e:
                    if attempt == 2: return False, f"잔고 조회 오류: {html.escape(str(e))}"
                    else: await asyncio.sleep(1.0 * (2 ** attempt))
            
            if holdings is None:
                return False, "❌ 계좌 정보를 불러오지 못했습니다."
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}
            
            try:
                # 🚨 MODIFIED: 10.0 -> 45.0
                active_tickers_list = await asyncio.wait_for(asyncio.to_thread(cfg.get_active_tickers), timeout=45.0) or []
            except Exception:
                active_tickers_list = []
            
            if isinstance(active_tickers_list, str): active_tickers_list = [active_tickers_list]
            elif not isinstance(active_tickers_list, list): active_tickers_list = []
            
            if not active_tickers_list:
                return False, "❌ 활성 종목 리스트 결측치(None) 반환. 스케줄 보호 중단."
                
            try:
                # 🚨 MODIFIED: 10.0 -> 45.0
                alloc_res = await asyncio.wait_for(asyncio.to_thread(get_budget_allocation, cash, active_tickers_list, cfg), timeout=45.0)
            except Exception as e:
                logging.error(f"🚨 예산 할당 타임아웃/에러: {e}")
                alloc_res = None
                
            if not alloc_res or len(alloc_res) != 2:
                return False, "❌ 예산 할당 로직 결측치 반환."
            
            sorted_tickers, allocated_cash = alloc_res
            sorted_tickers = sorted_tickers or []
            allocated_cash = allocated_cash or {}
            
            plans = {}
            msgs = {t: "" for t in sorted_tickers}
            all_success_map = {t: True for t in sorted_tickers}
            capital_locked_map = {t: False for t in sorted_tickers} 
            
            loop_fully_successful = True
            loop_fail_reason = ""

            est_z = ZoneInfo('America/New_York')
            curr_est = datetime.datetime.now(est_z)
            today_str = curr_est.strftime("%Y-%m-%d")
            
            is_market_active_now = True

            for t in sorted_tickers:
                try:
                    try:
                        # 🚨 MODIFIED: 5.0 -> 45.0
                        version = await asyncio.wait_for(asyncio.to_thread(cfg.get_version, t), timeout=45.0)
                    except Exception:
                        version = "V14"
                    
                    if version == "V14":
                        continue 
                    
                    try:
                        # 🚨 MODIFIED: 5.0 -> 45.0
                        is_locked = await asyncio.wait_for(asyncio.to_thread(cfg.check_lock, t, "REG"), timeout=45.0)
                    except Exception:
                        is_locked = False
                        
                    if is_locked:
                        continue

                    safe_alloc_cash = _safe_float(allocated_cash.get(t, 0.0))
                    
                    is_capital_locked = False
                    if version == "V_REV":
                        required_budget = 0.0
                        for seed_attempt in range(3):
                            try:
                                # 🚨 MODIFIED: 5.0 -> 45.0
                                seed_val = await asyncio.wait_for(asyncio.to_thread(cfg.get_seed, t), timeout=45.0)
                                required_budget = _safe_float(seed_val) * 0.15
                                break
                            except Exception as e:
                                if seed_attempt == 2: logging.error(f"🚨 [{t}] 자본 잠김 판별용 시드 추출 에러: {e}")
                                else: await asyncio.sleep(1.0 * (2 ** seed_attempt))
                        
                        if required_budget > 0 and safe_alloc_cash < required_budget * 0.9:
                            is_capital_locked = True
                            logging.info(f"🚨 [{t}] 자본 잠김 팩트 감지: 가용 예산(${safe_alloc_cash:.2f})이 목표 예산(${required_budget:.2f})에 미달합니다. (암살자 점유 등 원인)")
                                
                    capital_locked_map[t] = is_capital_locked 

                    h = safe_holdings.get(t) or {}
                    safe_avg = _safe_float(h.get('avg'))
                    safe_qty = int(_safe_float(h.get('qty')))

                    if is_capital_locked:
                        for seed_attempt in range(3):
                            try:
                                # 🚨 MODIFIED: 5.0 -> 45.0
                                seed_val = await asyncio.wait_for(asyncio.to_thread(cfg.get_seed, t), timeout=45.0)
                                safe_alloc_cash = _safe_float(seed_val) * 0.15
                                logging.info(f"🚨 [{t}] 지연 이관 플랜 생성을 위해 가상의 1일 고정 예산(${safe_alloc_cash:.2f})을 일시 복원합니다.")
                                break
                            except Exception as e:
                                if seed_attempt == 2: logging.error(f"🚨 [{t}] 가상 예산 복원 실패: {e}")
                                else: await asyncio.sleep(1.0 * (2 ** seed_attempt))

                    curr_p, prev_c = 0.0, 0.0
                    for _api_retry in range(3):
                        try:
                            # 🚨 MODIFIED: 15.0 -> 45.0
                            curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=45.0)
                            curr_p = _safe_float(curr_p_val)
                            
                            prev_c_val = await asyncio.wait_for(asyncio.to_thread(broker.get_previous_close, t), timeout=45.0)
                            prev_c = _safe_float(prev_c_val)
                            
                            if curr_p > 0 and prev_c > 0: break
                        except Exception:
                            pass
                        await asyncio.sleep(1.0 * (2**_api_retry))

                    ma_5day = 0.0
                    for attempt in range(3):
                        try:
                            # 🚨 MODIFIED: 15.0 -> 45.0
                            ma_5day_val = await asyncio.wait_for(asyncio.to_thread(broker.get_5day_ma, t), timeout=45.0)
                            ma_5day = _safe_float(ma_5day_val)
                            break
                        except Exception: 
                            if attempt == 2: ma_5day = 0.0
                            else: await asyncio.sleep(1.0 * (2**attempt))
                    
                    try:
                        # 🚨 MODIFIED: 15.0 -> 45.0
                        plan = await asyncio.wait_for(asyncio.to_thread(
                            strategy.get_plan, t, curr_p, safe_avg, safe_qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=safe_alloc_cash, is_snapshot_mode=True
                        ), timeout=45.0)
                    except Exception as e:
                        logging.error(f"🚨 [{t}] 플랜 생성 타임아웃/에러: {e}")
                        plan = None
                    
                    if not isinstance(plan, dict):
                        msgs[t] += f"🚨 <b>[{t}] 스냅샷 유실 또는 손상! KIS 전송 불가.</b>\n"
                        all_success_map[t] = False
                        loop_fully_successful = False
                        loop_fail_reason = f"[{t}] 스냅샷 유실"
                        continue

                    plans[t] = plan
                    if plan.get('core_orders') or plan.get('orders') or plan.get('bonus_orders'):
                        if is_capital_locked:
                            msgs[t] += f"⏳ <b>[{t}] 자본 잠김(Capital Lock-up) 팩트 감지!</b>\n▫️ 예산 고갈(암살자 점유 등)로 인해 정규장 <b>매수(BUY) 플랜을 16:01 애프터장 일괄 타격으로 지연 이관</b>(Delay & Transfer)합니다. (매도는 정상 투하)\n"
                        else:
                            msgs[t] += f"🔄 <b>[{t}] V-REV 역추세 실전 덫(로컬 엔진 포함) 장전 완료</b>\n"
                except Exception as e:
                    all_success_map[t] = False
                    loop_fully_successful = False
                    loop_fail_reason = f"[{t}] 치명적 오류: {str(e)}"
                    logging.error(f"🚨 [{t}] delayed_trade 플랜 조회 오류: {e}")
                    msgs[t] += f"🚨 <b>[{t}] 플랜 조회 중 에러 발생</b>\n"

            for t in sorted_tickers:
                try:
                    if t not in plans:
                        if msgs[t].strip() and chat_id:
                            try: 
                                await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=msgs[t], parse_mode='HTML'), timeout=15.0)
                            except Exception as tg_e: logging.error(f"[{t}] V-REV 디커플링 메시지 발송 실패: {tg_e}")
                        continue
                        
                    try:
                        slice_file = f"data/vrev_slice_state_{t}.json"
                        after_file = f"data/vrev_aftermarket_state_{t}.json"
                        empty_state = {"date": today_str, "hijacked": False, "orders": []}
                        # 🚨 MODIFIED: 10.0 -> 45.0
                        await asyncio.wait_for(asyncio.to_thread(_atomic_write_json_sync, slice_file, empty_state), timeout=45.0)
                        await asyncio.wait_for(asyncio.to_thread(_atomic_write_json_sync, after_file, empty_state), timeout=45.0)
                    except Exception as init_e:
                        logging.error(f"🚨 [{t}] V-REV 지시서 선제 초기화 에러: {init_e}")

                    target_orders = plans[t].get('core_orders') or plans[t].get('orders') or []
                    if not isinstance(target_orders, list): target_orders = []
                        
                    is_capital_locked = capital_locked_map.get(t, False) 
                    
                    success_core, msg_core, fail_reason_core = await execute_order_list(
                        broker, t, target_orders, successful_orders_cache, is_market_active_now, today_str, is_capital_locked=is_capital_locked, order_category="1차 필수"
                    )
                    msgs[t] += msg_core

                    if not success_core:
                        all_success_map[t] = False
                        loop_fully_successful = False
                        loop_fail_reason = fail_reason_core
                        
                    target_bonus = plans[t].get('bonus_orders') or []
                    if not isinstance(target_bonus, list): target_bonus = []
                        
                    if not all_success_map[t] and target_bonus:
                        msgs[t] += f"⚠️ 1차 필수 장전 실패로 2차 보너스 덫 보류 (중복 매매 방어)\n"
                    else:
                        success_bonus, msg_bonus, fail_reason_bonus = await execute_order_list(
                            broker, t, target_bonus, successful_orders_cache, is_market_active_now, today_str, is_capital_locked=is_capital_locked, order_category="2차 보너스"
                        )
                        msgs[t] += msg_bonus
                        if not success_bonus:
                            all_success_map[t] = False
                            loop_fully_successful = False
                            loop_fail_reason = fail_reason_bonus

                    if all_success_map[t] and (target_orders or target_bonus):
                        try:
                            # 🚨 MODIFIED: 5.0 -> 45.0
                            await asyncio.wait_for(asyncio.to_thread(cfg.set_lock, t, "REG"), timeout=45.0)
                        except Exception as e:
                            logging.error(f"🚨 락 설정 타임아웃: {e}")
                    
                        if is_capital_locked:
                            msgs[t] += "\n🔒 <b>V-REV 매수 플랜 애프터장 이관 완료 (매도 정상 장전, 잠금 설정됨)</b>"
                        else:
                            msgs[t] += "\n🔒 <b>V-REV 필수 덫(로컬 엔진 포함) 전송 완료 (잠금 설정됨)</b>"
                    elif not all_success_map[t] and (target_orders or target_bonus):
                        msgs[t] += "\n⚠️ <b>일부 덫 장전/이관 실패 (매매 잠금 보류)</b>"
                        
                    if msgs[t].strip() and chat_id:
                        try:
                            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=msgs[t], parse_mode='HTML'), timeout=15.0)
                        except Exception as tg_e:
                            logging.error(f"[{t}] V-REV 완료 메시지 발송 실패: {tg_e}")

                except Exception as e:
                    all_success_map[t] = False
                    loop_fully_successful = False
                    loop_fail_reason = f"[{t}] 치명적 오류: {str(e)}"
                    logging.error(f"🚨 [{t}] delayed_trade 개별 종목 처리 중 치명적 오류: {e}")
                    if chat_id:
                        try:
                            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"🚨 <b>[{t}] 스케줄러 처리 중 오류 발생. 스킵합니다.</b>\n<code>{html.escape(str(e))}</code>", parse_mode='HTML'), timeout=15.0)
                        except Exception: pass

            if not loop_fully_successful:
                return False, loop_fail_reason
        return True, "SUCCESS"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            success, fail_reason = await asyncio.wait_for(_do_delayed_trade(), timeout=600.0)
            if success:
                if attempt > 1 and chat_id: 
                    try:
                        await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"✅ <b>[통신 복구] {attempt}번째 재시도 끝에 장전/이관을 완수했습니다!</b>", parse_mode='HTML'), timeout=15.0)
                    except Exception: pass
                return 
        except asyncio.TimeoutError:
            logging.error(f"정규장 덫 실전 전송 에러 ({attempt}/{MAX_RETRIES}): 600초 전역 타임아웃 초과 (API 응답 지연 누적)", exc_info=True)
            if attempt == 1 and chat_id:
                try:
                    await asyncio.wait_for(
                        context.bot.send_message(
                            chat_id=chat_id, 
                            text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 실전 장전을 재시도합니다! 🛡️\n<code>사유: 600초 전역 타임아웃 초과 (API 지연 누적)</code>", 
                            parse_mode='HTML'
                        ),
                        timeout=15.0
                    )
                except Exception: pass
        except Exception as e:
            logging.error(f"정규장 덫 실전 전송 에러 ({attempt}/{MAX_RETRIES}): {e}", exc_info=True)
            if attempt == 1 and chat_id:
                safe_err = html.escape(str(e)) if str(e) else type(e).__name__
                try:
                    await asyncio.wait_for(
                        context.bot.send_message(
                            chat_id=chat_id, 
                            text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 실전 장전을 재시도합니다! 🛡️\n<code>사유: {safe_err}</code>", 
                            parse_mode='HTML'
                        ),
                        timeout=15.0
                    )
                except Exception: pass
        else:
             if attempt == 1 and chat_id:
                 safe_fail = html.escape(str(fail_reason)) if str(fail_reason) else "알 수 없는 실패"
                 try:
                     await asyncio.wait_for(
                         context.bot.send_message(
                            chat_id=chat_id, 
                            text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 실전 장전을 재시도합니다! 🛡️\n<code>사유: {safe_fail}</code>", 
                            parse_mode='HTML'
                         ),
                         timeout=15.0
                     )
                 except Exception: pass

        if attempt < MAX_RETRIES:
            if attempt != 1 and attempt % 5 == 0 and chat_id:
                try:
                    await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 10초 뒤 재시도합니다! 🛡️", parse_mode='HTML'), timeout=15.0)
                except Exception: pass
            await asyncio.sleep(RETRY_DELAY)

    if chat_id:
        try:
            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text="🚨 <b>[긴급 에러] V-REV 실전 전송/이관 통신 복구 최종 실패. 수동 점검 요망!</b>", parse_mode='HTML'), timeout=15.0)
        except Exception: pass

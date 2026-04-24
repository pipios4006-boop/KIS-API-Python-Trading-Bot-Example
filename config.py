# ==========================================================
# [config.py] - 🌟 100% 통합 완성본 🌟 (Part 1)
# ⚠️ V_REV 도입에 따른 P매매 잔재 완벽 소각 버전
# 💡 [V24.10 수술] 동적 에스크로 락다운 깃발(Flag) 제어 로직 추가
# 💡 [V25.00 수술] AVWAP 하이브리드 전술 상태 저장(캐싱) 파일 경로 및 함수 이식
# 🚨 [V25.19 핫픽스] 빈 장부 스캔 시 IndexError 런타임 붕괴 완벽 방어
# 🚨 [V25.19 핫픽스] 에스크로(Escrow) 3대 관리 함수(set/add/clear) 팩트 기반 완전 구현
# 🚀 [V26.00 승격] 수동 VWAP 시그널 모드(Manual Mode) 독립 플래그 및 캐싱 엔진 신설 탑재
# 🚀 [V26.07 확정 순수익 렌더링 패치] 명예의 전당 및 졸업 카드 발급 시 한투 OpenAPI 왕복 수수료(0.5%) 완벽 차감 이식
# 🚨 [V27.10 그랜드 수술] 에스크로 캐시 영구 박제(Ghost Escrow 방어), 액면분할 수학적 반올림(Banker's Rounding) 오류 교정 및 fsync 무결성 확보
# 🚨 [V27.11 핫픽스] I/O FD 누수 방어, TOCTOU 경쟁 상태 원천 차단 래퍼 추가
# MODIFIED: [V28.25 그랜드 수술] 수수료 하드코딩 전면 소각 및 동적 수수료(Fee) 설정 엔진 탑재
# MODIFIED: [V28.26 타임존 락온 그랜드 수술] KST 기준 날짜 연산을 전면 폐기하고,
# INIT 레코드 기록 및 락(Lock) 해제 등 모든 기준 시간을 EST(미국 동부)로 100% 형변환하여 
# 타임 패러독스로 인한 스냅샷 매핑 실패 버그를 영구 소각 완료. (EC-3 방어)
# 🚨 [V28.50 NEW] 사용자 맞춤형 AVWAP 암살자 조기 퇴근 설정(Early Exit/Target) 저장소 완비
# MODIFIED: [V29.16 핫픽스] 마스터 스위치 및 스나이퍼 락온(Buy/Sell) 영속성 Getter/Setter 팩트 이식 완료
# MODIFIED: [V30.09 핫픽스] pytz 영구 적출 및 ZoneInfo 도입으로 LMT 버그 차단 및 타임존 무결성 100% 확보
# ==========================================================
import json
import os
import datetime
# MODIFIED: [V30.09 핫픽스] LMT 오차 방어를 위해 pytz 적출 및 ZoneInfo 도입
# import pytz
from zoneinfo import ZoneInfo
import math
import time
import shutil
import tempfile
import pandas_market_calendars as mcal

# NEW: 다중 스레드/프로세스 환경에서 락 및 에스크로 동기화 제어를 위한 모듈 임포트
import threading
try:
    import fcntl
except ImportError:
    fcntl = None

try:
    from version_history import VERSION_HISTORY
except ImportError:
    VERSION_HISTORY = ["V14.x [-] 버전 기록 파일(version_history.py)을 찾을 수 없습니다."]

class ConfigManager:
    def __init__(self):
        self.FILES = {
            "TOKEN": "data/token.dat",
            "CHAT_ID": "data/chat_id.dat",
            "LEDGER": "data/manual_ledger.json",    
            "HISTORY": "data/manual_history.json",  
            "SPLIT": "data/split_config.json",
            "TICKER": "data/active_tickers.json",
            "UPWARD_SNIPER": "data/upward_sniper.json", 
            "SECRET_MODE": "data/secret_mode.dat",
            "PROFIT_CFG": "data/profit_config.json",
            "LOCKS": "data/trade_locks.json",
            "SEED_CFG": "data/seed_config.json",         
            "COMPOUND_CFG": "data/compound_config.json",
            "VERSION_CFG": "data/version_config.json",
            "REVERSE_CFG": "data/reverse_config.json",
            "SNIPER_MULTIPLIER_CFG": "data/sniper_multiplier.json",
            "SPLIT_HISTORY": "data/split_history.json",
            "AVWAP_HYBRID_CFG": "data/avwap_hybrid.json",
            "MANUAL_VWAP_CFG": "data/manual_vwap_config.json",
            "FEE_CFG": "data/fee_config.json", 
            "AVWAP_EARLY_EXIT_CFG": "data/avwap_early_exit.json",    
            "AVWAP_EARLY_TARGET_CFG": "data/avwap_early_target.json",
            # 🚨 [V29.16 수술 부위] 누락된 스나이퍼 락 및 스위치 파일 경로 이식
            "MASTER_SWITCH": "data/master_switch.json",
            "SNIPER_BUY_LOCKED": "data/sniper_buy_locked.json",
            "SNIPER_SELL_LOCKED": "data/sniper_sell_locked.json"
        }
        
        self.DEFAULT_SEED = {"SOXL": 6720.0, "TQQQ": 6720.0}
        self.DEFAULT_SPLIT = {"SOXL": 40.0, "TQQQ": 40.0}
        self.DEFAULT_TARGET = {"SOXL": 12.0, "TQQQ": 10.0}
        self.DEFAULT_VERSION = {"SOXL": "V14", "TQQQ": "V14"}
        self.DEFAULT_COMPOUND = {"SOXL": 70.0, "TQQQ": 70.0}
        self.DEFAULT_SNIPER_MULTIPLIER = {"SOXL": 1.0, "TQQQ": 0.9}
        self.DEFAULT_FEE = {"SOXL": 0.25, "TQQQ": 0.25} 
        
        self._escrow_cache = {}
        self._locks_mutex = threading.Lock()

    def _atomic_update_locks(self, update_fn):
        with self._locks_mutex:
            lock_file_path = self.FILES["LOCKS"]
            dir_name = os.path.dirname(lock_file_path) or '.'
            if not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
                
            sentinel = lock_file_path + ".lock"
            with open(sentinel, 'w') as lf:
                if fcntl:
                    fcntl.flock(lf, fcntl.LOCK_EX)
                try:
                    locks = self._load_json(lock_file_path, {})
                    update_fn(locks)
                    self._save_json(lock_file_path, locks)
                finally:
                    if fcntl:
                        fcntl.flock(lf, fcntl.LOCK_UN)

    def _load_json(self, filename, default=None):
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️ [Config] JSON 로드 에러 ({filename}): {e}")
                try:
                    shutil.copy(filename, filename + f".bak_{int(time.time())}")
                except Exception as backup_e:
                    print(f"⚠️ [Config] 백업 실패: {backup_e}")
                return default if default is not None else {}
        return default if default is not None else {}

    def _save_json(self, filename, data):
        fd = None
        temp_path = None
        try:
            dir_name = os.path.dirname(filename) or '.'
            if not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
                
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                fd = None
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush()         
                os.fsync(f.fileno()) 
                
            os.replace(temp_path, filename)
            temp_path = None
        except Exception as e:
            print(f"❌ [Config] JSON 저장 중 치명적 에러 발생 ({filename}): {e}")
            if fd is not None:
                try: os.close(fd)
                except OSError: pass
            if temp_path and os.path.exists(temp_path):
                try: os.remove(temp_path)
                except Exception: pass

    def _load_file(self, filename, default=None):
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    return f.read().strip()
            except Exception as e:
                print(f"⚠️ [Config] 파일 로드 에러 ({filename}): {e}")
        return default

    def _save_file(self, filename, content):
        fd = None
        temp_path = None
        try:
            dir_name = os.path.dirname(filename) or '.'
            if not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
                
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                fd = None
                f.write(str(content))
                f.flush()
                os.fsync(f.fileno()) 
            os.replace(temp_path, filename)
            temp_path = None
        except Exception as e:
            print(f"❌ [Config] 텍스트 파일 저장 에러 ({filename}): {e}")
            if fd is not None:
                try: os.close(fd)
                except OSError: pass
            if temp_path and os.path.exists(temp_path):
                try: os.remove(temp_path)
                except Exception: pass

    def get_last_split_date(self, ticker):
        return self._load_json(self.FILES["SPLIT_HISTORY"], {}).get(ticker, "")

    def set_last_split_date(self, ticker, date_str):
        d = self._load_json(self.FILES["SPLIT_HISTORY"], {})
        d[ticker] = date_str
        self._save_json(self.FILES["SPLIT_HISTORY"], d)

    def get_ledger(self):
        return self._load_json(self.FILES["LEDGER"], [])

    def get_escrow_cash(self, ticker):
        locks = self._load_json(self.FILES["LOCKS"], {})
        persistent_escrow = locks.get(f"ESCROW_{ticker}", None)
        
        if persistent_escrow is not None:
            return max(0.0, float(persistent_escrow))

        ledger = self.get_ledger()
        escrow = 0.0
        for r in reversed(ledger):
            if r.get('ticker') == ticker:
                if r.get('is_reverse', False):
                    if r['side'] == 'SELL':
                        escrow += (r['qty'] * r['price'])
                    elif r['side'] == 'BUY':
                        escrow -= (r['qty'] * r['price'])
                else:
                    break
        return max(0.0, float(escrow))

    def set_escrow_cash(self, ticker, amount):
        validated = max(0.0, float(amount))
        def _update(locks):
            locks[f"ESCROW_{ticker}"] = validated
        self._atomic_update_locks(_update)

    def add_escrow_cash(self, ticker, amount):
        def _update(locks):
            current = locks.get(f"ESCROW_{ticker}", 0.0)
            locks[f"ESCROW_{ticker}"] = max(0.0, current + float(amount))
        self._atomic_update_locks(_update)

    def clear_escrow_cash(self, ticker):
        def _update(locks):
            if f"ESCROW_{ticker}" in locks:
                del locks[f"ESCROW_{ticker}"]
        self._atomic_update_locks(_update)

    def get_total_locked_cash(self, exclude_ticker=None):
        total = 0.0
        try:
            tickers = self.get_active_tickers()
            for t in tickers:
                if t != exclude_ticker:
                    rev_state = self.get_reverse_state(t).get("is_active", False)
                    if rev_state:
                        total += self.get_escrow_cash(t)
        except Exception:
            pass
        return total

    def get_order_locked(self, ticker):
        locks = self._load_json(self.FILES["LOCKS"], {})
        return locks.get(f"ORDER_LOCKED_{ticker}", False)

    def set_order_locked(self, ticker, is_locked):
        def _update(locks):
            if is_locked:
                locks[f"ORDER_LOCKED_{ticker}"] = True
            else:
                if f"ORDER_LOCKED_{ticker}" in locks:
                    del locks[f"ORDER_LOCKED_{ticker}"]
        self._atomic_update_locks(_update)

    def set_lock(self, ticker, market_type):
        # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo 이식
        est = ZoneInfo('America/New_York')
        today = datetime.datetime.now(est).strftime('%Y-%m-%d')
        def _update(locks):
            locks[f"{today}_{ticker}_{market_type}"] = True
        self._atomic_update_locks(_update)

    def reset_locks(self):
        def _update(locks):
            keys_to_keep = [k for k in locks.keys() if k.startswith("ESCROW_") or k.startswith("ORDER_LOCKED_")]
            surviving_locks = {k: locks[k] for k in keys_to_keep}
            locks.clear()
            locks.update(surviving_locks)
        self._atomic_update_locks(_update)
        
    def reset_lock_for_ticker(self, ticker):
        # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo 이식
        est = ZoneInfo('America/New_York')
        today = datetime.datetime.now(est).strftime('%Y-%m-%d')
        def _update(locks):
            keys_to_delete = [k for k in locks.keys() if k.startswith(f"{today}_{ticker}")]
            for k in keys_to_delete:
                del locks[k]
        self._atomic_update_locks(_update)

    def check_lock(self, ticker, market_type):
        # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo 이식
        est = ZoneInfo('America/New_York')
        today = datetime.datetime.now(est).strftime('%Y-%m-%d')
        locks = self._load_json(self.FILES["LOCKS"], {})
        return locks.get(f"{today}_{ticker}_{market_type}", False)

    def get_absolute_t_val(self, ticker, actual_qty, actual_avg_price):
        seed = self.get_seed(ticker)
        split = self.get_split_count(ticker)
        one_portion = seed / split if split > 0 else 1
        t_val = (actual_qty * actual_avg_price) / one_portion if one_portion > 0 else 0.0
        return round(t_val, 4), one_portion

    def apply_stock_split(self, ticker, ratio):
        if ratio <= 0: return
        ledger = self.get_ledger()
        changed = False
        for r in ledger:
            if r.get('ticker') == ticker:
                raw_new_qty = r['qty'] * ratio
                new_qty = math.floor(raw_new_qty + 0.5)
                r['qty'] = new_qty if new_qty > 0 else (1 if r['qty'] > 0 else 0)
                r['price'] = round(r['price'] / ratio, 4)
                if 'avg_price' in r:
                    r['avg_price'] = round(r['avg_price'] / ratio, 4)
                changed = True
        if changed:
            self._save_json(self.FILES["LEDGER"], ledger)

    def overwrite_genesis_ledger(self, ticker, genesis_records, actual_avg):
        ledger = self.get_ledger()
        target_recs = [r for r in ledger if r['ticker'] == ticker]
        
        if len(target_recs) > 0:
            print(f"⚠️ [보안 차단] {ticker}의 장부 기록이 이미 존재하여 파괴적 Genesis 덮어쓰기를 차단했습니다.")
            return

        max_id = max([r.get('id', 0) for r in ledger] + [0])
        for i, rec in enumerate(genesis_records):
            max_id += 1
            ledger.append({
                "id": max_id,
                "date": rec['date'],
                "ticker": ticker,
                "side": rec['side'],
                "price": rec['price'],
                "qty": rec['qty'],
                "avg_price": actual_avg, 
                "exec_id": f"GENESIS_{int(time.time())}_{i}",
                "desc": "✨과거기록복원",
                "is_reverse": False 
            })
        self._save_json(self.FILES["LEDGER"], ledger)

    def overwrite_incremental_ledger(self, ticker, temp_recs, new_today_records):
        ledger = self.get_ledger()
        remaining = [r for r in ledger if r['ticker'] != ticker]
        updated_ticker_recs = list(temp_recs)
        
        current_rev_state = self.get_reverse_state(ticker).get("is_active", False)
        max_id = max([r.get('id', 0) for r in ledger] + [0])
        
        for i, rec in enumerate(new_today_records):
            max_id += 1
            new_row = {
                "id": max_id,
                "date": rec['date'],
                "ticker": ticker,
                "side": rec['side'],
                "price": rec['price'],
                "qty": rec['qty'],
                "avg_price": rec['avg_price'],
                "exec_id": rec.get("exec_id", f"FASTTRACK_{int(time.time())}_{i}"),
                "is_reverse": current_rev_state
            }
            if "desc" in rec:
                new_row["desc"] = rec["desc"]
                
            updated_ticker_recs.append(new_row)
            
        remaining.extend(updated_ticker_recs)
        self._save_json(self.FILES["LEDGER"], remaining)

    def overwrite_ledger(self, ticker, actual_qty, actual_avg):
        ledger = self.get_ledger()
        target_recs = [r for r in ledger if r['ticker'] == ticker]
        
        if len(target_recs) > 0:
            print(f"⚠️ [보안 차단] {ticker}의 장부 기록이 이미 존재하여 파괴적 INIT 덮어쓰기를 차단했습니다.")
            return
            
        # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo 이식
        est = ZoneInfo('America/New_York')
        today_str = datetime.datetime.now(est).strftime('%Y-%m-%d')
        new_id = 1 if not ledger else max(r.get('id', 0) for r in ledger) + 1
        
        ledger.append({
            "id": new_id, "date": today_str, "ticker": ticker, "side": "BUY",
            "price": actual_avg, "qty": actual_qty, "avg_price": actual_avg, 
            "exec_id": f"INIT_{int(time.time())}", "desc": "✨최초스냅샷", "is_reverse": False
        })
        self._save_json(self.FILES["LEDGER"], ledger)

    def calibrate_avg_price(self, ticker, actual_avg):
        ledger = self.get_ledger()
        target_recs = [r for r in ledger if r['ticker'] == ticker]
        if target_recs:
            for r in target_recs:
                r['avg_price'] = actual_avg
            self._save_json(self.FILES["LEDGER"], ledger)

    def calibrate_ledger_prices(self, ticker, target_date_str, exec_history):
        if not exec_history:
            return 0
            
        buy_qty = 0
        buy_amt = 0.0
        sell_qty = 0
        sell_amt = 0.0
        
        for ex in exec_history:
            side_cd = ex.get('sll_buy_dvsn_cd')
            qty = int(float(ex.get('ft_ccld_qty', '0')))
            price = float(ex.get('ft_ccld_unpr3', '0'))
            
            if qty > 0 and price > 0:
                if side_cd == "02": 
                    buy_qty += qty
                    buy_amt += (qty * price)
                elif side_cd == "01": 
                    sell_qty += qty
                    sell_amt += (qty * price)
                    
        actual_buy_price = round(buy_amt / buy_qty, 4) if buy_qty > 0 else 0.0
        actual_sell_price = round(sell_amt / sell_qty, 4) if sell_qty > 0 else 0.0
        
        if actual_buy_price == 0.0 and actual_sell_price == 0.0:
            return 0
            
        ledger = self.get_ledger()
        changed_count = 0
        
        for r in ledger:
            if r.get('ticker') == ticker and r.get('date') == target_date_str:
                exec_id = str(r.get('exec_id', ''))
                if 'INIT' in exec_id:
                    continue
                    
                if r['side'] == 'BUY' and actual_buy_price > 0.0:
                    if abs(r['price'] - actual_buy_price) >= 0.01:
                        r['price'] = actual_buy_price
                        changed_count += 1
                elif r['side'] == 'SELL' and actual_sell_price > 0.0:
                    if abs(r['price'] - actual_sell_price) >= 0.01:
                        r['price'] = actual_sell_price
                        changed_count += 1
                        
        if changed_count > 0:
            self._save_json(self.FILES["LEDGER"], ledger)
            
        return changed_count

    def clear_ledger_for_ticker(self, ticker):
        ledger = self.get_ledger()
        remaining = [r for r in ledger if r['ticker'] != ticker]
        self._save_json(self.FILES["LEDGER"], remaining)
        self.set_reverse_state(ticker, False, 0, 0.0)
        self.clear_escrow_cash(ticker)

    def calculate_holdings(self, ticker, records=None):
        if records is None:
            records = self.get_ledger()
        target_recs = [r for r in records if r['ticker'] == ticker]
        total_qty, total_invested, total_sold = 0, 0.0, 0.0    
        
        running_qty = 0
        running_cost = 0.0

        for r in target_recs:
            if r['side'] == 'BUY':
                total_qty += r['qty']
                total_invested += (r['price'] * r['qty'])
                running_qty += r['qty']
                running_cost += (r['price'] * r['qty'])
            elif r['side'] == 'SELL':
                total_qty -= r['qty']
                total_sold += (r['price'] * r['qty'])
                if running_qty > 0:
                    cost_per_share = running_cost / running_qty
                    running_cost -= cost_per_share * min(r['qty'], running_qty)
                    running_qty = max(0, running_qty - r['qty'])
        
        total_qty = max(0, int(total_qty))
        invested_up = math.ceil(total_invested * 100) / 100.0
        sold_up = math.ceil(total_sold * 100) / 100.0
        
        avg_price = 0.0
        if total_qty > 0 and target_recs:
            avg_price = float(target_recs[-1].get('avg_price', 0.0))
            if avg_price == 0.0:
                avg_price = (running_cost / running_qty) if running_qty > 0 else 0.0
        
        return total_qty, avg_price, invested_up, sold_up

    def get_reverse_state(self, ticker):
        d = self._load_json(self.FILES["REVERSE_CFG"], {})
        return d.get(ticker, {"is_active": False, "day_count": 0, "exit_target": 0.0, "last_update_date": ""})

    def set_reverse_state(self, ticker, is_active, day_count, exit_target=0.0, last_update_date=None):
        if last_update_date is None:
            # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo 이식
            est = ZoneInfo('America/New_York')
            last_update_date = datetime.datetime.now(est).strftime('%Y-%m-%d')
            
        d = self._load_json(self.FILES["REVERSE_CFG"], {})
        d[ticker] = {"is_active": is_active, "day_count": day_count, "exit_target": exit_target, "last_update_date": last_update_date}
        self._save_json(self.FILES["REVERSE_CFG"], d)

    def update_reverse_day_if_needed(self, ticker):
        pass

    def increment_reverse_day(self, ticker):
        state = self.get_reverse_state(ticker)
        if state.get("is_active"):
            # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo 이식
            est = ZoneInfo('America/New_York')
            now_est = datetime.datetime.now(est)
            today_est_str = now_est.strftime('%Y-%m-%d')
            
            if state.get("last_update_date") != today_est_str:
                is_trading_day = False
                try:
                    nyse = mcal.get_calendar('NYSE')
                    schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
                    is_trading_day = not schedule.empty
                except Exception as e:
                    print(f"⚠️ [Config] 달력 라이브러리 에러 발생. 평일 강제 개장 처리합니다: {e}")
                    is_trading_day = now_est.weekday() < 5
                
                if is_trading_day:
                    new_day = state.get("day_count", 0) + 1
                    self.set_reverse_state(ticker, True, new_day, state.get("exit_target", 0.0), today_est_str)
                    return True
                else:
                    self.set_reverse_state(ticker, True, state.get("day_count", 0), state.get("exit_target", 0.0), today_est_str)
                    return False
        return False

    def calculate_v14_state(self, ticker):
        ledger = self.get_ledger()
        target_recs = sorted([r for r in ledger if r['ticker'] == ticker], key=lambda x: x.get('id', 0))
        
        seed = self.get_seed(ticker)
        split = self.get_split_count(ticker)
        base_portion = seed / split if split > 0 else 1
        
        holdings = 0
        rem_cash = seed
        total_invested = 0.0
        
        for r in target_recs:
            if holdings == 0:
                rem_cash = seed
                total_invested = 0.0
                
            qty = r['qty']
            amt = qty * r['price']
            
            if r['side'] == 'BUY':
                rem_cash -= amt
                holdings += qty
                total_invested += amt
                
            elif r['side'] == 'SELL':
                if qty >= holdings: 
                    holdings = 0
                    rem_cash = seed
                    total_invested = 0.0
                else: 
                    if holdings > 0:
                        avg_price = total_invested / holdings
                        total_invested -= (qty * avg_price)
                    holdings -= qty
                    rem_cash += amt
                    
        avg_price = total_invested / holdings if holdings > 0 else 0.0
        t_val = (holdings * avg_price) / base_portion if base_portion > 0 else 0.0
            
        if holdings > 0:
            safe_denom = max(1.0, split - t_val)
            current_budget = rem_cash / safe_denom
        else:
            current_budget = base_portion
            t_val = 0.0
            
        return max(0.0, round(t_val, 4)), max(0.0, current_budget), max(0.0, rem_cash)

    def archive_graduation(self, ticker, end_date, prev_close=0.0):
        ledger = self.get_ledger()
        target_recs = [r for r in ledger if r['ticker'] == ticker]
        if not target_recs:
            return None, 0
        
        ledger_qty, avg_price, _, _ = self.calculate_holdings(ticker, target_recs)
        
        raw_total_buy = sum(r['price']*r['qty'] for r in target_recs if r['side']=='BUY')
        raw_total_sell = sum(r['price']*r['qty'] for r in target_recs if r['side']=='SELL')

        if ledger_qty > 0:
            split = self.get_split_count(ticker)
            is_reverse = self.get_reverse_state(ticker).get("is_active", False)

            if is_reverse:
                divisor = 10 if split <= 20 else 20
                loc_qty = math.floor(ledger_qty / divisor)
            else:
                loc_qty = math.ceil(ledger_qty / 4)

            limit_qty = ledger_qty - loc_qty
            if limit_qty < 0: 
                loc_qty = ledger_qty
                limit_qty = 0

            target_ratio = self.get_target_profit(ticker) / 100.0
            target_price = math.ceil(avg_price * (1 + target_ratio) * 100) / 100.0
            loc_price = prev_close if prev_close > 0 else avg_price

            new_id = max((r.get('id', 0) for r in ledger), default=0) + 1

            if loc_qty > 0:
                rec_loc = {"id": new_id, "date": end_date, "ticker": ticker, "side": "SELL", "price": loc_price, "qty": loc_qty, "avg_price": avg_price, "exec_id": f"GRAD_LOC_{int(time.time())}", "is_reverse": is_reverse}
                ledger.append(rec_loc)
                target_recs.append(rec_loc)
                new_id += 1

            if limit_qty > 0:
                rec_limit = {"id": new_id, "date": end_date, "ticker": ticker, "side": "SELL", "price": target_price, "qty": limit_qty, "avg_price": avg_price, "exec_id": f"GRAD_LMT_{int(time.time())}", "is_reverse": is_reverse}
                ledger.append(rec_limit)
                target_recs.append(rec_limit)

            self._save_json(self.FILES["LEDGER"], ledger)

        fee_rate = self.get_fee(ticker) / 100.0
        net_invested = raw_total_buy * (1.0 + fee_rate)
        net_revenue = raw_total_sell * (1.0 - fee_rate)
        
        profit = math.ceil((net_revenue - net_invested) * 100) / 100.0
        yield_pct = math.ceil(((profit / net_invested * 100) if net_invested > 0 else 0.0) * 100) / 100.0
        
        compound_rate = self.get_compound_rate(ticker) / 100.0
        added_seed = 0
        if profit > 0 and compound_rate > 0:
            added_seed = math.floor(profit * compound_rate)
            current_seed = self.get_seed(ticker)
            self.set_seed(ticker, current_seed + added_seed)

        history = self._load_json(self.FILES["HISTORY"], [])
        new_hist = {
            "id": len(history) + 1, "ticker": ticker, "end_date": end_date,
            "profit": profit, "yield": yield_pct, "revenue": net_revenue, "invested": net_invested, "trades": target_recs
        }
        history.append(new_hist)
        self._save_json(self.FILES["HISTORY"], history)
        
        self.clear_ledger_for_ticker(ticker)
        
        return new_hist, added_seed

    def get_full_version_history(self):
        return VERSION_HISTORY

    def get_version_history(self):
        return VERSION_HISTORY

    def get_latest_version(self):
        history = self.get_version_history()
        if history and len(history) > 0:
            latest_entry = history[-1]
            if isinstance(latest_entry, dict):
                return latest_entry.get("version", "V14.x")
            elif isinstance(latest_entry, str):
                return latest_entry.split(' ')[0] 
        return "V14.x"

    def get_history(self):
        return self._load_json(self.FILES["HISTORY"], [])

    def get_seed(self, t): return float(self._load_json(self.FILES["SEED_CFG"], self.DEFAULT_SEED).get(t, 6720.0))
    def set_seed(self, t, v): 
        d = self._load_json(self.FILES["SEED_CFG"], self.DEFAULT_SEED)
        d[t] = v
        self._save_json(self.FILES["SEED_CFG"], d)

    def get_compound_rate(self, t): return float(self._load_json(self.FILES["COMPOUND_CFG"], self.DEFAULT_COMPOUND).get(t, 70.0))
    def set_compound_rate(self, t, v):
        d = self._load_json(self.FILES["COMPOUND_CFG"], self.DEFAULT_COMPOUND)
        d[t] = v
        self._save_json(self.FILES["COMPOUND_CFG"], d)

    def get_version(self, t): return self._load_json(self.FILES["VERSION_CFG"], self.DEFAULT_VERSION).get(t, "V14")
    def set_version(self, t, v):
        d = self._load_json(self.FILES["VERSION_CFG"], self.DEFAULT_VERSION)
        d[t] = v
        self._save_json(self.FILES["VERSION_CFG"], d)

    def get_split_count(self, t): return self._load_json(self.FILES["SPLIT"], self.DEFAULT_SPLIT).get(t, 40.0)
    def get_target_profit(self, t): return self._load_json(self.FILES["PROFIT_CFG"], self.DEFAULT_TARGET).get(t, 10.0)
        
    def get_fee(self, t): 
        return float(self._load_json(self.FILES["FEE_CFG"], self.DEFAULT_FEE).get(t, 0.25))
    def set_fee(self, t, v):
        d = self._load_json(self.FILES["FEE_CFG"], self.DEFAULT_FEE)
        d[t] = float(v)
        self._save_json(self.FILES["FEE_CFG"], d)

    def get_sniper_multiplier(self, t):
        default_val = self.DEFAULT_SNIPER_MULTIPLIER.get(t, 1.0)
        return float(self._load_json(self.FILES["SNIPER_MULTIPLIER_CFG"], self.DEFAULT_SNIPER_MULTIPLIER).get(t, default_val))
        
    def set_sniper_multiplier(self, t, v):
        d = self._load_json(self.FILES["SNIPER_MULTIPLIER_CFG"], self.DEFAULT_SNIPER_MULTIPLIER)
        d[t] = float(v)
        self._save_json(self.FILES["SNIPER_MULTIPLIER_CFG"], d)

    def get_upward_sniper_mode(self, ticker): return self._load_json(self.FILES["UPWARD_SNIPER"], {}).get(ticker, False)
    def set_upward_sniper_mode(self, ticker, v):
        d = self._load_json(self.FILES["UPWARD_SNIPER"], {})
        d[ticker] = bool(v)
        self._save_json(self.FILES["UPWARD_SNIPER"], d)

    def get_avwap_hybrid_mode(self, ticker): return self._load_json(self.FILES["AVWAP_HYBRID_CFG"], {}).get(ticker, False)
    def set_avwap_hybrid_mode(self, ticker, v):
        d = self._load_json(self.FILES["AVWAP_HYBRID_CFG"], {})
        d[ticker] = bool(v)
        self._save_json(self.FILES["AVWAP_HYBRID_CFG"], d)

    def get_manual_vwap_mode(self, ticker): return self._load_json(self.FILES["MANUAL_VWAP_CFG"], {}).get(ticker, False)
    def set_manual_vwap_mode(self, ticker, v):
        d = self._load_json(self.FILES["MANUAL_VWAP_CFG"], {})
        d[ticker] = bool(v)
        self._save_json(self.FILES["MANUAL_VWAP_CFG"], d)

    # ==========================================================
    # 🚨 [V28.50 NEW] AVWAP 암살자 조기 퇴근 듀얼 코어 Getter/Setter
    # ==========================================================
    def get_avwap_early_exit_mode(self, ticker): 
        return self._load_json(self.FILES["AVWAP_EARLY_EXIT_CFG"], {}).get(ticker, False)
        
    def set_avwap_early_exit_mode(self, ticker, v):
        d = self._load_json(self.FILES["AVWAP_EARLY_EXIT_CFG"], {})
        d[ticker] = bool(v)
        self._save_json(self.FILES["AVWAP_EARLY_EXIT_CFG"], d)

    def get_avwap_early_target(self, ticker): 
        # 기본값 2.5(%) 로 설정
        return float(self._load_json(self.FILES["AVWAP_EARLY_TARGET_CFG"], {}).get(ticker, 2.5))
        
    def set_avwap_early_target(self, ticker, v):
        d = self._load_json(self.FILES["AVWAP_EARLY_TARGET_CFG"], {})
        d[ticker] = float(v)
        self._save_json(self.FILES["AVWAP_EARLY_TARGET_CFG"], d)

    # ==========================================================
    # 🚨 [V29.16 수술] 스나이퍼 락온(Lock-on) 및 마스터 스위치 영속성 모듈 이식
    # ==========================================================
    def get_master_switch(self, ticker): 
        return self._load_json(self.FILES["MASTER_SWITCH"], {}).get(ticker, "ALL")
        
    def set_master_switch(self, ticker, v):
        d = self._load_json(self.FILES["MASTER_SWITCH"], {})
        d[ticker] = str(v)
        self._save_json(self.FILES["MASTER_SWITCH"], d)

    def get_sniper_buy_locked(self, ticker): 
        return self._load_json(self.FILES["SNIPER_BUY_LOCKED"], {}).get(ticker, False)
        
    def set_sniper_buy_locked(self, ticker, v):
        d = self._load_json(self.FILES["SNIPER_BUY_LOCKED"], {})
        d[ticker] = bool(v)
        self._save_json(self.FILES["SNIPER_BUY_LOCKED"], d)

    def get_sniper_sell_locked(self, ticker): 
        return self._load_json(self.FILES["SNIPER_SELL_LOCKED"], {}).get(ticker, False)
        
    def set_sniper_sell_locked(self, ticker, v):
        d = self._load_json(self.FILES["SNIPER_SELL_LOCKED"], {})
        d[ticker] = bool(v)
        self._save_json(self.FILES["SNIPER_SELL_LOCKED"], d)

    # ==========================================================
    def get_secret_mode(self): return self._load_file(self.FILES["SECRET_MODE"]) == 'True'
    def set_secret_mode(self, v): self._save_file(self.FILES["SECRET_MODE"], str(v))
    def get_active_tickers(self): return self._load_json(self.FILES["TICKER"], ["SOXL", "TQQQ"])
    def set_active_tickers(self, v): self._save_json(self.FILES["TICKER"], v)
    def get_chat_id(self): 
        v = self._load_file(self.FILES["CHAT_ID"])
        return int(v) if v else None
    def set_chat_id(self, v): self._save_file(self.FILES["CHAT_ID"], v)

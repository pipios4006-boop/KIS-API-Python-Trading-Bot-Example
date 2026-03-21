import json
import os
import datetime
import pytz
import math
import time
import shutil
import tempfile
import pandas_market_calendars as mcal

try:
    from version_history import VERSION_HISTORY
except ImportError:
    VERSION_HISTORY = ["V14.x [-] 버전 기록 파일(version_history.py)을 찾을 수 없습니다."]

try:
    from version_archive import VERSION_ARCHIVE
except ImportError:
    VERSION_ARCHIVE = []

# 🦇 [V19.10] 버전 추가 시 중복 방어
_NEW_VERSION = "V19.10 [2026.03.21] 시스템 방탄 아키텍처 고도화 및 스나이퍼 로직 개조: JSON 원자적 쓰기(Atomic Write) 도입으로 비정상 종료 시 데이터 손실 원천 차단, 정규장 및 스나이퍼 예산 배분 로직 일치화, 구버전 파이썬 f-string 크래시 방어, 동시성 락(Race Condition) 해결, 타임존 경계 오류 수정, 거래소 하드코딩 제거. 스나이퍼 절대 기준을 '평단가'로 변경하여 공격적인 시장 폭락 줍줍 기회 창출 (수정: config, main, telegram_bot, broker, strategy)"

if _NEW_VERSION not in VERSION_HISTORY:
    VERSION_HISTORY.append(_NEW_VERSION)

class ConfigManager:
    def __init__(self):
        self.FILES = {
            "TOKEN": "data/token.dat",
            "CHAT_ID": "data/chat_id.dat",
            "LEDGER": "data/manual_ledger.json",    
            "HISTORY": "data/manual_history.json",  
            "SPLIT": "data/split_config.json",
            "TICKER": "data/active_tickers.json",
            "TURBO": "data/turbo_mode.dat",
            "PROFIT_CFG": "data/profit_config.json",
            "LOCKS": "data/trade_locks.json",
            "SEED_CFG": "data/seed_config.json",         
            "COMPOUND_CFG": "data/compound_config.json",
            "VERSION_CFG": "data/version_config.json",
            "REVERSE_CFG": "data/reverse_config.json",
            "BB_LOWER": "data/bb_lower.json",
            "SNIPER_CFG": "data/sniper_config.json",
            "SPLIT_HISTORY": "data/split_history.json" 
        }
        
        self.DEFAULT_SEED = {"SOXL": 6720.0, "TQQQ": 6720.0}
        self.DEFAULT_SPLIT = {"SOXL": 40.0, "TQQQ": 40.0}
        self.DEFAULT_TARGET = {"SOXL": 12.0, "TQQQ": 10.0}
        self.DEFAULT_COMPOUND = {"SOXL": 70.0, "TQQQ": 70.0}
        self.DEFAULT_VERSION = {"SOXL": "V14", "TQQQ": "V14"}
        self.DEFAULT_SNIPER = {"SOXL": 9.0, "TQQQ": 5.0}

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

    # 🦇 [V19.10] JSON 원자적 쓰기(Atomic Write) 도입: 정전 등 비정상 종료 시 파일 파괴(0 Byte) 원천 차단
    def _save_json(self, filename, data):
        try:
            dir_name = os.path.dirname(filename)
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
                
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
            os.replace(temp_path, filename)
        except Exception as e:
            print(f"❌ [Config] JSON 저장 중 치명적 에러 발생 ({filename}): {e}")
            if 'temp_path' in locals() and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass

    def _load_file(self, filename, default=None):
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    return f.read().strip()
            except Exception as e:
                print(f"⚠️ [Config] 파일 로드 에러 ({filename}): {e}")
        return default

    def _save_file(self, filename, content):
        try:
            dir_name = os.path.dirname(filename)
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
                
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                f.write(str(content))
            os.replace(temp_path, filename)
        except Exception as e:
            print(f"❌ [Config] 텍스트 파일 저장 에러 ({filename}): {e}")

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
        return float(locks.get(f"ESCROW_{ticker}", 0.0))

    def set_escrow_cash(self, ticker, amount):
        locks = self._load_json(self.FILES["LOCKS"], {})
        locks[f"ESCROW_{ticker}"] = float(amount)
        self._save_json(self.FILES["LOCKS"], locks)

    def add_escrow_cash(self, ticker, amount):
        current = self.get_escrow_cash(ticker)
        self.set_escrow_cash(ticker, current + float(amount))

    def clear_escrow_cash(self, ticker):
        locks = self._load_json(self.FILES["LOCKS"], {})
        if f"ESCROW_{ticker}" in locks:
            del locks[f"ESCROW_{ticker}"]
            self._save_json(self.FILES["LOCKS"], locks)

    def get_total_locked_cash(self, exclude_ticker=None):
        locks = self._load_json(self.FILES["LOCKS"], {})
        total = 0.0
        for k, v in locks.items():
            if k.startswith("ESCROW_"):
                ticker_in_lock = k.replace("ESCROW_", "")
                if ticker_in_lock != exclude_ticker:
                    total += float(v)
        return total

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
                new_qty = int(r['qty'] * ratio)
                r['qty'] = new_qty if new_qty > 0 else (1 if r['qty'] > 0 else 0)
                r['price'] = round(r['price'] / ratio, 4)
                if 'avg_price' in r:
                    r['avg_price'] = round(r['avg_price'] / ratio, 4)
                changed = True
        if changed:
            self._save_json(self.FILES["LEDGER"], ledger)

    def overwrite_genesis_ledger(self, ticker, genesis_records, actual_avg):
        ledger = self.get_ledger()
        remaining = [r for r in ledger if r['ticker'] != ticker]
        
        for i, rec in enumerate(genesis_records):
            remaining.append({
                "id": i + 1,
                "date": rec['date'],
                "ticker": ticker,
                "side": rec['side'],
                "price": rec['price'],
                "qty": rec['qty'],
                "avg_price": actual_avg, 
                "exec_id": f"GENESIS_{int(time.time())}_{i}",
                "is_reverse": False 
            })
        self._save_json(self.FILES["LEDGER"], remaining)

    def overwrite_incremental_ledger(self, ticker, temp_recs, new_today_records):
        ledger = self.get_ledger()
        remaining = [r for r in ledger if r['ticker'] != ticker]
        updated_ticker_recs = list(temp_recs)
        
        current_rev_state = self.get_reverse_state(ticker).get("is_active", False)
        max_id = max([r.get('id', 0) for r in ledger] + [0])
        
        for i, rec in enumerate(new_today_records):
            max_id += 1
            updated_ticker_recs.append({
                "id": max_id,
                "date": rec['date'],
                "ticker": ticker,
                "side": rec['side'],
                "price": rec['price'],
                "qty": rec['qty'],
                "avg_price": rec['avg_price'],
                "exec_id": f"FASTTRACK_{int(time.time())}_{i}",
                "is_reverse": current_rev_state
            })
            
        remaining.extend(updated_ticker_recs)
        self._save_json(self.FILES["LEDGER"], remaining)

    def overwrite_ledger(self, ticker, actual_qty, actual_avg):
        ledger = self.get_ledger()
        remaining = [r for r in ledger if r['ticker'] != ticker]
        
        kst = pytz.timezone('Asia/Seoul')
        today_str = datetime.datetime.now(kst).strftime('%Y-%m-%d')
        new_id = 1 if not remaining else max(r.get('id', 0) for r in remaining) + 1
        
        remaining.append({
            "id": new_id, "date": today_str, "ticker": ticker, "side": "BUY",
            "price": actual_avg, "qty": actual_qty, "avg_price": actual_avg, 
            "exec_id": f"INIT_{int(time.time())}", "desc": "초기동기화", "is_reverse": False
        })
        self._save_json(self.FILES["LEDGER"], remaining)

    def calibrate_avg_price(self, ticker, actual_avg):
        ledger = self.get_ledger()
        target_recs = [r for r in ledger if r['ticker'] == ticker]
        if target_recs:
            for r in target_recs:
                r['avg_price'] = actual_avg
            self._save_json(self.FILES["LEDGER"], ledger)

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
        
        for r in target_recs:
            if r['side'] == 'BUY':
                total_qty += r['qty']
                total_invested += (r['price'] * r['qty'])
            elif r['side'] == 'SELL':
                total_qty -= r['qty']
                total_sold += (r['price'] * r['qty'])
        
        total_qty = max(0, int(total_qty))
        invested_up = math.ceil(total_invested * 100) / 100.0
        sold_up = math.ceil(total_sold * 100) / 100.0
        
        avg_price = 0.0
        if total_qty > 0 and target_recs:
            avg_price = float(target_recs[-1].get('avg_price', 0.0))
            if avg_price == 0.0:
                buy_sum = sum(r['price']*r['qty'] for r in target_recs if r['side']=='BUY')
                buy_qty = sum(r['qty'] for r in target_recs if r['side']=='BUY')
                if buy_qty > 0:
                    avg_price = buy_sum / buy_qty
        
        return total_qty, avg_price, invested_up, sold_up

    def get_reverse_state(self, ticker):
        d = self._load_json(self.FILES["REVERSE_CFG"], {})
        return d.get(ticker, {"is_active": False, "day_count": 0, "exit_target": 0.0, "last_update_date": ""})

    def set_reverse_state(self, ticker, is_active, day_count, exit_target=0.0, last_update_date=None):
        if last_update_date is None:
            est = pytz.timezone('US/Eastern')
            last_update_date = datetime.datetime.now(est).strftime('%Y-%m-%d')
            
        d = self._load_json(self.FILES["REVERSE_CFG"], {})
        d[ticker] = {"is_active": is_active, "day_count": day_count, "exit_target": exit_target, "last_update_date": last_update_date}
        self._save_json(self.FILES["REVERSE_CFG"], d)

    def update_reverse_day_if_needed(self, ticker):
        return False

    def increment_reverse_day(self, ticker):
        state = self.get_reverse_state(ticker)
        if state.get("is_active"):
            est = pytz.timezone('US/Eastern')
            now_est = datetime.datetime.now(est)
            today_est_str = now_est.strftime('%Y-%m-%d')
            
            if state.get("last_update_date") != today_est_str:
                nyse = mcal.get_calendar('NYSE')
                is_trading_day = not nyse.schedule(start_date=now_est.date(), end_date=now_est.date()).empty
                
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
            current_budget = rem_cash / (split - t_val) if (split - t_val) > 0 else rem_cash
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

        total_buy = math.ceil(sum(r['price']*r['qty'] for r in target_recs if r['side']=='BUY') * 100) / 100.0
        total_sell = math.ceil(sum(r['price']*r['qty'] for r in target_recs if r['side']=='SELL') * 100) / 100.0
        
        profit = math.ceil((total_sell - total_buy) * 100) / 100.0
        yield_pct = math.ceil(((profit / total_buy * 100) if total_buy > 0 else 0.0) * 100) / 100.0
        
        compound_rate = self.get_compound_rate(ticker) / 100.0
        added_seed = 0
        if profit > 0 and compound_rate > 0:
            added_seed = math.floor(profit * compound_rate)
            current_seed = self.get_seed(ticker)
            self.set_seed(ticker, current_seed + added_seed)

        history = self._load_json(self.FILES["HISTORY"], [])
        new_hist = {
            "id": len(history) + 1, "ticker": ticker, "end_date": end_date,
            "profit": profit, "yield": yield_pct, "revenue": total_sell, "invested": total_buy, "trades": target_recs
        }
        history.append(new_hist)
        self._save_json(self.FILES["HISTORY"], history)
        
        self.clear_ledger_for_ticker(ticker)
        
        return new_hist, added_seed

    def get_full_version_history(self):
        return VERSION_ARCHIVE + VERSION_HISTORY

    def get_version_history(self):
        return VERSION_HISTORY

    def get_latest_version(self):
        history = self.get_version_history()
        if history and len(history) > 0:
            latest_entry = history[-1]
            if isinstance(latest_entry, str):
                return latest_entry.split(' ')[0] 
            return latest_entry.get("version", "V14.x")
        return "V14.x"

    def get_history(self):
        return self._load_json(self.FILES["HISTORY"], [])

    def check_lock(self, ticker, market_type):
        est = pytz.timezone('US/Eastern')
        today = datetime.datetime.now(est).strftime('%Y-%m-%d')
        locks = self._load_json(self.FILES["LOCKS"], {})
        return locks.get(f"{today}_{ticker}_{market_type}", False)

    def set_lock(self, ticker, market_type):
        est = pytz.timezone('US/Eastern')
        today = datetime.datetime.now(est).strftime('%Y-%m-%d')
        locks = self._load_json(self.FILES["LOCKS"], {})
        locks[f"{today}_{ticker}_{market_type}"] = True
        self._save_json(self.FILES["LOCKS"], locks)

    def reset_locks(self):
        locks = self._load_json(self.FILES["LOCKS"], {})
        surviving_locks = {k: v for k, v in locks.items() if k.startswith("ESCROW_")}
        self._save_json(self.FILES["LOCKS"], surviving_locks)
        
    def reset_lock_for_ticker(self, ticker):
        est = pytz.timezone('US/Eastern')
        today = datetime.datetime.now(est).strftime('%Y-%m-%d')
        locks = self._load_json(self.FILES["LOCKS"], {})
        
        keys_to_delete = [k for k in locks.keys() if k.startswith(f"{today}_{ticker}")]
        if keys_to_delete:
            for k in keys_to_delete:
                del locks[k]
            self._save_json(self.FILES["LOCKS"], locks)

    def get_daily_bb_lower(self, ticker):
        return float(self._load_json(self.FILES["BB_LOWER"], {}).get(ticker, 0.0))

    def set_daily_bb_lower(self, ticker, value):
        d = self._load_json(self.FILES["BB_LOWER"], {})
        d[ticker] = float(value)
        self._save_json(self.FILES["BB_LOWER"], d)
    
    def get_seed(self, t):
        return float(self._load_json(self.FILES["SEED_CFG"], self.DEFAULT_SEED).get(t, 6720.0))

    def set_seed(self, t, v): 
        d = self._load_json(self.FILES["SEED_CFG"], self.DEFAULT_SEED)
        d[t] = v
        self._save_json(self.FILES["SEED_CFG"], d)

    def get_compound_rate(self, t):
        return float(self._load_json(self.FILES["COMPOUND_CFG"], self.DEFAULT_COMPOUND).get(t, 70.0))

    def set_compound_rate(self, t, v):
        d = self._load_json(self.FILES["COMPOUND_CFG"], self.DEFAULT_COMPOUND)
        d[t] = v
        self._save_json(self.FILES["COMPOUND_CFG"], d)

    def get_version(self, t):
        return self._load_json(self.FILES["VERSION_CFG"], self.DEFAULT_VERSION).get(t, "V14")

    def set_version(self, t, v):
        d = self._load_json(self.FILES["VERSION_CFG"], self.DEFAULT_VERSION)
        d[t] = v
        self._save_json(self.FILES["VERSION_CFG"], d)

    def get_split_count(self, t):
        return self._load_json(self.FILES["SPLIT"], self.DEFAULT_SPLIT).get(t, 40.0)

    def get_target_profit(self, t):
        return self._load_json(self.FILES["PROFIT_CFG"], self.DEFAULT_TARGET).get(t, 10.0)
        
    def get_sniper_trigger(self, t):
        default_val = self.DEFAULT_SNIPER.get(t, 9.0)
        return float(self._load_json(self.FILES["SNIPER_CFG"], self.DEFAULT_SNIPER).get(t, default_val))
        
    def set_sniper_trigger(self, t, v):
        d = self._load_json(self.FILES["SNIPER_CFG"], self.DEFAULT_SNIPER)
        d[t] = float(v)
        self._save_json(self.FILES["SNIPER_CFG"], d)

    def get_turbo_mode(self):
        return self._load_file(self.FILES["TURBO"]) == 'True'

    def set_turbo_mode(self, v):
        self._save_file(self.FILES["TURBO"], str(v))

    def get_active_tickers(self):
        return self._load_json(self.FILES["TICKER"], ["SOXL", "TQQQ"])

    def set_active_tickers(self, v):
        self._save_json(self.FILES["TICKER"], v)

    def get_chat_id(self): 
        v = self._load_file(self.FILES["CHAT_ID"])
        return int(v) if v else None

    def set_chat_id(self, v):
        self._save_file(self.FILES["CHAT_ID"], v)

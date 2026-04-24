# ==========================================================
# [broker.py] - 🌟 100% 통합 무결점 완성본 (Full Version) 🌟
# MODIFIED: [V28.15 장부 2배 뻥튀기(Double Counting) 원천 차단]
# KIS API(TTTS3012R)가 동일 종목을 다중 거래소(NASD, AMEX 등) 응답으로 
# 중복 반환할 때 발생하던 누적 합산(21+21=42) 맹점 전면 수술. 
# 이종 거래소 분할 결제를 대비한 합산 로직은 유지하되, 동일한 수량과 
# 평단가로 들어오는 '유령 중복 응답'은 무시하도록 멱등성 가드 이식.
# MODIFIED: [V28.27 GCP 무한 대기 교착(Deadlock) 및 액면분할 에러 전면 수술]
# 타임아웃(Timeout) 족쇄가 없어 GCP 환경에서 봇을 영원히 기절시키던 
# yfinance의 fast_info 모듈을 전면 소각하고, 지연 발생 시 즉각 KIS API로 
# 우회(Fallback)하도록 Safe-Casting 방어막 이식. 액면분할 파싱 에러(str) 완벽 픽스.
# MODIFIED: [V28.28 yfinance 버전 호환 및 타임아웃 방어]
# 최신 yfinance 라이브러리가 액면분할 날짜 키를 문자열(str)로 반환 시 
# 발생하는 strftime 에러를 Timestamp 강제 변환 및 슬라이싱으로 완벽히 교정.
# MODIFIED: [V28.34 17시 잔고 스캔 API 크래시 완벽 방어 및 타입 세이프 쉴드 이식]
# KIS API가 0주 상태이거나 서버 응답 변동 시 output2를 빈 리스트([])로 반환하여
# AttributeError 런타임 붕괴를 유발하던 치명적 맹점을 isinstance 기반의 
# 타입 락온(Lock-on) 방어막으로 원천 차단 완료.
# MODIFIED: [V29.18 런타임 붕괴 방어 및 페이징 결함 수술]
# 1) get_unfilled_orders_detail 및 get_execution_history에서 ctx_area_fk200 파싱 시 
#    결측치(None) 유입으로 인한 AttributeError 런타임 붕괴를 Safe Casting으로 원천 차단.
# 2) get_account_balance 함수에 tr_cont 헤더 기반의 페이징(Pagination) 로직을 이식하여
#    20종목 초과 시 발생하는 잔고 스캔 데이터 기아(Data Starvation) 맹점 완벽 교정.
# 3) 논리적 앵커 통일을 위해 America/New_York을 US/Eastern으로 100% 락온(Lock-on) 형변환.
# MODIFIED: [V30.09 핫픽스] pytz 영구 적출 및 ZoneInfo 도입으로 LMT 버그 차단 및 타임존 무결성 100% 확보
# ==========================================================

import requests
import json
import time
import datetime
import os
import math
import yfinance as yf
# MODIFIED: [V30.09 핫픽스] LMT 오차 방어를 위해 pytz 적출 및 ZoneInfo 도입
# import pytz
from zoneinfo import ZoneInfo
import tempfile
import shutil  
import pandas as pd   
import numpy as np
import volatility_engine as ve
import logging  # NEW: 예외 발생 시 침묵 방지를 위한 로깅 모듈 추가

class KoreaInvestmentBroker:
    def __init__(self, app_key, app_secret, cano, acnt_prdt_cd="01"):
        self.app_key = app_key
        self.app_secret = app_secret
        self.cano = cano
        self.acnt_prdt_cd = acnt_prdt_cd
        self.base_url = "https://openapi.koreainvestment.com:9443"
        self.token_file = f"data/token_{cano}.dat" 
        self.token = None
        self._excg_cd_cache = {} 
        
        self._get_access_token()

    def _get_access_token(self, force=False):
        # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo 이식
        kst = ZoneInfo('Asia/Seoul')
        
        if not force and os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    saved = json.load(f)
                expire_time = datetime.datetime.strptime(saved['expire'], '%Y-%m-%d %H:%M:%S')
                now_kst_naive = datetime.datetime.now(kst).replace(tzinfo=None)
                
                if expire_time > now_kst_naive + datetime.timedelta(hours=1):
                    self.token = saved['token']
                    return
            except Exception: pass

        if force and os.path.exists(self.token_file):
            try: os.remove(self.token_file)
            except Exception: pass

        url = f"{self.base_url}/oauth2/tokenP"
        body = {"grant_type": "client_credentials", "appkey": self.app_key, "appsecret": self.app_secret}
        
        try:
            res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body), timeout=10)
            data = res.json()
            if 'access_token' in data:
                self.token = data['access_token']
                expire_str = (datetime.datetime.now(kst).replace(tzinfo=None) + datetime.timedelta(seconds=int(data['expires_in']))).strftime('%Y-%m-%d %H:%M:%S')
                
                dir_name = os.path.dirname(self.token_file)
                if dir_name and not os.path.exists(dir_name):
                    os.makedirs(dir_name, exist_ok=True)
                fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
                
                try:
                    with os.fdopen(fd, 'w', encoding='utf-8') as f:
                        json.dump({'token': self.token, 'expire': expire_str}, f)
                        f.flush()
                        os.fsync(f.fileno())
                    
                    shutil.move(temp_path, self.token_file)
                finally:
                    if os.path.exists(temp_path):
                        try: os.remove(temp_path)
                        except Exception: pass
            else:
                print(f"❌ [Broker] 토큰 발급 실패: {data.get('error_description', '알 수 없는 오류')}")
        except Exception as e:
            print(f"❌ [Broker] 토큰 통신 에러: {e}")

    def _get_header(self, tr_id):
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
            "custtype": "P"
        }

    def _api_request(self, method, url, headers, params=None, data=None):
        TOKEN_EXPIRY_KEYWORDS = frozenset([
            'expired', '인증', 'authorization', 'egt0001', 'egt0002', 'oauth', 
            '접근토큰이 만료', '토큰이 유효하지'
        ])
        
        for attempt in range(2): 
            try:
                if method.upper() == "GET":
                    res = requests.get(url, headers=headers, params=params, timeout=10)
                else:
                    res = requests.post(url, headers=headers, data=json.dumps(data) if data else None, timeout=10)
                    
                resp_json = res.json()
                
                if resp_json.get('rt_cd') != '0':
                    msg1_lower = resp_json.get('msg1', '').lower()
                    msg_cd = resp_json.get('msg_cd', '').lower()
                    
                    if any(x in msg1_lower or x in msg_cd for x in TOKEN_EXPIRY_KEYWORDS):
                        if attempt == 0: 
                            old_token = self.token 
                            print(f"\n🚨 [안전장치 가동] API 토큰 만료 감지! : {msg1_lower}")
                            self._get_access_token(force=True)
                            
                            if self.token == old_token or self.token is None:
                                print("🚨 [Broker] 토큰 갱신 실패. 재시도 중단.")
                                return res, resp_json
                                
                            headers["authorization"] = f"Bearer {self.token}"
                            time.sleep(1.0)
                            continue
                return res, resp_json
            except Exception as e:
                print(f"⚠️ API 통신 중 예외 발생: {e}")
                if attempt == 1: return None, {}
                time.sleep(1.0)
        return None, {}

    def _call_api(self, tr_id, url_path, method="GET", params=None, body=None):
        headers = self._get_header(tr_id)
        url = f"{self.base_url}{url_path}"
        res, resp_json = self._api_request(method, url, headers, params=params, data=body)
        if not resp_json: return {'rt_cd': '999', 'msg1': '통신 오류 또는 최대 재시도 횟수 초과'}
        return resp_json

    def _ceil_2(self, value):
        if value is None: return 0.0
        return max(0.01, math.ceil(value * 100) / 100.0)

    def _safe_float(self, value):
        try: return float(str(value).replace(',', ''))
        except Exception: return 0.0

    def _get_exchange_code(self, ticker, target_api="PRICE"):
        if ticker in self._excg_cd_cache:
            codes = self._excg_cd_cache[ticker]
            return codes['PRICE'] if target_api == "PRICE" else codes['ORDER']

        price_cd = "NAS"
        order_cd = "NASD"
        dynamic_success = False

        try:
            for prdt_type in ["512", "513", "529"]:
                params = {
                    "PRDT_TYPE_CD": prdt_type,
                    "PDNO": ticker
                }
                res = self._call_api("CTPF1702R", "/uapi/overseas-price/v1/quotations/search-info", "GET", params=params)
                
                if res.get('rt_cd') == '0' and res.get('output'):
                    excg_name = str(res['output'].get('ovrs_excg_cd', '')).upper()
                    if "NASD" in excg_name or "NASDAQ" in excg_name:
                        price_cd, order_cd = "NAS", "NASD"
                        dynamic_success = True
                        break
                    elif "NYSE" in excg_name or "NEW YORK" in excg_name:
                        price_cd, order_cd = "NYS", "NYSE"
                        dynamic_success = True
                        break
                    elif "AMEX" in excg_name:
                        price_cd, order_cd = "AMS", "AMEX"
                        dynamic_success = True
                        break
        except Exception as e:
            print(f"⚠️ [Broker] 거래소 동적 획득 실패: {ticker} - {e}")

        if not dynamic_success:
            if ticker == "SOXL": price_cd, order_cd = "AMS", "AMEX"
            elif ticker == "TQQQ": price_cd, order_cd = "NAS", "NASD"

        self._excg_cd_cache[ticker] = {'PRICE': price_cd, 'ORDER': order_cd}
        return price_cd if target_api == "PRICE" else order_cd

    def get_account_balance(self):
        cash = 0.0
        holdings = {}
        api_success = False 
        
        params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "WCRC_FRCR_DVSN_CD": "02", "NATN_CD": "840", "TR_MKET_CD": "00", "INQR_DVSN_CD": "00"}
        res = self._call_api("CTRP6504R", "/uapi/overseas-stock/v1/trading/inquire-present-balance", "GET", params=params)
        
        if res.get('rt_cd') == '0':
            api_success = True
            o2 = res.get('output2', {})
            
            if isinstance(o2, list):
                o2 = o2[0] if len(o2) > 0 else {}
            
            dncl_amt = self._safe_float(o2.get('frcr_dncl_amt_2', 0))       
            sll_amt = self._safe_float(o2.get('frcr_sll_amt_smtl', 0))      
            buy_amt = self._safe_float(o2.get('frcr_buy_amt_smtl', 0))      
            
            raw_bp = dncl_amt + sll_amt - buy_amt
            cash = max(0.0, math.floor((raw_bp * 0.9945) * 100) / 100.0)

        target_excgs = ["NASD", "AMEX", "NYSE"] 
        
        # MODIFIED: [V29.18 잔고 스캔 데이터 기아(Data Starvation) 원천 차단] 
        # 페이징(Pagination) 누락으로 20종목 이상 보유 시 발생하는 잔고 증발 맹점을 해결하기 위해 
        # _api_request 다이렉트 호출 및 tr_cont 기반 무한 루프 페이징 락온 이식.
        for excg in target_excgs:
            fk200, nk200 = "", ""
            for attempt in range(20): # 최대 20페이지 스캔 방어막
                params_hold = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg, "TR_CRCY_CD": "USD", "CTX_AREA_FK200": fk200, "CTX_AREA_NK200": nk200}
                headers = self._get_header("TTTS3012R")
                url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-balance"
                res_hold, resp_json = self._api_request("GET", url, headers, params=params_hold)
                
                if res_hold and resp_json.get('rt_cd') == '0':
                    api_success = True
                    if cash <= 0:
                        o2 = resp_json.get('output2', {})
                        if isinstance(o2, list):
                            o2 = o2[0] if len(o2) > 0 else {}
                        new_cash = self._safe_float(o2.get('ovrs_ord_psbl_amt', 0))
                        if new_cash > cash: cash = new_cash
                    
                    for item in (resp_json.get('output1') or []):
                        ticker = item.get('ovrs_pdno')
                        if not ticker:
                            continue
                            
                        qty = int(self._safe_float(item.get('ovrs_cblc_qty', 0)))
                        ord_psbl_qty = int(self._safe_float(item.get('ord_psbl_qty', 0)))
                        avg = self._safe_float(item.get('pchs_avg_pric', 0))
                        
                        if qty > 0 and ord_psbl_qty == 0:
                            ord_psbl_qty = qty
                        
                        if qty > 0:
                            if ticker not in holdings: 
                                holdings[ticker] = {'qty': qty, 'ord_psbl_qty': ord_psbl_qty, 'avg': avg}
                            else:
                                prev = holdings[ticker]
                                
                                # 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
                                # MODIFIED: KIS API가 동일 수량/동일 평단가의 데이터를 다른 거래소 응답으로 
                                # 한 번 더 보내는 경우(유령 중복 응답), 무지성으로 합산(+=)하지 않고 무시하도록 멱등성 필터링 이식.
                                if prev['qty'] == qty and abs(prev['avg'] - avg) < 0.001:
                                    continue 
                                    
                                total_qty = prev['qty'] + qty
                                new_avg = ((prev['avg'] * prev['qty']) + (avg * qty)) / total_qty if total_qty > 0 else avg
                                
                                holdings[ticker]['qty'] = total_qty
                                holdings[ticker]['ord_psbl_qty'] += ord_psbl_qty
                                holdings[ticker]['avg'] = new_avg

                    tr_cont = res_hold.headers.get('tr_cont', '') if hasattr(res_hold, 'headers') else ''
                    # MODIFIED: [V29.18 결측치 Safe Casting 방어막]
                    fk200 = (resp_json.get('ctx_area_fk200', '') or '').strip()
                    nk200 = (resp_json.get('ctx_area_nk200', '') or '').strip()

                    if tr_cont in ['M', 'F'] and nk200:
                        time.sleep(0.2)
                        continue
                    else:
                        break
                else:
                    break
        
        if api_success: return cash, holdings
        else: return cash, None

    def get_current_5min_candle(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period="5d", interval="1m", prepost=True, timeout=5)
            
            if df.empty: return None
                
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
                
            # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo('America/New_York') 이식
            est = ZoneInfo('America/New_York')
            
            if df.index.tz is None:
                df.index = df.index.tz_localize('UTC').tz_convert(est)
            else:
                df.index = df.index.tz_convert(est)
                
            regular_market = df.between_time('09:30', '15:59')
            
            if regular_market.empty: return None
            
            today_date = pd.Timestamp.now(tz=est).normalize()
            regular_market = regular_market[regular_market.index >= today_date]
            
            if regular_market.empty: return None
                
            regular_market = regular_market.dropna(subset=['Volume', 'High', 'Low', 'Close'])
            
            typical_price = (regular_market['High'] + regular_market['Low'] + regular_market['Close']) / 3.0
            vol_price = typical_price * regular_market['Volume']
            
            cum_vol_price = vol_price.cumsum()
            cum_vol = regular_market['Volume'].cumsum()
            
            vwap_series = pd.Series(np.where(cum_vol > 0, cum_vol_price / cum_vol, np.nan), index=cum_vol.index).ffill() 
            current_vwap = float(vwap_series.iloc[-1]) if not vwap_series.empty else 0.0
            
            if pd.isna(current_vwap):
                current_vwap = 0.0
            
            resampled = regular_market.resample('5min', label='left', closed='left').agg({
                'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
            }).dropna()
            
            if resampled.empty: return None
                
            resampled['Vol_MA10'] = resampled['Volume'].rolling(10, min_periods=1).mean()
            resampled['Vol_MA20'] = resampled['Volume'].rolling(20, min_periods=1).mean()
            
            last_candle = resampled.iloc[-1]
            vol_ma10 = float(last_candle['Vol_MA10']) if not pd.isna(last_candle['Vol_MA10']) else float(last_candle['Volume'])
            vol_ma20 = float(last_candle['Vol_MA20']) if not pd.isna(last_candle['Vol_MA20']) else float(last_candle['Volume'])
            
            latest_1m = regular_market.iloc[-1] 
            
            return {
                'open': float(last_candle['Open']),
                'high': float(last_candle['High']),  
                'low': float(last_candle['Low']),    
                'close': float(latest_1m['Close']), 
                'volume': float(last_candle['Volume']), 
                'vol_ma10': vol_ma10,
                'vol_ma20': vol_ma20,
                'vwap': current_vwap  
            }
        except Exception as e:
            print(f"⚠️ [Broker] 실시간 5분봉 조회 실패 ({ticker}): {e}")
            return None

    def get_current_price(self, ticker, is_market_closed=False):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1d", interval="1m", prepost=True, timeout=5)
            if not hist.empty: return float(hist['Close'].iloc[-1])
            else: raise ValueError("YF 실시간 데이터 응답 지연 (timeout)") 
        except Exception as e:
            print(f"⚠️ [야후] 현재가 에러, 한투 API 우회 가동: {e}")

        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
            res = self._call_api("HHDFS76200200", "/uapi/overseas-price/v1/quotations/price", "GET", params=params)
            if res.get('rt_cd') == '0':
                return float(res.get('output', {}).get('last', 0.0))
        except Exception as e:
            pass
        return 0.0

    def get_ask_price(self, ticker):
        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
            res = self._call_api("HHDFS76200100", "/uapi/overseas-price/v1/quotations/inquire-asking-price", "GET", params=params)
            if res.get('rt_cd') == '0':
                output2 = res.get('output2', [])
                if isinstance(output2, list) and len(output2) > 0: return float(output2[0].get('pask1', 0.0))
                elif isinstance(output2, dict): return float(output2.get('pask1', 0.0))
        except Exception as e:
            pass
        return 0.0

    def get_bid_price(self, ticker):
        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
            res = self._call_api("HHDFS76200100", "/uapi/overseas-price/v1/quotations/inquire-asking-price", "GET", params=params)
            if res.get('rt_cd') == '0':
                output2 = res.get('output2', [])
                if isinstance(output2, list) and len(output2) > 0: return float(output2[0].get('pbid1', 0.0))
                elif isinstance(output2, dict): return float(output2.get('pbid1', 0.0))
        except Exception as e:
            pass
        return 0.0

    def get_previous_close(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="5d", timeout=5)
            if not hist.empty:
                # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo('America/New_York') 이식
                est = ZoneInfo('America/New_York')
                now_est = datetime.datetime.now(est)
                
                cutoff_date = now_est.date()
                if now_est.time() <= datetime.time(16, 0, 30): cutoff_date -= datetime.timedelta(days=1)
                
                if hist.index.tzinfo is None: hist.index = hist.index.tz_localize('UTC').tz_convert(est)
                else: hist.index = hist.index.tz_convert(est)
                
                past_hist = hist[hist.index.date <= cutoff_date]
                if not past_hist.empty: return float(past_hist['Close'].dropna().iloc[-1])
        except Exception as e:
            print(f"⚠️ [야후] 전일 종가 파싱 에러, 한투 API 우회 가동: {e}")

        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
            res = self._call_api("HHDFS76200200", "/uapi/overseas-price/v1/quotations/price", "GET", params=params)
            if res.get('rt_cd') == '0': return float(res.get('output', {}).get('base', 0.0))
        except Exception as e:
            pass
        return 0.0

    def get_5day_ma(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="10d", timeout=5) 
            if len(hist) >= 5: return float(hist['Close'][-5:].mean())
        except Exception as e:
            pass
            
        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker, "GUBN": "0", "BYMD": "", "MODP": "1"}
            res = self._call_api("HHDFS76240000", "/uapi/overseas-price/v1/quotations/dailyprice", "GET", params=params)
            if res.get('rt_cd') == '0':
                output2 = res.get('output2', [])
                if isinstance(output2, list) and len(output2) >= 5:
                    closes = [float(x['clos']) for x in output2[:5]]
                    return sum(closes) / len(closes)
        except Exception as e:
            pass
        return 0.0

    def get_1min_candles_df(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period="1d", interval="1m", prepost=True, timeout=5)
            
            if df.empty: return None
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
                
            # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo('America/New_York') 이식
            est = ZoneInfo('America/New_York')
            if df.index.tz is None: df.index = df.index.tz_localize('UTC').tz_convert(est)
            else: df.index = df.index.tz_convert(est)
                
            df = df.rename(columns={'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})
            df['time_est'] = df.index.strftime('%H%M00')
            return df[['high', 'low', 'close', 'volume', 'time_est']]
        except Exception as e:
            return None

    def get_unfilled_orders_detail(self, ticker):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        valid_orders = []
        fk200, nk200 = "", ""
        
        for attempt in range(10):
            params = {
                "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, 
                "SORT_SQN": "DS", "CTX_AREA_FK200": fk200, "CTX_AREA_NK200": nk200
            }
            headers = self._get_header("TTTS3018R")
            url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-nccs"
            res, resp_json = self._api_request("GET", url, headers, params=params)
            
            if res and resp_json.get('rt_cd') == '0':
                output = resp_json.get('output', [])
                if isinstance(output, dict): output = [output]
                valid_orders.extend([item for item in output if item.get('pdno') == ticker])
                
                tr_cont = res.headers.get('tr_cont', '') if hasattr(res, 'headers') else ''
                # MODIFIED: [V29.18 결측치(None) 유입에 따른 AttributeError 런타임 붕괴 방어막] Safe Casting 이식
                fk200 = (resp_json.get('ctx_area_fk200', '') or '').strip()
                nk200 = (resp_json.get('ctx_area_nk200', '') or '').strip()
                
                if tr_cont in ['M', 'F'] and nk200:
                    time.sleep(0.3)
                    continue
                else: break
            else:
                return False
                
        return valid_orders

    def get_unfilled_orders(self, ticker):
        details = self.get_unfilled_orders_detail(ticker)
        if details is False:
            return []
        return [item.get('odno') for item in details]

    def cancel_all_orders_safe(self, ticker, side=None):
        for i in range(3):
            orders = self.get_unfilled_orders_detail(ticker)
            if orders is False:
                return False
            if not orders: return True
            
            target_orders = orders
            if side == "BUY": target_orders = [o for o in orders if o.get('sll_buy_dvsn_cd') == '02']
            elif side == "SELL": target_orders = [o for o in orders if o.get('sll_buy_dvsn_cd') == '01']
                
            if not target_orders: return True
            
            for o in target_orders: self.cancel_order(ticker, o.get('odno'))
            time.sleep(5)
            
        final_orders = self.get_unfilled_orders_detail(ticker)
        if final_orders is False:
            return False
            
        failed_orders = []
        if side == "BUY": failed_orders = [o for o in final_orders if o.get('sll_buy_dvsn_cd') == '02']
        elif side == "SELL": failed_orders = [o for o in final_orders if o.get('sll_buy_dvsn_cd') == '01']
        else: failed_orders = final_orders
            
        if failed_orders:
            return False
            
        return True
        
    def cancel_targeted_orders(self, ticker, side, target_ord_dvsn):
        sll_buy_cd = '02' if side == "BUY" else '01'
        orders = self.get_unfilled_orders_detail(ticker)
        if orders is False or not orders: return 0
        
        target_orders = []
        for o in orders:
            dvsn = o.get('ord_dvsn_cd') or o.get('ord_dvsn') or ''
            if o.get('sll_buy_dvsn_cd') == sll_buy_cd and dvsn == target_ord_dvsn:
                target_orders.append(o)
                
        for o in target_orders:
            self.cancel_order(ticker, o.get('odno'))
            time.sleep(0.3)
            
        return len(target_orders)

    def cancel_orders_by_price(self, ticker, side, target_prices):
        sll_buy_cd = '02' if side == "BUY" else '01'
        orders = self.get_unfilled_orders_detail(ticker)
        if orders is False or not orders: return 0
        
        target_orders = []
        for o in orders:
            if o.get('sll_buy_dvsn_cd') == sll_buy_cd:
                raw_p1, raw_p2, raw_p3 = o.get('ft_ord_unpr3', 0), o.get('ord_unpr', 0), o.get('ovrs_ord_unpr', 0)
                o_price = 0.0
                
                for rp in [raw_p1, raw_p2, raw_p3]:
                    try:
                        val = float(rp)
                        if val > 0:
                            o_price = val
                            break 
                    except (TypeError, ValueError):
                        pass
                        
                for tp in target_prices:
                    if o_price > 0 and abs(o_price - tp) < 0.005: 
                        target_orders.append(o)
                        break
                        
        for o in target_orders:
            self.cancel_order(ticker, o.get('odno'))
            time.sleep(0.3)
            
        return len(target_orders)

    def send_order(self, ticker, side, qty, price, order_type="LIMIT"):
        try:
            order_qty = int(float(qty))
        except (TypeError, ValueError):
            return {'rt_cd': '999', 'msg1': f'유효하지 않은 주문 수량 타입: {qty!r}'}

        if order_qty <= 0:
            return {'rt_cd': '999', 'msg1': f'유효하지 않은 주문 수량: {qty}'}

        for attempt in range(2):
            tr_id = "TTTT1002U" if side == "BUY" else "TTTT1006U"
            excg_cd = self._get_exchange_code(ticker, target_api="ORDER")

            if order_type == "LOC": ord_dvsn = "34"
            elif order_type == "MOC": ord_dvsn = "33"
            elif order_type == "LOO": ord_dvsn = "02"
            elif order_type == "MOO": ord_dvsn = "31"
            elif order_type == "AFTER_LIMIT": 
                ord_dvsn = "00"  
            else: ord_dvsn = "00"

            final_price = self._ceil_2(price)
            if order_type in ["MOC", "MOO"]: final_price = 0
            elif order_type not in ["MOC", "MOO"] and final_price <= 0.0:
                return {'rt_cd': '999', 'msg1': f'유효하지 않은 주문 가격: {price}'}
            
            body = {
                "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd,
                "PDNO": ticker, "ORD_QTY": str(order_qty), "OVRS_ORD_UNPR": str(final_price),
                "ORD_SVR_DVSN_CD": "0", "ORD_DVSN": ord_dvsn 
            }
            res = self._call_api(tr_id, "/uapi/overseas-stock/v1/trading/order", "POST", body=body)
            
            rt_cd = res.get('rt_cd', '999')
            msg1 = res.get('msg1', '오류')
            output = res.get('output', {})
            odno = output.get('ODNO', '') if isinstance(output, dict) else ''
            
            if rt_cd != '0' and attempt == 0 and ("거래소" in msg1 or "시장" in msg1 or "exchange" in msg1.lower() or "코드" in msg1):
                if ticker in self._excg_cd_cache:
                    del self._excg_cd_cache[ticker]
                time.sleep(0.5)
                continue
                
            return {'rt_cd': rt_cd, 'msg1': msg1, 'odno': odno}
            
        return {'rt_cd': '999', 'msg1': '거래소 캐시 재시도 최대 횟수 초과'}

    def cancel_order(self, ticker, order_id):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        body = {
            "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd,
            "PDNO": ticker, "ORGN_ODNO": order_id, "RVSE_CNCL_DVSN_CD": "02",
            "ORD_QTY": "0", "OVRS_ORD_UNPR": "0", "ORD_SVR_DVSN_CD": "0"
        }
        self._call_api("TTTT1004U", "/uapi/overseas-stock/v1/trading/order-rvsecncl", "POST", body=body)

    def get_execution_history(self, ticker, start_date, end_date):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        valid_execs = []
        odno_map = {}
        fk200, nk200 = "", ""
        
        for attempt in range(10): 
            params = {
                "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "PDNO": ticker,
                "ORD_STRT_DT": start_date, "ORD_END_DT": end_date, "SLL_BUY_DVSN": "00",      
                "CCLD_NCCS_DVSN": "00", "OVRS_EXCG_CD": excg_cd, "SORT_SQN": "DS",
                "ORD_DT": "", "ORD_GNO_BRNO": "", "ODNO": "", "CTX_AREA_FK200": fk200, "CTX_AREA_NK200": nk200
            }
            
            headers = self._get_header("TTTS3035R")
            url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-ccnl"
            res, resp_json = self._api_request("GET", url, headers, params=params)
            
            if res and resp_json.get('rt_cd') == '0':
                output = resp_json.get('output', [])
                if isinstance(output, dict): output = [output] 
                for item in output:
                    try:
                        raw_qty = item.get('ft_ccld_qty') or '0'
                        raw_unpr = item.get('ft_ccld_unpr3') or '0'
                        item_qty = float(raw_qty)
                        item_price = float(raw_unpr)
                        
                        if item_qty > 0:
                            odno = item.get('odno') or ''
                            if not odno:
                                odno_map[f"__nk_{id(item)}"] = {
                                    "item": dict(item),
                                    "total_qty": item_qty,
                                    "total_amt": item_qty * item_price
                                }
                            elif odno not in odno_map:
                                odno_map[odno] = {
                                    "item": dict(item),
                                    "total_qty": item_qty,
                                    "total_amt": item_qty * item_price
                                }
                            else:
                                odno_map[odno]["total_qty"] += item_qty
                                odno_map[odno]["total_amt"] += (item_qty * item_price)
                                
                    except (TypeError, ValueError) as e:
                        continue
                        
                tr_cont = res.headers.get('tr_cont', '') if hasattr(res, 'headers') else ''
                # MODIFIED: [V29.18 결측치(None) 유입에 따른 AttributeError 런타임 붕괴 방어막] Safe Casting 이식
                fk200 = (resp_json.get('ctx_area_fk200', '') or '').strip()
                nk200 = (resp_json.get('ctx_area_nk200', '') or '').strip()
                
                if tr_cont in ['M', 'F'] and nk200:
                    time.sleep(0.3) 
                    continue
                else: break 
            else:
                break

        for key, data in odno_map.items():
            merged_item = data["item"]
            merged_item["ft_ccld_qty"] = str(data["total_qty"])
            avg_price = data["total_amt"] / data["total_qty"] if data["total_qty"] > 0 else 0.0
            merged_item["ft_ccld_unpr3"] = str(avg_price)
            valid_execs.append(merged_item)
            
        return valid_execs

    def get_genesis_ledger(self, ticker, limit_date_str=None):
        _, holdings = self.get_account_balance()
        if holdings is None: return None, 0, 0.0
            
        ticker_info = holdings.get(ticker, {'qty': 0, 'avg': 0.0})
        curr_qty = int(ticker_info.get('qty', 0))
        final_qty = curr_qty
        final_avg = float(ticker_info.get('avg', 0.0))
        
        if curr_qty == 0: return [], 0, 0.0
            
        ledger_records = []
        # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo('America/New_York') 이식
        est = ZoneInfo('America/New_York')
        target_date = datetime.datetime.now(est)
        genesis_reached = False
        loop_counter = 0 
        
        while curr_qty > 0 and not genesis_reached and loop_counter < 365:
            if target_date.weekday() < 5:
                loop_counter += 1
                
            date_str = target_date.strftime('%Y%m%d')
            
            if limit_date_str and date_str < limit_date_str: break 
                
            execs = self.get_execution_history(ticker, date_str, date_str)
            
            if execs:
                execs.sort(key=lambda x: x.get('ord_tmd', '000000'), reverse=True)
                for ex in execs:
                    try:
                        side_cd = ex.get('sll_buy_dvsn_cd')
                        exec_qty = int(float(ex.get('ft_ccld_qty') or '0'))
                        exec_price = float(ex.get('ft_ccld_unpr3') or '0')
                    except (TypeError, ValueError) as e:
                        continue
                        
                    record_qty = exec_qty
                    
                    if side_cd == "02": 
                        if curr_qty <= exec_qty: 
                            record_qty = curr_qty 
                            curr_qty = 0
                            genesis_reached = True
                        else: curr_qty -= exec_qty
                    else: curr_qty += exec_qty
                    
                    ledger_records.append({
                        'date': f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}",
                        'side': "BUY" if side_cd == "02" else "SELL",
                        'qty': record_qty, 'price': exec_price
                    })
                    if genesis_reached: break
                        
            target_date -= datetime.timedelta(days=1)
            time.sleep(0.1) 
                
        if curr_qty > 0 and loop_counter >= 365:
            ledger_records.append({
                'date': 'INCOMPLETE', 'side': 'UNKNOWN', 'qty': curr_qty, 'price': final_avg, 'is_incomplete': True
            })
                
        ledger_records.reverse()
        return ledger_records, final_qty, final_avg

    def get_recent_stock_split(self, ticker, last_date_str):
        try:
            stock = yf.Ticker(ticker)
            splits = stock.splits
            if splits is not None and not splits.empty:
                if last_date_str == "":
                    # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo('America/New_York') 이식
                    est = ZoneInfo('America/New_York')
                    seven_days_ago = datetime.datetime.now(est) - datetime.timedelta(days=7)
                    safe_last_date = seven_days_ago.strftime('%Y-%m-%d')
                else: safe_last_date = last_date_str
                    
                for split_date_dt, ratio in splits.items():
                    if isinstance(split_date_dt, str):
                        split_date = split_date_dt[:10]
                    else:
                        split_date = pd.Timestamp(split_date_dt).strftime('%Y-%m-%d')
                    if split_date > safe_last_date: return float(ratio), split_date
        except Exception as e:
            logging.warning(f"⚠️ [야후 파이낸스] 액면분할 조회 에러: {e}")
        return 0.0, ""

    def get_dynamic_sniper_target(self, index_ticker):
        try:
            class TargetFloat(float): pass
            
            if index_ticker == "SOXX":
                hv_val, weight, target_drop, base_amp = ve.get_soxl_target_drop_full()
                ret = TargetFloat(target_drop)
                ret.metric_val, ret.weight, ret.base_amp, ret.metric_name = hv_val, weight, base_amp, "SOXX HV"
                ret.metric_base = round(hv_val / weight, 2) if weight > 0 else 25.0
            else:
                vxn_val, weight, target_drop, base_amp = ve.get_tqqq_target_drop_full()
                ret = TargetFloat(target_drop)
                ret.metric_val, ret.weight, ret.base_amp, ret.metric_name = vxn_val, weight, base_amp, "실시간 VXN"
                ret.metric_base = round(vxn_val / weight, 2) if weight > 0 else 20.0
            
            ret.is_panic = False
            ret.gap_pct = 0.0 
            return ret
            
        except Exception as e:
            fallback_val = -8.79 if index_ticker == "SOXX" else -4.95
            ret = TargetFloat(fallback_val)
            ret.metric_val, ret.weight, ret.base_amp, ret.metric_name, ret.metric_base = 0.0, 1.0, fallback_val, "통신오류(기본값)", 25.0 if index_ticker == "SOXX" else 20.0
            ret.is_panic, ret.gap_pct = False, 0.0
            return ret

    def get_day_high_low(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1d", interval="1m", prepost=True, timeout=5)
            if not hist.empty: return float(hist['High'].max()), float(hist['Low'].min())
            else: raise ValueError("YF 고가/저가 데이터 응답 지연 (timeout)")
        except Exception as e: pass

        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker} 
            res = self._call_api("HHDFS76200200", "/uapi/overseas-price/v1/quotations/price", "GET", params=params)
            if res.get('rt_cd') == '0':
                out = res.get('output', {})
                return float(out.get('high', 0.0)), float(out.get('low', 0.0))
        except Exception as e: pass
        return 0.0, 0.0

    def get_atr_data(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="30d", timeout=5)
            if hist.empty or len(hist) < 15: return 0.0, 0.0
                
            hist['Prev_Close'] = hist['Close'].shift(1)
            hist = hist.dropna(subset=['High', 'Low', 'Close']).copy()
            
            hist['TR'] = hist.apply(lambda row: max(
                row['High'] - row['Low'],
                abs(row['High'] - row['Prev_Close']) if not pd.isna(row['Prev_Close']) else 0,
                abs(row['Low'] - row['Prev_Close']) if not pd.isna(row['Prev_Close']) else 0
            ), axis=1)
            
            hist['ATR5'] = hist['TR'].rolling(window=5).mean()
            hist['ATR14'] = hist['TR'].rolling(window=14).mean()
            
            last_row = hist.iloc[-1]
            last_close = float(last_row['Close'])
            
            if last_close > 0:
                atr5_val  = last_row['ATR5']
                atr14_val = last_row['ATR14']
                
                if pd.isna(atr5_val) or pd.isna(atr14_val):
                    return 0.0, 0.0
                
                return round((float(atr5_val) / last_close) * 100, 1), round((float(atr14_val) / last_close) * 100, 1)
            return 0.0, 0.0
        except Exception as e:
            return 0.0, 0.0

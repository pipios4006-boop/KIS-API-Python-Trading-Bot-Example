# 파일명: broker.py
import requests
import json
import time
import datetime
import os
import math
import yfinance as yf

class KoreaInvestmentBroker:
    # 브로커 객체 초기화 (앱키, 시크릿키, 계좌번호 등 세팅)
    def __init__(self, app_key, app_secret, cano, acnt_prdt_cd="01"):
        self.app_key = app_key
        self.app_secret = app_secret
        self.cano = cano
        self.acnt_prdt_cd = acnt_prdt_cd
        self.base_url = "https://openapi.koreainvestment.com:9443"
        # [V14.2] 토큰 파일을 data/ 폴더 하위에 생성하도록 변경
        self.token_file = f"data/token_{cano}.dat" 
        self.token = None
        self._get_access_token()

    # KIS API 통신용 Oauth2 인증 토큰을 발급받거나 파일에서 불러옵니다.
    def _get_access_token(self, force=False):
        if not force and os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    saved = json.load(f)
                expire_time = datetime.datetime.strptime(saved['expire'], '%Y-%m-%d %H:%M:%S')
                if expire_time > datetime.datetime.now() + datetime.timedelta(hours=1):
                    self.token = saved['token']
                    return
            except: pass

        if force and os.path.exists(self.token_file):
            try: os.remove(self.token_file)
            except: pass

        url = f"{self.base_url}/oauth2/tokenP"
        body = {"grant_type": "client_credentials", "appkey": self.app_key, "appsecret": self.app_secret}
        
        try:
            res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body))
            data = res.json()
            if 'access_token' in data:
                self.token = data['access_token']
                expire_str = (datetime.datetime.now() + datetime.timedelta(seconds=int(data['expires_in']))).strftime('%Y-%m-%d %H:%M:%S')
                
                with open(self.token_file, 'w') as f:
                    json.dump({'token': self.token, 'expire': expire_str}, f)
            else:
                print(f"❌ [Broker] 토큰 발급 실패: {data.get('error_description', '알 수 없는 오류')}")
        except Exception as e:
            print(f"❌ [Broker] 토큰 통신 에러: {e}")

    # API 호출용 공통 헤더를 생성합니다.
    def _get_header(self, tr_id):
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
            "custtype": "P"
        }

    # 🛡️ [V15.2 안전장치] API 통신 중 토큰 만료(또는 오류) 발생 시 자동 감지 및 강제 재발급 후 재시도하는 래퍼 함수
    def _api_request(self, method, url, headers, params=None, data=None):
        for attempt in range(2): 
            try:
                if method.upper() == "GET":
                    res = requests.get(url, headers=headers, params=params)
                else:
                    res = requests.post(url, headers=headers, data=json.dumps(data) if data else None)
                    
                resp_json = res.json()
                
                if resp_json.get('rt_cd') != '0':
                    msg1 = resp_json.get('msg1', '')
                    if any(x in msg1.lower() for x in ['토큰', '접근토큰', 'token', 'expired', 'mig', '인증', 'authorization']):
                        if attempt == 0: 
                            print(f"\n🚨 [안전장치 가동] API 토큰 만료 감지! : {msg1}")
                            self._get_access_token(force=True)
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
        return math.ceil(value * 100) / 100.0

    def _safe_float(self, value):
        try: return float(str(value).replace(',', ''))
        except: return 0.0

    def get_account_balance(self):
        cash = 0.0
        holdings = None 
        
        params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "WCRC_FRCR_DVSN_CD": "02", "NATN_CD": "840", "TR_MKET_CD": "00", "INQR_DVSN_CD": "00"}
        res = self._call_api("CTRP6504R", "/uapi/overseas-stock/v1/trading/inquire-present-balance", "GET", params=params)
        
        if res.get('rt_cd') == '0':
            o2 = res.get('output2')
            if isinstance(o2, list):
                o2 = o2[0] if len(o2) > 0 else {}
            elif not isinstance(o2, dict):
                o2 = {}
            
            dncl_amt = self._safe_float(o2.get('frcr_dncl_amt_2', 0))       
            sll_amt = self._safe_float(o2.get('frcr_sll_amt_smtl', 0))      
            buy_amt = self._safe_float(o2.get('frcr_buy_amt_smtl', 0))      
            
            raw_bp = dncl_amt + sll_amt - buy_amt
            cash = math.floor((raw_bp * 0.9945) * 100) / 100.0              

        params_hold = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": "NASD", "TR_CRCY_CD": "USD", "CTX_AREA_FK200": "", "CTX_AREA_NK200": ""}
        res = self._call_api("TTTS3012R", "/uapi/overseas-stock/v1/trading/inquire-balance", "GET", params_hold)
        
        if res.get('rt_cd') == '0':
            holdings = {} 
            if cash <= 0:
                o2 = res.get('output2')
                if isinstance(o2, list):
                    o2 = o2[0] if len(o2) > 0 else {}
                elif not isinstance(o2, dict):
                    o2 = {}
                cash = self._safe_float(o2.get('ovrs_ord_psbl_amt', 0))
            
            for item in res.get('output1', []):
                ticker = item.get('ovrs_pdno')
                qty = int(self._safe_float(item.get('ovrs_cblc_qty', 0)))
                avg = self._safe_float(item.get('pchs_avg_pric', 0))
                if qty > 0: holdings[ticker] = {'qty': qty, 'avg': avg}
        
        return cash, holdings

    def get_current_price(self, ticker, is_market_closed=False):
        try:
            stock = yf.Ticker(ticker)
            if is_market_closed: return float(stock.fast_info['last_price'])
            hist = stock.history(period="1d", interval="1m", prepost=True)
            if not hist.empty: return float(hist['Close'].iloc[-1])
            else: return float(stock.fast_info['last_price'])
        except Exception as e:
            return 0.0

    def get_historical_close_prices(self, ticker, date_str):
        try:
            if len(date_str) == 8 and date_str.isdigit():
                date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                
            stock = yf.Ticker(ticker)
            start_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
            end_date = start_date + datetime.timedelta(days=1)
            
            daily_df = stock.history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), interval="1d")
            if daily_df.empty: return 0.0
            return round(float(daily_df['Close'].iloc[-1]), 2)
        except Exception as e:
            return 0.0

    def get_previous_close(self, ticker):
        try: return yf.Ticker(ticker).fast_info['previous_close']
        except: return 0.0

    def get_5day_ma(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="10d") 
            if len(hist) >= 5: return float(hist['Close'][-5:].mean())
            return 0.0
        except: return 0.0

    def check_recent_split(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            splits = stock.splits
            if not splits.empty:
                splits.index = splits.index.tz_localize(None)
                thirty_days_ago = datetime.datetime.now() - datetime.timedelta(days=30)
                recent_splits = splits[splits.index >= thirty_days_ago]
                if not recent_splits.empty: return float(recent_splits.iloc[-1])
        except Exception as e: pass
        return 0.0

    def get_unfilled_orders(self, ticker):
        params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": "NASD", "SORT_SQN": "DS", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""}
        res = self._call_api("TTTS3018R", "/uapi/overseas-stock/v1/trading/inquire-nccs", "GET", params=params)
        if res.get('rt_cd') == '0':
            return [item.get('odno') for item in res.get('output', []) if item.get('pdno') == ticker]
        return []

    def cancel_all_orders_safe(self, ticker):
        for i in range(3):
            orders = self.get_unfilled_orders(ticker)
            if not orders: return True
            for oid in orders: self.cancel_order(ticker, oid)
            time.sleep(5)
        return not bool(self.get_unfilled_orders(ticker))

    def send_order(self, ticker, side, qty, price, order_type="LIMIT"):
        tr_id = "TTTT1002U" if side == "BUY" else "TTTT1006U"
        excg_cd = "AMEX" if ticker == "SOXL" else "NASD"

        if order_type == "LOC": ord_dvsn = "34"
        elif order_type == "MOC": ord_dvsn = "33"
        elif order_type == "LOO": ord_dvsn = "02"
        elif order_type == "MOO": ord_dvsn = "31"
        else: ord_dvsn = "00"

        final_price = self._ceil_2(price)
        if order_type in ["MOC", "MOO"]: final_price = 0
        
        body = {
            "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd,
            "PDNO": ticker, "ORD_QTY": str(int(qty)), "OVRS_ORD_UNPR": str(final_price),
            "ORD_SVR_DVSN_CD": "0", "ORD_DVSN": ord_dvsn 
        }
        res = self._call_api(tr_id, "/uapi/overseas-stock/v1/trading/order", "POST", body=body)
        return {'rt_cd': res.get('rt_cd'), 'msg1': res.get('msg1')}

    def cancel_order(self, ticker, order_id):
        excg_cd = "AMEX" if ticker == "SOXL" else "NASD"
        body = {
            "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd,
            "PDNO": ticker, "ORGN_ODNO": order_id, "RVSE_CNCL_DVSN_CD": "02",
            "ORD_QTY": "0", "OVRS_ORD_UNPR": "0", "ORD_SVR_DVSN_CD": "0"
        }
        self._call_api("TTTS1004U", "/uapi/overseas-stock/v1/trading/order-rvsecncl", "POST", body=body)

    def get_execution_history(self, ticker, start_date, end_date):
        excg_cd = "AMEX" if ticker == "SOXL" else "NASD"
        valid_execs = []
        fk200 = ""
        nk200 = ""
        
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
                    if float(item.get('ft_ccld_qty', '0')) > 0:
                        valid_execs.append(item)
                        
                tr_cont = res.headers.get('tr_cont', '')
                fk200 = resp_json.get('ctx_area_fk200', '').strip()
                nk200 = resp_json.get('ctx_area_nk200', '').strip()
                
                if tr_cont in ['M', 'F'] and nk200:
                    time.sleep(0.3) 
                    continue
                else: break 
            else:
                error_msg = resp_json.get('msg1') if resp_json else "응답 없음"
                print(f"❌ [{ticker} 체결내역 오류] {error_msg}")
                break
        return valid_execs

    # 🚀 [V15.7] 제네시스 역산 엔진 (액면분할 대비 서킷 브레이커 탑재)
    def get_genesis_ledger(self, ticker, limit_date_str=None):
        _, holdings = self.get_account_balance()
        if holdings is None: return None, 0, 0.0
            
        ticker_info = holdings.get(ticker, {'qty': 0, 'avg': 0.0})
        curr_qty = int(ticker_info.get('qty', 0))
        final_qty = curr_qty
        final_avg = float(ticker_info.get('avg', 0.0))
        
        if curr_qty == 0: return [], 0, 0.0
            
        ledger_records = []
        target_date = datetime.datetime.now()
        genesis_reached = False
        
        while curr_qty > 0 and not genesis_reached:
            date_str = target_date.strftime('%Y%m%d')
            
            # 액면분할 방어 서킷 브레이커: 스냅샷 기준 날짜를 넘어 과거로 갔는데도 수량이 남아있으면 중단
            if limit_date_str and date_str < limit_date_str:
                return "CIRCUIT_BREAKER", final_qty, final_avg
                
            execs = self.get_execution_history(ticker, date_str, date_str)
            
            if execs:
                execs.sort(key=lambda x: x.get('ord_tmd', '000000'), reverse=True)
                for ex in execs:
                    side_cd = ex.get('sll_buy_dvsn_cd')
                    exec_qty = int(float(ex.get('ft_ccld_qty', '0')))
                    exec_price = float(ex.get('ft_ccld_unpr3', '0'))
                    
                    # 💡 [V15.4] 오버슈팅(Overshoot) 절단 로직
                    # 이전 사이클의 물량까지 긁어와서 장부를 오염시키는 무한루프 버그를 원천 차단합니다.
                    record_qty = exec_qty
                    
                    if side_cd == "02": # 매수 (과거로 가며 차감)
                        if curr_qty <= exec_qty: 
                            record_qty = curr_qty # 남은 수량만큼만 딱 맞게 잘라서 기록!
                            curr_qty = 0
                            genesis_reached = True
                        else:
                            curr_qty -= exec_qty
                    else: # 매도 (과거로 가며 더함)
                        curr_qty += exec_qty
                    
                    ledger_records.append({
                        'date': f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}",
                        'side': "BUY" if side_cd == "02" else "SELL",
                        'qty': record_qty,
                        'price': exec_price
                    })
                    
                    if genesis_reached:
                        break
                        
            target_date -= datetime.timedelta(days=1)
            time.sleep(0.1) 
            if (datetime.datetime.now() - target_date).days > 365: break 
                
        ledger_records.reverse()
        return ledger_records, final_qty, final_avg

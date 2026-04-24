# ==========================================================
# [queue_ledger.py]
# ⚠️ 신규 역추세 엔진(V_REV) 전용 LIFO 로트(Lot) 장부 관리 모듈
# 💡 [핵심 수술] 수량 동기화(CALIB) 및 Pop 차감 로직 내 Safe Casting (None 방어) 전면 이식 완료
# 🚨 [V27.02 핫픽스] 동일 일자(Same Day) 로트(Lot) 파편화 방지 및 자동 병합(Merge) 엔진 탑재
# 🚨 [V27.02 핫픽스] CALIB_ADD (보정 추가) 시 평단가 $0.00 붕괴 버그 원천 차단
# 🚨 [V27.14 그랜드 수술] 코파일럿 합작 - Atomic Write(장부 증발 방어), Thread Lock(동시접근 덮어쓰기 차단), 유령 로트(0주) 무한루프 소각, EST 타임존 병합 통일 및 백업 자가 치유(Self-Healing) 파이프라인 완벽 구축
# 🚨 [V27.15 핫픽스] 초기화 Torn Write 방어, add_lot $0.00 주입 차단, pop_lots 미달 차감 감사 추적 및 sync_with_broker 런타임 붕괴 방어막 이식
# MODIFIED: [V30.09 핫픽스] pytz 영구 적출 및 ZoneInfo('America/New_York') 이식으로 LMT 버그 차단
# ==========================================================
import os
import json
import time
import math
import threading
import shutil
# MODIFIED: [V30.09 핫픽스] LMT 오차 방어를 위해 pytz를 적출하고 ZoneInfo 도입
# import pytz
from zoneinfo import ZoneInfo
from datetime import datetime
import logging

class QueueLedger:
    def __init__(self, file_path="data/queue_ledger.json"):
        self.file_path = file_path
        # 🚨 [수술 완료] 동시 접근(Race Condition) 덮어쓰기 방지를 위한 스레드 락 도입
        self._lock = threading.Lock()
        self._ensure_file()

    def _ensure_file(self):
        dir_name = os.path.dirname(self.file_path)
        if dir_name and not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
        if not os.path.exists(self.file_path):
            # MODIFIED: [초기화 동시성 붕괴(Torn Write) 방어] 스레드 락 획득 후 안전하게 장부 파일 생성 (Double-checked locking)
            with self._lock:
                if not os.path.exists(self.file_path):
                    self._save_unsafe({})

    def _get_trading_date_str(self):
        # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo 이식
        est = ZoneInfo('America/New_York')
        return datetime.now(est).strftime("%Y-%m-%d")

    def _load_unsafe(self):
        """Must be called while holding self._lock."""
        last_exc = None
        for _ in range(3):
            try:
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if not content.strip():
                        return {} 
                    return json.loads(content)
            except json.JSONDecodeError as e:
                last_exc = e
                # 🚨 [수술 완료] JSON 파손 시 침묵하지 않고 즉시 루프 탈출 후 백업 복원 시도
                break
            except FileNotFoundError:
                return {}
            except Exception as e:
                last_exc = e
                time.sleep(0.1)
        
        # 🚨 [수술 완료] JSON 파일 손상 시 백업본 자동 복원 (Self-Healing)
        backup_path = self.file_path + ".bak"
        if os.path.exists(backup_path):
            logging.error(f"🚨 [QueueLedger] JSON 손상 감지. 백업 파일에서 복원 시도: {backup_path}")
            try:
                with open(backup_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as be:
                logging.error(f"🚨 [QueueLedger] 백업 복원도 실패: {be}")
        
        # 백업마저 실패하면 봇을 멈춰서 이중 매수/매도 대참사를 막음
        raise RuntimeError(f"🚨 [FATAL ERROR] {self.file_path} 장부 파일 읽기 실패. 데이터 유실 방지를 위해 시스템을 중단합니다. 원인: {last_exc}")

    def _save_unsafe(self, data):
        """Must be called while holding self._lock."""
        tmp_path = self.file_path + ".tmp"
        for attempt in range(3):
            try:
                # 🚨 [수술 완료] Atomic Write: 임시 파일에 먼저 쓰고 원자적 이름 변경(Rename)으로 장부 증발 원천 차단
                with open(tmp_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, self.file_path)
                
                # 성공 시 백업본 생성
                try: shutil.copy2(self.file_path, self.file_path + ".bak")
                except Exception: pass
                
                return
            except Exception as e:
                logging.warning(f"⚠️ [QueueLedger] 장부 저장 재시도 ({attempt+1}/3): {e}")
                time.sleep(0.1)
                
        try:
            if os.path.exists(tmp_path): os.remove(tmp_path)
        except: pass
        logging.error(f"🚨 [QueueLedger] 장부 저장 최종 실패: {self.file_path} — 데이터 유실 위험!")

    def get_queue(self, ticker):
        with self._lock:
            data = self._load_unsafe()
            q = data.get(ticker, [])
            # 🚨 [수술 완료] 유령 로트(0주) 선제적 청소
            return [lot for lot in q if int(float(lot.get("qty") or 0)) > 0]

    def get_total_qty(self, ticker):
        q = self.get_queue(ticker)
        return sum(int(float(item.get("qty") or 0)) for item in q)

    def add_lot(self, ticker, qty, price, lot_type="NORMAL"):
        qty = int(float(qty or 0))
        if qty <= 0: return
        
        # NEW: [add_lot 침묵하는 $0.00 로트 생성 방어] 가격 검증을 락 획득 이전에 수행하여 불량 데이터 유입 조기 차단
        price_f = float(price or 0.0)
        if price_f <= 0.0:
            logging.error(f"🚨 [QueueLedger] add_lot 중단: {ticker} — 유효하지 않은 매수 가격 (price={price}). 로트 추가 취소.")
            return
            
        with self._lock:
            data = self._load_unsafe()
            q = data.get(ticker, [])
            q = [lot for lot in q if int(float(lot.get("qty") or 0)) > 0] # 유령 로트 청소
            
            today_str = self._get_trading_date_str()
            
            if q and q[-1].get("date", "").startswith(today_str):
                old_qty = int(float(q[-1].get("qty", 0)))
                old_price = float(q[-1].get("price", 0.0))
                
                new_qty = old_qty + qty
                # MODIFIED: 검증 완료된 price_f 적용
                new_price = ((old_qty * old_price) + (qty * price_f)) / new_qty if new_qty > 0 else 0.0
                
                q[-1]["qty"] = new_qty
                q[-1]["price"] = round(new_price, 4)
                # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo 이식
                q[-1]["date"] = datetime.now(ZoneInfo('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")
            else:
                q.append({
                    "qty": qty,
                    "price": price_f, # MODIFIED: 검증 완료된 price_f 적용
                    # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo 이식
                    "date": datetime.now(ZoneInfo('America/New_York')).strftime("%Y-%m-%d %H:%M:%S"),
                    "type": lot_type
                })
                
            data[ticker] = q
            self._save_unsafe(data)

    def pop_lots(self, ticker, target_qty):
        # NEW: [pop_lots 미달 차감 감사 추적] 원본 요청 수량 보존 및 검증 변수 추가
        original_target = int(float(target_qty or 0))
        target_qty = original_target
        if target_qty <= 0: return 0
        
        with self._lock:
            data = self._load_unsafe()
            q = data.get(ticker, [])
            q = [lot for lot in q if int(float(lot.get("qty") or 0)) > 0] # 유령 로트 청소
            
            popped_total = 0

            while q and target_qty > 0:
                last_lot = q[-1]
                lot_qty = int(float(last_lot.get("qty") or 0))
                
                # 🚨 방어: 만약 0주짜리가 섞여있다면 쳐내고 계속
                if lot_qty == 0:
                    q.pop()
                    continue
                    
                if lot_qty <= target_qty:
                    popped = q.pop()
                    popped_qty = int(float(popped.get("qty") or 0))
                    popped_total += popped_qty
                    target_qty -= popped_qty
                else:
                    last_lot["qty"] = lot_qty - target_qty
                    popped_total += target_qty
                    target_qty = 0

            # NEW: [pop_lots 미달 차감 경고 로직] 실제 차감량과 요청량 불일치 시 강력한 로그(Audit Trail) 배출
            if popped_total < original_target:
                logging.error(f"🚨 [QueueLedger] pop_lots 미달: {ticker} — 요청 {original_target}주 중 {popped_total}주만 차감. 브로커 매도 수량과 장부 불일치 가능성. 즉시 sync_with_broker 실행 권고.")

            data[ticker] = q
            self._save_unsafe(data)
            return popped_total

    def sync_with_broker(self, ticker, actual_qty, actual_avg=0.0):
        with self._lock:
            data = self._load_unsafe()
            q = data.get(ticker, [])
            q = [lot for lot in q if int(float(lot.get("qty") or 0)) > 0] # 유령 로트 청소
            
            current_q_qty = sum(int(float(item.get("qty") or 0)) for item in q)
            actual_qty = int(float(actual_qty or 0))

            if current_q_qty == actual_qty:
                return False 

            today_str = self._get_trading_date_str()

            if current_q_qty < actual_qty:
                diff = actual_qty - current_q_qty
                
                # MODIFIED: [float(None) 런타임 붕괴 방어] 결측치(None) 유입 시 TypeError 즉사 방어 (Safe Casting)
                calib_price = float(actual_avg or 0.0)
                
                if calib_price <= 0.0:
                    calib_price = float(q[-1].get("price", 0.0)) if q else 0.0
                
                # NEW: [$0.00 평단가 유령 로트 주입 방어] 가격 확보 최종 실패 시 독극물(0달러 로트) 주입을 원천 차단
                if calib_price <= 0.0:
                    logging.error(f"🚨 [QueueLedger] sync_with_broker CALIB_ADD 중단: {ticker} — 실제 평단가 불명 (actual_avg={actual_avg}). $0 로트 주입 방지.")
                    data[ticker] = q
                    self._save_unsafe(data)
                    return True
                
                if q and q[-1].get("date", "").startswith(today_str):
                    old_qty = int(float(q[-1].get("qty", 0)))
                    old_price = float(q[-1].get("price", 0.0))
                    
                    new_qty = old_qty + diff
                    new_price = ((old_qty * old_price) + (diff * calib_price)) / new_qty if new_qty > 0 else 0.0
                    
                    q[-1]["qty"] = new_qty
                    q[-1]["price"] = round(new_price, 4)
                    # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo 이식
                    q[-1]["date"] = datetime.now(ZoneInfo('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    q.append({
                        "qty": diff,
                        "price": round(calib_price, 4), 
                        # MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo 이식
                        "date": datetime.now(ZoneInfo('America/New_York')).strftime("%Y-%m-%d %H:%M:%S"),
                        "type": "CALIB_ADD"
                    })
            else:
                diff = current_q_qty - actual_qty
                
                while q and diff > 0:
                    last_lot = q[-1]
                    lot_qty = int(float(last_lot.get("qty") or 0))
                    
                    if lot_qty == 0:
                        q.pop()
                        continue
                        
                    if lot_qty <= diff:
                        q.pop()
                        diff -= lot_qty # 🚨 [수술 완료] 무한 루프 방지
                    else:
                        last_lot["qty"] = lot_qty - diff
                        diff = 0
                        
                if diff > 0:
                    logging.warning(f"⚠️ [QueueLedger] sync_with_broker CALIB_SUB 미달: {ticker} 큐 물량이 브로커보다 {diff}주 부족합니다. 큐가 초기화되었습니다.")

            data[ticker] = q
            self._save_unsafe(data)
            return True

# ==========================================================
# [vwap_strategy.py]
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# ==========================================================
import math
import os
import json
import pytz
from datetime import datetime

class VwapStrategy:
    def __init__(self, config):
        self.cfg = config
        
        # 💡 [V23.01 멀티 코어 엔진] 백테스트 기반 종목별 1년 평균 U-Curve 가중치 딕셔너리
        self.raw_profiles = {
            "SOXL": [
                0.0308, 0.0220, 0.0190, 0.0228, 0.0179,
                0.0191, 0.0199, 0.0190, 0.0187, 0.0213,
                0.0216, 0.0234, 0.0222, 0.0212, 0.0211,
                0.0231, 0.0234, 0.0226, 0.0215, 0.0223,
                0.0518, 0.0361, 0.0369, 0.0400, 0.0655,
                0.0661, 0.0365, 0.0394, 0.0503, 0.1447
            ],
            "TQQQ": [
                0.0292, 0.0249, 0.0231, 0.0225, 0.0237,
                0.0222, 0.0253, 0.0242, 0.0223, 0.0184,
                0.0265, 0.0253, 0.0218, 0.0212, 0.0220,
                0.0273, 0.0230, 0.0246, 0.0240, 0.0286,
                0.0628, 0.0354, 0.0384, 0.0373, 0.0624,
                0.0564, 0.0321, 0.0382, 0.0441, 0.1129
            ]
        }
        
        # 기본값 (등록되지 않은 종목이 들어올 경우 S&P500 범용 유동성 프로파일 사용)
        self.default_profile = [
            0.010, 0.011, 0.012, 0.013, 0.014,
            0.015, 0.016, 0.018, 0.020, 0.022,
            0.025, 0.028, 0.031, 0.035, 0.039,
            0.043, 0.048, 0.053, 0.059, 0.065,
            0.071, 0.078, 0.085, 0.093, 0.101,
            0.110, 0.120, 0.131, 0.143, 0.160
        ]

    def _check_sniper_sell_lockdown(self, ticker):
        """스나이퍼 1/4 쿼터 매도 당일 성공 여부를 원자적 캐시에서 확인하여 제논의 역설(다중 매도) 방어"""
        flag_file = f"cache_sniper_sell_{ticker}.json"
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        if os.path.exists(flag_file):
            try:
                with open(flag_file, 'r') as f:
                    data = json.load(f)
                    if data.get("date") == today_str and data.get("QUARTER_SELL_COMPLETED"):
                        return True
            except Exception:
                pass
        return False

    def _get_vol_profile(self, ticker):
        """종목별 가중치를 가져오고 합이 1.0이 되도록 정규화(Normalization)"""
        raw_profile = self.raw_profiles.get(ticker, self.default_profile)
        total_weight = sum(raw_profile)
        return [round(w / total_weight, 4) for w in raw_profile]

    def _get_current_bin_index(self):
        est = pytz.timezone('US/Eastern')
        now = datetime.now(est)
        
        # 💡 장 마감 30분 전 타임 윈도우 락온 검증
        if now.hour == 15 and 30 <= now.minute <= 59:
            return now.minute - 30
        return -1

    def get_vwap_plan(self, ticker, current_price, remaining_target, side="BUY"):
        """
        [VWAP 동적 슬라이싱 엔진]
        remaining_target: BUY일 경우 '남은 매수 예산(USD)', SELL일 경우 '남은 매도 수량(주)'
        """
        bin_idx = self._get_current_bin_index()
        
        if bin_idx == -1 or current_price <= 0:
            return {
                "orders": [], 
                "process_status": "⏳VWAP대기/종료", 
                "allocated_qty": 0,
                "bin_weight": 0.0
            }

        # 🛡️ 2차/3차 방어막: 당일 상방 스나이퍼 격발 이력이 존재할 경우 VWAP 매도 엔진 락다운 처리
        if side == "SELL" and self._check_sniper_sell_lockdown(ticker):
            return {
                "orders": [], 
                "process_status": "⛔VWAP매도락다운(스나이퍼명중)", 
                "allocated_qty": 0,
                "bin_weight": 0.0
            }
            
        # 💡 종목(ticker)에 맞는 가중치 프로파일 동적 로드
        vol_profile = self._get_vol_profile(ticker)
        current_weight = vol_profile[bin_idx]
        remaining_weight = sum(vol_profile[bin_idx:])
        
        # 💡 ZeroDivision 방어
        if remaining_weight <= 0:
            remaining_weight = 1.0
            
        # 💡 남은 시간 대비 현재 분(Minute)의 상대적 할당 비율 연산
        slice_ratio = current_weight / remaining_weight
        
        orders = []
        process_status = f"🎯VWAP({bin_idx+1}/30분)"
        allocated_qty = 0
        
        if side == "BUY":
            # 예산 기반 분할 (Budget Slicing)
            slice_budget = remaining_target * slice_ratio
            allocated_qty = math.floor(slice_budget / current_price)
            
            if allocated_qty > 0:
                # 💡 실시간 1호가 정밀 타격용. 기준가는 순수 현재가로 세팅
                safe_price = max(0.01, round(current_price, 2)) 
                orders.append({
                    "side": "BUY", 
                    "price": safe_price, 
                    "qty": allocated_qty, 
                    "type": "LIMIT", 
                    "desc": f"🎯VWAP매수({bin_idx+1})"
                })
                
        elif side == "SELL":
            # 수량 기반 분할 (Quantity Slicing)
            allocated_qty = math.floor(remaining_target * slice_ratio)
            
            # 자연 종료 원칙 엄수
            if allocated_qty > 0:
                safe_price = max(0.01, round(current_price, 2))
                orders.append({
                    "side": "SELL", 
                    "price": safe_price, 
                    "qty": allocated_qty, 
                    "type": "LIMIT", 
                    "desc": f"🎯VWAP매도({bin_idx+1})"
                })

        return {
            "orders": orders,
            "process_status": process_status,
            "allocated_qty": allocated_qty,
            "bin_weight": current_weight
        }

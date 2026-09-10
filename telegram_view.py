# ==========================================================
# FILE: telegram_view.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 5대 헌법 및 43대 엣지 케이스 완벽 결속 교차 검증 완료.
# 🚨 MODIFIED: [UI 직관성 궁극 교정] 암살자 장부 0주 대기 상태 렌더링 시 'OFF' 표출을 'STANDBY (타점 감시 중 / 0주)'로 100% 팩트 교체하여 엔진 가동 상태(ON)와의 논리적 오해 원천 차단.
# 🚨 MODIFIED: [Phase 4 암살자 장부 상시 렌더링 락온] `create_ledger_dashboard` 메서드 호출 시 암살자 데이터가 0주라도 무조건 "OFF (대기 중 / 0주)"를 표출하여 사용자 팩트 검증을 보조.
# 🚨 MODIFIED: [Phase 4 암살자 지정 예산 렌더링] /settlement 호출 시 표출되는 설정 화면에 사용자가 지정한 '암살자 예산'과 '오버나이트 모드'를 100% 팩트로 표출.
# 🚨 MODIFIED: [Phase 4 인라인 버튼 결속] 암살자 설정 메뉴(SETTLEMENT)에 "🔫 암살자 1회 타격 예산" 및 "🌙 오버나이트 토글" 버튼 주입 완료.
# 🚨 MODIFIED: [Float 정밀도 붕괴 원천 차단] 뷰어 클래스 내에 `_safe_float` 래퍼를 전격 이식하여 파편화된 인라인 캐스팅을 통합하고 NaN/Inf 맹독성 붕괴 원천 차단.
# 🚨 MODIFIED: [Case 16 위반 교정] 이미지 렌더링(create_profit_image) 시 원자적 쓰기 실패에 따른 UnboundLocalError 연쇄 붕괴를 막기 위한 temp_path 스코프 최상단 전진 배치.
# 🚨 MODIFIED: [V14 LOC 전용 수동 제어망 결속] 통합 지시서(create_sync_report) 렌더링 시 V-REV 및 VWAP 모드를 배제하고 오직 V14 LOC 모드에만 수동 전송/취소 버튼이 스위칭 렌더링되도록 팩트 락온.
# 🚨 NEW: [V-REV 전용 수동 제어망 결속] 통합지시서에 V-REV 모드 전용 '1회분 수동매수/수동매도' 버튼 주입 완료. 오리지널(V14) 모드에서는 철저히 격리(Bypass)됨.
# 🚨 NEW: [큐 장부 매뉴얼 이식] get_queue_management_menu 화면에 추가/삭제/수정 등 수동 조작을 위한 명령어 가이드 표출 기능 팩트 결속.
# 🚨 NEW: [암살자 독립 소각망 결속] get_reset_menu에 암살자 전용 소각 버튼 추가 및 get_avwap_reset_confirm_menu 팩트 주입 완료.
# 🚨 MODIFIED: [퇴근 모드 UI 버튼 증발 누수 수술] 당일 매매가 잠금(is_locked=True) 상태일 경우 수동 개입(팻핑거) 버튼을 완벽히 은닉(Bypass)하여 렌더링 무결성 사수 완료.
# ==========================================================
import os
import math
import logging
import datetime 
import tempfile
import html
from zoneinfo import ZoneInfo
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from PIL import Image, ImageDraw, ImageFont

class TelegramView:
    def __init__(self, config=None):
        self.cfg = config
    
        self.bold_font_paths = [
            "NanumGothicBold.ttf", "font_bold.ttf", "font.ttf",
            "/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf", 
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "C:/Windows/Fonts/malgunbd.ttf", "C:/Windows/Fonts/arialbd.ttf",
            "AppleGothic.ttf", "Arial.ttf"
        ]
    
        self.reg_font_paths = [
            "NanumGothic.ttf", "font_reg.ttf", "font.ttf",
            "/usr/share/fonts/truetype/nanum/NanumGothic.ttf", 
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans.ttf",
            "C:/Windows/Fonts/malgun.ttf", "C:/Windows/Fonts/arial.ttf",
            "AppleGothic.ttf", "Arial.ttf"
        ]

    def _safe_float(self, val):
        try:
            f_val = float(str(val or 0.0).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val):
                return 0.0
            return f_val
        except Exception:
            return 0.0

    def _load_best_font(self, font_paths, size):
        for path in font_paths:
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
        return ImageFont.load_default()

    def _safe_draw_text(self, draw, xy, text, font, fill, anchor="mm"):
        try:
            draw.text(xy, str(text), font=font, fill=fill, anchor=anchor)
        except Exception:
            try:
                if anchor == "mm":
                    est_w = 6 * len(str(text))
                    est_h = 10
                    draw.text((xy[0] - est_w / 2, xy[1] - est_h / 2), str(text), font=font, fill=fill)
                else:
                    draw.text(xy, str(text), font=font, fill=fill)
            except Exception:
                pass

    def get_start_message(self, target_hour, season_icon, latest_version):
        est_tz = ZoneInfo('America/New_York')
        is_dst = bool(datetime.datetime.now(est_tz).dst())
        
        dst_state = "🌞서머타임 ON" if is_dst else "❄️서머타임 OFF"
        
        safe_ver = html.escape(str(latest_version))
        msg = f"🌌 <b>[ 옴니 매트릭스 퀀트 엔진 {safe_ver} ]</b>\n"
        msg += "💠 무결성 싱글 롱 모멘텀 (SOXL 전용) & 순수 돌파/추종 데이 트레이딩\n\n"
        
        msg += f"🕒 <b>[ 운영 스케줄 ({dst_state}) ]</b>\n"
        msg += f"🔹 04:00: 🌅 프리장 VWAP 스캔 개시\n"
        msg += f"🔹 09:30: 🔥 정규장 VWAP 초기화 및 스캔\n"
        msg += f"🔹 15:27: ⚖️ V-REV 1분 슬라이싱 타격\n"
        msg += f"🔹 15:59: 🛑 암살자 오버나이트 강제 덤핑\n"
        msg += f"🔹 16:05: 📝 정산 스캔 & 당일 사이클 졸업\n\n"
        
        msg += "🛠 <b>[ 주요 명령어 ]</b>\n"
        msg += "▶️ /sync : 📜 통합 지시서 조회\n"
        msg += "▶️ /record : 📊 장부 동기화 및 조회\n"
        msg += "▶️ /history : 🏆 졸업 명예의 전당\n"
        msg += "▶️ /settlement : ⚙️ 코어스위칭/전술설정\n"
        msg += "▶️ /seed : 💵 개별 시드머니 관리\n"
        msg += "▶️ /ticker : 🔄 운용 종목 선택\n"
        msg += "▶️ /mode : 🎯 상방 스나이퍼 ON/OFF\n"
        msg += "▶️ /version : 🛠️ 버전 및 업데이트 내역\n"
        msg += "▶️ /avwap : 🔫 트레이딩 레이더 관제탑\n"
        msg += "▶️ /log : 🔍 실시간 에러 원격 추출 진단망\n\n"

        msg += "⚠️ /reset : 🔓 비상 해제 메뉴 (당일 잠금 해제 및 장부 소각)\n"
        msg += "┗ 🚨 시드머니 증액 (수동 닻 올리기): 예수금 추가 입금 시 /seed 메뉴에서 해당 종목의 총 시드머니를 상향 업데이트 하십시오.\n\n"
        
        msg += "⚠️ /update : 🚀 시스템 자가 업데이트 (경고: 로컬 코드가 초기화됨)"
        return msg

    def get_update_confirm_menu(self):
        msg = "🚨 <b>[ 시스템 코어 자가 업데이트 (Self-Update) ]</b>\n\n"
        msg += "깃허브(GitHub) 원격 서버에 접속하여 <b>최신 퀀트 엔진 코드</b>를 로컬에 강제로 동기화(Hard Reset)합니다.\n\n"
        msg += "⚠️ <b>[ 파괴적 동기화 경고 ]</b>\n"
        msg += "▫️ 사용자가 직접 수정한 파이버 코드는 <b>전부 초기화</b>됩니다.\n"
        msg += "▫️ 단, 개인 설정(.env)과 장부 데이터(data/ 폴더)는 완벽히 <b>보존</b>됩니다.\n\n"
        msg += "포트폴리오 매니저의 최종 승인을 대기합니다."
        keyboard = [
            [InlineKeyboardButton("🔥 네, 즉시 업데이트를 강행합니다", callback_data="UPDATE:CONFIRM")],
            [InlineKeyboardButton("❌ 아니오, 취소합니다", callback_data="UPDATE:CANCEL")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_reset_menu(self, active_tickers):
        msg = "🔥 <b>[ 비상 초기화 (Reset) 프로토콜 ]</b>\n\n"
        msg += "⚠️ <b>장부 영구 소각 (삼위일체):</b> 본장부, 백업, V-REV 큐, 암살자 데이터를 100% 영구 삭제합니다. HTS로 100% 수동 청산 후 새출발할 때만 격발하십시오.\n"
        msg += "⚠️ <b>암살자 장부 초기화:</b> 수동 개입으로 어긋난 암살자 독립 장부 및 찌꺼기만 제거하고 본진은 안전하게 보존합니다.\n"
        msg += "🔓 <b>잠금 해제:</b> 금일 주문 완료(잠금) 상태를 풀고 추가 격발을 허용합니다.\n"
        
        keyboard = []
        for t in active_tickers:
            safe_t = html.escape(str(t))
            keyboard.append([
                 InlineKeyboardButton(f"🔥 {safe_t} 장부 영구 소각", callback_data=f"RESET:REV:{t}"),
                 InlineKeyboardButton(f"🔓 {safe_t} 당일 잠금 해제", callback_data=f"RESET:LOCK:{t}")
            ])
            keyboard.append([
                 InlineKeyboardButton(f"🔫 {safe_t} 암살자 장부 초기화", callback_data=f"RESET:AVWAP:{t}")
            ])
        keyboard.append([InlineKeyboardButton("❌ 취소 및 닫기", callback_data="RESET:CANCEL")])
        
        return msg, InlineKeyboardMarkup(keyboard)

    def get_reset_confirm_menu(self, ticker):
        safe_t = html.escape(str(ticker))
        msg = f"🚨 <b>[{safe_t} 삼위일체 소각 최종 확인]</b>\n\n"
        msg += f"정말 <b>{safe_t}</b>의 모든 퀀트 장부 데이터를 영구 삭제하시겠습니까?\n"
        msg += "이 작업은 되돌릴 수 없습니다!"
        keyboard = [
            [InlineKeyboardButton("🔥 네, 즉시 영구 소각합니다", callback_data=f"RESET:CONFIRM:{ticker}")],
            [InlineKeyboardButton("❌ 아니오, 취소합니다", callback_data="RESET:CANCEL")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_avwap_reset_confirm_menu(self, ticker):
        safe_t = html.escape(str(ticker))
        msg = f"🚨 <b>[{safe_t} 암살자 장부 소각 최종 확인]</b>\n\n"
        msg += f"정말 <b>{safe_t}</b>의 암살자 독립 장부 및 상태 캐시를 영구 삭제하시겠습니까?\n"
        msg += "▫️ 본진 장부 및 큐(Queue)는 안전하게 보존됩니다.\n"
        msg += "▫️ 수동으로 암살자 물량을 청산하여 찌꺼기를 지울 때만 사용하십시오."
        keyboard = [
            [InlineKeyboardButton("🔥 네, 암살자 정보만 소각합니다", callback_data=f"RESET:AVWAP_CONFIRM:{ticker}")],
            [InlineKeyboardButton("❌ 아니오, 취소합니다", callback_data="RESET:CANCEL")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_queue_management_menu(self, ticker, q_data):
        q_data = q_data or []
        valid_q = [item for item in q_data if isinstance(item, dict)]
        
        safe_t = html.escape(str(ticker))
        msg = f"🗄️ <b>[ {safe_t} V-REV 지층 큐(Queue) 정밀 관리 ]</b>\n\n"
        
        total_q = sum(int(self._safe_float(item.get('qty'))) for item in valid_q)
        total_invested = sum(int(self._safe_float(item.get('qty'))) * self._safe_float(item.get('price')) for item in valid_q)
        avg_p = total_invested / total_q if total_q > 0 else 0.0
        
        msg += f"▫️ 총 보유 지층 : {len(valid_q)} 개 지층\n"
        msg += f"▫️ 총 장전 수량 : {total_q} 주\n"
        msg += f"▫️ 지층 통합 평단가 : ${avg_p:.2f}\n\n"
        msg += "<b>[ LIFO 지층별 상세 (최근 매수 = 1지층) ]</b>\n"
        msg += "<code>지층 일자          수량   평단가\n"
        msg += "-"*30 + "\n"
        
        keyboard = []
        if not valid_q:
            msg += "📭 지층 데이터가 없습니다.\n"
        else:
            for idx, item in enumerate(reversed(valid_q)):
                qty = int(self._safe_float(item.get('qty')))
                price = self._safe_float(item.get('price'))
                item_date = item.get('date')
                layer_num = idx + 1 
                
                if item_date is None:
                    msg += f"⚠️ {layer_num:<3} [날짜 손상] {qty:>4}주 ${price:.2f}\n"
                    keyboard.append([
                        InlineKeyboardButton(f"⚠️ {layer_num}지층 (손상 - 수정 불가)", callback_data=f"QUEUE:VIEW:{ticker}")
                    ])
                else:
                    date_str = str(item_date)[:10] if len(str(item_date)) >= 10 else str(item_date)
                    display_date = date_str[5:] if len(date_str) >= 10 else date_str
                    msg += f"{layer_num:<3} {display_date} {qty:>4}주 ${price:.2f}\n"
                    keyboard.append([
                        InlineKeyboardButton(f"✏️ {layer_num}지층 수정", callback_data=f"EDIT_Q:{ticker}:{item_date}"),
                        InlineKeyboardButton(f"🗑️ {layer_num}지층 삭제", callback_data=f"DEL_REQ:{ticker}:{item_date}")
                    ])
        
        msg += "-"*30 + "</code>\n\n"

        msg += "🛠️ <b>[ 큐 장부 매뉴얼 (수동 조작 명령어) ]</b>\n"
        msg += f"▫️ 단일 지층 추가 : <code>/add_q {safe_t} 2026-07-11 55 192.31</code>\n"
        msg += f"▫️ 전체 장부 초기화 : <code>/clear_q {safe_t}</code>\n"
        msg += f"▫️ 지층 부분 수정 및 삭제 : 상단 <b>[✏️수정] / [🗑️삭제]</b> 버튼 클릭\n\n"

        msg += "🚨 <b>[ 비상 수혈 통제소 ]</b>\n"
        msg += "최근 매수한 <b>1지층</b>을 시장가(MOC)로 강제 덤핑하여 가용 예산을 확보합니다."
        keyboard.append([InlineKeyboardButton("🩸 1지층 수동 긴급 수혈 (MOC)", callback_data=f"EMERGENCY_REQ:{ticker}")])
        keyboard.append([InlineKeyboardButton("🔄 대시보드 새로고침", callback_data=f"QUEUE:VIEW:{ticker}")])
        
        return msg, InlineKeyboardMarkup(keyboard)

    def get_queue_action_confirm_menu(self, ticker, target_date, qty, price):
        safe_t = html.escape(str(ticker))
        short_date = html.escape(str(target_date)[:10]) if len(str(target_date)) >= 10 else html.escape(str(target_date))
        msg = f"🗑️ <b>[{safe_t} 지층 부분 삭제 확인]</b>\n\n"
        msg += f"선택하신 <b>[{short_date}]</b> 지층 (<b>{qty}주 / ${price:.2f}</b>) 데이터를 장부에서 도려내시겠습니까?\n"
        msg += "▫️ 실제 KIS 계좌의 주식은 매도되지 않습니다.\n"
        msg += "▫️ 계좌 수량과 장부가 어긋날 경우 /sync 시 비파괴 보정(CALIB)이 발동됩니다."
        keyboard = [
            [InlineKeyboardButton("🔥 네, 도려냅니다", callback_data=f"DEL_Q:{ticker}:{target_date}")],
            [InlineKeyboardButton("❌ 취소 (돌아가기)", callback_data=f"QUEUE:VIEW:{ticker}")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_emergency_moc_confirm_menu(self, ticker, emergency_qty, emergency_price):
        safe_t = html.escape(str(ticker))
        msg = f"🚨 <b>[{safe_t} 비상 수혈 최종 승인 대기]</b> 🚨\n\n"
        msg += f"가장 최근에 매수한 <b>1지층 {emergency_qty}주</b> (평단 <b>${emergency_price:.2f}</b>)를 KIS 서버로 즉각 시장가(MOC) 강제 매도 전송합니다.\n\n"
        msg += "⚠️ <b>포트폴리오 매니저 경고:</b>\n"
        msg += "1️⃣ 이 작업은 즉각 격발되며 취소할 수 없습니다.\n"
        msg += "2️⃣ 정규장/프리장 운영 시간에만 격발이 승인됩니다.\n"
        msg += "3️⃣ 체결 즉시 해당 지층 기록은 큐(Queue)에서 영구 소각됩니다.\n"
        
        keyboard = [
            [InlineKeyboardButton(f"🔥 [{safe_t}] {emergency_qty}주 강제 수혈 격발", callback_data=f"EMERGENCY_EXEC:{ticker}")],
            [InlineKeyboardButton("❌ 락온 해제 (안전 모드 복귀)", callback_data=f"QUEUE:VIEW:{ticker}")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_avwap_warning_menu(self, ticker):
        safe_t = html.escape(str(ticker))
        
        msg = f"👁️ <b>[{safe_t} 순수 돌파/추종 데이 트레이딩 관제탑 가동 승인]</b>\n\n"
        msg += "⚠️ <b>[ 수동 제어 및 팻핑거 뇌관 100% 소각 완료 ]</b>\n"
        msg += "과거의 복잡했던 휩소 방어(HA 컨펌) 및 수동 목표가/수익률 설정 로직은 시스템 전역에서 영구 소각되었습니다.\n\n"
        msg += "본 모드는 <b>수학적 팩트</b>에 기반한 <b>순수 돌파/추종 (1-Shot 1-Kill)</b> 아키텍처로 자동 가동됩니다.\n\n"
        msg += f"▫️ <b>타점:</b> <b>04:07 EST 타임쉴드 해제 후</b> 실시간 VWAP 상향 돌파 시, <b>매도 1호가 지정가(Software Trigger)</b>로 즉각 요격\n"
        msg += f"▫️ <b>익절:</b> 진입 평단가 기준 <b>+1.0% 고정 지정가</b> 전량 매도망 100% 자동 장전\n"
        msg += "▫️ <b>방어:</b> 15:59 EST 도달 시 미체결 덫 취소 및 매수 1호가 스윕 <b>강제 덤핑 (제로-오버나이트)</b>\n\n"
        msg += "포트폴리오 매니저의 관제탑 가동 승인을 대기합니다."
        
        keyboard = [
            [InlineKeyboardButton("👁️ 관제탑 락온(Lock-on) 가동 승인", callback_data=f"MODE:AVWAP_ON:{ticker}")],
            [InlineKeyboardButton("❌ 작전 취소 (안전 모드 유지)", callback_data="RESET:CANCEL")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_version_message(self, history_data, page_index=None):
        history_data = history_data or []
        ITEMS_PER_PAGE = 5
        
        total_pages = max(1, (len(history_data) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
        current_page = (total_pages - 1) if page_index is None else page_index
        
        if current_page < 0: current_page = 0
        if current_page >= total_pages: current_page = total_pages - 1
            
        start_idx = current_page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        page_items = history_data[start_idx:end_idx]

        msg = "🚀 <b>[ PIPIOS 퀀트 엔진 패치노트 ]</b>\n"
        msg += "▫️ 현재 시스템: <code>V93.00 지정 예산 & 오버나이트 락온 에디션</code>\n\n"
        
        for item in page_items:
            if isinstance(item, str):
                parts = item.split(" ", 2)
                if len(parts) >= 3:
                    ver = html.escape(str(parts[0]))
                    date_str = html.escape(str(parts[1].strip("[]")))
                    desc = html.escape(str(parts[2]))
                else:
                    ver = "V??"
                    date_str = "-"
                    desc = html.escape(str(item))
                msg += f"💠 <b>{ver}</b> ({date_str})\n"
                msg += f"▫️ {desc}\n\n"
            elif isinstance(item, dict):
                ver = html.escape(str(item.get('version', 'V??')))
                date_str = html.escape(str(item.get('date', '-')))
                msg += f"💠 <b>{ver}</b> ({date_str})\n"
                for desc in item.get('desc') or []:
                    msg += f"▫️ {html.escape(str(desc))}\n"
                msg += "\n"
                     
        msg += f"📄 <i>페이지 {current_page + 1} / {total_pages}</i>"

        keyboard = []
        nav_row = []
        if current_page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ 이전", callback_data=f"VERSION:PAGE:{current_page - 1}"))
        if current_page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("다음 ➡️", callback_data=f"VERSION:PAGE:{current_page + 1}"))
        
        if nav_row: keyboard.append(nav_row)
        keyboard.append([InlineKeyboardButton("❌ 닫기", callback_data="RESET:CANCEL")])
        
        return msg, InlineKeyboardMarkup(keyboard)

    def get_settlement_message(self, active_tickers, config, atr_data, tracking_cache=None):
        msg = ""
        keyboard = []
        ver = ""
        is_manual_vwap = False
        fee_rate = 0.0
        icon = ""
        ver_display = ""
        split_cnt = 0
        target_profit = 0.0
        comp_rate = 0.0
        v14_mode_txt = ""

        tracking_cache = tracking_cache or {}
        msg = "⚙️ <b>[ 현재 설정 및 복리 상태 ]</b>\n\n"
        
        active_tickers = active_tickers or []
        for t in active_tickers:
            safe_t = html.escape(str(t))
            ver = str(config.get_version(t) or "")
            is_manual_vwap = getattr(config, 'get_manual_vwap_mode', lambda x: False)(t)
            is_avwap_hybrid = getattr(config, 'get_avwap_hybrid_mode', lambda x: False)(t)
            fee_rate = self._safe_float(getattr(config, 'get_fee', lambda x: 0.25)(t))
            
            avwap_budget = self._safe_float(getattr(config, 'get_avwap_budget', lambda x: 10000.0)(t))
            is_overnight = bool(getattr(config, 'get_avwap_overnight_mode', lambda x: False)(t))
            
            if ver == "V_REV":
                icon, ver_display = "⚖️", "V_REV 역추세 (로컬 1분 VWAP)" 
            else:
                icon = "💎"
                ver_display = "무매4 (로컬 1분 VWAP)" if is_manual_vwap else "무매4 (LOC)" 
                
            split_cnt = int(self._safe_float(config.get_split_count(t)))
            target_profit = self._safe_float(config.get_target_profit(t))
            comp_rate = self._safe_float(config.get_compound_rate(t))
            msg += f"{icon} <b>{safe_t} ({ver_display} 모드)</b>\n"
            
            if ver == "V_REV":
                avwap_status = "🟢 ON (지정 예산 타격)" if is_avwap_hybrid else "⚪ OFF (가동 대기)"
                
                msg += f"▫️ 본진 예산: 총 시드의 15% (고정 할당)\n▫️ 본진 목표: [가상1층]+0.6% / [상위층]+0.5%\n▫️ 자동복리: {comp_rate}% | 수수료: <b>{fee_rate}%</b>\n▫️ 갭 스위칭: <b>🤖 자율주행 (상승장 자동 가동)</b>\n"
                msg += f"▫️ 암살자 타격망: <b>{avwap_status}</b>\n"
                
                if is_avwap_hybrid:
                    msg += f"▫️ 아키텍처: <b>본진(15%)과 암살자 100% 독립 병렬 가동 팩트 락온</b>\n"
                    msg += f"▫️ 암살자 예산: <b>${avwap_budget:,.2f}</b> (초과 시 팻핑거 방어)\n"
                    msg += f"▫️ 암살자 오버나이트: <b>{'🟢 허용' if is_overnight else '🔴 차단 (15:59 덤핑)'}</b>\n"
                    msg += f"▫️ 암살자 타점: <b>04:07 타임쉴드 해제 후 실시간 VWAP 상향 돌파 요격 (1-Shot 1-Kill)</b>\n"
                    msg += f"▫️ 암살자 익절: <b>+1.0% 지정가 전량 익절 (절대 락온)</b>\n"
                    msg += f"▫️ 데이 트레이딩 관제탑: <b>365일 상시 가동 📡</b>\n"
                msg += "⚖️ <b>본진 스탠바이:</b> 15:26 EST 예약 덫 관측 ➔ 15:27 로컬 자체 슬라이싱 가동\n\n" 
            else:
                msg += f"▫️ 분할: {split_cnt}회 | 목표: {target_profit}% | 복리: {comp_rate}%\n▫️ 수수료: <b>{fee_rate}%</b>\n"
                v14_mode_txt = "🕒 자체 1분 슬라이싱 VWAP 엔진" if is_manual_vwap else "📉 LOC 단일 타격 (초안정성)" 
                msg += f"▫️ 집행: <b>{v14_mode_txt}</b>\n\n"
         
            if t == "SOXL":
                keyboard.append([InlineKeyboardButton("💎 오리지널 V14 세팅", callback_data=f"SET_VER:V14:{t}"), InlineKeyboardButton("⚖️ 역추세 V-REV 세팅", callback_data=f"SET_VER:V_REV:{t}")])
            elif t == "TQQQ":
                keyboard.append([InlineKeyboardButton("💎 오리지널 V14 세팅", callback_data=f"SET_VER:V14:{t}")])

            if ver == "V_REV":
                if t == "SOXL": 
                    keyboard.append([InlineKeyboardButton(f"📡 {safe_t} 데이 트레이딩 관제탑 열기", callback_data=f"AVWAP:MENU:{t}")])
                    keyboard.append([InlineKeyboardButton("⚔️ 암살자 ON/OFF 토글", callback_data=f"CONFIG_AVWAP:TOGGLE:{t}")])
                    keyboard.append([
                        InlineKeyboardButton(f"🔫 {safe_t} 암살자 예산", callback_data=f"INPUT:AVWAP_BUDGET:{t}"),
                        InlineKeyboardButton(f"🌙 오버나이트 토글", callback_data=f"CONFIG_AVWAP:TOGGLE_OVERNIGHT:{t}")
                    ])
                    keyboard.append([InlineKeyboardButton(f"💸 {safe_t} 복리", callback_data=f"INPUT:COMPOUND:{t}"), InlineKeyboardButton(f"💳 {safe_t} 수수료", callback_data=f"INPUT:FEE:{t}")])
                    keyboard.append([InlineKeyboardButton(f"✂️ {safe_t} 액면보정", callback_data=f"INPUT:STOCK_SPLIT:{t}")])
                else:
                    keyboard.append([InlineKeyboardButton(f"💸 {safe_t} 복리", callback_data=f"INPUT:COMPOUND:{t}"), InlineKeyboardButton(f"💳 {safe_t} 수수료", callback_data=f"INPUT:FEE:{t}")])
                    keyboard.append([InlineKeyboardButton(f"✂️ {safe_t} 액면보정", callback_data=f"INPUT:STOCK_SPLIT:{t}")])
            else:
                keyboard.append([InlineKeyboardButton(f"⚙️ {safe_t} 분할", callback_data=f"INPUT:SPLIT:{t}"), InlineKeyboardButton(f"🎯 {safe_t} 목표", callback_data=f"INPUT:TARGET:{t}"), InlineKeyboardButton(f"💸 {safe_t} 복리", callback_data=f"INPUT:COMPOUND:{t}")])
                keyboard.append([InlineKeyboardButton(f"✂️ {safe_t} 액면보정", callback_data=f"INPUT:STOCK_SPLIT:{t}"), InlineKeyboardButton(f"💳 {safe_t} 수수료", callback_data=f"INPUT:FEE:{t}")])
    
        return msg, InlineKeyboardMarkup(keyboard)

    def create_sync_report(self, status_text, dst_text, cash, rp_amount, ticker_data, is_trade_active, p_trade_data=None, exchange_rate=None):
        ticker_data = ticker_data or []
        
        safe_status = html.escape(str(status_text))
        safe_dst = html.escape(str(dst_text))
        header_msg = f"📜 <b>[ 통합 지시서 ({safe_status}) ]</b>\n📅 <b>{safe_dst}</b>\n"
        
        header_msg += f"💵 주문가능금액: ${cash:,.2f}\n"
        header_msg += f"🏛️ RP 투자권장: ${rp_amount:,.2f}\n"
        header_msg += "----------------------------\n\n"
        
        keyboard = []
        body_msg = ""
        krw_profit = 0.0

        for t_info in ticker_data:
            if not isinstance(t_info, dict): continue
            
            t = html.escape(str(t_info.get('ticker') or 'UNK'))
            v_mode = str(t_info.get('version') or 'V14')
            is_manual_vwap = bool(t_info.get('is_manual_vwap'))
            
            is_zero_start = bool(t_info.get('is_zero_start'))
            
            safe_seed = self._safe_float(t_info.get('seed') or 0.0)
            safe_one_portion = self._safe_float(t_info.get('one_portion') or 0.0)
            safe_curr = self._safe_float(t_info.get('curr') or 0.0)
            safe_avg = self._safe_float(t_info.get('avg') or 0.0)
            fact_qty = int(self._safe_float(t_info.get('qty') or 0))
            safe_profit_amt = self._safe_float(t_info.get('profit_amt') or 0.0)
            safe_profit_pct = self._safe_float(t_info.get('profit_pct') or 0.0)
            safe_split = self._safe_float(t_info.get('split') or 40.0)
            safe_t_val = self._safe_float(t_info.get('t_val') or 0.0)
            
            v_mode_display = ""
            main_icon = ""
            bdg_txt = ""
            is_rev_logic = bool(t_info.get('is_reverse'))
            
            plan_dict = t_info.get('plan') or {}
            proc_status = html.escape(str(plan_dict.get('process_status') or ''))
            tracking_info = t_info.get('tracking_info') or {}
            
            snap_tag = " <code>[📸락온]</code>" if t_info.get('has_snapshot') else ""
            day_high = self._safe_float(t_info.get('day_high') or 0.0)
            day_low = self._safe_float(t_info.get('day_low') or 0.0)
            prev_close = self._safe_float(t_info.get('prev_close') or 0.0)
            sniper_status_txt = html.escape(str(t_info.get('upward_sniper') or 'OFF'))
            
            if safe_split > 0 and safe_t_val > (safe_split * 1.1):
                body_msg += "⚠️ <b>[🚨 시스템 긴급 경고: 비정상 T값 폭주 감지!]</b>\n"
                body_msg += f"🔎 현재 T값(<b>{safe_t_val:.4f}T</b>)이 설정된 분할수(<b>{int(safe_split)}분할</b>) 초과했습니다!\n"
                body_msg += "🛡️ <b>가동 조치:</b> 마이너스 호가 차단용 절대 하한선($0.01) 방어막 가동 중!\n\n"

            if v_mode == "V_REV":
                v_mode_display = "V_REV 역추세 (로컬 1분 VWAP)" 
                main_icon = "⚖️"
                bdg_txt = f"1회(1배수) 예산: ${safe_one_portion:,.0f}"
            else:
                v_mode_display = "무매4 (로컬 1분 VWAP)" if is_manual_vwap else "무매4 (LOC)" 
                main_icon = "💎"
                bdg_txt = f"당일 예산: ${safe_one_portion:,.0f}"

            if v_mode == "V_REV":
                body_msg += f"{main_icon} <b>[{t}] {v_mode_display}</b>{snap_tag}\n"
                v_rev_q_lots_safe = int(self._safe_float(t_info.get('v_rev_q_lots') or 0))
                v_rev_q_qty_safe = int(self._safe_float(t_info.get('v_rev_q_qty') or 0))
                body_msg += f"📈 큐(Queue): <b>{v_rev_q_lots_safe}개 지층 대기 중 (총 {v_rev_q_qty_safe}주)</b>\n"
            elif is_rev_logic:
                icon = "🩸" if "리버스(긴급수혈)" in proc_status else "🔄"
                bdg_txt = f"리버스 잔금쿼터: ${safe_one_portion:,.0f}"
                body_msg += f"{icon} <b>[{t}] {v_mode_display} 리버스</b>{snap_tag}\n"
                body_msg += f"📈 진행: <b>{safe_t_val:.4f}T / {int(safe_split)}분할</b>\n"
            else:
                body_msg += f"{main_icon} <b>[{t}] {v_mode_display}</b>{snap_tag}\n"
                body_msg += f"📈 진행: <b>{safe_t_val:.4f}T / {int(safe_split)}분할</b>\n"
            
            body_msg += f"💵 총 시드: ${safe_seed:,.0f}\n🛒 <b>{bdg_txt}</b>\n"
            body_msg += f"💰 현재 ${safe_curr:,.2f} / 평단 ${safe_avg:,.2f} ({fact_qty}주)\n"
            
            if prev_close > 0 and day_high > 0 and day_low > 0:
                high_pct = (day_high - prev_close) / prev_close * 100
                low_pct = (day_low - prev_close) / prev_close * 100
                body_msg += f"📈 금일 고가: ${day_high:.2f} ({'+' if high_pct > 0 else ''}{high_pct:.2f}%)\n"
                body_msg += f"📉 금일 저가: ${day_low:.2f} ({'+' if low_pct > 0 else ''}{low_pct:.2f}%)\n"

            sign = "+" if safe_profit_amt >= 0 else "-"
            icon = "🔺" if safe_profit_amt >= 0 else "🔻"
            if exchange_rate and self._safe_float(exchange_rate) > 0:
                krw_profit = abs(safe_profit_amt) * self._safe_float(exchange_rate)
                body_msg += f"{icon} 수익: {sign}{abs(safe_profit_pct):.2f}% ({sign}${abs(safe_profit_amt):,.2f} | {sign}₩{int(krw_profit):,})\n\n"
            else:
                body_msg += f"{icon} 수익: {sign}{abs(safe_profit_pct):.2f}% ({sign}${abs(safe_profit_amt):,.2f})\n\n"
            
            if is_zero_start and sniper_status_txt == "ON": sniper_status_txt = "OFF (0주 락온)"
            
            if v_mode != "V_REV":
                safe_target = self._safe_float(t_info.get('target') or 10.0)
                safe_star_pct = self._safe_float(t_info.get('star_pct') or 0.0)
                safe_star_price = self._safe_float(t_info.get('star_price') or 0.0)

                if is_rev_logic:
                    body_msg += f"⚙️ 🌟 5일선 별지점: ${safe_star_price:.2f} | 🎯감시: {sniper_status_txt}\n"
                else:
                    if fact_qty > 0 and safe_avg > 0:
                        target_price = safe_avg * (1 + safe_target / 100.0)
                        body_msg += f"⚙️ 🎯 익절 목표가: <b>${target_price:.2f}</b> (+{safe_target}%)\n"
                    body_msg += f"⚙️ ⭐ 별지점: {safe_star_pct}% | 🎯감시: {sniper_status_txt}\n"
                
                if sniper_status_txt == "ON":
                    if not is_trade_active:
                        body_msg += "🎯 상방 스나이퍼: 감시 종료 (장마감)\n"
                    elif tracking_info.get('is_trailing', False):
                        trigger_price = self._safe_float(tracking_info.get('trigger_price') or 0.0)
                        peak_price = self._safe_float(tracking_info.get('peak_price') or 0.0)
                        body_msg += f"🎯 상방 추적(${trigger_price:.2f}) 중 (고가: ${peak_price:.2f})\n"
                    else:
                        sn_target = safe_star_price if is_rev_logic else max(safe_star_price, math.ceil(safe_avg * 1.005 * 100) / 100.0)
                        if sn_target > 0: body_msg += f"🎯 상방 스나이퍼: ${sn_target:.2f} 이상 대기\n"
            else:
                body_msg += "⚖️ <b>역추세 LIFO 큐(Queue) 엔진 스탠바이</b>\n"
                body_msg += "⏱️ <b>스케줄:</b> 15:26 EST 로컬 엔진 스탠바이 ➔ 15:27 슬라이싱 타격 (자전거래 차단)\n" 
            
            if v_mode == "V_REV":
                body_msg += "📋 <b>[주문 가이던스 - ⚖️다중 LIFO 제어]</b>\n"
                body_msg += f"⚡ <b>[Gap Hijack 🤖자율주행]</b> 운용종목 갭 이탈 감지 시 잔여예산 스윕 대기\n"
                
                raw_guidance = ""
                plan_orders = plan_dict.get('orders') or []
                
                sell_orders = [o for o in plan_orders if isinstance(o, dict) and str(o.get('side')) == 'SELL']
                buy_orders = [o for o in plan_orders if isinstance(o, dict) and str(o.get('side')) == 'BUY']
                
                if sell_orders:
                    for o in sell_orders:
                        desc = str(o.get('desc', '매도')).split('(')[0]
                        raw_guidance += f" 🔵 {html.escape(desc)} ${self._safe_float(o.get('price')):.2f} <b>{int(self._safe_float(o.get('qty')))}주</b>\n"
                else:
                    raw_guidance += " 🔵 매도: 대기 물량 없음 (관망)\n"
                        
                if buy_orders:
                    for o in buy_orders:
                        desc = str(o.get('desc', '매수'))
                        raw_guidance += f" 🔴 {html.escape(desc)} ${self._safe_float(o.get('price')):.2f} <b>{int(self._safe_float(o.get('qty')))}주</b>\n"
                else:
                    raw_guidance += " 🔴 매수 대기: 타점 연산 대기 중\n"
                    
                body_msg += raw_guidance
            else:
                if is_manual_vwap and not is_rev_logic:
                    body_msg += "⏱️ <b>스케줄:</b> 17:05 KST 선제 덫 장전 ➔ 로컬 1분 VWAP 슬라이싱\n" 
                body_msg += f"📋 <b>[주문 계획 - {proc_status}]</b>\n"
                
                plan_orders = plan_dict.get('orders') or []
                if plan_orders:
                    plan_orders_sorted = sorted(plan_orders, key=lambda x: 1 if str(x.get('side', '')) == 'SELL' else 0)
                    jubjub_orders = [o for o in plan_orders_sorted if isinstance(o, dict) and "🧲줍줍" in str(o.get('desc', ''))]
                    rendered_jubjub = False

                    for o in plan_orders_sorted:
                        if not isinstance(o, dict): continue
                        if "🧲줍줍" in str(o.get('desc', '')):
                            if not rendered_jubjub:
                                if jubjub_orders:
                                    min_price = min(self._safe_float(x.get('price')) for x in jubjub_orders)
                                    max_price = max(self._safe_float(x.get('price')) for x in jubjub_orders)
                                    total_jub_shares = sum(int(self._safe_float(x.get('qty'))) for x in jubjub_orders)
                                    
                                    if min_price == max_price:
                                        price_str = f"${min_price:.2f}"
                                    else:
                                        price_str = f"(${min_price:.2f}~${max_price:.2f})"
                                      
                                    body_msg += f" 🔴 🧲줍줍: <b>{price_str} x {total_jub_shares}주</b> (LOC)\n"
                                rendered_jubjub = True
                            continue
                        
                        ico = "🔴" if str(o.get('side', '')) == 'BUY' else "🔵"
                        safe_desc = html.escape(str(o.get('desc', ''))).replace("🩸", "")
                        if "수혈" in str(o.get('desc', '')): ico = "🩸"
                        type_str = f"({html.escape(str(o.get('type', '')))})" if str(o.get('type', '')) != 'LIMIT' else ""
                        body_msg += f" {ico} {safe_desc}: <b>${self._safe_float(o.get('price')):.2f} x {int(self._safe_float(o.get('qty')))}주</b> {type_str}\n"
                else:
                    body_msg += "  💤 주문 없음 (관망/예산소진)\n"

            if is_trade_active:
                is_locked = t_info.get('is_locked', False)
                if is_locked:
                    body_msg += " (✅ 금일 주문 완료/잠금)\n"
                
                if v_mode != "V_REV" and not is_manual_vwap:
                    if is_locked:
                        keyboard.append([InlineKeyboardButton(f"🛑 {t} 수동 매매 취소", callback_data=f"CANCEL_EXEC:{t}")])
                    else:
                        keyboard.append([InlineKeyboardButton(f"🚀 {t} 수동 강제 전송", callback_data=f"EXEC:{t}")])
                elif v_mode == "V_REV":
                    # 🚨 MODIFIED: [퇴근 모드 UI 버튼 증발 누수 수술] 당일 매매가 잠금(is_locked=True) 상태일 경우 수동 개입(팻핑거) 버튼을 완벽히 은닉(Bypass)하여 렌더링 무결성 사수
                    if not is_locked:
                        keyboard.append([
                            InlineKeyboardButton(f"🟢 {t} 1회분 수동매수", callback_data=f"MANUAL_PORTION:BUY:{t}"),
                            InlineKeyboardButton(f"🔴 {t} 1회분 수동매도", callback_data=f"MANUAL_PORTION:SELL:{t}")
                        ])
            
        final_msg = header_msg + body_msg.strip()
        
        if not is_trade_active:
            final_msg += "\n\n⛔ 장마감/애프터마켓: 주문 불가"
            
        if any(str(t_info.get('version', '')) == 'V_REV' for t_info in ticker_data if isinstance(t_info, dict)):
            final_msg += "\n\n▶️ /avwap : 🔫 데이 트레이딩 레이더 관제탑"

        return final_msg, InlineKeyboardMarkup(keyboard) if keyboard else None

    def get_vrev_mode_selection_menu(self, ticker):
        safe_t = html.escape(str(ticker))
        msg = f"⚠️ <b>[{safe_t} V-REV 역추세 모드 전환]</b>\n\n"
        msg += "V-REV 전략은 로컬 자체 1분 슬라이싱 VWAP 엔진을 통해 1일치 예산을 집행합니다.\n\n"
        msg += "<b>🤖 자체 1분 슬라이싱 VWAP 엔진 (자율주행)</b>\n" 
        msg += "▫️ 15:27 ~ 15:56 EST 구간 누적 가중치 프로파일 기반 Slicing 타격을 집행합니다.\n" 
        msg += "▫️ 봇은 15:27~16:00 EST 구간에서 본종목의 갭(Gap) 이탈을 감시하며, 위급 시 스윕(Sweep) 타격으로 롤을 오버라이드합니다.\n\n" 
        msg += "V-REV 모드 전환을 승인하시겠습니까?"
        
        keyboard = [
            [InlineKeyboardButton("🔥 V-REV 역추세 모드 전환 승인", callback_data=f"SET_VER_CONFIRM:V_REV:{ticker}")],
            [InlineKeyboardButton("❌ 작전 취소 (이전 버전 유지)", callback_data="RESET:CANCEL")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_v14_mode_selection_menu(self, ticker):
        safe_t = html.escape(str(ticker))
        msg = f"💎 <b>[{safe_t} 오리지널 집행 방식 선택]</b>\n\n"
        msg += "오리지널 무한매수법(V14)의 당일 예산 집행 방식을 선택해 주십시오.\n\n"
        msg += "<b>1️⃣ 📉 LOC 방식 (기본)</b>\n▫️ 17:05 KST 선제 LOC 실전 덫 전송\n\n"
        msg += "<b>2️⃣ 🕒 VWAP 방식 (로컬 자체 1분 슬라이싱)</b>\n▫️ 15:27 EST부터 1호가 분할 타격 및 막판 덤핑\n\n" 
        msg += "원하시는 집행 방식을 선택해 주십시오."
        
        keyboard = [
            [InlineKeyboardButton("📉 LOC (종가 일괄 타격)", callback_data=f"SET_VER_CONFIRM:V14_LOC:{ticker}")],
            [InlineKeyboardButton("🕒 VWAP (유동성 분할 타격)", callback_data=f"SET_VER_CONFIRM:V14_VWAP:{ticker}")],
            [InlineKeyboardButton("❌ 작전 취소 (이전 버전 유지)", callback_data="RESET:CANCEL")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_history_delete_confirm_menu(self, hist_id):
        safe_id = html.escape(str(hist_id))
        msg = f"🚨 <b>[졸업 기록 영구 소각 최종 확인]</b>\n\n정말 이 명예의 전당 기록(ID: {safe_id})을 삭제하시겠습니까?\n이 작업은 되돌릴 수 없습니다!"
        keyboard = [
            [InlineKeyboardButton("🔥 네, 즉시 영구 소각합니다", callback_data=f"HIST:DEL_EXEC:{hist_id}")],
            [InlineKeyboardButton("❌ 아니오, 돌아가기", callback_data=f"HIST:VIEW:{hist_id}")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def create_ledger_dashboard(self, ticker, qty, avg, invested, sold, records, t_val, split, is_history=False, is_reverse=False, history_id=None, assassin_data=None):
        safe_t = html.escape(str(ticker))
        groups = {}
        agg_list = []
        report = ""
        profit = 0.0
        pct = 0.0
        keyboard = []

        records = records or []
        valid_records = [r for r in records if isinstance(r, dict)]
        
        for r in valid_records:
            key = (str(r.get('date') or '')[:10], r.get('side'))
            if key not in groups: groups[key] = {'sum_qty': 0, 'sum_cost': 0.0}
            groups[key]['sum_qty'] += int(self._safe_float(r.get('qty')))
            groups[key]['sum_cost'] += (int(self._safe_float(r.get('qty'))) * self._safe_float(r.get('price')))

        for (date, side), data in groups.items():
            if data['sum_qty'] > 0: agg_list.append({'date': date, 'side': side, 'qty': data['sum_qty'], 'avg': data['sum_cost'] / data['sum_qty']})

        agg_list.sort(key=lambda x: x['date'])
        for i, item in enumerate(agg_list): item['no'] = i + 1
        agg_list.reverse()

        report = f"📜 <b>[ {safe_t} {'과거 졸업 기록' if is_history else '일자별 매매'} (총 {len(agg_list)}일) ]</b>\n\n<code>No. 일자   구분  평균단가  수량\n{'-'*30}\n"
        for item in agg_list[:50]: report += f"{item['no']:<3} {item['date'][5:10].replace('-', '.')} {'🔴매수' if item['side'] == 'BUY' else '🔵매도'} ${item['avg']:<6.2f} {item['qty']}주\n"
        if len(agg_list) > 50: report += "... (이전 기록 생략)\n"
        report += f"{'-'*30}</code>\n\n📊 <b>[ 요약 ]</b>\n"
        
        if not is_history:
            if is_reverse: report += f"▪️ 운용 상태 : 🚨 <b>시드 소진 (리버스 가동)</b>\n▪️ 리버스 T값 : <b>{t_val:.4f} T</b>\n"
            else: report += f"▪️ <b>현재 T값 : {t_val:.4f} T</b> ({int(split)}분할)\n"
        report += f"▪️ 보유 수량 : {qty} 주 (평단 ${avg:.2f})\n"
        
        if is_history:
            profit = sold - invested
            pct = (profit/invested*100) if invested > 0 else 0
            report += f"▪️ <b>최종수익: {'+' if profit >= 0 else '-'}${abs(profit):,.2f} ({'+' if profit >= 0 else '-'}{abs(pct):.2f}%)</b>\n"
        report += f"▪️ 총 매수액 : ${invested:,.2f}\n▪️ 총 매도액 : ${sold:,.2f}\n"

        if not is_history and assassin_data is not None:
            a_qty = sum(int(self._safe_float(r.get('qty'))) for r in (assassin_data or []))
            a_inv = sum(int(self._safe_float(r.get('qty'))) * self._safe_float(r.get('price')) for r in (assassin_data or []))
            a_avg = a_inv / a_qty if a_qty > 0 else 0.0
            
            report += f"\n🔫 <b>[ 암살자 (aVWAP) 독립 장부 상태 ]</b>\n"
            if a_qty > 0:
                report += f"▪️ 진행 상태 : <b>ON (교전/오버나이트 중)</b>\n"
                report += f"▪️ 락온 수량 : <b>{a_qty} 주</b>\n"
                report += f"▪️ 진입 평단가: <b>${a_avg:,.2f}</b>\n"
            else:
                report += f"▪️ 진행 상태 : <b>STANDBY (타점 감시 중 / 0주)</b>\n"

        if not is_history:
            other = "TQQQ" if ticker == "SOXL" else "SOXL"
            keyboard.append([InlineKeyboardButton(f"🔄 {other} 장부 조회", callback_data=f"REC:VIEW:{other}")])
            keyboard.append([InlineKeyboardButton(f"🗄️ {safe_t} V-REV 큐 관리", callback_data=f"QUEUE:VIEW:{ticker}")])
            keyboard.append([InlineKeyboardButton("🔙 장부 업데이트", callback_data=f"REC:SYNC:{ticker}")])
        else:
            keyboard.append([InlineKeyboardButton("🖼️ 프리미엄 졸업 카드 발급", callback_data=f"HIST:IMG:{ticker}{f':{history_id}' if history_id else ''}")])
            if history_id:
                keyboard.append([InlineKeyboardButton("🗑️ 졸업 기록 영구 소각", callback_data=f"HIST:DEL_REQ:{history_id}")])
            keyboard.append([InlineKeyboardButton("🔙 역사 목록", callback_data="HIST:LIST")])

        return report, InlineKeyboardMarkup(keyboard)

    def create_profit_image(self, ticker, profit, yield_pct, invested, revenue, end_date):
        W, H, IMG_H = 600, 920, 430
        
        fd = None
        tmp_path = None
        
        try:
            os.makedirs("data", exist_ok=True)
        except OSError:
            pass
            
        f_title = self._load_best_font(self.bold_font_paths, 65)
        f_p = self._load_best_font(self.bold_font_paths, 85)
        f_y = self._load_best_font(self.reg_font_paths, 40)
        f_b_val = self._load_best_font(self.bold_font_paths, 32)
        f_b_lbl = self._load_best_font(self.reg_font_paths, 22)
        
        def apply_overlay(img_canvas):
            draw = ImageDraw.Draw(img_canvas)
            y_title = IMG_H + 60
            draw.rectangle([W/2 - 140, y_title - 45, W/2 + 140, y_title + 45], fill="#2A2F3D")
            self._safe_draw_text(draw, (W/2, y_title), f"{ticker}", font=f_title, fill="white")
            color = "#007AFF" if profit < 0 else "#FF3B30"
            y_profit = y_title + 105
            self._safe_draw_text(draw, (W/2, y_profit), f"{'-' if profit < 0 else '+'}${abs(profit):,.2f}", font=f_p, fill=color)
            y_yield = y_profit + 75
            self._safe_draw_text(draw, (W/2, y_yield), f"YIELD {'-' if profit < 0 else '+'}{abs(yield_pct):,.2f}%", font=f_y, fill=color)
            y_box = y_yield + 60
            draw.rectangle([40, y_box, 290, y_box + 100], fill="#2A2F3D")
            self._safe_draw_text(draw, (165, y_box + 35), f"${invested:,.2f}", font=f_b_val, fill="white")
            self._safe_draw_text(draw, (165, y_box + 75), "TOTAL INVESTED", font=f_b_lbl, fill="#8E8E93")
            draw.rectangle([310, y_box, 560, y_box + 100], fill="#2A2F3D")
            self._safe_draw_text(draw, (435, y_box + 35), f"${revenue:,.2f}", font=f_b_val, fill="white")
            self._safe_draw_text(draw, (435, y_box + 75), "TOTAL REVENUE", font=f_b_lbl, fill="#8E8E93")
            self._safe_draw_text(draw, (W/2, H - 35), f"{end_date}", font=f_b_lbl, fill="#636366")
            return img_canvas

        img = Image.new('RGB', (W, H), color='#1E222D')
        try:
            bg = Image.open("background.png").convert("RGB")
            bg_ratio = bg.width / bg.height
            if bg_ratio > (W / IMG_H):
                bg_res = bg.resize((int(IMG_H * bg_ratio), IMG_H), Image.Resampling.LANCZOS)
                img.paste(bg_res.crop(((bg_res.width - W) // 2, 0, (bg_res.width + W) // 2, IMG_H)), (0, 0))
            else:
                bg_res = bg.resize((W, int(W / bg_ratio)), Image.Resampling.LANCZOS)
                img.paste(bg_res.crop((0, (bg_res.height - IMG_H) // 2, W, (bg_res.height + IMG_H) // 2)), (0, 0))
        except Exception: 
            try:
                ImageDraw.Draw(img).rectangle([0, 0, W, IMG_H], fill="#111217")
            except Exception:
                pass
            
        img = apply_overlay(img)
        fname = f"data/profit_{ticker}.png"
        
        try:
            dir_name = os.path.dirname(fname) or '.'
            fd, tmp_path = tempfile.mkstemp(dir=dir_name, text=False)
            with os.fdopen(fd, 'wb') as f:
                img.save(f, format="PNG", quality=100)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, fname)
            tmp_path = None
        except Exception as e:
            if fd is not None:
                try: os.close(fd)
                except OSError: pass
            if tmp_path:
                try: os.remove(tmp_path)
                except OSError: pass
            raise e
        return fname

    def get_ticker_menu(self, current_tickers):
        keyboard = [
            [InlineKeyboardButton("🚀 오리지널 TQQQ 단독 운용", callback_data="TICKER:TQQQ")],
            [InlineKeyboardButton("🔥 오리지널 SOXL 단독 운용", callback_data="TICKER:SOXL")],
            [InlineKeyboardButton("💎 오리지널 TQQQ + SOXL 듀얼 콤보", callback_data="TICKER:ALL")]
        ]
        current_tickers = current_tickers or []
        safe_tickers = [html.escape(str(t)) for t in current_tickers if isinstance(t, str)]
        return f"🔄 <b>[ 운용 종목 선택 ]</b>\n현재 가동중: <b>{', '.join(safe_tickers)}</b>", InlineKeyboardMarkup(keyboard)

    def format_log_report(self, error_logs):
        error_logs = error_logs or []
        chronological_logs = list(reversed(error_logs))
        header = "🔍 <b>[ 시스템 원격 진단 리포트 (최근 50건) ]</b>\n\n<code>"
        footer = "</code>\n\n✅ <b>[진단 완료]</b>"
        body = ""
        for line in chronological_logs: body += f"{html.escape(str(line))}\n"
        if len(body) > (4000 - len(header) - len(footer)):
             body = "… (글자 수 제한으로 이전 로그 생략) …\n" + body[-(3800 - len(header) - len(footer)):]
        return header + body + footer


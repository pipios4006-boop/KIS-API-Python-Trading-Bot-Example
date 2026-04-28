# ==========================================================
# [telegram_view.py] - 🌟 100% 통합 무결점 완성본 (V31.00) 🌟
# 🚨 MODIFIED: [V42.09 핫픽스] 듀얼 모멘텀 타임쉴드 대기 메시지 시간(10:20 EST)을 서머타임 연동 한국시간(KST 23:20/00:20)으로 팩트 교정 완료.
# 🚨 MODIFIED: [V42.10 핫픽스] 5분 평균 VWAP 갭(Gap) 산출 공식 정방향((실시간-5분평균)/5분평균) 팩트 교정 완료.
# 🚨 MODIFIED: [V42.11 그랜드 핫픽스] 듀얼 모멘텀(Long/Short) 부등호 오염 원천 차단. 5분 평균 > 당일 실시간 = 상승(롱) 로직 팩트 교정 완료.
# 🚨 MODIFIED: [V42.12 그랜드 핫픽스] 부등호 논리 완벽 원상 복구! (당일 > 5분평균 = 상승 롱 / 당일 < 5분평균 = 하락 숏)
# 🚨 MODIFIED: [V42.13 핫픽스] 5분 평균 VWAP 갭 렌더링 수식을 (5분평균-실시간)/실시간으로 교정하여 직관적인 UI(+) 제공.
# 🚨 MODIFIED: [V42.14 핫픽스] 모멘텀 돌파 UI 텍스트 부등호(5분평균 > 당일 = 롱) 팩트 동기화 완료.
# 🚨 MODIFIED: [V42.15 핫픽스] /settlement 및 락온 경고창에 남아있던 과거의 잔재(2%/-6%)를 4.0%/-8.0%로 팩트 교정 완료.
# ==========================================================
import os
import math
import logging
import datetime 
from zoneinfo import ZoneInfo
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from PIL import Image, ImageDraw, ImageFont

class TelegramView:
    def __init__(self):
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
        
        fact_hour = 17 if is_dst else 18
        matrix_time = "23:20" if is_dst else "00:20"
        dst_state = "🌞서머타임 ON" if is_dst else "❄️서머타임 OFF"
        
        msg = f"🌌 [ 옴니 매트릭스 퀀트 엔진 {latest_version} ]\n"
        msg += "💠 무결성 듀얼 모멘텀 (SOXL/SOXS) & V-REV 갭 스위칭\n\n"
        
        msg += f"🕒 [ 운영 스케줄 ({dst_state}) ]\n"
        msg += "🔹 6시간 간격 : 🔑 API 토큰 자동 갱신\n"
        msg += "🔹 10:00 : 📝 확정 정산 스캔 & 졸업 발급\n"
        msg += f"🔹 {fact_hour}:00 : 🔐 매매 초기화 및 변동성 락온\n"
        msg += f"🔹 {fact_hour}:05 : 🌃 통합 주문 자동 실행\n"
        msg += f"🔹 {matrix_time} : 🏛️ 옴니 매트릭스 시장 국면 판별\n\n"
        
        msg += "🛠 [ 주요 명령어 ]\n"
        msg += "▶️ /sync : 📜 통합 지시서 조회\n"
        msg += "▶️ /record : 📊 장부 동기화 및 조회\n"
        msg += "▶️ /history : 🏆 졸업 명예의 전당\n"
        msg += "▶️ /settlement : ⚙️ 코어스위칭/전술설정\n"
        msg += "▶️ /seed : 💵 개별 시드머니 관리\n"
        msg += "▶️ /ticker : 🔄 운용 종목 선택\n"
        msg += "▶️ /mode : 🎯 상방 스나이퍼 ON/OFF\n"
        msg += "▶️ /version : 🛠️ 버전 및 업데이트 내역\n\n"
        
        msg += "⚠️ /reset : 🔓 비상 해제 메뉴 (락/리버스)\n"
        msg += "┗ 🚨 수동 닻 올리기: 예산 부족으로 리버스 진입 후 외화RP매도 등 예수금을 추가 입금하셨다면, 이 메뉴에서 반드시 '리버스 강제 해제' 버튼을 눌러주세요!\n\n"
        
        msg += "⚠️ /update : 🚀 시스템 자가 업데이트 (경고: 로컬 코드가 초기화됨)\n"
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
        msg = "🔥 <b>[ 삼위일체 소각 (Nuke) 프로토콜 ]</b>\n\n"
        msg += "⚠️ <b>경고:</b> 이 기능은 해당 종목의 본장부, 백업장부, 에스크로, V-REV 큐(Queue) 데이터를 100% 영구 삭제합니다.\n"
        msg += "▫️ 실제 계좌의 주식은 매도되지 않습니다.\n"
        msg += "▫️ HTS/MTS에서 수동으로 물량을 완전히 청산한 뒤, 봇을 0주 새출발 모드로 초기화할 때만 격발하십시오.\n\n"
        msg += "🔓 <b>[ 당일 매매 잠금(Lock) 해제 ]</b>\n"
        msg += "▫️ 금일 필수 주문이 완료되어 '잠금'된 상태를 강제로 풀고 추가 격발을 허용합니다.\n"
        
        keyboard = []
        for t in active_tickers:
            keyboard.append([
                InlineKeyboardButton(f"🔥 {t} 장부 영구 소각", callback_data=f"RESET:REV:{t}"),
                InlineKeyboardButton(f"🔓 {t} 당일 잠금 해제", callback_data=f"RESET:LOCK:{t}")
            ])
        keyboard.append([InlineKeyboardButton("❌ 취소 및 닫기", callback_data="RESET:CANCEL")])
        
        return msg, InlineKeyboardMarkup(keyboard)

    def get_reset_confirm_menu(self, ticker):
        msg = f"🚨 <b>[{ticker} 삼위일체 소각 최종 확인]</b>\n\n"
        msg += f"정말 <b>{ticker}</b>의 모든 퀀트 장부 데이터를 영구 삭제하시겠습니까?\n"
        msg += "이 작업은 되돌릴 수 없습니다!"
        
        keyboard = [
            [InlineKeyboardButton("🔥 네, 즉시 영구 소각합니다", callback_data=f"RESET:CONFIRM:{ticker}")],
            [InlineKeyboardButton("❌ 아니오, 취소합니다", callback_data="RESET:CANCEL")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_queue_management_menu(self, ticker, q_data):
        msg = f"🗄️ <b>[ {ticker} V-REV 지층 큐(Queue) 정밀 관리 ]</b>\n\n"
        
        total_q = sum(item.get('qty', 0) for item in q_data)
        total_invested = sum(item.get('qty', 0) * item.get('price', 0.0) for item in q_data)
        avg_p = total_invested / total_q if total_q > 0 else 0.0
        
        msg += f"▫️ 총 보유 로트(Lot) : {len(q_data)} 개 층\n"
        msg += f"▫️ 총 장전 수량 : {total_q} 주\n"
        msg += f"▫️ 큐 통합 평단가 : ${avg_p:.2f}\n\n"
        msg += "<b>[ LIFO 층별 상세 (최근 매수 순) ]</b>\n"
        msg += "<code>No. 일자        수량   평단가\n"
        msg += "-"*30 + "\n"
        
        keyboard = []
        if not q_data:
            msg += "📭 지층 데이터가 없습니다.\n"
        else:
            for idx, item in enumerate(reversed(q_data)):
                qty = item.get('qty', 0)
                price = item.get('price', 0.0)
                item_date = item.get('date')
                real_idx = len(q_data) - idx
                
                if item_date is None:
                    msg += f"⚠️ {real_idx:<3} [날짜 손상] {qty:>4}주 ${price:.2f}\n"
                    keyboard.append([
                        InlineKeyboardButton(f"⚠️ {real_idx}층 (손상 - 수정 불가)", callback_data=f"QUEUE:VIEW:{ticker}")
                    ])
                else:
                    date_str = item_date[:10]
                    msg += f"{real_idx:<3} {date_str[5:]} {qty:>4}주 ${price:.2f}\n"
                    keyboard.append([
                        InlineKeyboardButton(f"✏️ {real_idx}층 수정", callback_data=f"EDIT_Q:{ticker}:{item_date}"),
                        InlineKeyboardButton(f"🗑️ {real_idx}층 삭제", callback_data=f"DEL_REQ:{ticker}:{item_date}")
                    ])
                
        msg += "-"*30 + "</code>\n\n"
        msg += "🚨 <b>[ 비상 수혈 통제소 ]</b>\n"
        msg += "최근 로트(상단 1개 층)를 시장가(MOC)로 강제 덤핑하여 가용 예산을 확보합니다."

        keyboard.append([InlineKeyboardButton("🩸 최근 로트 수동 긴급 수혈 (MOC)", callback_data=f"EMERGENCY_REQ:{ticker}")])
        keyboard.append([InlineKeyboardButton("🔄 대시보드 새로고침", callback_data=f"QUEUE:VIEW:{ticker}")])
        
        return msg, InlineKeyboardMarkup(keyboard)

    def get_queue_action_confirm_menu(self, ticker, target_date, qty, price):
        short_date = target_date[:10]
        msg = f"🗑️ <b>[{ticker} 지층 부분 삭제 확인]</b>\n\n"
        msg += f"선택하신 <b>[{short_date}]</b> 지층 (<b>{qty}주 / ${price:.2f}</b>) 데이터를 장부에서 도려내시겠습니까?\n"
        msg += "▫️ 실제 KIS 계좌의 주식은 매도되지 않습니다.\n"
        msg += "▫️ 계좌 수량과 장부가 어긋날 경우 /sync 시 비파괴 보정(CALIB)이 발동됩니다."
        
        keyboard = [
            [InlineKeyboardButton("🔥 네, 도려냅니다", callback_data=f"DEL_Q:{ticker}:{target_date}")],
            [InlineKeyboardButton("❌ 취소 (돌아가기)", callback_data=f"QUEUE:VIEW:{ticker}")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_emergency_moc_confirm_menu(self, ticker, emergency_qty, emergency_price):
        msg = f"🚨 <b>[{ticker} 비상 수혈 최종 승인 대기]</b> 🚨\n\n"
        msg += f"가장 최근에 물린 로트(Lot) <b>{emergency_qty}주</b> (평단 <b>${emergency_price:.2f}</b>)를 KIS 서버로 즉각 시장가(MOC) 강제 매도 전송합니다.\n\n"
        msg += "⚠️ <b>포트폴리오 매니저 경고:</b>\n"
        msg += "1. 이 작업은 즉각 격발되며 취소할 수 없습니다.\n"
        msg += "2. 정규장/프리장 운영 시간에만 격발이 승인됩니다.\n"
        msg += "3. 체결 즉시 해당 로트 기록은 큐(Queue)에서 영구 소각됩니다.\n"
        
        keyboard = [
            [InlineKeyboardButton(f"🔥 [{ticker}] {emergency_qty}주 강제 수혈 격발", callback_data=f"EMERGENCY_EXEC:{ticker}")],
            [InlineKeyboardButton("❌ 락온 해제 (안전 모드 복귀)", callback_data=f"QUEUE:VIEW:{ticker}")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_avwap_warning_menu(self, ticker):
        msg = f"🛑 <b>[{ticker}] V41 차세대 AVWAP 무장 해제 및 경고</b>\n\n"
        msg += "현재 <b>AVWAP 암살자 모드</b> 가동을 지시하셨습니다.\n"
        msg += "이 전술은 잉여 현금의 100%를 장중 딥매수 모멘텀 타격에 쏟아붓는 초공격형 옵션입니다.\n\n"
        msg += "⚠️ <b>[ 파괴적 제약 사항 (V41 락온) ]</b>\n"
        msg += "1. 기존 V14의 상방 스나이퍼 기능은 즉시 영구 셧다운됩니다.\n"
        msg += "2. V-REV 큐(Queue)와는 물량과 평단가가 100% 분리되어 독립 연산됩니다.\n"
        # 🚨 [V42.15 핫픽스] 8.0% 경고창 팩트 교정
        msg += "3. 손절(-8.0%) 피격 시에도 당일 영구 동결이 해제되고 즉각 다음 타점을 탐색합니다.\n\n"
        msg += "포트폴리오 매니저의 최종 승인을 대기합니다."
        
        keyboard = [
            [InlineKeyboardButton("🔥 리스크 확인. AVWAP 락온(Lock-on) 승인", callback_data=f"MODE:AVWAP_ON:{ticker}")],
            [InlineKeyboardButton("❌ 작전 취소 (안전 모드 유지)", callback_data="RESET:CANCEL")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_avwap_console_menu(self, t):
        msg = f"🔫 <b>[ {t} V41 파격적 VWAP 모멘텀 돌파 콘솔 ]</b>\n\n"
        msg += "💼 <b>현재 가동 모드: [ 무제한 다중 타격 (Multi-Strike) 락온 ]</b>\n"
        msg += f"▫️ 당일 실시간 VWAP이 전일 VWAP과 5분 평균 VWAP을 동시에 돌파하는 <b>강력한 모멘텀</b>에서만 타격합니다.\n"
        msg += f"▫️ 목표 수익 도달 또는 손절(-8.0%) 피격 시에도 <b>쿨다운 없이 즉각 다음 타점을 무제한 스캔</b>합니다.\n"
        msg += f"▫️ <b>[오버나이트 방어]</b> 15:55 EST 타임스탑 강제 청산 시에만 당일 매매가 영구 동결됩니다.\n\n"
        msg += f"🎯 <b>목표 익절가: 진입가 대비 +4.0% (고정)</b>\n"
        msg += f"🚨 <b>하드스탑 컷: 진입가 대비 -8.0% (고정)</b>\n"

        keyboard = [
            [InlineKeyboardButton("🔙 닫기 (설정 락온 완료)", callback_data=f"RESET:CANCEL")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_version_message(self, history_data, page_index=None):
        ITEMS_PER_PAGE = 5
        total_pages = max(1, (len(history_data) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
        
        current_page = (total_pages - 1) if page_index is None else page_index
        
        if current_page < 0:
            current_page = 0
        if current_page >= total_pages:
            current_page = total_pages - 1
            
        start_idx = current_page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        page_items = history_data[start_idx:end_idx]

        msg = "🚀 <b>[ PIPIOS 퀀트 엔진 패치노트 ]</b>\n"
        msg += "▫️ 현재 시스템: <code>V42.15 옴니 매트릭스 듀얼 코어</code>\n\n"
        
        for item in page_items:
            if isinstance(item, str):
                parts = item.split(" ", 2)
                if len(parts) >= 3:
                    ver = parts[0]
                    date_str = parts[1].strip("[]")
                    desc = parts[2]
                else:
                    ver = "V??"
                    date_str = "-"
                    desc = item
                msg += f"💠 <b>{ver}</b> ({date_str})\n"
                msg += f"▫️ {desc}\n\n"
            elif isinstance(item, dict):
                ver = item.get('version', 'V??')
                date_str = item.get('date', '-')
                msg += f"💠 <b>{ver}</b> ({date_str})\n"
                for desc in item.get('desc', []):
                    msg += f"▫️ {desc}\n"
                msg += "\n"
            
        msg += f"📄 <i>페이지 {current_page + 1} / {total_pages}</i>"

        keyboard = []
        nav_row = []
        if current_page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ 이전", callback_data=f"VERSION:PAGE:{current_page - 1}"))
        if current_page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("다음 ➡️", callback_data=f"VERSION:PAGE:{current_page + 1}"))
        
        if nav_row:
            keyboard.append(nav_row)
        keyboard.append([InlineKeyboardButton("❌ 닫기", callback_data="RESET:CANCEL")])
        
        return msg, InlineKeyboardMarkup(keyboard)

    def create_sync_report(self, status_text, dst_text, cash, rp_amount, ticker_data, is_trade_active, p_trade_data=None):
        total_locked = sum(t_info.get('escrow', 0.0) for t_info in ticker_data)
        
        header_msg = f"📜 <b>[ 통합 지시서 ({status_text}) ]</b>\n📅 <b>{dst_text}</b>\n"
        
        if total_locked > 0:
            real_cash = max(0, cash - total_locked)
            header_msg += f"💵 한투 전체 잔고: ${cash:,.2f}\n"
            header_msg += f"🔒 에스크로 격리금: -${total_locked:,.2f}\n"
            header_msg += f"✅ 실질 가용 예산: ${real_cash:,.2f}\n"
        else:
            header_msg += f"💵 주문가능금액: ${cash:,.2f}\n"
            
        header_msg += f"🏛️ RP 투자권장: ${rp_amount:,.2f}\n"
        header_msg += "----------------------------\n\n"
        
        body_msg = ""
        keyboard = []

        avwap_tickers_data = {}
        for t_info in ticker_data:
            if t_info.get('avwap_active', False):
                avwap_tickers_data[t_info['ticker']] = t_info

        for t_info in ticker_data:
            t = t_info['ticker']
            v_mode = t_info['version']
            
            if t == "SOXS":
                continue 
            
            is_manual_vwap = t_info.get('is_manual_vwap', False)
            is_zero_start = t_info.get('is_zero_start', False)
            
            fact_qty = t_info.get('qty', 0)
            if fact_qty == 0 and not is_zero_start:
                is_zero_start = True
                if 'plan' in t_info and 'orders' in t_info['plan']:
                    t_info['plan']['orders'] = []
                    half_budget = (t_info.get('seed', 0.0) * 0.15) * 0.5
                    prev_c = t_info.get('prev_close', 0.0)
                    if prev_c > 0:
                        p1_trigger_fact = round(prev_c / 0.935, 2)
                        p2_trigger_fact = round(prev_c * 0.999, 2)
                        q1 = math.floor(half_budget / p1_trigger_fact)
                        q2 = math.floor(half_budget / p2_trigger_fact)
                        if q1 > 0:
                            t_info['plan']['orders'].append({"side": "BUY", "qty": q1, "price": p1_trigger_fact, "type": "LOC", "desc": "예방적 매수(Buy1)"})
                        if q2 > 0:
                            t_info['plan']['orders'].append({"side": "BUY", "qty": q2, "price": p2_trigger_fact, "type": "LOC", "desc": "예방적 매수(Buy2)"})
            
            if t_info.get('t_val', 0.0) > (t_info.get('split', 40.0) * 1.1):
                body_msg += "⚠️ <b>[🚨 시스템 긴급 경고: 비정상 T값 폭주 감지!]</b>\n"
                body_msg += f"🔎 현재 T값(<b>{t_info['t_val']:.4f}T</b>)이 설정된 분할수(<b>{int(t_info['split'])}분할</b>) 초과했습니다!\n"
                body_msg += "💡 <b>원인 역산 추정:</b> 수동 매수로 수량이 급증했거나, '/seed' 시드머니 설정이 대폭 축소되었습니다.\n"
                body_msg += "🛡️ <b>가동 조치:</b> 마이너스 호가 차단용 절대 하한선($0.01) 방어막 가동 중!\n\n"

            if v_mode == "V_REV":
                v_mode_display = "V_REV 역추세(한투위임)" if is_manual_vwap else "V_REV 역추세(자체엔진)"
                main_icon = "⚖️"
            else:
                v_mode_display = "무매4 (VWAP)" if is_manual_vwap else "무매4 (LOC)"
                main_icon = "💎"
                
            is_rev = t_info.get('is_reverse', False)
            proc_status = t_info.get('plan', {}).get('process_status', '')
            tracking_info = t_info.get('tracking_info', {})
            
            if proc_status == "🩸리버스(긴급수혈)":
                body_msg += f"⚠️ <b>[🚨 비상 상황: {t} 긴급 수혈 중]</b>\n"
                body_msg += "❗ <i>에스크로 금고가 바닥나 강제 매도를 통해 현금을 생성합니다.</i>\n\n"
            
            if is_rev:
                bdg_txt = f"리버스 잔금쿼터: ${t_info['one_portion']:,.0f}"
                icon = "🩸" if proc_status == "🩸리버스(긴급수혈)" else "🔄"
                body_msg += f"{icon} <b>[{t}] {v_mode_display} 리버스</b>\n"
                body_msg += f"📈 진행: <b>{t_info['t_val']:.4f}T / {int(t_info['split'])}분할</b>\n"
            elif v_mode == "V_REV":
                bdg_txt = f"1회(1배수) 예산: ${t_info['one_portion']:,.0f}"
                body_msg += f"{main_icon} <b>[{t}] {v_mode_display}</b>\n"
                body_msg += f"📈 큐(Queue): <b>{t_info.get('v_rev_q_lots', 0)}개 로트 대기 중 (총 {t_info.get('v_rev_q_qty', 0)}주)</b>\n"
            else:
                bdg_txt = f"당일 예산: ${t_info['one_portion']:,.0f}"
                body_msg += f"{main_icon} <b>[{t}] {v_mode_display}</b>\n"
                body_msg += f"📈 진행: <b>{t_info['t_val']:.4f}T / {int(t_info['split'])}분할</b>\n"
            
            body_msg += f"💵 총 시드: ${t_info['seed']:,.0f}\n"
            body_msg += f"🛒 <b>{bdg_txt}</b>\n"
            
            escrow = t_info.get('escrow', 0.0)
            if escrow > 0:
                body_msg += f"🔐 내 금고 보호액: ${escrow:,.2f}\n"
            elif is_rev and proc_status == "🩸리버스(긴급수혈)":
                body_msg += "🔐 내 금고 보호액: $0.00 (Empty 🚨)\n"
                
            body_msg += f"💰 현재 ${t_info['curr']:,.2f} / 평단 ${t_info['avg']:,.2f} ({t_info['qty']}주)\n"
            
            day_high = t_info.get('day_high', 0.0)
            day_low = t_info.get('day_low', 0.0)
            prev_close = t_info.get('prev_close', 0.0)
            
            if prev_close > 0 and day_high > 0 and day_low > 0:
                high_pct = (day_high - prev_close) / prev_close * 100
                low_pct = (day_low - prev_close) / prev_close * 100
                high_sign = "+" if high_pct > 0 else ""
                low_sign = "+" if low_pct > 0 else ""
                body_msg += f"📈 금일 고가: ${day_high:.2f} ({high_sign}{high_pct:.2f}%)\n"
                body_msg += f"📉 금일 저가: ${day_low:.2f} ({low_sign}{low_pct:.2f}%)\n"

            sign = "+" if t_info['profit_amt'] >= 0 else "-"
            icon = "🔺" if t_info['profit_amt'] >= 0 else "🔻"
            body_msg += f"{icon} 수익: {sign}{abs(t_info['profit_pct']):.2f}% ({sign}${abs(t_info['profit_amt']):,.2f})\n\n"
            
            sniper_status_txt = t_info.get('upward_sniper', 'OFF')
            
            if is_zero_start and sniper_status_txt == "ON":
                sniper_status_txt = "OFF (0주 락온)"
            
            if v_mode != "V_REV":
                if is_rev:
                    body_msg += f"⚙️ 🌟 5일선 별지점: ${t_info['star_price']:.2f} | 🎯감시: {sniper_status_txt}\n"
                else:
                    body_msg += f"⚙️ 🎯 {t_info['target']}% | ⭐ {t_info['star_pct']}% | 🎯감시: {sniper_status_txt}\n"
                    
                if sniper_status_txt == "ON":
                    if not is_trade_active:
                        body_msg += "🎯 상방 스나이퍼: 감시 종료 (장마감)\n"
                    elif tracking_info.get('is_trailing', False):
                        peak_price = tracking_info.get('peak_price', 0.0)
                        trigger_price = tracking_info.get('trigger_price', 0.0)
                        body_msg += f"🎯 상방 추적(${trigger_price:.2f}) 중 (고가: ${peak_price:.2f})\n"
                    else:
                        if is_rev:
                            sn_target = t_info['star_price']
                        else:
                            safe_floor = math.ceil(t_info['avg'] * 1.005 * 100) / 100.0
                            sn_target = max(t_info['star_price'], safe_floor)
                        
                        if sn_target > 0:
                            body_msg += f"🎯 상방 스나이퍼: ${sn_target:.2f} 이상 대기\n"
            elif v_mode == "V_REV":
                body_msg += "⚖️ <b>역추세 LIFO 큐(Queue) 엔진 스탠바이</b>\n"
                if is_manual_vwap:
                    body_msg += "⏱️ <b>VWAP 스케줄:</b> <b>(수동) 한투 앱에서 직접 알고리즘 장전 대기</b>\n"
                else:
                    body_msg += "⏱️ <b>VWAP 스케줄:</b> 15:30 EST 앵커 세팅 ➔ 1분 단위 교차 타격\n"
            
            if v_mode == "V_REV":
                body_msg += "📋 <b>[주문 가이던스 - ⚖️다중 LIFO 제어]</b>\n"
                
                plan_info = t_info.get('plan', {})
                omni_msg = plan_info.get('omni_msg', '')
                if omni_msg:
                    body_msg += f"⛔ <b>옴니 매트릭스 락다운:</b> {omni_msg}\n"
                
                body_msg += f"⚡ <b>[Gap Hijack 🤖자율주행]</b> 상승장 판별 시 잔여예산 스윕 대기\n"
                
                raw_guidance = t_info.get('v_rev_guidance', " (가이던스 대기 중)")
                
                if is_zero_start:
                    filtered_lines = [line for line in raw_guidance.split('\n') if "잭팟" not in line and "상위층" not in line]
                    raw_guidance = '\n'.join(filtered_lines)

                raw_guidance = raw_guidance.rstrip('\n')
                body_msg += raw_guidance + "\n\n"

                if is_trade_active:
                    keyboard.append([InlineKeyboardButton(f"🚀 {t} V-REV 방어선 수동 장전", callback_data=f"EXEC:{t}")])
                
            else:
                if is_manual_vwap and not is_rev:
                    body_msg += "⏱️ <b>VWAP 스케줄:</b> 장 마감 30분 전 ➔ 1분 단위 유동성 분할 타격\n"
                    
                plan_info = t_info.get('plan', {})
                omni_msg = plan_info.get('omni_msg', '')
                if omni_msg:
                    body_msg += f"⛔ <b>옴니 매트릭스 락다운:</b> {omni_msg}\n"
                
                body_msg += f"📋 <b>[주문 계획 - {proc_status}]</b>\n"
                plan_orders = plan_info.get('orders', [])
                
                if plan_orders:
                    jup_orders = [o for o in plan_orders if "줍줍" in o.get('desc', '')]
                    n_orders = [o for o in plan_orders if "줍줍" not in o.get('desc', '')]
    
                    for o in n_orders:
                        if not all(k in o for k in ('side', 'desc', 'type', 'price', 'qty')):
                            body_msg += " ⚠️ <i>[렌더링 오류: 주문 데이터 불완전 - 렌더링 스킵]</i>\n"
                            continue
                            
                        ico = "🔴" if o['side'] == 'BUY' else "🔵"
                        desc = o['desc']
                        
                        if "수혈" in desc: 
                            ico = "🩸"
                            desc = desc.replace("🩸", "")
                            
                        type_str = "" if o['type'] == 'LIMIT' else f"({o['type']})"
                        type_disp = f" {type_str}" if type_str else ""
                        
                        body_msg += f" {ico} {desc}: <b>${o['price']} x {o['qty']}주</b>{type_disp}\n"
    
                    if jup_orders:
                        prices = sorted([o['price'] for o in jup_orders if 'price' in o], reverse=True)
                        if prices:
                            body_msg += f" 🧹 줍줍({len(jup_orders)}개): <b>${prices[0]} ~ ${prices[-1]} (LOC)</b>\n"
                    
                    if is_trade_active:
                        if t_info.get('is_locked', False):
                            body_msg += " (✅ 금일 주문 완료/잠금)\n"
                        else:
                            keyboard.append([InlineKeyboardButton(f"🚀 {t} 주문 실행", callback_data=f"EXEC:{t}")])
                else:
                    body_msg += " 💤 주문 없음 (관망/예산소진)\n"
                
            body_msg += "\n"

        final_msg = header_msg + body_msg
        
        est_tz = ZoneInfo('America/New_York')
        is_dst = bool(datetime.datetime.now(est_tz).dst())
        shield_time = "23:20" if is_dst else "00:20"

        if avwap_tickers_data:
            ref_info = avwap_tickers_data.get('SOXL') or list(avwap_tickers_data.values())[0]
            base_tkr = ref_info.get('avwap_base_ticker', 'N/A')
            base_vwap = ref_info.get('avwap_base_vwap', 0.0)
            prev_vwap = ref_info.get('avwap_prev_vwap', 0.0)
            avg_vwap_5m = ref_info.get('avwap_avg_vwap_5m', 0.0) 
            
            final_msg += f"⚔️ <b>[ V41 VWAP 듀얼 모멘텀 암살자 ]</b>\n"
            final_msg += f"▫️ 기초자산(Base): <b>{base_tkr}</b>\n"
            
            if prev_vwap > 0:
                final_msg += f"▫️ 전일 VWAP: ${prev_vwap:,.2f}\n"
                
                rt_gap = ((base_vwap - prev_vwap) / prev_vwap) * 100
                final_msg += f"▫️ 실시간 VWAP: ${base_vwap:,.2f} ({rt_gap:+.2f}%)\n"
                
                if avg_vwap_5m > 0 and base_vwap > 0:
                    avg_5m_gap = ((avg_vwap_5m - base_vwap) / base_vwap) * 100
                    final_msg += f"▫️ 5분 평균 VWAP: ${avg_vwap_5m:,.2f} ({avg_5m_gap:+.2f}%)\n"
                elif avg_vwap_5m > 0:
                    final_msg += f"▫️ 5분 평균 VWAP: ${avg_vwap_5m:,.2f}\n"
            else:
                final_msg += f"▫️ 실시간 VWAP: ${base_vwap:,.2f}\n"
                if avg_vwap_5m > 0:
                    final_msg += f"▫️ 5분 평균 VWAP: ${avg_vwap_5m:,.2f}\n"
                
            for t in ['SOXL', 'SOXS', 'TQQQ']:
                if t in avwap_tickers_data:
                    t_info = avwap_tickers_data[t]
                    avwap_qty = t_info.get('avwap_qty', 0)
                    avwap_avg = t_info.get('avwap_avg', 0.0)
                    avwap_status = t_info.get('avwap_status', f'👀 장초반 10시 필터 대기')
                    
                    if "10:20" in avwap_status:
                        avwap_status = avwap_status.replace("10:20", shield_time)
                        
                    avwap_strikes = t_info.get('avwap_strikes', 0)
                    
                    label = "롱" if t in ["SOXL", "TQQQ"] else "숏"
                    final_msg += f"\n🎯 <b>[ {t} ({label}) ]</b>\n"
                    
                    if avwap_strikes > 0:
                        final_msg += f"💼 <b>다중 출장 모드: {avwap_strikes}회차 교전 완료</b>\n"
                        
                    if prev_vwap > 0:
                        if t == "SOXS":
                            momentum_color = "🟢" if base_vwap < prev_vwap and avg_vwap_5m < base_vwap else "🔴"
                            trend_str = "하락 돌파 (진입허용)" if base_vwap < prev_vwap and avg_vwap_5m < base_vwap else "조건 미달 (대기)"
                            final_msg += f"▫️ 모멘텀 돌파: {momentum_color} {trend_str}\n"
                            final_msg += f" ↳ (당일 &lt; 전일 &amp; 5분평균 &lt; 당일)\n"
                        else:
                            momentum_color = "🟢" if base_vwap > prev_vwap and avg_vwap_5m > base_vwap else "🔴"
                            trend_str = "상승 돌파 (진입허용)" if base_vwap > prev_vwap and avg_vwap_5m > base_vwap else "조건 미달 (대기)"
                            final_msg += f"▫️ 모멘텀 돌파: {momentum_color} {trend_str}\n"
                            final_msg += f" ↳ (당일 &gt; 전일 &amp; 5분평균 &gt; 당일)\n"
                    
                    if t == "SOXS":
                        d_high = t_info.get('day_high', 0.0)
                        d_low = t_info.get('day_low', 0.0)
                        p_close = t_info.get('prev_close', 0.0)
                        final_msg += f"▫️ 현재가: ${t_info.get('curr', 0.0):.2f}\n"
                        if p_close > 0:
                            h_pct = (d_high - p_close) / p_close * 100
                            l_pct = (d_low - p_close) / p_close * 100
                            final_msg += f"▫️ 금일 고가: ${d_high:.2f} ({h_pct:+.2f}%)\n"
                            final_msg += f"▫️ 금일 저가: ${d_low:.2f} ({l_pct:+.2f}%)\n"
                        
                    final_msg += f"▫️ 독립 물량/평단: {avwap_qty}주 / ${avwap_avg:.2f}\n"
                    final_msg += f"▫️ 작전 상태: <b>{avwap_status}</b>\n"
            final_msg += "\n"

        if not is_trade_active:
            final_msg += "💡 <i>※ 현재 표출된 계획은 전일 17:05 기준 박제된 스냅샷이며, 금일 17:05에 최신 팩트 잔고를 바탕으로 리셋됩니다.</i>\n\n"
            final_msg += "⛔ 장마감/애프터마켓: 주문 불가"
            
        return final_msg, InlineKeyboardMarkup(keyboard) if keyboard else None

    def get_settlement_message(self, active_tickers, config, atr_data, dynamic_target_data=None):
        msg = "⚙️ <b>[ 현재 설정 및 복리 상태 ]</b>\n\n"
        keyboard = []
        
        for t in active_tickers:
            ver = config.get_version(t)
            is_manual_vwap = getattr(config, 'get_manual_vwap_mode', lambda x: False)(t)
            fee_rate = getattr(config, 'get_fee', lambda x: 0.25)(t)
            
            if ver == "V_REV":
                icon = "⚖️"
                ver_display = "V_REV 역추세"
            else:
                icon = "💎"
                ver_display = "무매4 (VWAP)" if is_manual_vwap else "무매4 (LOC)"
                
            split_cnt = int(config.get_split_count(t))
            target_pct = config.get_target_profit(t)
            comp_rate = config.get_compound_rate(t)
            
            msg += f"{icon} <b>{t} ({ver_display} 모드)</b>\n"
            
            if ver == "V_REV":
                msg += "▫️ 1회 예산: 총 시드의 15% (고정 할당)\n"
                msg += "▫️ 목표: [1층] 매수단가+0.6%\n"
                msg += "              [상위층] 평단가+0.5% (디커플링)\n"
                msg += f"▫️ 자동복리: {comp_rate}%\n"
                msg += f"▫️ 증권사 수수료: <b>{fee_rate}%</b>\n"
                
                msg += "▫️ 막판 갭 스위칭: <b>🤖 자율주행 (상승장 자동 가동)</b>\n"
                
                if hasattr(config, 'get_avwap_hybrid_mode') and config.get_avwap_hybrid_mode(t):
                    # 🚨 [V42.15 핫픽스] 4.0% 팩트 교정
                    status_label = f"💼 V41 다중 출장 락온 (+4.0% 고정)"
                    msg += f"▫️ AVWAP 암살자: <b>{status_label}</b>\n"
                elif hasattr(config, 'get_avwap_hybrid_mode'):
                    msg += f"▫️ AVWAP 암살자: <b>비활성 (OFF)</b>\n"
                    
                msg += "⚖️ <b>역추세(Reversion) 하이브리드 엔진 스탠바이:</b>\n"
                msg += "▫️ 전일 종가 앵커 기준 LIFO 큐 교차 매매 대기 중\n\n"
            else:
                msg += f"▫️ 분할: {split_cnt}회\n▫️ 목표: {target_pct}%\n▫️ 자동복리: {comp_rate}%\n"
                msg += f"▫️ 증권사 수수료: <b>{fee_rate}%</b>\n"
                v14_mode_txt = "🕒 VWAP 1분 타임 슬라이싱 (자체엔진)" if is_manual_vwap else "📉 LOC 단일 타격 (초안정성)"
                msg += f"▫️ 집행: <b>{v14_mode_txt}</b>\n\n"
                
            if t == "SOXL":
                row1 = [
                    InlineKeyboardButton("💎 오리지널 V14 세팅", callback_data=f"SET_VER:V14:{t}"),
                    InlineKeyboardButton("⚖️ 역추세 V-REV 세팅", callback_data=f"SET_VER:V_REV:{t}")
                ]
            elif t == "TQQQ":
                row1 = [
                    InlineKeyboardButton("💎 오리지널 V14 세팅", callback_data=f"SET_VER:V14:{t}")
                ]
            else:
                row1 = []
                
            if row1:
                keyboard.append(row1)

            if ver == "V_REV":
                is_avwap = config.get_avwap_hybrid_mode(t) if hasattr(config, 'get_avwap_hybrid_mode') else False
                
                avwap_txt = "⚔️ 파격적 AVWAP 모멘텀 [ OFF ]"
                avwap_cb = f"MODE:AVWAP_WARN:{t}" 
                
                if is_avwap:
                    avwap_txt = "⚔️ 파격적 AVWAP 모멘텀 [ 가동중 ]"
                    avwap_cb = f"MODE:AVWAP_OFF:{t}" 
                
                keyboard.append([InlineKeyboardButton(avwap_txt, callback_data=avwap_cb)])
                
                if is_avwap and t == "SOXL":
                    keyboard.append([InlineKeyboardButton(f"🔫 {t} (롱) + SOXS (숏) 모멘텀 콘솔", callback_data=f"AVWAP:MENU:{t}")])
            
            if ver == "V_REV":
                row2 = [
                    InlineKeyboardButton(f"💸 {t} 복리", callback_data=f"INPUT:COMPOUND:{t}"),
                    InlineKeyboardButton(f"💳 {t} 수수료", callback_data=f"INPUT:FEE:{t}")
                ]
                keyboard.append(row2)
                row3 = [
                    InlineKeyboardButton(f"✂️ {t} 액면보정", callback_data=f"INPUT:STOCK_SPLIT:{t}")
                ]
                keyboard.append(row3)
            else:
                row2 = [
                    InlineKeyboardButton(f"⚙️ {t} 분할", callback_data=f"INPUT:SPLIT:{t}"), 
                    InlineKeyboardButton(f"🎯 {t} 목표", callback_data=f"INPUT:TARGET:{t}"),
                    InlineKeyboardButton(f"💸 {t} 복리", callback_data=f"INPUT:COMPOUND:{t}")
                ]
                keyboard.append(row2)
                row3 = [
                    InlineKeyboardButton(f"✂️ {t} 액면보정", callback_data=f"INPUT:STOCK_SPLIT:{t}"),
                    InlineKeyboardButton(f"💳 {t} 수수료", callback_data=f"INPUT:FEE:{t}")
                ]
                keyboard.append(row3)
            
        return msg, InlineKeyboardMarkup(keyboard)

    def get_vrev_mode_selection_menu(self, ticker):
        msg = f"⚠️ <b>[{ticker} 운용 방식 (알고리즘 주체) 선택]</b>\n\n"
        msg += "V-REV 전략의 장 마감 전 VWAP 집행 주체를 선택해 주십시오.\n"
        msg += "(※ 두 방식 모두 한국투자증권 매매 수수료는 동일하게 적용됩니다.)\n\n"
        msg += "<b>1. 🤖 자동 모드 (자체 U-Curve 엔진)</b>\n"
        msg += "▫️ 봇이 장 마감 30분 전부터 1분 단위로 VWAP 타임 슬라이싱 자동 격발\n"
        msg += "▫️ 세밀한 정밀 타격 및 편의성 극대화\n\n"
        msg += "<b>2. 🖐️ 수동 모드 (한투 자체 알고리즘 위임)</b>\n"
        msg += "▫️ 봇은 타점 시그널 알림만 제공하며 API 자동주문을 100% 락다운함\n"
        msg += "▫️ <b>[필수]</b> 지시서를 보고 한투 앱(MTS)에서 직접 <b>'장 마감 30분 전' VWAP 조건</b>으로 수동 장전해야 함\n\n"
        msg += "원하시는 운용 방식을 선택해 주십시오."
        
        keyboard = [
            [InlineKeyboardButton("🤖 자동 모드 (자체 엔진 1분 타격)", callback_data=f"SET_VER_CONFIRM:AUTO:{ticker}")],
            [InlineKeyboardButton("🖐️ 수동 모드 (한투 알고리즘 위임)", callback_data=f"SET_VER_CONFIRM:MANUAL:{ticker}")],
            [InlineKeyboardButton("❌ 작전 취소 (이전 버전 유지)", callback_data="RESET:CANCEL")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_v14_mode_selection_menu(self, ticker):
        msg = f"💎 <b>[{ticker} 오리지널 집행 방식 선택]</b>\n\n"
        msg += "오리지널 무한매수법(V14)의 당일 예산 집행 방식을 선택해 주십시오.\n\n"
        msg += "<b>1. 📉 LOC 방식 (기본)</b>\n"
        msg += "▫️ 17:05 KST 정규장 주문 시 전량 장마감시지정가(LOC)로 일괄 전송\n"
        msg += "▫️ 호가창 슬리피지 최소화 및 초안정성 지향\n\n"
        msg += "<b>2. 🕒 VWAP 방식 (유동성 추적)</b>\n"
        msg += "▫️ 17:05 KST에는 예방적 LOC 덫만 장전\n"
        msg += "▫️ 장 마감 30분 전부터 1분 단위로 예산을 분할하여 U-Curve 궤적으로 타격\n"
        msg += "▫️ 물리적 미체결 엣지 케이스 방어 및 시장 합의 가격 수렴\n\n"
        msg += "원하시는 집행 방식을 선택해 주십시오."
        
        keyboard = [
            [InlineKeyboardButton("📉 LOC (종가 일괄 타격)", callback_data=f"SET_VER_CONFIRM:V14_LOC:{ticker}")],
            [InlineKeyboardButton("🕒 VWAP (유동성 분할 타격)", callback_data=f"SET_VER_CONFIRM:V14_VWAP:{ticker}")],
            [InlineKeyboardButton("❌ 작전 취소 (이전 버전 유지)", callback_data="RESET:CANCEL")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def create_ledger_dashboard(self, ticker, qty, avg, invested, sold, records, t_val, split, is_history=False, is_reverse=False, history_id=None):
        groups = {}
        for r in records:
            date_only = r['date'][:10]
            key = (date_only, r['side'])
            if key not in groups:
                groups[key] = {'sum_qty': 0, 'sum_cost': 0}
            groups[key]['sum_qty'] += r['qty']
            groups[key]['sum_cost'] += (r['qty'] * r['price'])

        agg_list = []
        for (date, side), data in groups.items():
            if data['sum_qty'] > 0:
                avg_p = data['sum_cost'] / data['sum_qty']
                agg_list.append({'date': date, 'side': side, 'qty': data['sum_qty'], 'avg': avg_p})

        agg_list.sort(key=lambda x: x['date'])
        for i, item in enumerate(agg_list):
            item['no'] = i + 1
        agg_list.reverse()

        title = "과거 졸업 기록" if is_history else "일자별 매매 (통합 변동분)"
        msg = f"📜 <b>[ {ticker} {title} (총 {len(agg_list)}일) ]</b>\n\n"
        
        msg += "<code>No. 일자   구분  평균단가  수량\n"
        msg += "-"*30 + "\n"
        
        for item in agg_list[:50]: 
            d_str = item['date'][5:10].replace('-', '.')
            s_str = "🔴매수" if item['side'] == 'BUY' else "🔵매도"
            msg += f"{item['no']:<3} {d_str} {s_str} ${item['avg']:<6.2f} {item['qty']}주\n"
            
        if len(agg_list) > 50:
            msg += "... (이전 기록 생략)\n"
            
        msg += "-"*30 + "</code>\n"

        msg += "📊 <b>[ 현재 진행 상황 요약 ]</b>\n"
        if not is_history:
            if is_reverse:
                msg += "▪️ 운용 상태 : 🚨 <b>시드 소진 (리버스모드 가동 중)</b>\n"
                msg += f"▪️ 리버스 T값 : <b>{t_val} T</b> (특수연산 적용됨)\n"
            else:
                msg += f"▪️ <b>현재 T값 : {t_val} T</b> ({int(split)}분할)\n"
            msg += f"▪️ 보유 수량 : {qty} 주 (평단 ${avg:.2f})\n"
        else:
            profit = sold - invested
            pct = (profit/invested*100) if invested > 0 else 0
            sign = "+" if profit >= 0 else "-"
            msg += f"▪️ <b>최종수익: {sign}${abs(profit):,.2f} ({sign}{abs(pct):.2f}%)</b>\n"

        msg += f"▪️ 총 매수액 : ${invested:,.2f}\n▪️ 총 매도액 : ${sold:,.2f}\n"

        keyboard = []
        if not is_history:
            other = "TQQQ" if ticker == "SOXL" else "SOXL"
            keyboard.append([InlineKeyboardButton(f"🔄 {other} 장부 조회", callback_data=f"REC:VIEW:{other}")])
            keyboard.append([InlineKeyboardButton(f"🗄️ {ticker} V-REV 큐(Queue) 정밀 관리", callback_data=f"QUEUE:VIEW:{ticker}")])
            keyboard.append([InlineKeyboardButton("🔙 장부 대시보드 업데이트", callback_data=f"REC:SYNC:{ticker}")])
        else:
            if history_id is not None:
                keyboard.append([InlineKeyboardButton("🖼️ 프리미엄 졸업 카드 발급", callback_data=f"HIST:IMG:{ticker}:{history_id}")])
            else:
                keyboard.append([InlineKeyboardButton("🖼️ 프리미엄 졸업 카드 발급", callback_data=f"HIST:IMG:{ticker}")])
            keyboard.append([InlineKeyboardButton("🔙 역사 목록으로 돌아가기", callback_data="HIST:LIST")])

        return msg, InlineKeyboardMarkup(keyboard)

    def create_profit_image(self, ticker, profit, yield_pct, invested, revenue, end_date):
        W, H = 600, 920 
        IMG_H = 430 
        os.makedirs("data", exist_ok=True)
        
        f_title = self._load_best_font(self.bold_font_paths, 65)
        f_p = self._load_best_font(self.bold_font_paths, 85)
        f_y = self._load_best_font(self.reg_font_paths, 40)
        f_b_val = self._load_best_font(self.bold_font_paths, 32)
        f_b_lbl = self._load_best_font(self.reg_font_paths, 22)
        
        def apply_overlay(img_canvas):
            draw = ImageDraw.Draw(img_canvas)
            y_title = IMG_H + 60
            draw.rectangle([W/2 - 140, y_title - 45, W/2 + 140, y_title + 45], fill="#2A2F3D")
            self._safe_draw_text(draw, (W/2, y_title), f"{ticker}", font=f_title, fill="white", anchor="mm")
            
            color = "#007AFF" if profit < 0 else "#FF3B30"
            sign = "-" if profit < 0 else "+"
            
            y_profit = y_title + 105
            self._safe_draw_text(draw, (W/2, y_profit), f"{sign}${abs(profit):,.2f}", font=f_p, fill=color, anchor="mm")
            y_yield = y_profit + 75
            self._safe_draw_text(draw, (W/2, y_yield), f"YIELD {sign}{abs(yield_pct):,.2f}%", font=f_y, fill=color, anchor="mm")
            
            y_box = y_yield + 60
            draw.rectangle([40, y_box, 290, y_box + 100], fill="#2A2F3D")
            self._safe_draw_text(draw, (165, y_box + 35), f"${invested:,.2f}", font=f_b_val, fill="white", anchor="mm")
            self._safe_draw_text(draw, (165, y_box + 75), "TOTAL INVESTED", font=f_b_lbl, fill="#8E8E93", anchor="mm")
            
            draw.rectangle([310, y_box, 560, y_box + 100], fill="#2A2F3D")
            self._safe_draw_text(draw, (435, y_box + 35), f"${revenue:,.2f}", font=f_b_val, fill="white", anchor="mm")
            self._safe_draw_text(draw, (435, y_box + 75), "TOTAL REVENUE", font=f_b_lbl, fill="#8E8E93", anchor="mm")
            
            self._safe_draw_text(draw, (W/2, H - 35), f"{end_date}", font=f_b_lbl, fill="#636366", anchor="mm")
            return img_canvas

        def resize_and_crop(bg_frame):
            bg_ratio = bg_frame.width / bg_frame.height
            if bg_ratio > (W / IMG_H):
                new_w = int(IMG_H * bg_ratio)
                bg_res = bg_frame.resize((new_w, IMG_H), Image.Resampling.LANCZOS)
                return bg_res.crop(((new_w - W) // 2, 0, (new_w + W) // 2, IMG_H))
            else:
                new_h = int(W / bg_ratio)
                bg_res = bg_frame.resize((W, new_h), Image.Resampling.LANCZOS)
                return bg_res.crop((0, (new_h - IMG_H) // 2, W, (new_h + IMG_H) // 2))

        img = Image.new('RGB', (W, H), color='#1E222D')
        try:
            if os.path.exists("background.png"):
                bg = Image.open("background.png").convert("RGB")
                bg_cropped = resize_and_crop(bg)
                img.paste(bg_cropped, (0, 0))
            else:
                draw = ImageDraw.Draw(img)
                draw.rectangle([0, 0, W, IMG_H], fill="#111217")
        except Exception as e:
            logging.error(f"🚨 배경 이미지 로드 실패: {e}")
            draw = ImageDraw.Draw(img)
            draw.rectangle([0, 0, W, IMG_H], fill="#111217")
            
        img = apply_overlay(img)
        fname = f"data/profit_{ticker}.png"
        img.save(fname, format="PNG", quality=100)
        return fname

    def get_ticker_menu(self, current_tickers):
        keyboard = [
            [InlineKeyboardButton("🚀 오리지널 TQQQ 단독 운용", callback_data="TICKER:TQQQ")],
            [InlineKeyboardButton("🔥 오리지널 SOXL 단독 운용", callback_data="TICKER:SOXL")],
            [InlineKeyboardButton("💎 오리지널 TQQQ + SOXL 듀얼 콤보", callback_data="TICKER:ALL")]
        ]
        return f"🔄 <b>[ 운용 종목 선택 ]</b>\n현재 가동중: <b>{', '.join(current_tickers)}</b>", InlineKeyboardMarkup(keyboard)

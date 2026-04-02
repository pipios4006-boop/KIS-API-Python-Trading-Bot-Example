# ==========================================================
# [telegram_view.py] - Part 1
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# ==========================================================
import os
import math
import json
from PIL import Image, ImageDraw, ImageFont
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

class TelegramView:
    def __init__(self):
        pass

    def get_start_message(self, target_hour, season_icon, latest_version):
        init_time = f"{target_hour:02d}:00"
        order_time = f"{target_hour:02d}:05"
        season_short = "🌞서머타임 ON" if "Summer" in season_icon else "❄️서머타임 OFF"
        sync_time = "08:30" if target_hour == 17 else "09:30"

        return (
            f"🌌 <b>[ 인피니트 스노우볼 {latest_version} ]</b>\n" 
            f"⚡ <b> 유동성 스나이퍼 & 멀티 코어 엔진</b> \n\n"
            f"🕒 <b>[ 운영 스케줄 ({season_short}) ]</b>\n"
            f"🔹 6시간 간격 : 🔑 API 토큰 자동 갱신\n"
            f"🔹 {sync_time} : 📝 잔고 동기화 & 자동 복리\n"
            f"🔹 {init_time} : 🔐 매매 초기화 및 변동성 락온\n"
            f"🔹 {order_time} : 🌃 통합 주문 자동 실행\n\n"
            "🛠 <b>[ 주요 명령어 ]</b>\n"
            "▶️ <b>/sync</b> : 📜 통합 지시서 조회\n"
            "▶️ <b>/record</b> : 📊 장부 동기화 및 조회\n"
            "▶️ <b>/history</b> : 🏆 졸업 명예의 전당\n"
            "▶️ <b>/settlement</b> : ⚙️ 분할/복리/액면 설정\n"
            "▶️ <b>/seed</b> : 💵 개별 시드머니 관리\n"
            "▶️ <b>/ticker</b> : 🔄 운용 종목 선택\n"
            "▶️ <b>/mode</b> : 🎯 상방 스나이퍼 ON/OFF\n"
            "▶️ <b>/version</b> : 🛠️ 버전 및 업데이트 내역\n\n" 
            "⚠️ <b>/reset</b> : 🔓 비상 해제 메뉴 (락/리버스)\n" 
            "<i>┗ 🚨 수동 닻 올리기: 예산 부족으로 리버스 진입 후 외화RP매도 등 예수금을 추가 입금하셨다면, 이 메뉴에서 반드시 '리버스 강제 해제'를 눌러 닻을 올려주세요!</i>"
        )

    # ==========================================================
    # 💡 [핵심 수술] P매매 (VWAP) 시크릿 모드 전용 UI 파이프라인
    # ==========================================================
    def get_p_trade_unlocked_message(self, ticker, seed, multiplier):
        return (
            f"🟢 <b>[ P-VWAP Engine Unlocked ]</b>\n"
            f"접속 시간 검증 완료 (Time Window: Valid)\n"
            f"입력 기한: 정규장 마감 30분 전(15:30 EST) 락다운\n\n"
            f"연동 종목: <b>{ticker}</b> (고정)\n"
            f"연동 시드: <b>${seed:,.0f}</b> (고정)\n"
            f"자동 할당 배수(n): <b>{multiplier:.2f} 배</b>\n\n"
            f"명령어 양식: <code>매도/매수 [타겟가] [기본수량]</code> (쉼표/줄바꿈으로 다중 입력)\n"
            f"입력 예시:\n"
            f"<code>매도 45.50 10, 매도 46.00 10, 매수 42.00 15, 매수 41.50 20</code>\n\n"
            f"⚠️ <b>끄기(OFF)</b> : P매매를 소각하려면 <code>OFF</code> 또는 <code>취소</code>를 입력하세요."
        )

    def get_p_trade_locked_message(self):
        return (
            f"⛔ <b>[ P-VWAP Engine Access Denied ]</b>\n"
            f"현재는 P매매 지시서 입력 허용 시간이 아닙니다.\n\n"
            f"⏱️ <b>허용 시간:</b> 애프터마켓 종료 후 ~ 정규장 마감 30분 전 (15:30 EST)까지"
        )

    def get_p_trade_parsed_message(self, multiplier, parsed_list):
        msg = (
            f"🎯 <b>[ P-VWAP Multi-Target Lock-on ]</b>\n"
            f"배수(n) {multiplier:.2f}배 스케일링 및 타격 대기열 장전 완료.\n\n"
        )
        for i, item in enumerate(parsed_list):
            side_str = "SELL" if item['side'] == 'SELL' else "BUY"
            cond_str = "이상" if item['side'] == 'SELL' else "이하"
            ico = "🔵" if item['side'] == 'SELL' else "🔴"
            msg += f"{i+1}. {ico} {side_str} : ${item['target_price']:.2f} {cond_str} / 최종 {item['qty']} 주\n"
        
        msg += f"\n상태: 총 {len(parsed_list)}개 슬롯, 15:30 ~ 15:59 U-Curve 타임 슬라이싱 대기열 장전."
        return msg

    # ==========================================================
    # 기존 UI 렌더링 파이프라인
    # ==========================================================
    def get_reset_menu(self, active_tickers):
        msg = (
            "🛠️ <b>[ 시스템 안전 통제실 ]</b>\n"
            "⚠️ 주의: 강제 초기화할 항목을 선택하세요."
        )
        keyboard = []
        
        for t in active_tickers:
            keyboard.append([InlineKeyboardButton(f"🔓 [{t}] 매매 잠금 해제", callback_data=f"RESET:LOCK:{t}")])
            
        for t in active_tickers:
            keyboard.append([InlineKeyboardButton(f"🚨 [{t}] 리버스/장부 초기화", callback_data=f"RESET:REV:{t}")])
            
        keyboard.append([InlineKeyboardButton("❌ 취소 및 닫기", callback_data="RESET:CANCEL")])
        return msg, InlineKeyboardMarkup(keyboard)

    def get_reset_confirm_menu(self, ticker):
        msg = (
            f"⚠️ <b>[ 경고: {ticker} 리버스 및 가상장부 강제 초기화 ]</b>\n\n"
            f"정말로 {ticker}의 리버스 모드를 종료하고 <b>가상장부(Escrow) 격리금액을 0원으로 완전 소각</b>하시겠습니까?\n"
            "<i>(충분한 시드가 추가되었거나 로직 꼬임 시에만 권장하며, 해제 시 다음부터 일반 모드로 돌아갑니다.)</i>"
        )
        keyboard = [
            [InlineKeyboardButton("✅ 네, 모두 초기화합니다", callback_data=f"RESET:CONFIRM:{ticker}")],
            [InlineKeyboardButton("❌ 아니오, 유지합니다", callback_data="RESET:MENU")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_version_message(self, history_data, page_index=None):
        if not history_data:
            return "📭 기록된 버전 히스토리가 없습니다.", None

        items_per_page = 5
        total_items = len(history_data)
        total_pages = math.ceil(total_items / items_per_page)

        if page_index is None:
            page_index = 0
        else:
            page_index = max(0, min(page_index, total_pages - 1))

        end_idx = total_items - (page_index * items_per_page)
        start_idx = max(0, end_idx - items_per_page)
        items = history_data[start_idx:end_idx]

        if page_index == 0:
            title = "🛠️ <b>[ 최신 업데이트 내역 ]</b>\n\n"
        else:
            title = f"📚 <b>[ 과거 업데이트 내역 (Page {page_index + 1}/{total_pages}) ]</b>\n\n"

        msg = title
        for h in items:
            if isinstance(h, str):
                parts = h.split(' ', 2)
                if len(parts) >= 3:
                    ver = parts[0]
                    date = parts[1]
                    summary = parts[2]
                    msg += f"📌 <b>{ver}</b> {date}\n▫️ {summary}\n\n"
                else:
                    msg += f"📌 {h}\n\n"
            elif isinstance(h, dict): 
                msg += f"📌 <b>{h.get('version', '')}</b> ({h.get('date', '')})\n▫️ {h.get('summary', '')}\n\n"
        
        msg = msg.strip()
        keyboard = []
        
        nav_row = []
        if page_index < total_pages - 1:
            nav_row.append(InlineKeyboardButton("◀️ 과거 기록", callback_data=f"VERSION:PAGE:{page_index + 1}"))
        if page_index > 0:
            nav_row.append(InlineKeyboardButton("최신 기록 ▶️", callback_data=f"VERSION:PAGE:{page_index - 1}"))
            
        if nav_row:
            keyboard.append(nav_row)
            
        if page_index > 0:
            keyboard.append([InlineKeyboardButton("⬆️ 접기 (최신 버전만 보기)", callback_data="VERSION:LATEST")])
            
        return msg, InlineKeyboardMarkup(keyboard) if keyboard else None
    
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

        for t_info in ticker_data:
            t = t_info['ticker']
            v_mode = t_info['version']
            
            if t_info.get('t_val', 0.0) > (t_info.get('split', 40.0) * 1.1):
                body_msg += f"⚠️ <b>[🚨 시스템 긴급 경고: 비정상 T값 폭주 감지!]</b>\n"
                body_msg += f"🔎 현재 T값(<b>{t_info['t_val']:.4f}T</b>)이 설정된 분할수(<b>{int(t_info['split'])}분할</b>) 초과했습니다!\n"
                body_msg += f"💡 <b>원인 역산 추정:</b> 수동 매수로 수량이 급증했거나, '/seed' 시드머니 설정이 대폭 축소되었습니다.\n"
                body_msg += f"🛡️ <b>가동 조치:</b> 마이너스 호가 차단용 절대 하한선($0.01) 방어막 가동 중!\n\n"

            if v_mode == "V17":
                v_mode_display = "V17 시크릿"
                main_icon = "🦇"
            elif v_mode == "V_VWAP":
                v_mode_display = "VWAP 자율주행"
                main_icon = "⏳"
            elif v_mode == "V14":
                v_mode_display = "무매4"
                main_icon = "💎"
            else:
                v_mode_display = "무매3"
                main_icon = "💎"
                
            is_rev = t_info.get('is_reverse', False)
            proc_status = t_info.get('plan', {}).get('process_status', '')
            tracking_info = t_info.get('tracking_info', {})
            
            if proc_status == "🩸리버스(긴급수혈)":
                body_msg += f"⚠️ <b>[🚨 비상 상황: {t} 긴급 수혈 중]</b>\n"
                body_msg += f"❗ <i>에스크로 금고가 바닥나 강제 매도를 통해 현금을 생성합니다.</i>\n\n"
            
            if is_rev:
                bdg_txt = f"리버스 잔금쿼터: ${t_info['one_portion']:,.0f}"
                icon = "🩸" if proc_status == "🩸리버스(긴급수혈)" else "🔄"
                body_msg += f"{icon} <b>[{t}] {v_mode_display} 리버스</b>\n"
                body_msg += f"📈 진행: <b>{t_info['t_val']:.4f}T / {int(t_info['split'])}분할</b>\n"
            else:
                bdg_txt = f"당일 예산: ${t_info['one_portion']:,.0f}" if v_mode in ["V14", "V17", "V_VWAP"] else f"1회 매수금: ${t_info['one_portion']:,.0f}"
                body_msg += f"{main_icon} <b>[{t}] {v_mode_display}</b>\n"
                body_msg += f"📈 진행: <b>{t_info['t_val']:.4f}T / {int(t_info['split'])}분할</b>\n"
            
            body_msg += f"💵 총 시드: ${t_info['seed']:,.0f}\n"
            body_msg += f"🛒 <b>{bdg_txt}</b>\n"
            
            escrow = t_info.get('escrow', 0.0)
            if escrow > 0:
                body_msg += f"🔐 내 금고 보호액: ${escrow:,.2f}\n"
            elif is_rev and proc_status == "🩸리버스(긴급수혈)":
                body_msg += f"🔐 내 금고 보호액: $0.00 (Empty 🚨)\n"
                
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
            body_msg += f"{icon} 수익: {sign}{abs(t_info['profit_pct']):.2f}% ({sign}${abs(t_info['profit_amt']):,.2f})\n"
            
            sniper_status_txt = t_info.get('upward_sniper', 'OFF')
            
            if is_rev:
                if v_mode == "V17":
                    body_msg += f"⚙️ 🌟 5일선 별지점: ${t_info['star_price']:.2f}\n"
                else:
                    body_msg += f"⚙️ 🌟 5일선 별지점: ${t_info['star_price']:.2f} | 🎯감시: {sniper_status_txt}\n"
            else:
                if v_mode == "V17":
                    body_msg += f"⚙️ 🎯 {t_info['target']}% | ⭐ {t_info['star_pct']}%\n"
                else:
                    body_msg += f"⚙️ 🎯 {t_info['target']}% | ⭐ {t_info['star_pct']}% | 🎯감시: {sniper_status_txt}\n"
                
            if sniper_status_txt == "ON" and v_mode != "V17":
                if not is_trade_active:
                    body_msg += f"🎯 상방 스나이퍼: 감시 종료 (장마감)\n"
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
            
            hybrid_target = t_info.get('hybrid_target', 0.0)
            sniper_pct = t_info.get('sniper_trigger', 0.0) 
            secret_quarter_target = t_info.get('secret_quarter_target', 0.0)
            
            if v_mode == "V17":
                dyn_obj = t_info.get('dynamic_obj')
                weight = dyn_obj.weight if dyn_obj and hasattr(dyn_obj, 'weight') else 1.0
                
                if not is_trade_active:
                    if weight <= 1.0:
                        body_msg += f"📉 <b>스나이퍼: 금일 감시 종료 (장마감)</b>\n"
                    else:
                        body_msg += f"🦇 <b>쿼터 스나이퍼: 금일 감시 종료 (장마감)</b>\n"
                else:
                    if "가로채기" in proc_status:
                        hit_price = tracking_info.get('hit_price', 0.0)
                        lowest = tracking_info.get('lowest_price', 0.0)
                        if hit_price == 0.0:
                            cache_file = f"data/sniper_cache_{t}.json"
                            if os.path.exists(cache_file):
                                try:
                                    with open(cache_file, 'r') as f:
                                        c_data = json.load(f)
                                        hit_price = c_data.get('hit_price', 0.0)
                                        lowest = c_data.get('lowest_price', 0.0)
                                except: pass
                        if hit_price > 0 and lowest > 0:
                            bounce_pct = ((hit_price - lowest) / lowest) * 100
                            body_msg += f"💥 <b>명중: ${hit_price:.2f} (${lowest:.2f} 대비 +{bounce_pct:.2f}%)</b>\n"
                        else:
                            body_msg += f"💥 <b>스나이퍼: 명중 완료</b>\n"
                    else:
                        if weight <= 1.0:
                            if tracking_info.get('is_tracking', False):
                                lowest = tracking_info.get('lowest_price', 0.0)
                                trigger_val = 1.5 if t == "SOXL" else 1.0
                                body_msg += f"🎯 <b>장전선 이탈! 장중 바닥 추적 중</b>\n  <b>(최저: ${lowest:.2f} / 목표반등: +{trigger_val}%)</b>\n"
                            elif hybrid_target > 0:
                                body_msg += f"📉 <b>스나이퍼: {sniper_pct:.2f}% 진폭(TR) 대기</b>\n"
                            else:
                                body_msg += f"📉 <b>스나이퍼: 장전 대기 중</b>\n"
                        else:
                            if tracking_info.get('is_trailing', False):
                                peak_price = tracking_info.get('peak_price', 0.0)
                                trigger_price = tracking_info.get('trigger_price', 0.0)
                                body_msg += f"🎯 <b>쿼터 추적(${trigger_price:.2f}) 중 (고가: ${peak_price:.2f})</b>\n"
                            elif secret_quarter_target > 0:
                                body_msg += f"🦇 <b>쿼터 스나이퍼: ${secret_quarter_target:.2f} 이상 대기</b>\n"
                            else:
                                body_msg += f"🦇 <b>쿼터 스나이퍼: 장전 대기 중</b>\n"
            
            elif v_mode == "V_VWAP":
                body_msg += f"⏱️ <b>페일세이프(Fail-Safe):</b> 정규장 17:05 KST 무매 덫 선제 장전\n"
                body_msg += f"⏱️ <b>VWAP 스케줄:</b> 15:30 EST 기존 LOC 철거 ➔ 지정가 분할 타격\n"
# ==========================================================
# [telegram_view.py] - Part 2 (이어서 작성)
# ==========================================================

            body_msg += f"📋 <b>[주문 계획 - {proc_status}]</b>\n"
            
            plan_orders = t_info.get('plan', {}).get('orders', [])
            if plan_orders:
                jup_orders = [o for o in plan_orders if "줍줍" in o['desc']]
                n_orders = [o for o in plan_orders if "줍줍" not in o['desc']]

                for o in n_orders:
                    ico = "🔴" if o['side'] == 'BUY' else "🔵"
                    desc = o['desc']
                    
                    if "수혈" in desc: 
                        ico = "🩸"
                        desc = desc.replace("🩸", "")
                    elif "시크릿" in desc: 
                        ico = "🦇"
                        desc = desc.replace("🦇", "")
                        
                    type_str = "" if o['type'] == 'LIMIT' else f"({o['type']})"
                    type_disp = f" {type_str}" if type_str else ""
                    
                    body_msg += f" {ico} {desc}: <b>${o['price']} x {o['qty']}주</b>{type_disp}\n"

                if jup_orders:
                    prices = sorted([o['price'] for o in jup_orders], reverse=True)
                    body_msg += f" 🧹 줍줍({len(jup_orders)}개): <b>${prices[0]} ~ ${prices[-1]} (LOC)</b>\n"
                
                if is_trade_active:
                    if t_info.get('is_locked', False): body_msg += f" (✅ 금일 주문 완료/잠금)\n"
                    else: keyboard.append([InlineKeyboardButton(f"🚀 {t} 주문 실행", callback_data=f"EXEC:{t}")])
            else:
                body_msg += f" 💤 주문 없음 (관망/예산소진)\n"
            
            body_msg += "\n"

        final_msg = header_msg + body_msg

        # ==========================================================
        # 💡 [핵심 수술] P매매 (VWAP) 독립 대기열 렌더링 파이프라인
        # ==========================================================
        if p_trade_data:
            has_p_orders = False
            p_msg = "----------------------------\n"
            p_msg += "🛡️ <b>[ P-VWAP 단독 대기열 (수동/방치형) ]</b>\n"
            p_msg += "종목 / 방향 / 타겟가 / 할당수량 / 진행상태\n"
            
            for p_ticker, p_orders in p_trade_data.items():
                if p_orders:
                    has_p_orders = True
                    for o in p_orders:
                        side_str = "BUY" if o['side'] == 'BUY' else "SELL"
                        ico = "🔴" if side_str == "BUY" else "🔵"
                        p_msg += f"▫️ {p_ticker} / {ico}{side_str} / ${o['target_price']:.2f} / {o['qty']}주 / ⏳ 15:30 가동\n"
            
            if has_p_orders:
                p_msg += "<i>※ P-VWAP 주문은 15:30 EST부터 1분 단위로 분할 타격되며 미체결분은 장 마감 시 자동 소멸됩니다. (봇의 무매 예산 연동 없음)</i>\n\n"
                final_msg += p_msg

        # ==========================================================
        # 💡 [핵심 수술] 1-Tier 자율주행 지표 1줄 요약 조립 및 하단 강제 노출
        # ==========================================================
        vol_summaries = []
        for t_info in ticker_data:
            if 'vol_weight' in t_info and 'vol_status' in t_info:
                vol_summaries.append(f"{t_info['ticker']}: {t_info['vol_weight']} ({t_info['vol_status']})")
        
        if vol_summaries:
            final_msg += "📊 <b>[자율지표]</b> " + " | ".join(vol_summaries) + "\n<i>(상세: /mode)</i>\n\n"

        if not is_trade_active: final_msg += "⛔ 장마감/애프터마켓: 주문 불가"
        return final_msg, InlineKeyboardMarkup(keyboard) if keyboard else None

    def get_settlement_message(self, active_tickers, config, atr_data, dynamic_target_data=None):
        msg = "⚙️ <b>[ 현재 설정 및 복리 상태 ]</b>\n\n"
        keyboard = []
        
        if dynamic_target_data is None:
            dynamic_target_data = {}
        
        for t in active_tickers:
            ver = config.get_version(t)
            
            if ver == "V17":
                icon = "🦇"
                ver_display = "V17 시크릿"
            elif ver == "V_VWAP":
                icon = "⏳"
                ver_display = "VWAP 자율주행"
            elif ver == "V14":
                icon = "💎"
                ver_display = "무매4"
            else:
                icon = "💎"
                ver_display = "무매3"
                
            split_cnt = int(config.get_split_count(t))
            target_pct = config.get_target_profit(t)
            comp_rate = config.get_compound_rate(t)
            
            msg += f"{icon} <b>{t} ({ver_display} 모드)</b>\n"
            msg += f"▫️ 분할: {split_cnt}회\n▫️ 목표: {target_pct}%\n▫️ 자동복리: {comp_rate}%\n"
            
            if ver == "V17":
                atr5, atr14 = atr_data.get(t, (0.0, 0.0))
                target_obj = dynamic_target_data.get(t)
                
                if target_obj is not None and hasattr(target_obj, 'metric_val'):
                    m_val = target_obj.metric_val
                    m_name = target_obj.metric_name
                    m_base = float(target_obj.metric_base)
                    weight = target_obj.weight
                    base_amp = abs(target_obj.base_amp)
                    
                    msg += f"📊 <b>실시간 동적 변동성 (V3.2 마스터 스위치):</b>\n"
                    msg += f"▫️ ATR5 ({atr5:.1f}%) / ATR14 ({atr14:.1f}%)\n"
                    msg += f"▫️ {m_name}: {m_val:.2f} / {m_base:.2f}\n     (가중치 {weight:.2f}배)\n"
                    msg += f"▫️ 고정 타격선(1년 ATR): -{base_amp:.2f}%\n"
                    
                    if weight <= 1.0:
                        msg += f"▫️ 자율제어: 🔫하방[ON] / 🛡️상방[OFF]\n\n"
                    else:
                        msg += f"▫️ 자율제어: 🔫하방[OFF] / 🛡️상방[ON]\n\n"
                else:
                    base_amp = 8.79 if t == "SOXL" else 4.95
                    msg += f"📊 <b>실시간 동적 변동성 (V3.2 마스터 스위치):</b>\n"
                    msg += f"▫️ ATR5 ({atr5:.1f}%) / ATR14 ({atr14:.1f}%)\n"
                    msg += f"▫️ 지표 연산 실패\n     (가중치 1.00배 기본값 방어 중)\n"
                    msg += f"▫️ 고정 타격선(1년 ATR): -{base_amp:.2f}%\n"
                    msg += f"▫️ 자율제어: 🔫하방[ON] / 🛡️상방[OFF]\n\n"
                    
            elif ver == "V_VWAP":
                msg += f"📊 <b>VWAP 유동성 프로파일 엔진 대기 중:</b>\n"
                msg += f"▫️ 15:30 EST부터 30분간 U-Curve 가중치 분할 매매 작동\n\n"
                
            else:
                msg += "\n"
                
            row1 = [
                InlineKeyboardButton("💎 V3 (무매3)", callback_data=f"SET_VER:V13:{t}"),
                InlineKeyboardButton("💎 V4 (무매4)", callback_data=f"SET_VER:V14:{t}"),
                InlineKeyboardButton("⏳ VWAP (자율)", callback_data=f"SET_VER:V_VWAP:{t}")
            ]
            keyboard.append(row1)
            
            row2 = [
                InlineKeyboardButton(f"⚙️ {t} 분할", callback_data=f"INPUT:SPLIT:{t}"), 
                InlineKeyboardButton(f"🎯 {t} 목표", callback_data=f"INPUT:TARGET:{t}"),
                InlineKeyboardButton(f"💸 {t} 복리", callback_data=f"INPUT:COMPOUND:{t}")
            ]
            keyboard.append(row2)
            
            row3 = [
                InlineKeyboardButton(f"✂️ {t} 액면보정", callback_data=f"INPUT:STOCK_SPLIT:{t}")
            ]
            keyboard.append(row3)
            
        return msg, InlineKeyboardMarkup(keyboard)

    def create_ledger_dashboard(self, ticker, qty, avg, invested, sold, records, t_val, split, is_history=False, is_reverse=False):
        groups = {}
        for r in records:
            key = (r['date'], r['side'])
            if key not in groups: groups[key] = {'sum_qty': 0, 'sum_cost': 0}
            groups[key]['sum_qty'] += r['qty']
            groups[key]['sum_cost'] += (r['qty'] * r['price'])

        agg_list = []
        for (date, side), data in groups.items():
            if data['sum_qty'] > 0:
                avg_p = data['sum_cost'] / data['sum_qty']
                agg_list.append({'date': date, 'side': side, 'qty': data['sum_qty'], 'avg': avg_p})

        agg_list.sort(key=lambda x: x['date'])
        for i, item in enumerate(agg_list): item['no'] = i + 1
        agg_list.reverse()

        title = "과거 졸업 기록" if is_history else "일자별 매매 (통합 변동분)"
        msg = f"📜 <b>[ {ticker} {title} (총 {len(agg_list)}일) ]</b>\n\n"
        
        msg += "<code>No. 일자   구분  평균단가  수량\n"
        msg += "-"*30 + "\n"
        
        for item in agg_list[:50]: 
            d_str = item['date'][5:].replace('-', '.')
            s_str = "🔴매수" if item['side'] == 'BUY' else "🔵매도"
            msg += f"{item['no']:<3} {d_str} {s_str} ${item['avg']:<6.2f} {item['qty']}주\n"
            
        if len(agg_list) > 50: msg += "... (이전 기록 생략)\n"
        msg += "-"*30 + "</code>\n"

        msg += f"📊 <b>[ 현재 진행 상황 요약 ]</b>\n"
        if not is_history:
            if is_reverse:
                msg += f"▪️ 운용 상태 : 🚨 <b>시드 소진 (리버스모드 가동 중)</b>\n"
                msg += f"▪️ 리버스 T값 : <b>{t_val} T</b> (특수연산 적용됨)\n"
            else:
                msg += f"▪️ <b>현재 T값 : {t_val} T</b> ({int(split)}분할)\n"
            msg += f"▪️ 보유 수량 : {qty} 주 (평단 ${avg:.2f})\n"
        else:
            profit = sold - invested
            pct = (profit/invested*100) if invested > 0 else 0
            sign = "+" if profit >= 0 else "-"
            msg += f"▪️ <b>최종수익: {sign}${abs(profit):,.2f} ({pct:.2f}%)</b>\n"

        msg += f"▪️ 총 매수액 : ${invested:,.2f}\n▪️ 총 매도액 : ${sold:,.2f}\n"

        keyboard = []
        if not is_history:
            other = "TQQQ" if ticker == "SOXL" else "SOXL"
            keyboard.append([InlineKeyboardButton(f"🔄 {other} 장부 조회", callback_data=f"REC:VIEW:{other}")])
            keyboard.append([InlineKeyboardButton("🔙 장부 대시보드 업데이트", callback_data=f"REC:SYNC:{ticker}")])
        else:
            keyboard.append([InlineKeyboardButton("🔙 역사 목록으로 돌아가기", callback_data="HIST:LIST")])

        return msg, InlineKeyboardMarkup(keyboard)

    def create_profit_image(self, ticker, profit, yield_pct, invested, revenue, end_date):
        W, H = 600, 920 
        IMG_H = 430 
        img = Image.new('RGB', (W, H), color='#121212')
        draw = ImageDraw.Draw(img)
        try:
            if os.path.exists("background.png"):
                bg = Image.open("background.png").convert("RGB")
                bg_ratio = bg.width / bg.height
                if bg_ratio > (W / IMG_H):
                    new_w = int(IMG_H * bg_ratio)
                    bg = bg.resize((new_w, IMG_H), Image.Resampling.LANCZOS).crop(((new_w - W) // 2, 0, (new_w + W) // 2, IMG_H))
                else:
                    new_h = int(W / bg_ratio)
                    bg = bg.resize((W, new_h), Image.Resampling.LANCZOS).crop((0, (new_h - IMG_H) // 2, W, (new_h + IMG_H) // 2))
                img.paste(bg, (0, 0))
        except: draw.rectangle([0, 0, W, IMG_H], fill="#252525")

        try:
            f_p = ImageFont.truetype("arial.ttf", 85)
            f_y = ImageFont.truetype("arial.ttf", 45)
            f_b = ImageFont.truetype("arial.ttf", 35)
        except: f_p = f_y = f_b = ImageFont.load_default()

        y = IMG_H + 50
        draw.text((W/2, y), ticker, font=f_b, fill="white", anchor="mm")
        y += 100
        color = "#FF453A" if profit < 0 else "#30D158"
        sign = "-" if profit < 0 else "+"
        draw.text((W/2, y), f"{sign}${abs(profit):,.2f}", font=f_p, fill=color, anchor="mm")
        y += 75
        draw.text((W/2, y), f"{sign}{abs(yield_pct):,.2f}", font=f_y, fill=color, anchor="mm")
        
        y += 110
        draw.rectangle([40, y, 290, y + 110], fill="#1C1C1E")
        draw.text((165, y + 35), f"${invested:,.2f}", font=f_b, fill="white", anchor="mm")
        draw.text((165, y + 75), "Total Invested", font=f_b, fill="#8E8E93", anchor="mm")
        
        draw.rectangle([310, y, 560, y + 110], fill="#1C1C1E")
        draw.text((435, y + 35), f"${revenue:,.2f}", font=f_b, fill="white", anchor="mm")
        draw.text((435, y + 75), "Total Revenue", font=f_b, fill="#8E8E93", anchor="mm")
        
        draw.text((W/2, H - 40), f"Graduation Date: {end_date}", font=f_b, fill="#636366", anchor="mm")
        
        fname = f"data/profit_{ticker}.png"
        img.save(fname)
        return fname

    def get_ticker_menu(self, current_tickers):
        keyboard = [
            [InlineKeyboardButton("🔥 SOXL 전용", callback_data="TICKER:SOXL")],
            [InlineKeyboardButton("🚀 TQQQ 전용", callback_data="TICKER:TQQQ")],
            [InlineKeyboardButton("💎 SOXL + TQQQ 통합", callback_data="TICKER:ALL")]
        ]
        return f"🔄 <b>[ 운용 종목 선택 ]</b>\n현재: <b>{', '.join(current_tickers)}</b>", InlineKeyboardMarkup(keyboard)

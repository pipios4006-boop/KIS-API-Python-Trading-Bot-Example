# ==========================================================
# [telegram_avwap_console.py] - 🌟 V43.14 신규 AVWAP 독립 관제탑 플러그인 🌟
# 🚨 NEW: 통합지시서(/sync)의 과부하를 막기 위해 AVWAP 듀얼 모멘텀 레이더를 분리 독립시킴.
# 🚨 MODIFIED: [V43.07] 당일 저가(Day Low) 0점 앵커 기반 ATR5/ATR14 체력 소진율 시각화 바(Bar) 이식.
# 🚨 NEW: [V43.07] 체력 소진율(90%, 80%, 70%)에 따른 목표 수익률 자율주행(Auto) 엔진 및 스위치 장착.
# 🚨 MODIFIED: [V43.08] 전일 VWAP 연산 중 발생하던 존재하지 않는 메서드 런타임 에러 팩트 수술 완료.
# 🚨 MODIFIED: [V43.09 핫픽스] 모든 외부 API 통신에 asyncio.wait_for 족쇄(Timeout)를 강제 적용하여 봇 무반응(Deadlock) 현상 영구 소각 완료.
# 🚨 MODIFIED: [V43.09 UI/UX 패치] 모바일 화면 줄바꿈 방지를 위한 게이지 바 다이어트, 모멘텀 판별식 명시 및 조건 미달 시 정보 은폐(Clean UI) 동적 렌더링 이식 완료.
# 🚨 MODIFIED: [V43.11 극한 다이어트] 수동 모드 전환과 목표가 입력을 1개 버튼으로 통폐합하고, 1개 종목의 모든 제어 버튼을 가로 1줄에 진공 압축 완료.
# 🚨 MODIFIED: [V43.12 텔레그램 멱등성 붕괴 방어] 메시지 하단에 초(Second) 단위 타임스탬프를 팩트 주입하여 'Message is not modified' 400 에러를 원천 차단.
# 🚨 MODIFIED: [V43.14 직관적 버튼 렌더링] 버튼 텍스트가 '현재 적용 중인 모드와 퍼센트(%)'를 직관적으로 표출하도록 완전 개편 및 원터치 토글 로직 적용 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
import asyncio
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

class AvwapConsolePlugin:
    def __init__(self, config, broker, strategy, tx_lock):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.tx_lock = tx_lock

    async def get_console_message(self, app_data):
        est = ZoneInfo('America/New_York')
        now_est = datetime.datetime.now(est)
        
        active_tickers = self.cfg.get_active_tickers()
        avwap_tickers = [t for t in active_tickers if t == "SOXL"]
        if "SOXL" in avwap_tickers:
            avwap_tickers.append("SOXS")
            
        if not avwap_tickers:
            return "⚠️ <b>[AVWAP 암살자 오프라인]</b>\n▫️ AVWAP 지원 종목이 없습니다.", None
            
        active_avwap = [t for t in avwap_tickers if self.cfg.get_avwap_hybrid_mode("SOXL" if t == "SOXS" else t)]
        if not active_avwap:
            return "⚠️ <b>[AVWAP 암살자 오프라인]</b>\n▫️ <code>/settlement</code> 메뉴에서 AVWAP 하이브리드 모드를 켜주세요.", None

        tracking_cache = app_data.get('sniper_tracking', {})
        
        base_tkr = "SOXX"
        base_prev_vwap, base_curr_vwap = 0.0, 0.0
        avg_vwap_5m = 0.0
        try:
            avwap_ctx = None
            if hasattr(self.strategy, 'v_avwap_plugin'):
                avwap_ctx = await asyncio.wait_for(
                    asyncio.to_thread(self.strategy.v_avwap_plugin.fetch_macro_context, base_tkr), timeout=4.0
                )
            
            if avwap_ctx:
                base_prev_vwap = float(avwap_ctx.get('prev_vwap', 0.0))
                
            df_1m = await asyncio.wait_for(
                asyncio.to_thread(self.broker.get_1min_candles_df, base_tkr), timeout=4.0
            )
            if df_1m is not None and not df_1m.empty:
                df = df_1m.copy()
                df['tp'] = (df['high'].astype(float) + df['low'].astype(float) + df['close'].astype(float)) / 3.0
                df['vol'] = df['volume'].astype(float)
                df['vol_tp'] = df['tp'] * df['vol']
                
                cum_vol = df['vol'].sum()
                if cum_vol > 0:
                    base_curr_vwap = df['vol_tp'].sum() / cum_vol
                else:
                    base_curr_vwap = float(df['close'].iloc[-1])
                    
                recent_5 = df.tail(5)
                sum_vol_5 = recent_5['vol'].sum()
                if sum_vol_5 > 0:
                    avg_vwap_5m = recent_5['vol_tp'].sum() / sum_vol_5
                else:
                    avg_vwap_5m = base_curr_vwap
        except asyncio.TimeoutError:
            logging.error(f"🚨 AVWAP 관제탑 기초자산({base_tkr}) 스캔 타임아웃 발생")
        except Exception as e:
            logging.error(f"🚨 AVWAP 관제탑 기초자산 스캔 에러: {e}")

        msg = f"🔫 <b>[ 차세대 AVWAP 듀얼 모멘텀 관제탑 ]</b>\n\n"
        msg += f"🏛️ <b>[ 기초자산 ({base_tkr}) 모멘텀 스캔 ]</b>\n"
        
        if base_prev_vwap > 0:
            msg += f"▫️ 전일 VWAP: <b>${base_prev_vwap:,.2f}</b>\n"
            rt_gap = ((base_curr_vwap - base_prev_vwap) / base_prev_vwap) * 100
            msg += f"▫️ 당일 VWAP: <b>${base_curr_vwap:,.2f}</b> ({rt_gap:+.2f}%)\n"
            if avg_vwap_5m > 0 and base_curr_vwap > 0:
                avg_5m_gap = ((avg_vwap_5m - base_curr_vwap) / base_curr_vwap) * 100
                msg += f"▫️ 5분 평균 VWAP: <b>${avg_vwap_5m:,.2f}</b> ({avg_5m_gap:+.2f}%)\n"
        else:
            msg += f"▫️ 당일 VWAP: <b>${base_curr_vwap:,.2f}</b>\n"
            if avg_vwap_5m > 0:
                msg += f"▫️ 5분 평균 VWAP: <b>${avg_vwap_5m:,.2f}</b>\n"

        keyboard = []

        for t in active_avwap:
            try:
                curr_p = await asyncio.wait_for(asyncio.to_thread(self.broker.get_current_price, t), timeout=2.0)
            except Exception: curr_p = 0.0
            
            try:
                prev_c = await asyncio.wait_for(asyncio.to_thread(self.broker.get_previous_close, t), timeout=2.0)
            except Exception: prev_c = 0.0
            
            try:
                day_high, day_low = await asyncio.wait_for(asyncio.to_thread(self.broker.get_day_high_low, t), timeout=2.0)
            except Exception: day_high, day_low = 0.0, 0.0
            
            try:
                atr5, atr14 = await asyncio.wait_for(asyncio.to_thread(self.broker.get_atr_data, t), timeout=3.0)
            except Exception: atr5, atr14 = 0.0, 0.0
            
            curr_p = float(curr_p) if curr_p else 0.0
            prev_c = float(prev_c) if prev_c else 0.0
            day_low = float(day_low) if day_low else prev_c
            
            avwap_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
            avwap_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
            strikes = tracking_cache.get(f"AVWAP_STRIKES_{t}", 0)
            is_shutdown = tracking_cache.get(f"AVWAP_SHUTDOWN_{t}", False)
            
            is_multi = getattr(self.cfg, 'get_avwap_multi_strike_mode', lambda x: False)(t)
            user_target_pct = getattr(self.cfg, 'get_avwap_target_profit', lambda x: 4.0)(t)
            target_mode = tracking_cache.get(f"AVWAP_TARGET_MODE_{t}", "AUTO") 
            
            label = "롱" if t == "SOXL" else "숏"
            msg += f"\n🎯 <b>[ {t} ({label}) 작전반 ]</b>\n"

            momentum_met = False
            trend_str = "🔴 <b>조건 미달 (대기)</b>"
            
            if t == "SOXS":
                criteria = "당일VWAP &lt; 전일VWAP &amp; 5분평균 &lt; 당일VWAP"
                if base_prev_vwap > 0 and base_curr_vwap > 0 and avg_vwap_5m > 0:
                    if base_curr_vwap < base_prev_vwap and avg_vwap_5m < base_curr_vwap:
                        momentum_met = True
                        trend_str = "🟢 <b>조건 충족 (숏 타격 허용)</b>"
                    else:
                        trend_str = "🔴 <b>조건 미달 (진입 차단)</b>"
                else:
                    trend_str = "⚠️ 데이터 수집 대기 중"
            else:
                criteria = "당일VWAP &gt; 전일VWAP &amp; 5분평균 &gt; 당일VWAP"
                if base_prev_vwap > 0 and base_curr_vwap > 0 and avg_vwap_5m > 0:
                    if base_curr_vwap > base_prev_vwap and avg_vwap_5m > base_curr_vwap:
                        momentum_met = True
                        trend_str = "🟢 <b>조건 충족 (롱 타격 허용)</b>"
                    else:
                        trend_str = "🔴 <b>조건 미달 (진입 차단)</b>"
                else:
                    trend_str = "⚠️ 데이터 수집 대기 중"

            msg += f"▫️ 판별 기준: <code>{criteria}</code>\n"
            msg += f"▫️ 모멘텀 상태: {trend_str}\n"

            show_details = momentum_met or (avwap_qty > 0) or is_shutdown

            if not show_details:
                msg += "💤 <i>(조건 미달로 세부 관측망을 숨기고 대기합니다)</i>\n"
            else:
                strike_icon_txt = "💼 무제한 출장" if is_multi else "🏠 조기퇴근(1회)"
                if strikes > 0:
                    msg += f"▫️ 모드: <b>{strike_icon_txt} ({strikes}회차 교전 완료)</b>\n"
                else:
                    msg += f"▫️ 모드: <b>{strike_icon_txt} 가동 중</b>\n"

                msg += f"▫️ 독립 물량/평단: {avwap_qty}주 / ${avwap_avg:.2f}\n"

                if atr5 > 0 and atr14 > 0 and prev_c > 0 and day_low > 0:
                    ref_price = avwap_avg if (avwap_qty > 0 and avwap_avg > 0) else curr_p
                    ref_label = "매수평단" if (avwap_qty > 0 and avwap_avg > 0) else "현재가"
                    
                    atr5_price = prev_c * (atr5 / 100.0)
                    atr14_price = prev_c * (atr14 / 100.0)
                    
                    atr5_limit = day_low + atr5_price
                    atr14_limit = day_low + atr14_price
                    
                    exh_5 = ((ref_price - day_low) / atr5_price * 100) if atr5_price > 0 else 0
                    exh_14 = ((ref_price - day_low) / atr14_price * 100) if atr14_price > 0 else 0
                    
                    def make_bar(exh):
                        pos = min(5, max(0, int(exh / 20)))
                        return "━" * pos + "🎯" + "━" * (5 - pos)
                    
                    msg += f"▫️ 0점 앵커(당일 저가): <b>${day_low:.2f}</b>\n"
                    msg += f"▫️ {ref_label} 위치: <b>${ref_price:.2f}</b>\n\n"
                    
                    msg += f"🔋 <b>단기 체력 (ATR5: ${atr5_limit:.2f})</b>\n"
                    msg += f"   [0%] {make_bar(exh_5)} [100%] <b>({exh_5:.0f}%)</b>\n"
                    
                    msg += f"🔋 <b>중기 체력 (ATR14: ${atr14_limit:.2f})</b>\n"
                    msg += f"   [0%] {make_bar(exh_14)} [100%] <b>({exh_14:.0f}%)</b>\n"
                    
                    if exh_5 >= 90:
                        msg += " ⚠️ <i>[경고] 단기 체력 90% 소진. 익절라인 하향 권장!</i>\n"

                if target_mode == "AUTO":
                    if exh_5 >= 90: dynamic_target = 2.0
                    elif exh_5 >= 80: dynamic_target = 3.0
                    elif exh_5 >= 70: dynamic_target = 4.0
                    else: dynamic_target = user_target_pct
                    target_display = f"🤖자율주행 (+{dynamic_target:.1f}%)"
                    
                    # 💡 [V43.14] 버튼 표출: 현재 '자율'이며, 클릭하면 '수동'으로 전환됨
                    btn_mode_text = f"🤖자율 (+{dynamic_target:.1f}%)"
                    toggle_target_action = f"TARGET_MANUAL"
                else:
                    target_display = f"🖐️수동고정 (+{user_target_pct:.1f}%)"
                    
                    # 💡 [V43.14] 버튼 표출: 현재 '수동'이며, 클릭하면 '자율'로 전환됨
                    btn_mode_text = f"🖐️수동 (+{user_target_pct:.1f}%)"
                    toggle_target_action = f"TARGET_AUTO"

                msg += f"▫️ 목표 익절: <b>{target_display}</b> | 하드스탑: <b>-8.0%</b>\n"

                status_txt = "👀 타점 대기"
                if is_shutdown: status_txt = "🛑 당일 영구동결 (SHUTDOWN)"
                elif avwap_qty > 0: status_txt = "🎯 딥매수 완료 (익절 감시중)"
                msg += f"▫️ 상태: <b>{status_txt}</b>\n"

            # 💡 [V43.14] 원터치 토글 및 명확한 상태 표시를 위한 3버튼 렌더링
            btn_toggle_mode = InlineKeyboardButton(btn_mode_text, callback_data=f"AVWAP_SET:{toggle_target_action}:{t}")
            btn_input_target = InlineKeyboardButton("✏️타점수정", callback_data=f"AVWAP_SET:TARGET:{t}")
            
            strike_icon_btn = "💼조기퇴근" if not is_multi else "🔁다중출장"
            strike_action = "MULTI" if not is_multi else "EARLY"
            btn_strike = InlineKeyboardButton(strike_icon_btn, callback_data=f"AVWAP_SET:{strike_action}:{t}")

            keyboard.append([btn_toggle_mode, btn_input_target, btn_strike])

        keyboard.append([
            InlineKeyboardButton("🔄 관제탑 새로고침", callback_data="AVWAP_SET:REFRESH:NONE"),
            InlineKeyboardButton("🔙 닫기", callback_data="RESET:CANCEL")
        ])

        msg += f"\n\n⏱️ <i>마지막 스캔: {now_est.strftime('%Y-%m-%d %H:%M:%S')} (EST)</i>"

        return msg, InlineKeyboardMarkup(keyboard)

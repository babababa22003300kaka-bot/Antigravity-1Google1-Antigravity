#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🔧 تعديل بيانات السيندر - نظام منفصل مع Burst Mode Monitoring
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ زر تفاعلي في جروب "جميع الحالات" فقط
✅ نظام مرن لإدخال البيانات (email, password, backup)
✅ متوافق مع المشروع الحالي
✅ 🆕 Burst Mode monitoring بعد التعديل
✅ 🆕 Smart Edit Summary - رسالة ديناميكية
✅ 🆕 Execute Button Monitoring - مراقبة بعد التنفيذ المباشر
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import json
import re
from typing import Dict, Optional, Tuple

import aiohttp
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

# ═══════════════════════════════════════════════════════════
# ⚙️ تحميل الإعدادات
# ═══════════════════════════════════════════════════════════

with open("config.json", "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

# استخراج معلومات الجروب "جميع الحالات"
ALL_STATES_GROUP_ID = None
for group in CONFIG.get("notification_groups", {}).get("groups", []):
    if group.get("name") == "جميع الحالات":
        ALL_STATES_GROUP_ID = group.get("group_id")
        break

# معلومات الموقع
WEBSITE_CONFIG = CONFIG.get("website", {})
BASE_URL = WEBSITE_CONFIG.get("urls", {}).get("base", "https://utautotransfer.com")
COOKIES = WEBSITE_CONFIG.get("cookies", {})

# ═══════════════════════════════════════════════════════════
# 🆕 استيرادات إضافية للتكامل مع المراقبة
# ═══════════════════════════════════════════════════════════

from core import monitor_account_task

# 📊 Global vars
api_manager_instance = None  # 🆕 سيتم تعيينه من main.py عند التشغيل


def set_api_manager(api_mgr):
    """دالة لتعيين api_manager من main.py"""
    global api_manager_instance
    api_manager_instance = api_mgr


# ═══════════════════════════════════════════════════════════
# 🧠 دوال التحليل الذكي
# ═══════════════════════════════════════════════════════════


def convert_arabic_numbers(text: str) -> str:
    """تحويل الأرقام العربية إلى إنجليزية"""
    arabic_to_english = {
        "٠": "0", "١": "1", "٢": "2", "٣": "3", "٤": "4",
        "٥": "5", "٦": "6", "٧": "7", "٨": "8", "٩": "9",
    }
    processed_text = text
    for ar, en in arabic_to_english.items():
        processed_text = processed_text.replace(ar, en)
    return processed_text


def detect_field_type(value):
    """كشف نوع الخانة تلقائياً"""
    if not value or not value.strip():
        return None, None

    value = value.strip()

    if "@" in value and "." in value:
        return "email", value

    value_normalized = convert_arabic_numbers(value)

    if "," in value_normalized:
        return "backup", value_normalized

    eight_digit_codes = re.findall(r"\d{8,}", value_normalized)
    if eight_digit_codes:
        return "backup", value_normalized

    digits_only = re.sub(r"\D", "", value_normalized)
    if len(digits_only) >= 16:
        return "backup", value_normalized

    if len(value) >= 1 and len(value) <= 4:
        return "trigger", value

    return "password", value


def clean_backup_codes(raw_codes: str) -> str:
    """تنظيف وتنسيق الأكواد الاحتياطية بمرونة"""
    normalized = convert_arabic_numbers(raw_codes)
    standardized = re.sub(r"[,\n]+", " ", normalized)
    found_codes = re.findall(r"\d{8,}", standardized)
    cleaned_codes = [code[-8:] for code in found_codes]
    unique_codes = list(dict.fromkeys(cleaned_codes))
    return ",".join(unique_codes)


def parse_inputs(field1, field2, field3):
    """تحليل الـ 3 خانات"""
    data = {"email": None, "password": None, "backup": None, "has_trigger": False}

    for field in [field1, field2, field3]:
        if not field:
            continue

        field_type, field_value = detect_field_type(field)

        if field_type == "trigger":
            data["has_trigger"] = True
        elif field_type == "backup":
            cleaned_codes = clean_backup_codes(field_value)
            data["backup"] = cleaned_codes
        elif field_type and field_value:
            data[field_type] = field_value

    return data


# ═══════════════════════════════════════════════════════════
# 🔐 CSRF Token Manager (مبسط)
# ═══════════════════════════════════════════════════════════


class SimpleCSRFManager:
    """مدير CSRF Token بسيط"""

    def __init__(self, base_url: str, cookies: dict):
        self.base_url = base_url
        self.cookies = cookies
        self.token = None
        self.session = None

    async def get_token(self) -> str:
        """الحصول على CSRF Token"""
        if self.token:
            return self.token

        await self._refresh_token()
        return self.token

    async def _refresh_token(self) -> bool:
        """جلب Token جديد من الموقع"""
        print(f"\n🔄 جلب CSRF Token جديد...")

        try:
            if not self.session or self. closed:
                self.session = aiohttp.ClientSession(cookies=self.cookies)

            async with self.session.get(f"{self.base_url}/senderPage") as resp:
                if resp.status != 200:
                    print(f"❌ فشل الطلب: {resp.status}")
                    return False

                html = await resp.text()

                match = re.search(r'<meta name="csrf-token" content="([^"]+)"', html)
                if not match:
                    print("❌ لم يتم العثور على CSRF Token في الصفحة")
                    return False

                self.token = match.group(1)
                print(f"✅ تم جلب Token جديد")
                return True

        except Exception as e:
            print(f"❌ خطأ في جلب Token: {e}")
            return False

    async def close(self):
        """إغلاق الـ Session"""
        if self.session and not self.session.closed:
            await self.session.close()


# مثيل عام من الـ CSRF Manager
csrf_manager = SimpleCSRFManager(BASE_URL, COOKIES)


# ═══════════════════════════════════════════════════════════
# 🌐 دوال التعامل مع الموقع
# ═══════════════════════════════════════════════════════════


async def get_account_data(session, account_id):
    """جلب بيانات الحساب الحالية"""
    try:
        csrf = await csrf_manager.get_token()
        get_data = f"idAccount={account_id}&csrf_token={csrf}"

        async with session.post(
            f"{BASE_URL}/dataFunctions/getAccountData",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=get_data,
        ) as resp:

            if resp.status != 200:
                return None

            result = await resp.json()
            account_data = result.get("data", [])

            if not account_data or len(account_data) < 3:
                return None

            return {
                "email": account_data[1] if len(account_data) > 1 else "",
                "password": account_data[2] if len(account_data) > 2 else "",
                "backup": account_data[3] if len(account_data) > 3 else "",
                "group": account_data[6] if len(account_data) > 6 else "1111",
            }

    except Exception as e:
        print(f"❌ خطأ في جلب البيانات: {e}")
        return None


async def edit_account(session, account_id, final_data):
    """إرسال طلب التعديل للسيرفر"""
    try:
        csrf = await csrf_manager.get_token()

        edit_payload = {
            "idAccount": account_id,
            "email": final_data["email"],
            "password": final_data["password"],
            "amountToTake": "",
            "amountToKeep": "",
            "backupCodes": final_data["backup"] or "",
            "groupName": final_data.get("group", "1111"),
            "priority": "0",
            "accountLock": 1,
            "forceProxy": "",
            "userPrice": "",
            "csrf_token": csrf,
        }

        async with session.post(
            f"{BASE_URL}/dataFunctions/editAccount",
            headers={"Content-Type": "application/json"},
            json=edit_payload,
        ) as resp:

            text = await resp.text()

            if resp.status == 200:
                return True, text
            else:
                return False, text

    except Exception as e:
        return False, str(e)


async def smart_edit_account(account_id, field1="", field2="", field3="") -> Tuple[bool, str, Optional[str], Dict]:
    """التعديل الذكي بنظام 3 خانات
    
    Returns:
        Tuple[bool, str, Optional[str], Dict]: (success, response, email, changes_report)
    """

    print("=" * 60)
    print(f"[SMART EDIT] 🎯 Starting edit for account: {account_id}")
    print("=" * 60)

    print("\n[SMART EDIT] 1️⃣ Analyzing inputs...")

    parsed = parse_inputs(field1, field2, field3)
    
    # 🆕 إنشاء تقرير التغييرات للرسالة الديناميكية
    changes_report = {
        "email": bool(parsed["email"]),
        "password": bool(parsed["password"]),
        "codes": bool(parsed["backup"]),
        "is_execute_only": not (parsed["email"] or parsed["password"] or parsed["backup"])
    }

    print("\n[SMART EDIT] 🔍 Parse results:")
    if parsed["email"]:
        print(f"[SMART EDIT]   ✅ Email found: {parsed['email']}")
    if parsed["password"]:
        print(f"[SMART EDIT]   ✅ Password found: {'*' * len(parsed['password'])}")
    if parsed["backup"]:
        codes_list = parsed["backup"].split(",")
        codes_count = len(codes_list)
        print(f"[SMART EDIT]   ✅ Backup codes found: {codes_count} code(s)")
        for i, code in enumerate(codes_list[:3], 1):
            print(f"[SMART EDIT]      • {code}")
        if codes_count > 3:
            print(f"[SMART EDIT]      ... and {codes_count - 3} more")
    if parsed["has_trigger"]:
        print(f"[SMART EDIT]   🔄 Trigger detected (will execute)")

    print("\n[SMART EDIT] 2️⃣ Fetching current account data...")

    headers = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
        "Origin": BASE_URL,
        "Referer": f"{BASE_URL}/senderPage",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

    async with aiohttp.ClientSession(cookies=COOKIES, headers=headers) as session:

        current_data = await get_account_data(session, account_id)

        if not current_data:
            print("[SMART EDIT]   ❌ Failed to fetch current data")
            return False, "فشل جلب البيانات", None, changes_report

        print(f"[SMART EDIT]   ✅ Current email: {current_data['email']}")
        print(f"[SMART EDIT]   ✅ Group: {current_data['group']}")

        print("\n[SMART EDIT] 3️⃣ Preparing final data for edit...")

        final_data = {
            "email": parsed["email"] or current_data["email"],
            "password": parsed["password"] or current_data["password"],
            "backup": parsed["backup"] or current_data["backup"],
            "group": current_data["group"],
        }

        print(f"[SMART EDIT]   📧 Final email: {final_data['email']}")
        if parsed["email"]:
            print(f"[SMART EDIT]      ↪️ Changed from: {current_data['email']}")
        if parsed["password"]:
            print(f"[SMART EDIT]   🔑 Password: Will be changed")
        if parsed["backup"]:
            codes_count = len(parsed["backup"].split(","))
            print(f"[SMART EDIT]   📋 Backup codes: Will be changed ({codes_count} code(s))")

        print("\n[SMART EDIT] 4️⃣ Sending edit request to server...")

        success, response = await edit_account(session, account_id, final_data)

        print("\n" + "=" * 60)
        if success:
            print("[SMART EDIT] ✅ Edit completed successfully!")
            print(f"[SMART EDIT] 📋 Response: {response[:100]}")
        else:
            print("[SMART EDIT] ❌ Ed failed!")
            print(f"[SMART EDIT] 📋 Response: {response[:200]}")
        print("=" * 60)

        # 🆕 إرجاع Email + changes_report للرسالة الديناميكية
        return success, response, final_data.get("email"), changes_report


# ═══════════════════════════════════════════════════════════
# 🔧 دوال مساعدة للبوت
# ═══════════════════════════════════════════════════════════


def is_all_states_group(chat_id: int) -> bool:
    """التحقق إذا كان الجروب هو 'جميع الحالات'"""
    return chat_id == ALL_STATES_GROUP_ID


def create_edit_sender_button(account_id: str) -> InlineKeyboardMarkup:
    """إنشاء زر 'تعديل سيندر' مرتبط بالحساب"""
    keyboard = [
        [
            InlineKeyboardButton(
                "🔧 تعديل بيانات السيندر", callback_data=f"edit_sender:{account_id}"
            )
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


# ═══════════════════════════════════════════════════════════
# 🤖 معالجات البوت
# ═══════════════════════════════════════════════════════════

# 🆕 حفظ حالة المستخدمين (account_id + email)
user_editing_state: Dict[int, Dict[str, str]] = {}


async def handle_edit_sender_button(update, context):
    """معالج زر 'تعديل سيندر'"""
    query = update.callback_query
    await query.answer()

    account_id = query.data.split(":")[1] if ":" in query.data else None

    if not account_id:
        await query.message.reply_text("❌ خطأ: لم يتم العثور على معرف الحساب")
        return

    # 🆕 جلب Email وحفظه في الحالة
    email_to_store = None
    try:
        async with aiohttp.ClientSession(cookies=COOKIES) as session:
            account_data = await get_account_data(session, account_id)
            if account_data:
                email_to_store = account_data.get("email")
    except Exception as e:
        print(f"[EDIT MODE] ⚠️ Failed to fetch email for {account_id}: {e}")

    # حفظ الحساب في حالة المستخدم (مع Email)
    user_id = query.from_user.id
    user_editing_state[user_id] = {
        "account_id": account_id,
        "email": email_to_store or "unknown",
    }
    
    print(f"\n[EDIT MODE] 🎯 User {user_id} started editing account {account_id}")
    print(f"[EDIT MODE] 📧 Email stored: {email_to_store}")
    print(f"[EDIT MODE] 📊 Current editing users: {list(user_editing_state.keys())}")

    keyboard = [
        [
            InlineKeyboardButton(
                "🔄 تنفيذ التعديل", callback_data=f"execute_edit:{account_id}"
            )
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.message.reply_text(
        f"✅ تم اختيار الحساب: `{account_id}`\n\n"
        f"📝 الآن:\n"
        f"• اكتب البيانات الجديدة (كل معلومة في سطر)\n"
        f"• أو اضغط على زر التنفيذ المباشر\n\n"
        f"╔═══════════════════════════════╗\n"
        f"║  📧 الإيميل (سطر 1)          ║\n"
        f"║  🔑 الباسورد (سطر 2)         ║\n"
        f"║  🔢 الأكواد (سطر 3، 4، ...) ║\n"
        f"╚═══════════════════════════════╝",
        parse_mode="Markdown",
        reply_markup=reply_markup,
    )


async def handle_execute_edit_button(update, context):
    """معالج زر 'تنفيذ التعديل' - مع Burst Mode Monitoring"""
    query = update.callback_query
    await query.answer()

    account_id = query.data.split(":")[1] if ":" in query.data else None

    if not account_id:
        await query.message.reply_text("❌ خطأ: لم يتم العثور على معرف الحساب")
        return

    print(f"\n🔄 تنفيذ مباشر للحساب: {account_id}")

    # الحصول على معلومات الحساب من الحالة المحفوظة
    user_id = query.from_user.id
    user_state = user_editing_state.get(user_id, {})
    stored_email = user_state.get("email", "unknown")

    # تنفيذ التعديل
    msg = await query.message.reply_text("⏳ جاري التنفيذ بالبيانات الحالية...")
    success, response, final_email, changes_report = await smart_edit_account(account_id)

    if success:
        # استخدام Email المحدث أو المحفوظ
        email_for_monitoring = final_email or stored_email
        
        # 🆕 عرض رسالة الملخص (Execute Only)
        await msg.edit_text("✅ تم تنفيذ الأوامر بنجاح.")
        await asyncio.sleep(3)  # نفس التوقيت
        
        # 🆕 عرض رسالة بدء المراقبة
        await msg.edit_text(
            f"✅ *تم التعديل!*\n"
            f"📧 `{email_for_monitoring}`\n"
            f"🆔 ID: `{account_id}`\n\n"
            f"🚀 *تفعيل BURST MODE...*\n"
            f"⏱️ متوقع: 3-10 ثوانٍ",
            parse_mode="Markdown",
        )
        
        # 🆕 بدء المراقبة
        if api_manager_instance and email_for_monitoring and account_id:
            asyncio.create_task(
                monitor_account_task(
                    api_manager_instance,
                    email_for_monitoring,
                    msg,
                    query.message.chat.id,
                    CONFIG["website"]["defaults"]["group_name"],
                    is_edit_context=True,
                    account_id=account_id,
                )
            )
            print(f"[EDIT MODE] 🚀 Started monitoring for EXECUTE_ONLY on account_id: {account_id}")
        else:
            print(f"[EDIT MODE] ⚠️ Cannot start monitoring: Missing api_manager or account info")
            
    else:
        await msg.edit_text(
            f"❌ فشل التنفيذ\n\n"
            f"📋 الرد: {response[:200]}\n\n"
            f"💡 تحقق من:\n"
            f"• الكوكيز في config.json\n"
            f"• CSRF Token\n"
            f"• اتصال الإنترنت"
        )

    # مسح الحالة
    if user_id in user_editing_state:
        del user_editing_state[user_id]


async def process_edit_input(update, context):
    """
    [EDIT MODE] معالج الإدخال النصي للبيانات - يُستدعى فقط من main.py
    🆕 مع Burst Mode Monitoring بعد التعديل الناجح
    🆕 مع Smart Edit Summary - رسالة ديناميكية
    """
    user_id = update.effective_user.id

    if user_id not in user_editing_state:
        print(f"[EDIT MODE] ⚠️ User {user_id} not in editing state. Ignoring.")
        return

    user_state = user_editing_state[user_id]
    account_id = user_state["account_id"]
    stored_email = user_state["email"]
    
    text = update.message.text
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    print(f"\n[EDIT MODE] 📝 Received data for account: {account_id}")
    print(f"[EDIT MODE] 📊 Number of lines: {len(lines)}")

    field1 = lines[0] if len(lines) > 0 else ""
    field2 = lines[1] if len(lines) > 1 else ""
    field3 = "\n".join(lines[2:]) if len(lines) > 2 else ""

    print(f"[EDIT MODE] 🔄 Starting smart edit process...")
    msg = await update.message.reply_text("⏳ جاري تحليل البيانات...")

    # تنفيذ التعديل مع changes_report
    success, response, final_email, changes_report = await smart_edit_account(account_id, field1, field2, field3)
    
    print(f"[EDIT MODE] 📋 Edit result: {'✅ SUCCESS' if success else '❌ FAILED'}")

    if success:
        # استخدام Email: المحدّث (لو اتغير) أو المحفوظ
        email_for_monitoring = final_email or stored_email
        
        # 🆕 بناء رسالة ديناميكية بناءً على changes_report
        summary_message = ""
        changed_items = []

        if changes_report["is_execute_only"]:
            summary_message = "✅ تم تنفيذ الأوامر بنجاح."
        else:
            if changes_report["email"]:
                changed_items.append("• 📧 الإيميل")
            if changes_report["password"]:
                changed_items.append("• 🔑 الباسورد")
            if changes_report["codes"]:
                changed_items.append("• 🔢 الأكواد")
            
            if changed_items:
                summary_message = "✅ تم التعديل بنجاح:\n" + "\n".join(changed_items)

        # عرض رسالة الملخص أولاً
        if summary_message:
            await msg.edit_text(summary_message)
            await asyncio.sleep(3)  # انتظار لعرض الملخص

        # ثم رسالة بدء المراقبة
        await msg.edit_text(
            f"✅ *تم التعديل!*\n"
            f"📧 `{email_for_monitoring}`\n"
            f"🆔 ID: `{account_id}`\n\n"
            f"🚀 *تفعيل BURST MODE...*\n"
            f"⏱️ متوقع: 3-10 ثوانٍ",
            parse_mode="Markdown",
        )
        
        # 🆕 استدعاء المراقبة
        if api_manager_instance:
            asyncio.create_task(
                monitor_account_task(
                    api_manager_instance,
                    email_for_monitoring,
                    msg,
                    update.effective_chat.id,
                    CONFIG["website"]["defaults"]["group_name"],
                    is_edit_context=True,
                    account_id=account_id,
                )
            )
            print(f"[EDIT MODE] 🚀 Started monitoring with account_id: {account_id}")
        else:
            print(f"[EDIT MODE] ⚠️ Cannot start monitoring: api_manager not available")
    else:
        await msg.edit_text(
            f"❌ فشل التعديل\n\n"
            f"📋 الرد: {response[:200]}"
        )

    # مسح الحالة
    del user_editing_state[user_id]
    print(f"[EDIT MODE] 🧹 Cleared editing state for user {user_id}")


# ═══════════════════════════════════════════════════════════
# 📝 دالة تسجيل المعالجات
# ═══════════════════════════════════════════════════════════


def register_handlers(application):
    """تسجيل معالجات التعديل في التطبيق"""
    from telegram.ext import CallbackQueryHandler

    application.add_handler(
        CallbackQueryHandler(handle_edit_sender_button, pattern="^edit_sender:")
    )
    application.add_handler(
        CallbackQueryHandler(handle_execute_edit_button, pattern="^execute_edit:")
    )

    print("✅ Edit sender handlers registered successfully!")


# ═══════════════════════════════════════════════════════════
# 🧹 دالة التنظيف
# ═══════════════════════════════════════════════════════════


async def cleanup():
    """تنظيف الموارد"""
    await csrf_manager.close()

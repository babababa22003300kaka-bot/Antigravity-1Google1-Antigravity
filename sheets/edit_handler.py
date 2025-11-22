#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
✏️ Edit Handler - معالج تحديث Emails للحسابات الموجودة
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ تحديث Email في نفس الصف (بدون إضافة صف جديد)
✅ البحث بالـ ID في عمود Z
✅ المقارنة قبل التحديث (توفير الموارد)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import json
import logging
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .error_notifier import track_sheets_errors

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
# 📂 ثوابت
# ═══════════════════════════════════════════════════════════

EDIT_QUEUE_FILE = Path("data/edit_queue.json")


# ═══════════════════════════════════════════════════════════
# 📝 Queue Management
# ═══════════════════════════════════════════════════════════


def load_edit_queue() -> List[Dict]:
    """تحميل queue التعديلات"""
    if EDIT_QUEUE_FILE.exists():
        try:
            with open(EDIT_QUEUE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get("edits", [])
        except Exception as e:
            logger.error(f"❌ Error loading edit_queue.json: {e}")
    return []


def save_edit_queue(edits: List[Dict]):
    """حفظ queue التعديلات"""
    try:
        EDIT_QUEUE_FILE.parent.mkdir(exist_ok=True)
        with open(EDIT_QUEUE_FILE, "w", encoding="utf-8") as f:
            json.dump({"edits": edits}, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"❌ Error saving edit_queue.json: {e}")


def add_to_edit_queue(account_id: str, new_email: str) -> bool:
    """
    إضافة تعديل جديد للـ queue

    Args:
        account_id: ID الحساب
        new_email: البريد الإلكتروني الجديد

    Returns:
        True إذا تمت الإضافة بنجاح
    """
    try:
        edits = load_edit_queue()

        # تجنب التكرار - نسجل آخر تعديل فقط (overwrite)
        edits = [edit for edit in edits if edit.get("id") != account_id]

        new_edit = {
            "id": account_id,
            "new_email": new_email,
            "edited_at": datetime.now().isoformat(),
        }

        edits.append(new_edit)
        save_edit_queue(edits)

        logger.info(
            f"📝 Added to Edit queue: ID {account_id} → {new_email}"
        )
        return True

    except Exception as e:
        logger.error(f"❌ Error adding to Edit queue: {e}")
        return False


def clear_edit_entry(account_id: str):
    """مسح تعديل من الـ queue (نجاح أو فشل)"""
    try:
        edits = load_edit_queue()
        original_count = len(edits)

        edits = [edit for edit in edits if edit.get("id") != account_id]

        if len(edits) < original_count:
            save_edit_queue(edits)
            logger.info(f"🗑️ Cleared from Edit queue: ID {account_id}")
            return True

        return False

    except Exception as e:
        logger.error(f"❌ Error clearing from Edit queue: {e}")
        return False


# ═══════════════════════════════════════════════════════════
# 🔍 البحث والقراءة من Google Sheet
# ═══════════════════════════════════════════════════════════


def find_row_by_id(sheets_api, account_id: str) -> Optional[int]:
    """
    البحث عن ID في عمود Z والحصول على رقم الصف

    Args:
        sheets_api: Google Sheets API instance
        account_id: ID الحساب

    Returns:
        رقم الصف (1-based) أو None
    """
    try:
        logger.info(f"🔍 Searching for ID {account_id} in column Z...")

        # قراءة عمود Z كامل
        column_range = f"{sheets_api.sheet_name}!Z:Z"
        result = (
            sheets_api.service.spreadsheets()
            .values()
            .get(spreadsheetId=sheets_api.spreadsheet_id, range=column_range)
            .execute()
        )

        values = result.get("values", [])

        if not values:
            logger.warning("⚠️ Column Z is empty")
            return None

        # البحث عن ID
        for idx, row in enumerate(values, start=1):
            if row and str(row[0]).strip() == str(account_id).strip():
                logger.info(f"✅ Found ID {account_id} at row {idx}")
                return idx

        logger.warning(f"⚠️ ID {account_id} not found in Sheet")
        return None

    except Exception as e:
        logger.error(f"❌ Error searching Sheet: {e}")
        return None


def read_email_from_sheet(sheets_api, row_number: int) -> Optional[str]:
    """
    قراءة Email من عمود A

    Args:
        sheets_api: Google Sheets API instance
        row_number: رقم الصف (1-based)

    Returns:
        Email أو None
    """
    try:
        cell_range = f"{sheets_api.sheet_name}!A{row_number}"

        logger.debug(f"📖 Reading {cell_range}...")

        result = (
            sheets_api.service.spreadsheets()
            .values()
            .get(spreadsheetId=sheets_api.spreadsheet_id, range=cell_range)
            .execute()
        )

        values = result.get("values", [])

        if values and values[0]:
            email = values[0][0]
            logger.debug(f"✅ Read email: {email}")
            return email

        logger.warning(f"⚠️ Cell A{row_number} is empty")
        return None

    except Exception as e:
        logger.error(f"❌ Error reading cell A{row_number}: {e}")
        return None


# ═══════════════════════════════════════════════════════════
# ✏️ تحديث Email في Google Sheet
# ═══════════════════════════════════════════════════════════


@track_sheets_errors(operation="update_email_cell", worker="edit")
def update_email_cell(
    sheets_api, row_number: int, new_email: str
) -> Tuple[bool, str]:
    """
    تحديث Email في عمود A

    Args:
        sheets_api: Google Sheets API instance
        row_number: رقم الصف (1-based)
        new_email: البريد الإلكتروني الجديد

    Returns:
        (success: bool, message: str)
    """
    try:
        cell_range = f"{sheets_api.sheet_name}!A{row_number}"

        logger.info(f"✏️ Updating {cell_range} with email: '{new_email}'")

        body = {"values": [[new_email]]}

        sheets_api.service.spreadsheets().values().update(
            spreadsheetId=sheets_api.spreadsheet_id,
            range=cell_range,
            valueInputOption="USER_ENTERED",
            body=body,
        ).execute()

        logger.info(f"✅ Successfully updated {cell_range}")
        return True, f"Updated {cell_range}"

    except Exception as e:
        logger.error(f"❌ Error updating cell A{row_number}: {e}")
        return False, str(e)


# ═══════════════════════════════════════════════════════════
# 🔄 المعالج الرئيسي
# ═══════════════════════════════════════════════════════════


def update_email_in_sheet(
    sheets_api, account_id: str, new_email: str
) -> Tuple[bool, str]:
    """
    تحديث Email لحساب موجود (بدون إضافة صف جديد)

    الخطوات:
    1. البحث في عمود Z عن الـ ID
    2. الحصول على رقم الصف
    3. قراءة Email الحالي من عمود A
    4. المقارنة: لو مختلف → تحديث عمود A في نفس الصف
    5. لو متطابق → مافيش حاجة تتعمل

    Args:
        sheets_api: Google Sheets API instance
        account_id: ID الحساب
        new_email: البريد الإلكتروني الجديد

    Returns:
        (success: bool, message: str)
    """
    try:
        logger.info(
            f"🔄 Starting email update for ID {account_id} → {new_email}"
        )

        # 1. البحث عن ID في عمود Z
        row_number = find_row_by_id(sheets_api, account_id)

        if not row_number:
            msg = f"ID {account_id} not found in Sheet"
            logger.warning(f"⚠️ {msg}")
            return False, msg

        # 2. قراءة Email الحالي من عمود A
        current_email = read_email_from_sheet(sheets_api, row_number)

        if current_email is None:
            msg = f"Could not read email from row {row_number}"
            logger.error(f"❌ {msg}")
            return False, msg

        # 3. المقارنة
        if current_email.strip().lower() == new_email.strip().lower():
            msg = f"Email unchanged for ID {account_id} - no update needed"
            logger.info(f"ℹ️ {msg}")
            return True, msg  # مافيش داعي للتحديث

        # 4. تحديث عمود A فقط
        success, update_msg = update_email_cell(sheets_api, row_number, new_email)

        if success:
            logger.info(
                f"✅ Updated email in row {row_number}: {current_email} → {new_email}"
            )
            return True, f"Updated row {row_number}"
        else:
            return False, update_msg

    except Exception as e:
        logger.exception(f"❌ Error in update_email_in_sheet: {e}")
        return False, str(e)


# ═══════════════════════════════════════════════════════════
# ⚙️ Edit Worker
# ═══════════════════════════════════════════════════════════


@track_sheets_errors(operation="edit_worker", worker="edit")
async def edit_worker(config: Dict, sheets_api):
    """
    🔄 Worker معالجة تعديلات Emails

    التدفق:
    1. قراءة edit_queue.json كل 1-10 ثواني
    2. لكل تعديل:
       - البحث في Sheet (عمود Z)
       - قراءة Email الحالي (عمود A)
       - المقارنة والتحديث إذا مختلف
       - مسح من edit_queue.json

    Args:
        config: إعدادات التطبيق
        sheets_api: Google Sheets API instance
    """
    handler_config = config.get("edit_handler", {})

    # التحقق من التفعيل
    if not handler_config.get("enabled", True):
        logger.info("⚠️ Edit handler is disabled in config")
        return

    # قراءة الإعدادات
    interval_min = handler_config.get("interval_min", 1)
    interval_max = handler_config.get("interval_max", 10)

    logger.info(
        f"🚀 Edit Worker started (interval: {interval_min}-{interval_max}s)"
    )

    while True:
        try:
            # قراءة Queue
            edits = load_edit_queue()

            if not edits:
                # لا يوجد شيء للمعالجة
                await asyncio.sleep(random.uniform(interval_min, interval_max))
                continue

            logger.info(f"📋 Processing {len(edits)} edits from Edit queue")

            for edit_item in edits:
                try:
                    account_id = edit_item.get("id", "")
                    new_email = edit_item.get("new_email", "")

                    if not account_id or not new_email:
                        logger.warning("⚠️ Invalid edit item - skipping")
                        clear_edit_entry(account_id)
                        continue

                    logger.info(
                        f"🔄 Processing edit: ID {account_id} → {new_email}"
                    )

                    # تحديث Email في Sheet
                    success, message = update_email_in_sheet(
                        sheets_api, account_id, new_email
                    )

                    if success:
                        logger.info(f"✅ Edit processed successfully: {message}")
                    else:
                        logger.error(f"❌ Edit failed: {message}")

                    # مسح من Queue (نجح أو فشل - بدون retry)
                    clear_edit_entry(account_id)

                except Exception as e:
                    logger.exception(f"❌ Error processing edit item: {e}")
                    # مسح حتى لو حصل خطأ
                    clear_edit_entry(edit_item.get("id", ""))

            # انتظار عشوائي قبل الدورة التالية
            interval = random.uniform(interval_min, interval_max)
            logger.debug(f"💤 Next check in {interval:.1f}s")
            await asyncio.sleep(interval)

        except Exception as e:
            logger.exception(f"❌ Fatal error in Edit Worker: {e}")
            await asyncio.sleep(30)


# ═══════════════════════════════════════════════════════════
# 🚀 تشغيل Worker (يُستدعى من worker.py)
# ═══════════════════════════════════════════════════════════


async def start_edit_worker(config: Dict, sheets_api):
    """
    تشغيل Edit Worker

    Args:
        config: إعدادات التطبيق
        sheets_api: Google Sheets API instance
    """
    try:
        logger.info("✏️ Starting Edit Worker...")
        await edit_worker(config, sheets_api)
    except Exception as e:
        logger.exception(f"❌ Fatal error in Edit Worker: {e}")

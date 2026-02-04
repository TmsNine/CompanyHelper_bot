import os
import asyncio
import logging
from math import ceil
from datetime import datetime, timedelta, timezone, date
from math import ceil
from dateutil import parser as dateparser
from html import escape


from aiogram.filters import StateFilter
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.filters.command import CommandObject
from aiogram.filters import Command

from dotenv import load_dotenv
from pathlib import Path

# Загружаем ровно .env, который лежит РЯДОМ с bot.py (без вариантов)
ENV_PATH = Path(__file__).resolve().with_name(".env")
load_dotenv(ENV_PATH, override=True)
print("DEBUG GSHEET_ID:", os.getenv("GSHEET_ID"))

import aiosqlite

# --- Google Sheets (async) ---
import json
import gspread_asyncio
from google.oauth2.service_account import Credentials

# =========================
# Конфиг и TZ
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_TG_ID = int(os.getenv("OWNER_TG_ID", "0"))
TZ_NAME = os.getenv("TZ", "UTC")
DEVELOPER_TG_ID = int(os.getenv("DEVELOPER_TG_ID", "0"))

UTC = timezone.utc
try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo(TZ_NAME)
except Exception:
    LOCAL_TZ = UTC

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN is not set")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(
        parse_mode=ParseMode.HTML,          # глобально включаем HTML
        # если раньше использовал:
        # link_preview_is_disabled=True,     # = disable_web_page_preview
        # protect_content=True,              # защита сообщений от пересылки
    )
)
dp = Dispatcher()
router = Router()
dp.include_router(router)

DB_PATH = "bot.db"
PAGE_SIZE = 8  # постраничный выбор сотрудников

# =========================
# Инициализация БД
# =========================
CREATE_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tg_id INTEGER UNIQUE NOT NULL,
  full_name TEXT,
  role TEXT CHECK(role IN ('employee','lead','head','developer')) NOT NULL DEFAULT 'employee',
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS manager_links (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  manager_user_id INTEGER NOT NULL,
  subordinate_user_id INTEGER NOT NULL,
  FOREIGN KEY(manager_user_id) REFERENCES users(id),
  FOREIGN KEY(subordinate_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  description TEXT NOT NULL,
  deadline TEXT, -- ISO8601 (UTC)
  status TEXT CHECK(status IN ('new','in_progress','almost_done','done')) NOT NULL DEFAULT 'new',
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now')),
  last_reminder_at TEXT,
  next_reminder_at TEXT,
  last_postpone_reason TEXT,
  started_at TEXT,
  planned_start_at TEXT,
  assigned_by_user_id INTEGER,
  FOREIGN KEY(user_id) REFERENCES users(id),
  FOREIGN KEY(assigned_by_user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_tasks_user_status ON tasks(user_id, status);
CREATE INDEX IF NOT EXISTS idx_tasks_nextrem ON tasks(next_reminder_at);

-- === projects & project_links =====================================
CREATE TABLE IF NOT EXISTS projects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,               -- имя проекта (в меню кнопками)
  created_by_id INTEGER NOT NULL,          -- кто создал
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY(created_by_id) REFERENCES users(id)
);

-- === projects meta & tasks (для второй таблицы) ====================
CREATE TABLE IF NOT EXISTS project_meta (
  project_id   INTEGER PRIMARY KEY,
  prj_type     TEXT,               -- 3D | 2D | дизайн | монтаж
  start_date   TEXT NOT NULL,      -- YYYY-MM-DD
  deadline     TEXT NOT NULL,      -- YYYY-MM-DD
  sheet_title  TEXT NOT NULL,      -- имя листа в 2-й таблице
  FOREIGN KEY(project_id) REFERENCES projects(id)
);

CREATE TABLE IF NOT EXISTS project_tasks (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id        INTEGER NOT NULL,
  row_index         INTEGER NOT NULL,        -- номер строки в листе
  task_text         TEXT    NOT NULL,
  assignee_user_id  INTEGER NOT NULL,
  planned_date      TEXT    NOT NULL,        -- YYYY-MM-DD (по локальному TZ)
  duration_days     INTEGER NOT NULL DEFAULT 1,
  status            TEXT    NOT NULL DEFAULT 'open', -- open|done
  created_at        TEXT DEFAULT (datetime('now')),
  FOREIGN KEY(project_id) REFERENCES projects(id),
  FOREIGN KEY(assignee_user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_proj_tasks_proj ON project_tasks(project_id);
CREATE INDEX IF NOT EXISTS idx_proj_tasks_date ON project_tasks(planned_date);

CREATE TABLE IF NOT EXISTS project_links (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL,
  title TEXT NOT NULL,                     -- как показываем ссылку
  url TEXT NOT NULL,                       -- сама ссылка
  created_by_id INTEGER NOT NULL,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY(project_id) REFERENCES projects(id),
  FOREIGN KEY(created_by_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_project_links_proj ON project_links(project_id);
"""

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        # Включаем внешние ключи
        await db.execute("PRAGMA foreign_keys = ON;")

        # Базовая схема (создаст таблицы, индексы и PRAGMA из CREATE_SQL)
        await db.executescript(CREATE_SQL)

        # Таблица для элементов плана — создаём один раз
        await db.execute("""
        CREATE TABLE IF NOT EXISTS daily_plan_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            plan_date TEXT NOT NULL,      -- YYYY-MM-DD
            text TEXT NOT NULL,           -- сырой пункт (с временем)
            time_str TEXT NOT NULL,       -- HH:MM
            task_id INTEGER,              -- связанная задача (если создана)
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """)
        await db.commit()

            # --- Credentials (пароли командных сервисов) ---
        await db.execute("""
        CREATE TABLE IF NOT EXISTS creds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,             -- название сервиса (например, 'Figma', 'Notion', 'Jira')
            login TEXT NOT NULL,
            password TEXT NOT NULL,
            note TEXT,
            created_by_id INTEGER NOT NULL,  -- кто добавил (users.id)
            created_at TEXT NOT NULL,
            FOREIGN KEY(created_by_id) REFERENCES users(id)
        );
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_creds_title ON creds(title);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_creds_created_by ON creds(created_by_id);")
        await db.commit()

        # Аккуратные ALTER для старых баз — каждый в try/except
        alters = [
            # Этап A: статистика/аналитика
            "ALTER TABLE tasks ADD COLUMN completed_at TEXT",
            "ALTER TABLE tasks ADD COLUMN delay_minutes INTEGER",

            # дополнительные служебные поля (если их ещё нет)
            "ALTER TABLE tasks ADD COLUMN last_postpone_reason TEXT",
            "ALTER TABLE tasks ADD COLUMN started_at TEXT",
            "ALTER TABLE tasks ADD COLUMN planned_start_at TEXT",
            "ALTER TABLE tasks ADD COLUMN assigned_by_user_id INTEGER",
            "ALTER TABLE tasks ADD COLUMN last_reminder_msg_id INTEGER",
            "ALTER TABLE users ADD COLUMN last_plan_msg_id INTEGER",
            "ALTER TABLE users ADD COLUMN last_plan_date TEXT",
            "ALTER TABLE tasks ADD COLUMN completed_by_user_id INTEGER",
            "ALTER TABLE tasks ADD COLUMN overdue_minutes INTEGER"

            "CREATE TABLE IF NOT EXISTS projects ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " name TEXT NOT NULL UNIQUE,"
            " created_by_id INTEGER NOT NULL,"
            " created_at TEXT DEFAULT (datetime('now')),"
            " FOREIGN KEY(created_by_id) REFERENCES users(id)"
            ")",
            "CREATE TABLE IF NOT EXISTS project_links ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " project_id INTEGER NOT NULL,"
            " title TEXT NOT NULL,"
            " url TEXT NOT NULL,"
            " created_by_id INTEGER NOT NULL,"
            " created_at TEXT DEFAULT (datetime('now')),"
            " FOREIGN KEY(project_id) REFERENCES projects(id),"
            " FOREIGN KEY(created_by_id) REFERENCES users(id)"
            ")",
            "CREATE INDEX IF NOT EXISTS idx_project_links_proj ON project_links(project_id)",
        ]

                # --- Журнал событий задач (для Ганта) ---
        await db.execute("""
        CREATE TABLE IF NOT EXISTS task_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            event TEXT NOT NULL,           -- 'create' | 'start' | 'deadline_set' | 'postpone' | 'done'
            at TEXT NOT NULL,              -- ISO-UTC timestamp
            meta TEXT,                     -- произвольные данные (старый/новый дедлайн, причина и т.п.)
            FOREIGN KEY(task_id) REFERENCES tasks(id)
        )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_task_events_task ON task_events(task_id)")
        await db.commit()

        for alter in alters:
            try:
                await db.execute(alter)
                await db.commit()
            except Exception:
                # колонка уже есть — пропускаем
                pass
                # Новые колонки для управления доступом
            try:
                await db.execute("ALTER TABLE users ADD COLUMN registered INTEGER NOT NULL DEFAULT 0")
                await db.commit()
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
                await db.commit()
            except Exception:
                pass
            # --- отдел пользователя ---
            try:
                await db.execute("ALTER TABLE users ADD COLUMN dept TEXT")
                await db.commit()
            except Exception:
                pass


        # Промоущаем разработчика (даже если он не зарегистрирован формально)
        if DEVELOPER_TG_ID:
            await db.execute("UPDATE users SET role='developer', is_active=1 WHERE tg_id=?", (DEVELOPER_TG_ID,))
            await db.commit()

# === FULL RESET: утилита жёсткого сброса базы ===
async def db_full_reset():
    """
    Полностью очищает все основные таблицы и делает VACUUM.
    Затем (если есть DEVELOPER_TG_ID в .env) — создаёт запись разработчика.
    """
    TABLES = [
        "task_events",
        "reminders",
        "project_tasks",
        "projects",
        "tasks",
        "users",
    ]
    async with aiosqlite.connect(DB_PATH) as db:
        for t in TABLES:
            try:
                await db.execute(f"DELETE FROM {t}")
            except Exception:
                pass
        try:
            await db.execute("VACUUM")
        except Exception:
            pass
        await db.commit()

    # --- автосоздание разработчика после сброса (опционально) ---
    # Берём tg_id из переменной окружения DEVELOPER_TG_ID.
    # Можно задать в .env: DEVELOPER_TG_ID=462362231
    from os import getenv

    dev_tg = getenv("DEVELOPER_TG_ID")  # <— имя переменной окружения
    if dev_tg:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO users(tg_id, full_name, role, created_at) VALUES(?,?,?,?)",
                (int(dev_tg), "Developer", "developer", datetime.now(UTC).isoformat()),
            )
            await db.commit()

# =========================
# Утилиты
# =========================

import html

STATUS_LABELS = {
    "new": "Ожидает",
    "in_progress": "В процессе",
    "almost_done": "Почти готово",
    "done": "Завершена",
}

def status_human(s: str) -> str:
    return STATUS_LABELS.get(s, s)

def task_line_html(tid: int, desc: str, status: str, deadline_iso: str | None) -> str:
    # безопасно экранируем произвольные тексты
    d = html.escape(desc or "")
    st = html.escape(status_human(status))
    dl = html.escape(fmt_dt_local(deadline_iso))
    return (
        f"#{tid}: <b>{d}</b> | <u>{st}</u>\n"
        f"> <b>Дедлайн</b>: {dl}"
    )

def _format_task_line(task_id: int, desc: str, status: str, deadline_iso: str | None) -> str:
    status_map = {"new": "Ожидает", "in_progress": "В работе", "done": "Готово"}
    status_h = status_map.get((status or "").lower(), status or "")
    line = f"#{task_id}: <b>{H(desc)}</b> | <u>{H(status_h)}</u>"
    if deadline_iso:
        line += f"\n{Q('Дедлайн: ' + fmt_dt_local(deadline_iso))}"
    return line

def render_task_card(
    task_id: int,
    description: str,
    status: str | None,
    deadline_iso: str | None,
) -> str:
    """
    HTML-карточка задачи с цветными индикаторами статуса (эмодзи).
    """
    s = (status or "").lower()
    # Индикаторы:
    # new -> ⚪, in_progress -> 🟡, almost_done -> 🟠, done -> 🟢, просрочка -> 🔴
    dot = "⚪"
    if s == "in_progress":
        dot = "🟡"
    elif s == "almost_done":
        dot = "🟠"
    elif s == "done":
        dot = "🟢"

    # Если есть дедлайн и он в прошлом — пометим красным
    overdue = False
    if deadline_iso:
        try:
            dl = dateparser.parse(deadline_iso)
            if (dl.replace(tzinfo=dl.tzinfo or UTC)) < datetime.now(UTC) and s != "done":
                overdue = True
        except Exception:
            pass
    if overdue:
        dot = "🔴"

    title = f"{dot} #{task_id}: <b>{H(description)}</b>"
    st = STATUS_LABELS.get(s, s)
    status_line = f"<u>{H(st)}</u>"
    dl_line = f"<blockquote><b>Дедлайн:</b> {fmt_dt_local(deadline_iso)}</blockquote>" if deadline_iso else ""
    return f"{title} | {status_line}\n{dl_line}"

def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить задачу"), KeyboardButton(text="📋 Мои задачи")],
            [KeyboardButton(text="🔗 Важные ссылки"),   KeyboardButton(text="🔐 Пароли")],
            [KeyboardButton(text="🛠 Изменить статус задачи")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие…",
        one_time_keyboard=False,
        selective=False,
        is_persistent=True,
    )

from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

def manager_reply_kb(is_head: bool, is_dev: bool = False) -> ReplyKeyboardMarkup:
    """
    Reply-меню руководителя/разработчика (кнопки с текстом).
    Всегда возвращаем раскладку 2×N.
    """
    def _row(a: str, b: str) -> list[KeyboardButton]:
        return [KeyboardButton(text=a), KeyboardButton(text=b)]

    rows: list[list[KeyboardButton]] = []

    # Базовый блок
    rows += [
        _row("👤 Назначить задачу", "📊 Сводка по сотруднику"),
        _row("👥 Мои подчинённые", "🔗 Связать иерархию"),
    ]

    # Только для руководителей
    if is_head:
        rows += [
            _row("📒 Руководители", "🛠 Назначить роль"),
        ]

    # Для руководителя ИЛИ разработчика
    if is_head or is_dev:
        rows += [
            _row("🏷 Определить в отдел", "📨 Запросить план"),
            _row("Проекты", "⬅️ В главное меню"),
        ]

    # Только разработчику (служебные)
    if is_dev:
        rows += [
            _row("📈 Зарегистрировались", "👥 Сотрудники (удаление)"),
            [KeyboardButton(text="🧨 FULL RESET")],
        ]

    kb = ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        input_field_placeholder="Меню руководителя..."
    )
    return kb

async def _remove_kb_safe(msg: Message) -> None:
    """Снять inline-клавиатуру у сообщения (если можно)."""
    try:
        await msg.edit_reply_markup(reply_markup=None)
    except Exception:
        # сообщение могло быть не нашим/устаревшим — просто игнорируем
        pass

async def _delete_msg_safe(msg: Message) -> None:
    """Удалить сообщение (если можно)."""
    try:
        await msg.delete()
    except Exception:
        pass

async def _delete_msg_id_safe(chat_id: int, message_id: int) -> None:
    """Удалить сообщение по ID (если можно)."""
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass

async def _track_form_message(state: FSMContext, msg: Message | None) -> None:
    if not msg:
        return
    data = await state.get_data()
    ids = data.get("form_msg_ids", [])
    ids.append(msg.message_id)
    await state.update_data(form_msg_ids=ids)

async def _cleanup_form_messages(state: FSMContext, chat_id: int) -> None:
    data = await state.get_data()
    for mid in data.get("form_msg_ids", []):
        await _delete_msg_id_safe(chat_id, mid)
    await state.update_data(form_msg_ids=[])

class BigProjectCreate(StatesGroup):
    waiting_name = State()
    waiting_type = State()
    waiting_start = State()
    waiting_deadline = State()

class ProjTaskAdd(StatesGroup):
    waiting_text = State()
    picking_assignee = State()
    picking_date = State()

# ===== Большие проекты: создание и план =====

PAGE_DAYS = 28  # сколько дат показываем на странице

def _build_dates_kb(start_d: date, end_d: date, page: int, mk_date_cb, mk_page_cb) -> InlineKeyboardBuilder:
    days = []
    d = start_d
    while d <= end_d:
        days.append(d)
        d += timedelta(days=1)

    total = len(days)
    pages = max(1, ceil(total / PAGE_DAYS))
    page = max(0, min(page, pages - 1))
    i1, i2 = page * PAGE_DAYS, page * PAGE_DAYS + PAGE_DAYS
    chunk = days[i1:i2]

    kb = InlineKeyboardBuilder()
    for dd in chunk:
        kb.button(text=dd.strftime("%d.%m"), callback_data=mk_date_cb(dd.isoformat()))

    if pages > 1:
        if page > 0:
            kb.button(text="◀", callback_data=mk_page_cb(page - 1))
        kb.button(text=f"{page+1}/{pages}", callback_data=_noop_cb())
        if page < pages - 1:
            kb.button(text="▶", callback_data=mk_page_cb(page + 1))
        kb.adjust(7, 3)  # сетка дат 7xN + навигация 3 кнопки
    else:
        kb.adjust(7)

    return kb

def _proj_type_cb(t: str) -> str: return f"proj:new:type:{t}"
def _proj_plan_add_cb(pid: int) -> str: return f"proj:plan_add:{pid}"
def _proj_plan_later_cb(pid: int) -> str: return f"proj:plan_later:{pid}"
def _proj_user_list_cb(page: int) -> str: return f"projuser_list:{page}"
def _proj_user_pick_cb(uid: int) -> str: return f"projuser_user:{uid}"
def _proj_date_pick_cb(pid: int, iso: str) -> str: return f"projdate:{pid}:{iso}"
def _proj_move_start_cb(tid: int) -> str: return f"projmove:{tid}"
def _proj_move_date_cb(tid: int, iso: str) -> str: return f"projmove_date:{tid}:{iso}"
def _proj_extend1_cb(tid: int) -> str: return f"projextend1:{tid}"
def _proj_done_cb(tid: int) -> str: return f"projdone:{tid}"


@router.message(F.text == "🧩 Добавить проект")
async def bigproj_start(m: Message, state: FSMContext):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
    if me["role"] not in ("head","developer"):
        await m.answer("Нет доступа."); return
    await state.update_data(form_msg_ids=[])
    await state.set_state(BigProjectCreate.waiting_name)
    prompt = await m.answer("Название проекта?")
    await _track_form_message(state, prompt)

@router.message(BigProjectCreate.waiting_name)
async def bigproj_name(m: Message, state: FSMContext):
    """
    Шаг 1: ввод названия проекта.
    Фикс: не принимаем текст из главного меню/команд как название;
    при «Проекты» выходим из мастера и открываем корень раздела.
    """
    text = (m.text or "").strip()

    await _track_form_message(state, m)

    # 1) Любая команда вида /... — не принимаем как название
    if text.startswith("/"):
        await state.clear()
        return  # просто игнорируем чужую команду

    # 2) Кнопки главного меню (в т.ч. «Проекты») — не принимать как название
    try:
        from_main_menu = text in MAIN_ENTRY_TEXTS  # у тебя уже объявлен список главных пунктов
    except Exception:
        from_main_menu = (text == "Проекты")

    if from_main_menu:
        # Выходим из мастера и открываем меню «Проекты»
        await state.clear()
        return await mgr_projects_menu(m, state)

    # 3) Пустота/мусор – снова попросим ввести название
    if not text:
        prompt = await m.answer(
            "Введите, пожалуйста, <b>название проекта</b> текстом (например: <code>Nora Space</code>).",
            parse_mode="HTML",
        )
        await _track_form_message(state, prompt)
        return

    # ---- дальше твоя логика создания проекта как была ----
    # сохраняем название в FSM и переводим на шаг выбора типа
    await state.update_data(prj_name=text)

    # показываем выбор типа проекта (используй те же кнопки, что уже есть у тебя)
    kb = InlineKeyboardBuilder()
    kb.button(text="3D",     callback_data="proj:new:type:3d")
    kb.button(text="2D",     callback_data="proj:new:type:2d")
    kb.button(text="дизайн", callback_data="proj:new:type:design")
    kb.button(text="монтаж", callback_data="proj:new:type:montage")
    kb.adjust(2)
    await state.set_state(BigProjectCreate.waiting_type)
    prompt = await m.answer("Тип проекта?", reply_markup=kb.as_markup())
    await _track_form_message(state, prompt)

@router.callback_query(F.data.startswith("proj:new:type:"))
async def bigproj_type(cq: CallbackQuery, state: FSMContext):
    t = cq.data.split(":")[3]
    await state.update_data(prj_type=t)
    await state.set_state(BigProjectCreate.waiting_start)
    prompt = await cq.message.answer("Дата начала (ДД.ММ.ГГГГ)?")
    await _track_form_message(state, prompt)
    await cq.answer()

def _parse_dmy(s: str) -> date | None:
    try:
        d, m, y = [int(x) for x in s.strip().split(".")]
        return date(y, m, d)
    except Exception:
        return None

@router.message(BigProjectCreate.waiting_start)
async def bigproj_startdate(m: Message, state: FSMContext):
    d = _parse_dmy(m.text or "")
    if not d:
        await _track_form_message(state, m)
        prompt = await m.answer("Формат даты: ДД.ММ.ГГГГ")
        await _track_form_message(state, prompt)
        return
    await _track_form_message(state, m)
    await state.update_data(start_date=d.isoformat())
    await state.set_state(BigProjectCreate.waiting_deadline)
    prompt = await m.answer("Дедлайн (ДД.ММ.ГГГГ)?")
    await _track_form_message(state, prompt)

@router.message(BigProjectCreate.waiting_deadline)
async def bigproj_deadline(m: Message, state: FSMContext):
    dl = _parse_dmy(m.text or "")
    if not dl:
        await _track_form_message(state, m)
        prompt = await m.answer("Формат даты: ДД.ММ.ГГГГ")
        await _track_form_message(state, prompt)
        return

    await _track_form_message(state, m)

    data = await state.get_data()
    start = date.fromisoformat(data["start_date"])
    if dl < start:
        prompt = await m.answer("Дедлайн раньше даты начала — поправь.")
        await _track_form_message(state, prompt)
        return

    name = data["prj_name"]
    prj_type = data["prj_type"]

    # 1) создаём/находим проект в нашей БД
    async with aiosqlite.connect(DB_PATH) as db:
        me = await ensure_user(db, m.from_user.id, m.from_user.full_name or "")
        # projects.name уже уникально (существует в схеме)
        cur = await db.execute("SELECT id FROM projects WHERE name=?", (name,))
        row = await cur.fetchone()
        if row:
            pid = row[0]
        else:
            cur2 = await db.execute("INSERT INTO projects(name, created_by_id) VALUES(?,?)", (name, me["id"]))
            await db.commit()
            pid = cur2.lastrowid

    # 2) создаём лист во 2-й таблице
    sh = await _gs_open_projects()
    sheet_title = await _dedupe_sheet_title(sh, _sheet_title_from_name(name))
    ws = await _ensure_project_ws(sh, sheet_title, start, dl)

    # 3) сохраняем мету (upsert)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO project_meta(project_id, prj_type, start_date, deadline, sheet_title)
            VALUES(?,?,?,?,?)
            ON CONFLICT(project_id) DO UPDATE SET prj_type=excluded.prj_type,
                start_date=excluded.start_date, deadline=excluded.deadline, sheet_title=excluded.sheet_title
        """, (pid, prj_type, start.isoformat(), dl.isoformat(), sheet_title))
        await db.commit()

    await _cleanup_form_messages(state, m.chat.id)
    await state.clear()

    # 4) спрашиваем про план
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить задачу", callback_data=_proj_plan_add_cb(pid))
    kb.button(text="📤 Сводка/экспорт", callback_data=f"proj:summary:{pid}")
    kb.button(text="⏰ Вернуться позже", callback_data=_proj_plan_later_cb(pid))
    kb.adjust(1)
    await m.answer(
        f"✅ Проект «{name}» создан. Лист: <code>{sheet_title}</code>\nСоставим план работ?",
        reply_markup=kb.as_markup()
    )

@router.callback_query(F.data.startswith("proj:plan_add:"))
async def proj_plan_add(cq: CallbackQuery, state: FSMContext):
    pid = int(cq.data.split(":")[2])

    # удалить сообщение со списком проектов
    try:
        await cq.message.delete()
    except Exception:
        pass

    await state.update_data(form_msg_ids=[])
    await state.update_data(add_pid=pid)
    await state.set_state(ProjTaskAdd.waiting_text)
    prompt = await cq.message.answer("Опиши задачу одним сообщением:")
    await _track_form_message(state, prompt)
    await cq.answer()

@router.callback_query(F.data.startswith("proj:plan_later:"))
async def proj_plan_later(cq: CallbackQuery):
    await cq.message.answer("Ок, можно вернуться к плану в любое время через кнопку проекта.")
    await cq.answer()

@router.callback_query(F.data.startswith("proj:summary:"))
async def proj_summary(cq: CallbackQuery):
    pid = int(cq.data.split(":")[2])
    today = datetime.now(LOCAL_TZ).date()

    async with aiosqlite.connect(DB_PATH) as db:
        # имя проекта
        cur = await db.execute("SELECT name FROM projects WHERE id=?", (pid,))
        r = await cur.fetchone()
        prj_name = (r[0] if r else f"#{pid}")

        # для сводки (по людям)
        cur = await db.execute("""
            SELECT pt.assignee_user_id, u.full_name, pt.status, pt.planned_date
            FROM project_tasks pt
            JOIN users u ON u.id = pt.assignee_user_id
            WHERE pt.project_id=?
        """, (pid,))
        rows = await cur.fetchall()

        # для CSV
        cur2 = await db.execute("""
            SELECT pt.id, pt.task_text, u.full_name, pt.planned_date, pt.duration_days, pt.status
            FROM project_tasks pt
            JOIN users u ON u.id = pt.assignee_user_id
            WHERE pt.project_id=?
            ORDER BY pt.id
        """, (pid,))
        csv_rows = await cur2.fetchall()

    # посчитать: done / overdue / open
    stats = {}
    for uid, full, st, planned_iso in rows:
        key = (uid, full or f"user_{uid}")
        stats.setdefault(key, {"done": 0, "overdue": 0, "open": 0})
        if st == "done":
            stats[key]["done"] += 1
        else:
            d = date.fromisoformat(planned_iso)
            if d < today:
                stats[key]["overdue"] += 1
            else:
                stats[key]["open"] += 1

    lines = [f"📤 Сводка по проекту «{H(prj_name)}»:"]  # H — твой helper экранирования HTML
    for (_, full), c in sorted(stats.items(), key=lambda x: (x[0][1] or "").lower()):
        lines.append(f"• {H(full)} — готово: {c['done']}, просрочено: {c['overdue']}, в работе: {c['open']}")

    await cq.message.answer("\n".join(lines), parse_mode="HTML")

    # CSV
    import io, csv
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=';')
    w.writerow(["id","task","assignee","planned_date","duration_days","status"])
    for row in csv_rows:
        w.writerow(row)
    bio = io.BytesIO(buf.getvalue().encode("utf-8"))
    bio.name = "project_tasks.csv"
    try:
        await cq.message.answer_document(bio, caption="Экспорт задач проекта (CSV)")
    except Exception:
        pass

    await cq.answer()

# --- выбор исполнителя (отдельный picker, чтобы не мешать существующему assign_user) ---
async def show_user_picker_project(m_or_cq, page: int, for_tg_id: int) -> Message | None:
    is_cq = isinstance(m_or_cq, CallbackQuery)
    chat_id = m_or_cq.message.chat.id if is_cq else m_or_cq.chat.id

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, for_tg_id)
        if me["role"] == "developer":
            cur = await db.execute("""SELECT id, full_name, tg_id FROM users
                                      WHERE is_active=1 AND role='employee'
                                      ORDER BY full_name COLLATE NOCASE""")
        else:
            cur = await db.execute("""SELECT id, full_name, tg_id FROM users
                                      WHERE is_active=1 AND role='employee'
                                        AND COALESCE(dept,'') = COALESCE(?, '')
                                      ORDER BY full_name COLLATE NOCASE""", (me.get("dept") or "",))
        candidates = await cur.fetchall()

    total = len(candidates)
    if total == 0:
        txt = "Нет доступных сотрудников."
        if is_cq:
            msg = await m_or_cq.message.edit_text(txt)
            await m_or_cq.answer()
        else:
            msg = await bot.send_message(chat_id, txt)
        return msg

    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, pages-1))
    start, end = page*PAGE_SIZE, page*PAGE_SIZE+PAGE_SIZE
    chunk = candidates[start:end]

    kb = InlineKeyboardBuilder()
    for uid, full, tg in chunk:
        label = full if full and full != "unknown" else f"user_{tg}"
        kb.button(text=label, callback_data=_proj_user_pick_cb(uid))
    if page > 0:
        kb.button(text="« Назад", callback_data=_proj_user_list_cb(page-1))
    if page < pages-1:
        kb.button(text="Далее »", callback_data=_proj_user_list_cb(page+1))
    kb.adjust(1)

    txt = f"Кто будет делать задачу? (стр {page+1}/{pages})"
    if is_cq:
        msg = await m_or_cq.message.edit_text(txt, reply_markup=kb.as_markup())
        await m_or_cq.answer()
    else:
        msg = await bot.send_message(chat_id, txt, reply_markup=kb.as_markup())
    return msg

@router.message(ProjTaskAdd.waiting_text)
async def proj_task_got_text(m: Message, state: FSMContext):
    text = (m.text or "").strip()
    if len(text) < 2:
        await _track_form_message(state, m)
        prompt = await m.answer("Слишком коротко, опиши задачу.")
        await _track_form_message(state, prompt)
        return
    await _track_form_message(state, m)
    await state.update_data(add_text=text)
    await state.set_state(ProjTaskAdd.picking_assignee)
    prompt = await show_user_picker_project(m, 0, for_tg_id=m.from_user.id)
    await _track_form_message(state, prompt)

@router.callback_query(F.data.startswith("projuser_list:"))
async def proj_user_list(cq: CallbackQuery, state: FSMContext):
    page = int(cq.data.split(":")[1])
    await show_user_picker_project(cq, page, for_tg_id=cq.from_user.id)

@router.callback_query(F.data.startswith("projuser_user:"))
async def proj_user_pick(cq: CallbackQuery, state: FSMContext):
    uid = int(cq.data.split(":")[1])
    await state.update_data(add_assignee=uid)

    # достанем диапазон дат проекта для клавиатуры
    data = await state.get_data()
    pid = int(data["add_pid"])
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT start_date, deadline FROM project_meta WHERE project_id=?", (pid,))
        row = await cur.fetchone()
    if not row:
        await cq.answer("Нет метаданных проекта.", show_alert=True); return
    start, dl = date.fromisoformat(row[0]), date.fromisoformat(row[1])

    # кнопки дат
    kb = InlineKeyboardBuilder()
    d = start
    page = 0
    kb = _build_dates_kb(
        start, dl, page,
        lambda iso: _proj_date_pick_cb(pid, iso),
        lambda p: _proj_dates_page_cb(pid, p)
    )
    kb.adjust(4)

    await state.set_state(ProjTaskAdd.picking_date)
    await cq.message.edit_text("На какую дату поставить задачу?", reply_markup=kb.as_markup())
    await cq.answer()

@router.callback_query(F.data.startswith("projdate:"))
async def proj_date_pick(cq: CallbackQuery, state: FSMContext):
    _, pid, iso = cq.data.split(":")
    pid = int(pid)
    day = date.fromisoformat(iso)

    data = await state.get_data()
    task_text = data["add_text"]
    assignee_id = int(data["add_assignee"])

    # получим мету проекта и лист
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT sheet_title, start_date FROM project_meta WHERE project_id=?", (pid,))
        row = await cur.fetchone()
        cur2 = await db.execute("SELECT full_name FROM users WHERE id=?", (assignee_id,))
        ass = (await cur2.fetchone() or [""])[0]
    if not row:
        await cq.answer("Проект не найден.", show_alert=True); return
    sheet_title, start_iso = row[0], row[1]
    start = date.fromisoformat(start_iso)

    sh = await _gs_open_projects()
    ws = await _gs_ensure_ws(sh, sheet_title)

    # запись строки
    next_row = await _projects_next_row(ws)
    await ws.update_cell(next_row, 1, task_text)
    await ws.update_cell(next_row, 2, ass or "—")

    # зелёная ячейка в колонке нужной даты
    col = 3 + (day - start).days
    await _projects_paint_cell(ws, next_row, col, GREEN)

    # сохраняем в БД строку задачи проекта
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            INSERT INTO project_tasks(project_id, row_index, task_text, assignee_user_id, planned_date, duration_days, status)
            VALUES(?,?,?,?,?,1,'open')
        """, (pid, next_row, task_text, assignee_id, day.isoformat()))
        tid = cur.lastrowid
        await db.commit()

    # кнопки после создания
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Ещё задача", callback_data=_proj_plan_add_cb(pid))
    kb.button(text="📅 Сдвинуть дату", callback_data=_proj_move_start_cb(tid))
    kb.button(text="➕ Продлить ещё на день", callback_data=_proj_extend1_cb(tid))
    kb.button(text="✅ Завершить задачу", callback_data=_proj_done_cb(tid))
    kb.adjust(1)

    await _cleanup_form_messages(state, cq.message.chat.id)
    await state.clear()
    await cq.message.answer("✅ Задача добавлена.", reply_markup=kb.as_markup())
    await cq.answer()

@router.callback_query(F.data.startswith("projextend1:"))
async def proj_extend1(cq: CallbackQuery):
    tid = int(cq.data.split(":")[1])

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT pt.project_id, pt.row_index, pt.planned_date, pt.duration_days, pt.status,
                   pm.sheet_title, pm.start_date, pm.deadline
            FROM project_tasks pt
            JOIN project_meta pm ON pm.project_id = pt.project_id
            WHERE pt.id=?
        """, (tid,))
        row = await cur.fetchone()

    if not row:
        await cq.answer("Задача не найдена.", show_alert=True); return

    pid, row_index, planned_iso, duration_days, status, sheet_title, start_iso, deadline_iso = row
    if status == "done":
        await cq.answer("Задача уже завершена.", show_alert=True); return

    start    = date.fromisoformat(start_iso)
    planned  = date.fromisoformat(planned_iso)
    deadline = date.fromisoformat(deadline_iso)

    # Последний день текущей длительности +1
    end_day  = planned + timedelta(days=duration_days - 1)
    next_day = end_day + timedelta(days=1)
    if next_day > deadline:
        await cq.answer("Нельзя продлить — дальше дедлайна проекта.", show_alert=True); return

    sh = await _gs_open_projects()
    ws = await _gs_ensure_ws(sh, sheet_title)
    col = 3 + (next_day - start).days
    await _projects_paint_cell(ws, row_index, col, BLUE)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE project_tasks SET duration_days = duration_days + 1 WHERE id=?", (tid,))
        await db.commit()

    await cq.message.edit_text("✅ Продлено на 1 день.")
    await cq.answer()

@router.callback_query(F.data.startswith("projdone:"))
async def proj_done(cq: CallbackQuery):
    tid = int(cq.data.split(":")[1])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE project_tasks SET status='done' WHERE id=?", (tid,))
        await db.commit()
    await cq.message.edit_text("✅ Задача завершена.")
    await cq.answer("Готово")

@router.callback_query(F.data.startswith("projmove:"))
async def proj_move_start(cq: CallbackQuery):
    tid = int(cq.data.split(":")[1])
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT pt.project_id, pm.start_date, pm.deadline
            FROM project_tasks pt
            JOIN project_meta pm ON pm.project_id=pt.project_id
            WHERE pt.id=?
        """, (tid,))
        row = await cur.fetchone()
    if not row:
        await cq.answer("Задача не найдена.", show_alert=True); return
    pid, start_iso, dl_iso = row
    start, dl = date.fromisoformat(start_iso), date.fromisoformat(dl_iso)

    kb = InlineKeyboardBuilder()
    page = 0
    kb = _build_dates_kb(
        start, dl, page,
        lambda iso: _proj_move_date_cb(tid, iso),
        lambda p: _proj_move_page_cb(tid, p)
    )
    await cq.message.edit_text("Новая дата задачи:", reply_markup=kb.as_markup())
    await cq.answer()

@router.callback_query(F.data.startswith("projdates_page:"))
async def proj_dates_page(cq: CallbackQuery, state: FSMContext):
    _, pid_s, page_s = cq.data.split(":")
    pid, page = int(pid_s), int(page_s)

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT start_date, deadline FROM project_meta WHERE project_id=?", (pid,))
        row = await cur.fetchone()
    if not row:
        await cq.answer("Проект не найден.", show_alert=True); return

    start, dl = date.fromisoformat(row[0]), date.fromisoformat(row[1])
    kb = _build_dates_kb(start, dl, page,
                         lambda iso: _proj_date_pick_cb(pid, iso),
                         lambda p: _proj_dates_page_cb(pid, p))
    await cq.message.edit_text("На какую дату поставить задачу?", reply_markup=kb.as_markup())
    await cq.answer()

@router.callback_query(F.data.startswith("projmove_page:"))
async def proj_move_page(cq: CallbackQuery):
    _, tid_s, page_s = cq.data.split(":")
    tid, page = int(tid_s), int(page_s)

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT pm.start_date, pm.deadline
            FROM project_tasks pt
            JOIN project_meta pm ON pm.project_id=pt.project_id
            WHERE pt.id=?
        """, (tid,))
        row = await cur.fetchone()
    if not row:
        await cq.answer("Задача не найдена.", show_alert=True); return

    start, dl = date.fromisoformat(row[0]), date.fromisoformat(row[1])
    kb = _build_dates_kb(start, dl, page,
                         lambda iso: _proj_move_date_cb(tid, iso),
                         lambda p: _proj_move_page_cb(tid, p))
    await cq.message.edit_text("Новая дата задачи:", reply_markup=kb.as_markup())
    await cq.answer()

@router.callback_query(F.data.startswith("projmove_date:"))
async def proj_move_date(cq: CallbackQuery):
    _, tid, iso = cq.data.split(":")
    tid = int(tid)
    new_day = date.fromisoformat(iso)

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT pt.project_id, pt.row_index, pt.planned_date, pm.sheet_title, pm.start_date
            FROM project_tasks pt
            JOIN project_meta pm ON pm.project_id=pt.project_id
            WHERE pt.id=?
        """, (tid,))
        row = await cur.fetchone()
    if not row:
        await cq.answer("Данные не найдены.", show_alert=True); return
    pid, row_index, old_iso, sheet_title, start_iso = row
    start = date.fromisoformat(start_iso)
    old_day = date.fromisoformat(old_iso)

    sh = await _gs_open_projects()
    ws = await _gs_ensure_ws(sh, sheet_title)

    # старую ячейку -> жёлтый, новую -> зелёный
    old_col = 3 + (old_day - start).days
    new_col = 3 + (new_day - start).days
    await _projects_paint_cell(ws, row_index, old_col, YELLOW)
    await _projects_paint_cell(ws, row_index, new_col, GREEN)

    # обновим дату в БД
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE project_tasks SET planned_date=? WHERE id=?", (new_day.isoformat(), tid))
        await db.commit()

    await cq.message.edit_text("✅ Дата задачи обновлена.")
    await cq.answer()

def _proj_dates_page_cb(pid: int, page: int) -> str: return f"projdates_page:{pid}:{page}"
def _proj_move_page_cb(tid: int, page: int) -> str:  return f"projmove_page:{tid}:{page}"

def _noop_cb() -> str: return "noop"

@router.callback_query(F.data == "noop")
async def cb_noop(cq: CallbackQuery):
    await cq.answer()

# --- Периодическая перекраска просроченных (красный) ---
async def projects_sync_overdues():
    """Каждый запуск подсвечивает красным все open-задачи, чей planned_date < сегодня."""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("""
                SELECT pt.id, pt.project_id, pt.row_index, pt.planned_date, pm.sheet_title, pm.start_date
                FROM project_tasks pt
                JOIN project_meta pm ON pm.project_id=pt.project_id
                WHERE pt.status='open'
            """)
            rows = await cur.fetchall()
        if not rows:
            return

        today = datetime.now(LOCAL_TZ).date()
        sh = await _gs_open_projects()

        for tid, pid, row_index, planned_iso, sheet_title, start_iso in rows:
            d = date.fromisoformat(planned_iso)
            if d >= today:
                continue
            ws = await _gs_ensure_ws(sh, sheet_title)
            start = date.fromisoformat(start_iso)
            col = 3 + (d - start).days
            await _projects_paint_cell(ws, row_index, col, RED)
    except Exception as e:
        logging.warning(f"projects_sync_overdues: {e}")

from aiogram import F
from aiogram.types import Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

# было: def _creds_menu_kb() -> InlineKeyboardBuilder:
def _creds_menu_kb(can_add: bool) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    if can_add:
        kb.button(text="➕ Добавить", callback_data="creds:add")
    kb.button(text="🔎 Найти", callback_data="creds:find")
    kb.button(text="🗂 Список", callback_data="creds:list")
    kb.adjust(1)
    return kb

@router.message(F.text.in_({"🔐 Пароли", "Пароли", "Доступы"}))
async def creds_menu_entry(m: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        u = await ensure_user(db, m.from_user.id, m.from_user.full_name or "")
    can_add = _can_manage_creds(u)

    text_lines = [
        "Хранилище доступов:",
        "• «🔎 Найти» — быстрый поиск по названию/сервису",
        "• «🗂 Список» — последние добавления",
    ]
    if can_add:
        text_lines.insert(1, "• «➕ Добавить» — сохранить логин/пароль/заметку")

    await m.answer(
        "\n".join(text_lines),
        reply_markup=_creds_menu_kb(can_add).as_markup()
    )


from aiogram.utils.keyboard import InlineKeyboardBuilder
import asyncio
from html import escape as _html_escape

_ALLOWED_ROLES_FOR_CREDS = {"developer", "head", "lead"}

def _can_manage_creds(user_row) -> bool:
    """Кто может добавлять и смотреть пароли."""
    try:
        return (user_row.get("role") or "").lower() in _ALLOWED_ROLES_FOR_CREDS
    except Exception:
        return False

def _mask_pwd(pwd: str) -> str:
    """Маска пароля для списка (не выводим пароль в явном виде)."""
    if not pwd:
        return "—"
    # показываем первые 1–2 символа и длину, остальное маской
    visible = pwd[:2]
    return f"{visible}{'•' * max(0, len(pwd)-2)}"

def _creds_main_kb(can_add: bool) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="📂 Выбрать сервис", callback_data="creds:choose")
    if can_add:
        kb.button(text="➕ Добавить сервис", callback_data="creds:add")
    kb.adjust(1)
    return kb


def _creds_back_to_main_kb() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data="creds:menu")
    kb.adjust(1)
    return kb

async def _creds_autodelete(bot, chat_id: int, message_id: int, seconds: int = 30):
    """Удалить сообщение через N секунд."""
    try:
        await asyncio.sleep(seconds)
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass
    
def _links_root_kb(is_editor: bool) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="📂 Выбрать проект", callback_data="pl:choose")
    if is_editor:
        kb.button(text="➕ Добавить проект", callback_data="pl:add_project")
    kb.adjust(1)
    return kb

def _project_menu_kb(project_id: int, is_editor: bool) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    if is_editor:
        kb.button(text="➕ Добавить ссылку", callback_data=f"pl:add_link:{project_id}")
    kb.button(text="⬅️ Назад к проектам", callback_data="pl:choose")
    kb.adjust(1)
    return kb

def _admin_users_page_cb(page: int) -> str:
    return f"admin:users_page:{page}"

def _admin_fire_cb(user_id: int) -> str:
    return f"admin:fire:{user_id}"

@router.callback_query(F.data == "admin:users")
async def admin_users_root(cq: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)

    if not me or not me.get("is_active", 1):
        await cq.answer("❌ Пользователь удалён/заблокирован.", show_alert=True); return
    if me["role"] != "developer":
        await cq.answer("Нет доступа", show_alert=True); return

    await admin_users_show_page(cq, 0)

@router.callback_query(F.data == "admin:stats")
async def admin_stats(cq: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
        if not me or not me.get("is_active", 1):
            await cq.answer("❌ Пользователь удалён или заблокирован.", show_alert=True)
            return

        # Общая статистика по пользователям
        async def _count(sql, params=()):
            cur = await db.execute(sql, params); r = await cur.fetchone(); return r[0] if r else 0

        total = await _count("SELECT COUNT(*) FROM users WHERE is_active=1")
        employees = await _count("SELECT COUNT(*) FROM users WHERE role='employee' AND is_active=1")
        leads = await _count("SELECT COUNT(*) FROM users WHERE role='lead' AND is_active=1")
        heads = await _count("SELECT COUNT(*) FROM users WHERE role='head' AND is_active=1")
        devs = await _count("SELECT COUNT(*) FROM users WHERE role='developer' AND is_active=1")

        # Сколько «активных» за последние 7 дней (писали боту)
        # Если столбца activity нет — покажем только роли (без падения)
        active_7d = 0
        try:
            since = (datetime.now(UTC) - timedelta(days=7)).isoformat()
            active_7d = await _count("SELECT COUNT(*) FROM users WHERE created_at >= ?", (since,))
        except Exception:
            pass

    text = (
        "📈 Статистика пользователей:\n"
        f"• Всего: {total}\n"
        f"• Сотрудники: {employees}\n"
        f"• Лиды: {leads}\n"
        f"• Хеды: {heads}\n"
        f"• Девелоперы: {devs}\n"
    )
    if active_7d:
        text += f"• Активны за 7 дней: {active_7d}\n"

    await cq.message.answer(text)
    await cq.answer()

# === FULL RESET: отмена ===
@router.callback_query(F.data == "admin:reset_cancel")
async def admin_full_reset_cancel(cq: CallbackQuery):
    try:
        await cq.message.edit_text("Сброс отменён.")
    except Exception:
        pass
    await cq.answer()

# === FULL RESET: выполнить ===
@router.callback_query(F.data == "admin:reset_go")
async def admin_full_reset_go(cq: CallbackQuery):
    # Жёсткий сброс
    await db_full_reset()

    # Обновляем текст того же сообщения, где была клавиатура подтверждения
    try:
        await cq.message.edit_text("✅ Сброс выполнен. База очищена.")
    except Exception:
        pass

    await cq.answer("Готово")


def _admin_role_menu_cb(user_id: int) -> str:
    return f"admin:role:{user_id}"

def _admin_role_set_cb(user_id: int, role: str) -> str:
    return f"admin:role_set:{user_id}:{role}"

async def admin_users_show_page(cq: CallbackQuery, page: int):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("""
                SELECT id, full_name, tg_id, role
                FROM users
                WHERE role!='developer' AND is_active=1
                ORDER BY role DESC, full_name COLLATE NOCASE
            """)
            rows = await cur.fetchall()
    except Exception as e:
        logging.exception("admin_users_show_page query failed: %s", e)
        await cq.answer("Ошибка загрузки списка.", show_alert=True); return

    if not rows:
        await cq.message.answer("Нет пользователей для отображения.")
        await cq.answer(); return

    PAGE = 8
    total = len(rows)
    pages = max(1, (total + PAGE - 1) // PAGE)
    page = max(0, min(page, pages - 1))
    start, end = page * PAGE, page * PAGE + PAGE
    chunk = rows[start:end]

    kb = InlineKeyboardBuilder()
    lines = ["Сотрудники (для удаления):"]
    for (uid, name, tg, role) in chunk:
        safe_name = (name or f"user_{tg}")
        lines.append(f"• {safe_name} (tg_id: {tg}, role: {role})")
        kb.button(text=f"👢 Уволить: {safe_name[:20]}", callback_data=_admin_fire_cb(uid))
        kb.button(text=f"⚙ Роль: {safe_name[:20]}", callback_data=_admin_role_menu_cb(uid))
    if page > 0:
        kb.button(text="« Назад", callback_data=_admin_users_page_cb(page - 1))
    if page < pages - 1:
        kb.button(text="Далее »", callback_data=_admin_users_page_cb(page + 1))
    kb.adjust(1)

    await cq.message.answer("\n".join(lines), reply_markup=kb.as_markup())
    await cq.answer()

def _admin_fire_confirm_cb(user_id: int) -> str:
    return f"admin:fire_confirm:{user_id}"

def _admin_fire_cancel_cb(user_id: int) -> str:
    return f"admin:fire_cancel:{user_id}"

@router.callback_query(F.data.startswith("admin:fire:"))
async def admin_fire_prompt(cq: CallbackQuery):
    parts = (cq.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await cq.answer("Некорректный запрос.", show_alert=True); return
    user_id = int(parts[2])

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            me = await get_user_by_tg(db, cq.from_user.id)
            if not me or not me.get("is_active", 1):
                await cq.answer("❌ Пользователь удалён/заблокирован.", show_alert=True); return
            if me.get("role") != "developer":
                await cq.answer("Нет доступа", show_alert=True); return

            tgt = await get_user_by_id(db, user_id)
    except Exception as e:
        logging.exception("admin_fire_prompt failed: %s", e)
        await cq.answer("Ошибка обработки запроса.", show_alert=True); return

    if not tgt:
        await cq.answer("Пользователь не найден", show_alert=True); return
    if tgt.get("tg_id") == DEVELOPER_TG_ID:
        await cq.answer("Нельзя удалять разработчика.", show_alert=True); return

    kb = InlineKeyboardBuilder()
    kb.button(text="⚠️ Подтвердить увольнение", callback_data=_admin_fire_confirm_cb(user_id))
    kb.button(text="Отмена", callback_data=_admin_fire_cancel_cb(user_id))
    kb.adjust(1)

    await cq.message.answer(
        f"Уволить {tgt.get('full_name','(без имени)')} (tg_id: {tgt.get('tg_id','?')})?",
        reply_markup=kb.as_markup()
    )
    await cq.answer()


@router.callback_query(F.data.startswith("admin:fire_cancel:"))
async def admin_fire_cancel(cq: CallbackQuery):
    await cq.answer("Отменено")
    await cq.message.edit_text("❎ Увольнение отменено.")

@router.callback_query(F.data.startswith("admin:fire_confirm:"))
async def admin_fire_confirm(cq: CallbackQuery):
    parts = (cq.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await cq.answer("Некорректный запрос.", show_alert=True); return
    user_id = int(parts[2])

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            me = await get_user_by_tg(db, cq.from_user.id)
            if not me or not me.get("is_active", 1):
                await cq.answer("❌ Пользователь удалён/заблокирован.", show_alert=True); return
            if me.get("role") != "developer":
                await cq.answer("Нет доступа", show_alert=True); return

            tgt = await get_user_by_id(db, user_id)
            if not tgt:
                await cq.answer("Пользователь уже удалён.", show_alert=True); return
            if tgt.get("tg_id") == DEVELOPER_TG_ID:
                await cq.answer("Нельзя удалять разработчика.", show_alert=True); return

            # soft-delete
            await db.execute("UPDATE users SET is_active=0 WHERE id=?", (user_id,))
            await db.execute("DELETE FROM manager_links WHERE manager_user_id=? OR subordinate_user_id=?", (user_id, user_id))
            await db.execute("DELETE FROM daily_plan_items WHERE user_id=?", (user_id,))
            # опционально закрыть открытые задачи:
            await db.execute("UPDATE tasks SET status='done', next_reminder_at=NULL WHERE user_id=? AND status!='done'", (user_id,))
            await db.commit()
    except Exception as e:
        logging.exception("admin_fire_confirm failed: %s", e)
        await cq.answer("Ошибка при увольнении.", show_alert=True); return

    await cq.message.edit_text(
        f"✅ Доступ пользователя {tgt.get('full_name','(без имени)')} (tg_id: {tgt.get('tg_id','?')}) закрыт."
    )
    await cq.answer("Удалён")

@router.callback_query(F.data.startswith("admin:users_page:"))
async def admin_users_page(cq: CallbackQuery):
    page = int(cq.data.split(":")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
    if me["role"] != "developer":
        await cq.answer("Нет доступа", show_alert=True); return
    await admin_users_show_page(cq, page)

@router.callback_query(F.data.startswith("admin:role:"))
async def admin_role_menu(cq: CallbackQuery):
    parts = (cq.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await cq.answer("Некорректный запрос.", show_alert=True); return
    user_id = int(parts[2])

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
        if not me or me.get("role") != "developer":
            await cq.answer("Нет доступа", show_alert=True); return
        tgt = await get_user_by_id(db, user_id)
        if not tgt:
            await cq.answer("Пользователь не найден", show_alert=True); return

    # Нельзя менять разработчику роль через это меню
    if tgt.get("tg_id") == DEVELOPER_TG_ID:
        await cq.answer("Нельзя менять роль разработчика.", show_alert=True); return

    kb = InlineKeyboardBuilder()
    for role in ("employee", "lead", "head"):
        kb.button(text=role, callback_data=_admin_role_set_cb(user_id, role))
    kb.button(text="Отмена", callback_data=_admin_users_page_cb(0))
    kb.adjust(1)

    await cq.message.answer(
        f"Выберите роль для {tgt.get('full_name','(без имени)')} (сейчас: {tgt.get('role')}):",
        reply_markup=kb.as_markup()
    )
    await cq.answer()

@router.callback_query(F.data.startswith("admin:role_set:"))
async def admin_role_set(cq: CallbackQuery):
    parts = (cq.data or "").split(":")
    if len(parts) != 4 or not parts[2].isdigit():
        await cq.answer("Некорректный запрос.", show_alert=True); return
    user_id = int(parts[2])
    new_role = parts[3]
    if new_role not in ("employee","lead","head"):
        await cq.answer("Недопустимая роль.", show_alert=True); return

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
        if not me or me.get("role") != "developer":
            await cq.answer("Нет доступа", show_alert=True); return
        tgt = await get_user_by_id(db, user_id)
        if not tgt:
            await cq.answer("Пользователь не найден", show_alert=True); return
        if tgt.get("tg_id") == DEVELOPER_TG_ID:
            await cq.answer("Нельзя менять роль разработчика.", show_alert=True); return

        await db.execute("UPDATE users SET role=? WHERE id=?", (new_role, user_id))
        await db.commit()

    await cq.message.edit_text(
        f"✅ Роль пользователя {tgt.get('full_name','(без имени)')} обновлена: {tgt.get('role')} → {new_role}"
    )
    await cq.answer("Роль изменена")

from aiogram import F
from aiogram.types import CallbackQuery

@router.callback_query(F.data.startswith("start_task_from_list:"))
async def cb_start_task_from_list(cq: CallbackQuery):
    """
    Из списка «Мои задачи»: ставим статус in_progress и
    РЕДАКТИРУЕМ текущее сообщение карточки вместо отправки нового.
    """
    rid = int(cq.data.split(":")[1])

    async with aiosqlite.connect(DB_PATH) as db:
        user = await ensure_user(db, cq.from_user.id, cq.from_user.full_name or "")
        now = datetime.now(UTC).isoformat()

        await db.execute("""
            UPDATE tasks
               SET status='in_progress',
                   started_at=COALESCE(started_at, ?),
                   updated_at=?
             WHERE id=? AND user_id=?
        """, (now, now, rid, user["id"]))
        await db.commit()
        await log_task_event(db, rid, "status", meta="in_progress")

        cur = await db.execute("SELECT description, deadline FROM tasks WHERE id=?", (rid,))
        row = await cur.fetchone()

    desc = (row[0] if row else "") or ""
    dl   =  row[1] if row else None

    text = "Статус обновлён: 🚀 Начал работу.\n\n" + _format_task_line(rid, desc, "in_progress", dl)
    kb = await build_task_buttons(rid)
    kb.adjust(1)

    try:
        await cq.message.edit_text(text, parse_mode="HTML", reply_markup=kb.as_markup())
    except Exception:
        # на случай если сообщение уже не редактируется
        await disable_kb_and_optionally_edit(cq.message, text, parse_mode="HTML")

    await cq.answer()

@router.callback_query(F.data == "start_task_later")
async def cb_start_task_later(cq: CallbackQuery):
    # СКРЫВАЕМ КНОПКИ У СООБЩЕНИЯ «Какой задачей займёмся следующей?»
    await hide_inline_kb(cq)

    await cq.message.answer("Ок, вернёмся к выбору позже.")
    await cq.answer()

@router.callback_query(F.data == "creds:menu")
async def creds_menu_cb(cq: CallbackQuery):
    await _remove_kb_safe(cq.message)
    async with aiosqlite.connect(DB_PATH) as db:
        u = await ensure_user(db, cq.from_user.id, cq.from_user.full_name or "")
    can_add = _can_manage_creds(u)

    text_lines = [
        "Хранилище паролей:",
        "• «📂 Выбрать сервис» — открыть список сохранённых сервисов.",
    ]
    if can_add:
        text_lines.append("• «➕ Добавить сервис» — добавить запись.")

    await cq.message.answer("\n".join(text_lines), reply_markup=_creds_main_kb(can_add).as_markup())
    await cq.answer()

@router.callback_query(F.data == "creds:choose")
async def creds_choose_cb(cq: CallbackQuery):
    await _remove_kb_safe(cq.message)

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT title, COUNT(*) as cnt
            FROM creds
            GROUP BY title
            ORDER BY LOWER(title) ASC
            LIMIT 100
        """)
        rows = await cur.fetchall()

    if not rows:
        async with aiosqlite.connect(DB_PATH) as db:
            u = await ensure_user(db, cq.from_user.id, cq.from_user.full_name or "")
        await cq.message.answer(
            "Пока нет ни одного сервиса.",
            reply_markup=_creds_main_kb(_can_manage_creds(u)).as_markup()
        )
        return

    kb = InlineKeyboardBuilder()
    for title, cnt in rows:
        kb.button(text=f"{title} ({cnt})", callback_data=f"creds:open:{title}")
    kb.button(text="⬅️ Назад", callback_data="creds:menu")
    kb.adjust(1)
    await cq.message.answer("Выберите сервис:", reply_markup=kb.as_markup())
    await cq.answer()

@router.callback_query(F.data.startswith("creds:open:"))
async def creds_open_by_title(cq: CallbackQuery):
    # удалить сообщение "Выберите сервис"
    await _delete_msg_safe(cq.message)

    parts = (cq.data or "").split(":", 2)
    if len(parts) != 3:
        await cq.answer("Некорректный запрос.", show_alert=True); return
    title = parts[2]

    async with aiosqlite.connect(DB_PATH) as db:
        u = await ensure_user(db, cq.from_user.id, cq.from_user.full_name or "")
        cur = await db.execute("""
            SELECT id, title, login
            FROM creds
            WHERE title=?
            ORDER BY id DESC
            LIMIT 20
        """, (title,))
        rows = await cur.fetchall()

    if not rows:
        await cq.message.answer("Записей не найдено.")
        await cq.answer(); return

    # даём выбор конкретной записи (по логину), дальше откроется карточка через cred_open:<id>
    kb = InlineKeyboardBuilder()
    for cid, t, login in rows:
        label = f"{t} — {login}"[:60]
        kb.button(text=label, callback_data=f"cred_open:{cid}")
    kb.adjust(1)
    await cq.message.answer("Выберите учётку:", reply_markup=kb.as_markup())
    await cq.answer()

@router.callback_query(F.data.startswith("cred_open:"))
async def cred_open(cq: CallbackQuery):
    # удаляем сообщение со списком, где была нажата кнопка
    await _delete_msg_safe(cq.message)

    cred_id = int(cq.data.split(":")[1])

    async with aiosqlite.connect(DB_PATH) as db:
        u = await ensure_user(db, cq.from_user.id, cq.from_user.full_name or "")
        rec = await _get_cred_by_id(db, cred_id, u["id"])

    if not rec:
        await cq.message.answer("Запись не найдена или у вас нет доступа.")
        await cq.answer()
        return

    await cq.message.answer(_render_cred_html(rec), parse_mode="HTML")
    await cq.answer()

@router.callback_query(F.data.startswith("creds:reveal:"))
async def creds_reveal_cb(cq: CallbackQuery):
    # доступ только head/lead/developer
    async with aiosqlite.connect(DB_PATH) as db:
        u = await ensure_user(db, cq.from_user.id, cq.from_user.full_name or "")
        if not _can_manage_creds(u):
            await cq.answer("Нет прав", show_alert=True)
            return

        cid = int(cq.data.split(":")[2])
        cur = await db.execute("SELECT title, login, password FROM creds WHERE id=?", (cid,))
        row = await cur.fetchone()

    if not row:
        await cq.answer("Запись не найдена", show_alert=True)
        return

    title, login, pwd = row
    text = (
        f"<b>{_html_escape(title)}</b>\n"
        f"Логин: <b>{_html_escape(login)}</b>\n"
        f"Пароль: <code>{_html_escape(pwd)}</code>\n\n"
        f"⚠️ Это сообщение исчезнет через 30 сек."
    )
    msg = await cq.message.answer(text, parse_mode="HTML")
    # автоудаление
    # разовая синхронизация (если нужно запустить вручную из этого обработчика)
    try:
        await gs_sync_all()
    except Exception as e:
        logging.exception("Manual gs_sync_all() failed: %s", e)
    await cq.answer()


@router.callback_query(F.data == "creds:add")
async def creds_add_start_cb(cq: CallbackQuery, state: FSMContext):
    async with aiosqlite.connect(DB_PATH) as db:
        u = await ensure_user(db, cq.from_user.id, cq.from_user.full_name or "")
        if not _can_manage_creds(u):
            await cq.answer("Нет прав", show_alert=True)
            return

    await _remove_kb_safe(cq.message)
    await state.set_state(CredsState.waiting_add)
    await cq.message.answer(
        "Пришли <b>одно</b> сообщение с данными. Можно в любой простой форме — я распарсю:\n\n"
        "Вариант 1 (одна строка):\n"
        "<code>Название сервиса — логин — пароль — комментарий</code>\n"
        "Разделители: тире/двоеточие/точка с запятой/вертикальная черта (—, -, :, ;, |)\n\n"
        "Вариант 2 (по строкам):\n"
        "<code>Figma\nuser@company.com\nQwerty123\nаккаунт команды</code>",
        parse_mode="HTML"
    )
    await cq.answer()

@router.callback_query(F.data == "creds:find")
async def creds_find_start(cq: CallbackQuery, state: FSMContext):
    await state.set_state(CredsState.waiting_find)
    await cq.message.answer("Что ищем? Напиши название сервиса или часть логина.")
    await cq.answer()

@router.callback_query(F.data == "creds:list")
async def creds_list(cq: CallbackQuery):
    # убираем кнопки у текущего сообщения
    await _remove_kb_safe(cq.message)

    async with aiosqlite.connect(DB_PATH) as db:
        u = await ensure_user(db, cq.from_user.id, cq.from_user.full_name or "")

        cur = await db.execute("""
            SELECT id, title
            FROM creds
            ORDER BY id DESC
            LIMIT 50
        """,)
        rows = await cur.fetchall()

    if not rows:
        await cq.message.answer(
            "Пока нет сохранённых доступов.",
            reply_markup=_creds_menu_kb(_can_manage_creds(u)).as_markup()
        )
        await cq.answer()
        return

    kb = InlineKeyboardBuilder()
    for cid, title in rows:
        text = (title or f"#{cid}")[:40]
        kb.button(text=text, callback_data=f"cred_open:{cid}")
    kb.adjust(1)

    await cq.message.answer("Выберите сервис:", reply_markup=kb.as_markup())
    await cq.answer()

# --- FSM для хранилища логинов/паролей ---
class CredsState(StatesGroup):
    waiting_add  = State()   # было
    waiting_find = State()   # добавили — для ввода текста запроса

@router.message(StateFilter(CredsState.waiting_add))
async def creds_add_apply(m: Message, state: FSMContext):
    raw = (m.text or "").strip()
    if not raw:
        await m.answer("Нужен текст.")
        return

    # Универсальный парсер: пробуем построчно, потом — по разделителям
    title = login = password = note = ""

    lines = [s.strip() for s in raw.splitlines() if s.strip()]
    if len(lines) >= 3:
        title, login, password = lines[0], lines[1], lines[2]
        if len(lines) >= 4:
            note = "\n".join(lines[3:])
    else:
        # Один ряд, делим по набору разделителей
        import re
        parts = [p.strip() for p in re.split(r"[|;:—\-]{1,}", raw) if p.strip()]
        # минимум: 3 поля
        if len(parts) >= 3:
            title, login, password = parts[0], parts[1], parts[2]
            if len(parts) >= 4:
                note = " ".join(parts[3:])

    if not title or not login or not password:
        await m.answer("Не смог разобрать. Минимум нужно: «сервис — логин — пароль». Попробуй ещё раз.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        u = await ensure_user(db, m.from_user.id, m.from_user.full_name or "")
        if not _can_manage_creds(u):
            await m.answer("Нет прав.")
            await state.clear()
            return

        now = datetime.now(UTC).isoformat()
        await db.execute("""
            INSERT INTO creds(title, login, password, note, created_by_id, created_at)
            VALUES(?,?,?,?,?,?)
        """, (title, login, password, note, u["id"], now))
        await db.commit()

    await state.clear()
    await m.answer(
        f"✅ Сохранено:\n<b>{_html_escape(title)}</b>\nЛогин: <b>{_html_escape(login)}</b>\n"
        f"Пароль: <code>{_html_escape(_mask_pwd(password))}</code>",
        parse_mode="HTML",
        reply_markup=_creds_main_kb().as_markup()
    )

@router.message(CredsState.waiting_find)
async def creds_find_apply(m: Message, state: FSMContext):
    q = (m.text or "").strip()
    if not q:
        await m.answer("Нужен текст запроса.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        u = await ensure_user(db, m.from_user.id, m.from_user.full_name or "")
        like = f"%{q}%"
        cur = await db.execute("""
            SELECT id, title
            FROM creds
            WHERE (title LIKE ? OR login LIKE ?)
            ORDER BY id DESC
            LIMIT 30
        """, (like, like))
        rows = await cur.fetchall()

    await state.clear()

    if not rows:
        await m.answer("Ничего не нашёл. Попробуй другое слово.")
        return

    kb = InlineKeyboardBuilder()
    for cid, title in rows:
        kb.button(text=(title or f"#{cid}")[:40], callback_data=f"cred_open:{cid}")
    kb.adjust(1)

    await m.answer("Нашёл это:", reply_markup=kb.as_markup())

# --- CREDS helpers (карточка и загрузка записи) ---

def _render_cred_html(rec: dict) -> str:
    """
    Карточка учётки:
      • Название сервиса — <b>…</b> в <blockquote>
      • Логин — <code>…</code>
      • Пароль — <tg-spoiler><code>…</code></tg-spoiler>
    """
    title = (rec.get("title") or "").strip()
    login = (rec.get("login") or "").strip()
    pwd   = (rec.get("password") or "").strip()

    return (
        f"<blockquote><b>{escape(title)}</b></blockquote>\n"
        f"<b>Логин:</b> <code>{escape(login)}</code>\n"
        f"<b>Пароль:</b> <tg-spoiler><code>{escape(pwd)}</code></tg-spoiler>"
    )

async def _get_cred_by_id(db, cred_id: int, owner_id: int) -> dict | None:
    # owner_id больше не используется — чтение доступно всем активным пользователям
    cur = await db.execute(
        "SELECT id, title, login, password, note FROM creds WHERE id=?",
        (cred_id,),
    )
    row = await cur.fetchone()
    if not row:
        return None
    return {"id": row[0], "title": row[1], "login": row[2], "password": row[3], "note": row[4]}

from datetime import time as dtime

WORK_START_H, WORK_END_H = 10, 19  # 10:00–19:00 по LOCAL_TZ

def _to_local(dt_utc):
    return dt_utc.astimezone(LOCAL_TZ) if dt_utc.tzinfo else dt_utc.replace(tzinfo=UTC).astimezone(LOCAL_TZ)

def in_work_hours(dt_utc) -> bool:
    dl = _to_local(dt_utc)
    t = dl.timetz()
    return (dtime(hour=WORK_START_H) <= t.replace(tzinfo=None) < dtime(hour=WORK_END_H))

def next_work_start_after(dt_utc):
    dl = _to_local(dt_utc)
    # если уже в рабочем окне — вернуть dt_utc как есть
    if in_work_hours(dt_utc):
        return dt_utc
    # если до начала — сегодня в 10:00
    if dl.time() < dtime(hour=WORK_START_H):
        wstart_local = dl.replace(hour=WORK_START_H, minute=0, second=0, microsecond=0)
    else:
        # после 19:00 — завтра в 10:00
        wstart_local = (dl + timedelta(days=1)).replace(hour=WORK_START_H, minute=0, second=0, microsecond=0)
    return wstart_local.astimezone(UTC)

def clamp_to_work_hours(dt_utc):
    """Если время попало вне рабочего окна — перенесём на следующее начало рабочего времени."""
    return next_work_start_after(dt_utc) if not in_work_hours(dt_utc) else dt_utc

# helper: отключить кнопки у сообщения и (опц.) изменить текст
async def disable_kb_and_optionally_edit(message, extra_note: str | None = None):
    try:
        if extra_note:
            new_text = (message.text or "") + f"\n\n{extra_note}"
            await message.edit_text(new_text)
        else:
            await message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

async def get_user_by_tg(db, tg_id: int):
    cur = await db.execute("SELECT id, tg_id, full_name, role, registered, is_active, dept FROM users WHERE tg_id=?", (tg_id,))
    row = await cur.fetchone()
    return dict(zip(["id","tg_id","full_name","role","registered","is_active","dept"], row)) if row else None

async def get_user_by_id(db, user_id: int):
    cur = await db.execute(
        "SELECT id, tg_id, full_name, role, registered, is_active, dept FROM users WHERE id=?",
        (user_id,)
    )
    row = await cur.fetchone()
    return dict(zip(["id","tg_id","full_name","role","registered","is_active","dept"], row)) if row else None

async def rehire_user_by_tg(db, tg_id: int, role: str | None = None, set_registered: bool | None = None):
    # role: 'employee' | 'lead' | 'head' | 'developer' | None (оставить как есть)
    # set_registered: True/False/None (оставить как есть)
    sets = ["is_active=1"]
    params = []

    if role:
        sets.append("role=?")
        params.append(role)
    if set_registered is True:
        sets.append("registered=1")
    elif set_registered is False:
        sets.append("registered=0")

    sql = f"UPDATE users SET {', '.join(sets)} WHERE tg_id=?"
    params.append(tg_id)

    cur = await db.execute(sql, tuple(params))
    await db.commit()
    return cur.rowcount  # 0 — не нашли, 1 — ок

async def log_task_event(db, task_id: int, event: str, meta: str | None = None):
    """
    Сохранить событие по задаче в журнале (для будущей диаграммы Ганта и отчётов).
    event: 'create' | 'start' | 'deadline_set' | 'postpone' | 'done'
    meta:  произвольный текст (например, "old=..., new=..., reason=...")
    """
    try:
        await db.execute(
            "INSERT INTO task_events(task_id, event, at, meta) VALUES(?,?,?,?)",
            (task_id, event, datetime.now(UTC).isoformat(), meta)
        )
    except Exception:
        # журнал — служебный, не должен ломать основной сценарий
        pass

def is_dev_tg(tg_id: int) -> bool:
    return DEVELOPER_TG_ID and tg_id == DEVELOPER_TG_ID

async def ensure_user(db, tg_id: int, full_name: str | None):
    u = await get_user_by_tg(db, tg_id)
    safe_name = (full_name or "unknown").strip() or "unknown"

    if u:
        # Обновим имя при необходимости
        if safe_name and (not u["full_name"] or u["full_name"] == "unknown"):
            await db.execute("UPDATE users SET full_name=? WHERE tg_id=?", (safe_name, tg_id))
            await db.commit()
            u["full_name"] = safe_name
        # Разработчик — всегда developer и активен
        if is_dev_tg(tg_id) and (u["role"] != "developer" or u["is_active"] != 1):
            await db.execute("UPDATE users SET role='developer', is_active=1 WHERE tg_id=?", (tg_id,))
            await db.commit()
            u["role"] = "developer"; u["is_active"] = 1
        # Владелец (если не дев) — head, но не перебивает developer
        elif OWNER_TG_ID and tg_id == OWNER_TG_ID and u["role"] not in ("developer","head"):
            await db.execute("UPDATE users SET role='head' WHERE tg_id=?", (tg_id,))
            await db.commit()
            u["role"] = "head"
        return u

    # Создание новой карточки: еще НЕ зарегистрирован
    if is_dev_tg(tg_id):
        role, is_active, registered = "developer", 1, 1  # дев активен и «считаем зарегистрированным»
    elif OWNER_TG_ID and tg_id == OWNER_TG_ID:
        role, is_active, registered = "head", 1, 0
    else:
        role, is_active, registered = "employee", 1, 0

    await db.execute(
        "INSERT INTO users(tg_id, full_name, role, registered, is_active) VALUES(?,?,?,?,?)",
        (tg_id, safe_name, role, registered, is_active)
    )
    await db.commit()
    return await get_user_by_tg(db, tg_id)

async def user_has_active_task(db, user_id: int) -> bool:
    """
    True, если у пользователя уже есть активная задача.
    Страхуемся на случай “нестандартных” значений статуса.
    """
    cur = await db.execute(
        """
        SELECT 1
        FROM tasks
        WHERE user_id = ?
          AND status IN ('in_progress', 'in progress', 'started')
        LIMIT 1
        """,
        (user_id,),
    )
    return (await cur.fetchone()) is not None

async def is_manager_of(db, manager_id: int, subordinate_id: int) -> bool:
    # developer может всё
    cur = await db.execute("SELECT role FROM users WHERE id=?", (manager_id,))
    row = await cur.fetchone()
    if row and row[0] == "developer":
        return True
    sql = """
    WITH RECURSIVE chain(manager_id, subordinate_id) AS (
      SELECT manager_user_id, subordinate_user_id FROM manager_links
      UNION
      SELECT ml.manager_user_id, c.subordinate_id
      FROM manager_links ml
      JOIN chain c ON ml.subordinate_user_id = c.manager_id
    )
    SELECT 1 FROM chain WHERE manager_id=? AND subordinate_id=? LIMIT 1;
    """
    cur = await db.execute(sql, (manager_id, subordinate_id))
    return (await cur.fetchone()) is not None

async def get_manager_tg_ids(db, subordinate_user_id: int):
    sql = """
    WITH RECURSIVE chain(manager_id, subordinate_id) AS (
      SELECT manager_user_id, subordinate_user_id FROM manager_links
      UNION
      SELECT ml.manager_user_id, c.subordinate_id
      FROM manager_links ml
      JOIN chain c ON ml.subordinate_user_id = c.manager_id
    )
    SELECT DISTINCT u.tg_id
    FROM chain ch
    JOIN users u ON u.id = ch.manager_id
    WHERE ch.subordinate_id = ?;
    """
    cur = await db.execute(sql, (subordinate_user_id,))
    rows = await cur.fetchall()
    tg_ids = {r[0] for r in rows}
    # developer всегда получает уведомления
    if DEVELOPER_TG_ID:
        tg_ids.add(DEVELOPER_TG_ID)
    return list(tg_ids)

def fmt_dt_local(iso: str | None) -> str:
    if not iso:
        return "не указан"
    try:
        dt = dateparser.parse(iso)
        dt_local = dt.astimezone(LOCAL_TZ) if dt.tzinfo else dt.replace(tzinfo=UTC).astimezone(LOCAL_TZ)
        return dt_local.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return iso
    
# === helpers/formatting ===
STATUS_RU = {
    "new": "Ожидает",
    "in_progress": "В работе",
    "done": "Завершена",
}

def render_task_card_html(t: dict) -> str:
    """
    t: dict со столбцами из tasks + description, status, deadline
    """
    title = f"#{t['id']}: <b>{t['description']}</b> | <u>{STATUS_RU.get(t['status'], t['status'])}</u>"
    dl = fmt_dt_local(t.get("deadline"))
    quote = f"<blockquote><b>Дедлайн:</b> {dl}</blockquote>"
    return f"{title}\n{quote}"

# === helper: безопасно убрать inline-кнопки у сообщения ===
async def hide_inline_kb(cq: CallbackQuery):
    # 1) обычный путь
    try:
        await cq.message.edit_reply_markup(reply_markup=None)
        return
    except Exception as e:
        logging.warning(f"edit_reply_markup failed: {e}")

    # 2) правим текст без клавиатуры (сохраняем HTML)
    try:
        if getattr(cq.message, "html_text", None):
            await cq.message.edit_text(
                cq.message.html_text,
                parse_mode="HTML",
                reply_markup=None
            )
        else:
            await cq.message.edit_text(
                cq.message.text or "",
                reply_markup=None
            )
        return
    except Exception as e:
        logging.warning(f"edit_text(no kb) failed: {e}")

    # 3) прямой вызов API — на случай, если объект message «капризничает»
    try:
        await cq.message.bot.edit_message_reply_markup(
            chat_id=cq.message.chat.id,
            message_id=cq.message.message_id,
            reply_markup=None
        )
    except Exception as e:
        logging.warning(f"bot.edit_message_reply_markup failed: {e}")
    
# --- helpers for pretty HTML messages ---
import html

def H(s: str) -> str:   # escape
    return html.escape(s or "")

def B(s: str) -> str:   # <b>...</b>
    return f"<b>{H(s)}</b>"

def U(s: str) -> str:   # <u>...</u>
    return f"<u>{H(s)}</u>"

def Q(s: str) -> str:   # blockquote (фиолетовая цитата в Telegram)
    return f"<blockquote>{H(s)}</blockquote>"

def _esc(s: str | None) -> str:
    return html.escape(s or "")

def _q_deadline(deadline_iso: str | None) -> str:
    """Строка-дедлайн в цитате."""
    return f"<blockquote><b>Дедлайн:</b> {fmt_dt_local(deadline_iso)}</blockquote>"

# --- text helpers: единый стиль всех сообщений ---

# Если этих двух функций у тебя нет выше по файлу — оставь их.
# Если уже определены, этот блок можно опустить (или оставить — повторное определение не критично).
from html import escape as _esc
def H(s: str) -> str:
    return _esc(s or "")

def Q(s: str) -> str:
    # Цитата для Telegram HTML. Если <blockquote> у клиента не поддерживается — «| » всё равно красиво.
    return f"<blockquote>{H(s)}</blockquote>"

def text_overdue_emp(emp_name: str, task_id: int, desc: str, deadline_iso: str) -> str:
    return (
        f"⛔ {H(emp_name)} ваша задача <u>просрочена</u>:\n"
        f"#{task_id} — <b>{H(desc)}</b>\n"
        f"{Q('Дедлайн: ' + fmt_dt_local(deadline_iso))}"
    )

def text_overdue_mgr(emp_name: str, task_id: int, desc: str, deadline_iso: str) -> str:
    return (
        f"⛔ Просрочка у {H(emp_name)}:\n"
        f"#{task_id} — <b>{H(desc)}</b>\n"
        f"{Q('Дедлайн: ' + fmt_dt_local(deadline_iso))}"
    )

def text_deadline_reached(task_id: int, desc: str, deadline_iso: str) -> str:
    # Текст сообщения РОВНО в момент дедлайна
    return (
        "🕒 <b>Время дедлайна по задаче вышло:</b>\n"
        f"#{task_id}: <b>{H(desc)}</b>\n"
        f"{Q('Дедлайн: ' + fmt_dt_local(deadline_iso))}\n\n"
        "Выберите действие ниже или ответьте на ЭТО сообщение реплаем — я отправлю комментарий руководителю."
    )

def _kb_overdue(task_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Готово",  callback_data=f"task_done:{task_id}")
    kb.button(text="🔔 +10м",    callback_data=f"overdue_snooze:{task_id}:10")
    kb.button(text="🔔 +15м",    callback_data=f"overdue_snooze:{task_id}:15")
    kb.button(text="🔔 +30м",    callback_data=f"overdue_snooze:{task_id}:30")
    kb.button(text="🔔 +1ч",     callback_data=f"overdue_snooze:{task_id}:60")
    kb.button(text="⌨️ Ввести время", callback_data=f"overdue_custom:{task_id}")
    kb.button(text="📅 Изменить дедлайн", callback_data=f"task_extend:{task_id}")
    kb.adjust(2, 2, 2)
    return kb

# === helper: перерисовать карточку просрочки ===
async def _refresh_overdue_card(
    db,
    chat_id: int,
    message_id: int,
    task_id: int,
    extra_line: str | None = None,
):
    # Берём описание и дедлайн из БД
    cur = await db.execute(
        "SELECT description, deadline FROM tasks WHERE id=?",
        (task_id,),
    )
    row = await cur.fetchone()
    if not row:
        return
    desc, deadline = row

    # Текст карточки в нашем уже используемом стиле
    text = text_deadline_reached(task_id, desc or "", deadline or "")
    if extra_line:
        text = f"{text}\n\n{extra_line}"

    # Перерисовываем исходное сообщение с теми же кнопками
    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=_kb_overdue(task_id).as_markup(),
            parse_mode="HTML",
        )
    except Exception as e:
        logging.warning(f"overdue edit failed: {e}")

# --- end helpers ---

import re
from datetime import datetime, timedelta, timezone

TIME_HHMM_COLON = re.compile(r"\b([01]?\d|2[0-3]):([0-5]\d)\b")
TIME_HH_ONLY    = re.compile(r"\b([01]?\d|2[0-3])\b")

def parse_human_time(text: str, base_tz=LOCAL_TZ):
    """
    Понимает: 21:43, 2143, 'в 19', 'сегодня в 19:00', 'завтра в 10', 'через 20 минут',
    '30.09 в 11', '01.10.2025 09:30' и т.п.
    Возвращает aware datetime в UTC или None.
    """
    if not text:
        return None
    s = (text or "").strip().lower()
    now_local = datetime.now(base_tz)

    # 0) "через N минут/часов"
    m = re.search(r"через\s+(\d+)\s*(минут|мин|м)\b", s)
    if m:
        dt_local = now_local + timedelta(minutes=int(m.group(1)))
        return dt_local.astimezone(UTC)

    m = re.search(r"через\s+(\d+)\s*(час|часа|часов|ч)\b", s)
    if m:
        dt_local = now_local + timedelta(hours=int(m.group(1)))
        return dt_local.astimezone(UTC)

    # 1) 'сегодня ...'
    if "сегодня" in s:
        # ищем время
        hh, mm = None, 0
        m = TIME_HHMM_COLON.search(s)
        if m:
            hh, mm = int(m.group(1)), int(m.group(2))
        else:
            m = re.search(r"\bв\s+([01]?\d|2[0-3])\b", s)
            if m:
                hh = int(m.group(1))
        if hh is None:
            return None  # «сегодня» без времени — не принимаем
        d = now_local.date()
        dt_local = datetime(d.year, d.month, d.day, hh, mm, tzinfo=base_tz)
        if dt_local <= now_local:
            # если указали прошедшее на сегодня — перенесём на завтра
            dt_local += timedelta(days=1)
        return dt_local.astimezone(UTC)

    # 2) 'завтра ...'
    if "завтра" in s:
        hh, mm = None, 0
        m = TIME_HHMM_COLON.search(s)
        if m:
            hh, mm = int(m.group(1)), int(m.group(2))
        else:
            m = re.search(r"\bв\s+([01]?\d|2[0-3])\b", s)
            if m:
                hh = int(m.group(1))
        if hh is None:
            hh, mm = 10, 0
        d = (now_local + timedelta(days=1)).date()
        dt_local = datetime(d.year, d.month, d.day, hh, mm, tzinfo=base_tz)
        return dt_local.astimezone(UTC)

    # 3) 'в HH[:MM]'
    m = re.search(r"\bв\s+([01]?\d|2[0-3])(?::([0-5]\d))?\b", s)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2) or 0)
        d = now_local.date()
        dt_local = datetime(d.year, d.month, d.day, hh, mm, tzinfo=base_tz)
        if dt_local <= now_local:
            dt_local += timedelta(days=1)
        return dt_local.astimezone(UTC)

    # 4) голое HH:MM
    m = TIME_HHMM_COLON.search(s)
    if m:
        hh, mm = int(m.group(1)), int(m.group(2))
        d = now_local.date()
        dt_local = datetime(d.year, d.month, d.day, hh, mm, tzinfo=base_tz)
        if dt_local <= now_local:
            dt_local += timedelta(days=1)
        return dt_local.astimezone(UTC)

    # 5) голые 4 цифры 2143
    if re.fullmatch(r"\d{4}", s):
        hh, mm = int(s[:2]), int(s[2:])
        if 0 <= hh < 24 and 0 <= mm < 60:
            d = now_local.date()
            dt_local = datetime(d.year, d.month, d.day, hh, mm, tzinfo=base_tz)
            if dt_local <= now_local:
                dt_local += timedelta(days=1)
            return dt_local.astimezone(UTC)

    # 6) DD.MM[.YYYY] (опц. «в HH[:MM]»)
    dm = re.search(r"\b([0-3]?\d)\.([01]?\d)(?:\.(\d{4}))?\b", s)
    if dm:
        dd, mm, yyyy = int(dm.group(1)), int(dm.group(2)), int(dm.group(3) or now_local.year)
        hh, mi = 10, 0
        tm = TIME_HHMM_COLON.search(s)
        if tm:
            hh, mi = int(tm.group(1)), int(tm.group(2))
        else:
            tm = re.search(r"\bв\s+([01]?\d|2[0-3])\b", s)
            if tm:
                hh, mi = int(tm.group(1)), 0
        try:
            dt_local = datetime(yyyy, mm, dd, hh, mi, tzinfo=base_tz)
        except ValueError:
            return None
        if dt_local <= now_local:
            if dm.group(3):
                return None
            try:
                dt_local = datetime(yyyy + 1, mm, dd, hh, mi, tzinfo=base_tz)
            except ValueError:
                return None
        return dt_local.astimezone(UTC)

    return None

def parsed_dt_to_utc(dt):
    if dt.tzinfo:
        return dt.astimezone(UTC)
    return dt.replace(tzinfo=LOCAL_TZ).astimezone(UTC)

async def active_tasks_summary(db, user_id: int) -> str:
    cur = await db.execute("""
        SELECT id, description, status, deadline
        FROM tasks
        WHERE user_id=? AND status!='done'
        ORDER BY COALESCE(deadline,'9999-12-31'), id
    """, (user_id,))
    rows = await cur.fetchall()

    if not rows:
        return "Активных задач нет."

    parts = [f"Активных задач: {len(rows)}"]
    for tid, desc, st, dl in rows:
        parts.append(_format_task_line(tid, desc or "", st or "new", dl))
    return "\n".join(parts)

async def render_user_summary(db, user_id: int) -> str:
    user = await get_user_by_id(db, user_id)
    if not user:
        return "Пользователь не найден."

    # Активные задачи (всё, что не done)
    cur = await db.execute("""
        SELECT id, description, deadline, status, planned_start_at, updated_at, started_at
        FROM tasks
        WHERE user_id=? AND status!='done'
        ORDER BY COALESCE(deadline, '9999') ASC, id DESC
        LIMIT 100
    """, (user_id,))
    rows = await cur.fetchall()

    title = f"<b>Сводка по сотруднику — {H(user['full_name'])}:</b>"
    if not rows:
        return f"{title}\nАктивных задач нет."

    out = [title, f"Активных задач: <b>{len(rows)}</b>", ""]
    for (tid, desc, deadline, status, planned_start_at, updated_at, started_at) in rows:
        started_line = f"\n• Стартовал: {fmt_dt_local(started_at)}" if started_at else ""
        dl_line = f"\n• Дедлайн: {fmt_dt_local(deadline)}" if deadline else ""
        out.append(
            f"#{tid} — {H(desc or '')}\n"
            f"• Статус: {H(status or '')}{started_line}{dl_line}"
        )
    return "\n".join(out[:300])

def next_reminder_after(deadline_iso: str | None) -> str:
    """
    Если до дедлайна < 1 часа — напомнить через 5 минут ПОСЛЕ дедлайна.
    Иначе — напомнить через час.
    Всегда придерживаемся рабочего окна.
    """
    now = datetime.now(UTC)
    try:
        if not deadline_iso:
            nxt = now + timedelta(hours=1)
            return clamp_to_work_hours(nxt).isoformat()

        dl = dateparser.parse(deadline_iso)
        # грейс 5 минут после дедлайна
        if (dl - now) <= timedelta(hours=1):
            nxt = dl + timedelta(minutes=5)
            return clamp_to_work_hours(nxt).isoformat()

        nxt = now + timedelta(hours=1)
        return clamp_to_work_hours(nxt).isoformat()
    except Exception:
        return clamp_to_work_hours(now + timedelta(hours=1)).isoformat()
    
# ===== Google Sheets (вторая таблица для больших проектов) =====

def _require_gs_projects_config():
    from pathlib import Path
    gs_id = os.getenv("GSHEET_PROJECTS_ID", "").strip()
    cred_env = os.getenv("GOOGLE_CREDENTIALS_FILE", "").strip()
    errs = []
    if not gs_id:
        errs.append("GSHEET_PROJECTS_ID пуст (нет ID второй таблицы).")
    if not cred_env:
        errs.append("GOOGLE_CREDENTIALS_FILE пуст (нет пути к service_account.json).")
    cred_path = Path(cred_env) if cred_env else None
    if cred_path and not cred_path.is_absolute():
        cred_path = Path(__file__).resolve().parent / cred_path
    if cred_path and not cred_path.exists():
        errs.append(f"Файл кредов не найден: {cred_path}")
    if errs:
        raise RuntimeError("Конфиг второй таблицы не задан:\n- " + "\n- ".join(errs))
    return gs_id, str(cred_path)

# кэш отдельный, чтобы не мешать первой таблице
_agcm_cache_projects = {"path": None, "mgr": None}

async def _gs_open_projects():
    gs_id, cred_abs = _require_gs_projects_config()
    if _agcm_cache_projects["mgr"] is None or _agcm_cache_projects["path"] != cred_abs:
        _agcm_cache_projects["mgr"] = _agcm_builder(cred_abs)  # из первой части helpers
        _agcm_cache_projects["path"] = cred_abs
    agc = await _agcm_cache_projects["mgr"].authorize()
    return await agc.open_by_key(gs_id)

def _sheet_title_from_name(name: str) -> str:
    # Google ограничивает длину и спецсимволы
    t = (name or "Project")[:95]
    # запрещённые символы: []:*?/\
    for ch in '[]:*?/\\':
        t = t.replace(ch, ' ')
    return t.strip() or "Project"

async def _dedupe_sheet_title(sh, desired: str) -> str:
    """Если такой лист уже есть — добавляем суффикс ' (2)', '(3)', ..."""
    try:
        existing = {ws.title for ws in await sh.worksheets()}
    except Exception:
        existing = set()
    if desired not in existing:
        return desired
    base = desired
    i = 2
    while True:
        cand = f"{base} ({i})"
        if cand not in existing:
            return cand
        i += 1

async def _ensure_project_ws(sh, sheet_title: str, start_date: date, end_date: date):
    """Создаёт/обновляет лист проекта: A=Задача, B=Исполнитель, C..=дни."""
    ws = await _gs_ensure_ws(sh, sheet_title, rows=200, cols=200)

    # Шапка
    dates = []
    d = start_date
    while d <= end_date:
        dates.append(d.strftime("%d.%m.%Y"))
        d += timedelta(days=1)
    header = ["Задача", "Исполнитель"] + dates
    await ws.update('A1', [header], value_input_option="USER_ENTERED")

    # Заморозка и немного форматирования
    try:
        await ws.freeze(rows=1, cols=2)
    except Exception:
        pass
    try:
        await ws.format('A1:B1', {'textFormat': {'bold': True}})
    except Exception:
        pass
    return ws

async def _projects_next_row(ws) -> int:
    """Номер следующей строки (1-based) после последней заполненной в колонке A."""
    try:
        colA = await ws.col_values(1)
    except Exception:
        colA = []
    return max(2, len(colA) + 1)

async def _projects_paint_cell(ws, row_index: int, col_index: int, color: dict):
    """
    Закрасить ячейку (row_index, col_index) указанным цветом.
    Работает с gspread_asyncio через Worksheet.format(A1, {...}).
    """
    # импорт локально, чтобы не трогать импортов сверху
    from gspread.utils import rowcol_to_a1

    # A1-нотация для конкретной ячейки
    a1 = rowcol_to_a1(row_index, col_index)

    try:
        # основной путь — форматирование ячейки на уровне листа
        await ws.format(a1, {"backgroundColor": color})
    except Exception as e:
        # резерв: просто проглатываем, чтобы не ронять обработчик (логировать при желании)
        import logging
        logging.warning("projects_paint_cell fallback/skip: %s", e)

# =========================
# FSM
# =========================
class RegisterForm(StatesGroup):
    waiting_fullname = State()
    waiting_dept = State() 

class TaskForm(StatesGroup):
    waiting_desc = State()
    waiting_deadline = State()

class OverdueForm(StatesGroup):
    waiting_time = State()

class ExtendReason(StatesGroup):
    waiting_for_reason = State()
    waiting_for_datetime = State()

class ExtendDeadline(StatesGroup):
    waiting_for_deadline = State()

class AssignPick(StatesGroup):
    picking_user = State()
class AssignTask(StatesGroup):
    waiting_desc = State()
    waiting_deadline = State()
class DeptAssign(StatesGroup):
    picking_user = State()    # выбор сотрудника
    waiting_dept = State()    # ввод названия отдела

class SetRoleState(StatesGroup):
    waiting = State()
class LinkState(StatesGroup):
    waiting = State()

class LinkProjectCreate(StatesGroup):
    waiting_name = State()

class LinkAdd(StatesGroup):
    picking_project = State()     # выбираем проект из списка (если пришли не из кнопки проекта)
    waiting_title = State()       # как назвать ссылку
    waiting_url = State()         # сам URL

# =========================
# Access middleware (после FSM!)
# =========================
from aiogram import BaseMiddleware
from aiogram.types import Update
from aiogram.fsm.context import FSMContext
from typing import Callable, Dict, Any, Awaitable

ALLOW_CMDS_UNREG = {"/start", "/register", "/help", "/id", "/whoami"}
ALLOW_BTNS_UNREG = {"📝 Регистрация", "ℹ️ Помощь", "🆔 Мой ID"}
# Любые «верхнеуровневые» действия, при которых надо сбрасывать FSM,
# чтобы текст не улетал в текущую форму.
MAIN_ENTRY_TEXTS = {
    "Меню", "Проекты", "📋 Мои задачи", "➕ Добавить задачу",
    "📝 Регистрация", "🆔 Мой ID", "ℹ️ Помощь",
    "🔗 Важные ссылки", "🔐 Пароли"
}

class AccessMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Update, Dict[str, Any]], Awaitable[Any]],
        event: Update,
        data: Dict[str, Any]
    ) -> Any:
        # Извлекаем tg_id и текст
        tg_id, text = None, ""
        if event.message:
            tg_id = event.message.from_user.id
            text = (event.message.text or "").strip()
        elif event.callback_query:
            tg_id = event.callback_query.from_user.id
            text = (event.callback_query.data or "").strip()
        else:
            return await handler(event, data)
        
        # --- Сбрасываем FSM при явном переходе на команды/верхнее меню ---
        state: FSMContext | None = data.get("state")
        if state:
            # 1) Любая команда вида /something
            if text.startswith("/"):
                await state.clear()
            # 2) Нажатие кнопок основного меню (ReplyKeyboard)
            elif event.message and text in MAIN_ENTRY_TEXTS:
                await state.clear()


        # Пользователь из БД (создадим карточку при первом заходе)
        async with aiosqlite.connect(DB_PATH) as db:
            u = await get_user_by_tg(db, tg_id)
            if not u:
                u = await ensure_user(db, tg_id, None)

        # Блок для уволенных (кроме developer)
        if u["is_active"] != 1 and not is_dev_tg(tg_id):
            msg = "⛔ Доступ к боту закрыт. Обратитесь к руководителю."
            if event.message:
                await event.message.answer(msg)
            else:
                await event.callback_query.answer(msg, show_alert=True)
            return

        state: FSMContext | None = data.get("state")
        current_state = await state.get_state() if state else None
        # пропускаем ограничения на обоих шагах регистрации
        in_registration = current_state in {
            RegisterForm.waiting_fullname.state,
            RegisterForm.waiting_dept.state,
        }

        # Ограничения для незарегистрированных (кроме developer)
        if u["registered"] != 1 and not is_dev_tg(tg_id) and not in_registration:
            allowed = False
            if event.message:
                # Разрешаем базовые команды/кнопки, ведущие к регистрации
                if text in ALLOW_BTNS_UNREG or any(text.startswith(cmd) for cmd in ALLOW_CMDS_UNREG):
                    allowed = True
            # callbacks до регистрации запрещаем
            if not allowed:
                warn = "⚠️ Сначала пройдите регистрацию: нажмите «📝 Регистрация» или команду /register."
                if event.message:
                    await event.message.answer(warn)
                else:
                    await event.callback_query.answer("Сначала зарегистрируйтесь: /register", show_alert=True)
                return

        # Всё ок — пропускаем дальше
        data["current_user"] = u

        # Нужно ли удалять пользовательский ответ?
        # Удаляем только если:
        #  - это обычное сообщение пользователя (не callback)
        #  - пользователь сейчас в любом состоянии FSM (идёт диалог/форма)
        #  - это не команда вида /... и не нажатие кнопки основного меню
        autodel = False
        if event.message:
            try:
                st = await state.get_state() if state else None
                if st and not text.startswith("/") and text not in MAIN_ENTRY_TEXTS:
                    autodel = True
            except Exception:
                pass

        result = await handler(event, data)

        if autodel:
            try:
                await event.message.delete()
            except Exception:
                pass

        return result

# =========================
# Общие команды и кнопки
# =========================
@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    async with aiosqlite.connect(DB_PATH) as db:
        user = await ensure_user(db, m.from_user.id, m.from_user.full_name or m.from_user.username or "unknown")

    # Если не зарегистрирован → сразу в форму
    if user["registered"] != 1 and not is_dev_tg(m.from_user.id):
        await state.set_state(RegisterForm.waiting_fullname)
        await m.answer(
            "Привет! Это внутренний бот задач.\n"
            "Перед началом работы укажи Фамилию и Имя (например: Иванов Иван).",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="📝 Регистрация")],[KeyboardButton(text="ℹ️ Помощь"), KeyboardButton(text="🆔 Мой ID")]],
                resize_keyboard=True
            )
        )
        return

    # Зарегистрирован — обычное меню
    role_hint = user["role"]
    txt = [
        "Привет! Это внутренний бот задач.",
        f"Твоя роль: {role_hint}",
        "",
        "Доступные действия на кнопках ниже 👇",
        "• ➕ Добавить задачу",
        "• 📋 Мои задачи",
        "• 📝 Регистрация",
        "• 🆔 Мой ID",
        "• ℹ️ Помощь",
    ]
    await m.answer("\n".join(txt), reply_markup=main_menu_kb())


@router.message(F.text.in_({"ℹ️ Помощь", "/help"}))
async def cmd_help(m: Message):
    text = (
        "<b>Команды и разделы</b>\n\n"
        "• <b>Задачи</b>: добавление, перенос, статусы, напоминания.\n"
        "  — Кнопки: «➕ Добавить задачу», «📋 Мои задачи».\n\n"
        "• <b>Важные ссылки</b>: проекты и их ссылки.\n"
        "  — Кнопка: «🔗 Важные ссылки».\n\n"
        "• <b>Пароли</b>: доступы к командным сервисам (для head/lead/developer).\n"
        "  — Кнопка: «🔐 Пароли».\n\n"
        "<b>Подсказки по времени</b>:\n"
        "• Можно писать: <code>в 19</code>, <code>завтра 10:00</code>, <code>через 20 минут</code>, <code>30.09 11:00</code>.\n"
        "• Отвечайте реплаем на напоминание — я отправлю комментарий руководителю."
    )
    await m.answer(text, parse_mode="HTML")

@router.message(Command("id"))
async def cmd_id(m: Message):
    tg_id = m.from_user.id

    # читаем роль из БД
    role_suffix = ""
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, tg_id)  # уже существующая функция
        if me and me.get("role"):
            # показываем только допустимые роли
            r = me["role"]
            title = {"head": "Head", "lead": "Lead", "employee": "Employee", "developer": "Dev"}.get(r)
            if title:
                role_suffix = f" ({title})"

    # как на скриншоте: кликабельный "user id", ниже — сам id, затем роль (если есть)
    await m.answer(
        f'Твой Telegram <a href="tg://user?id={tg_id}">user id</a>:\n'
        f'<code>{tg_id}</code>{role_suffix}'
    )

@router.message(Command("tz"))
async def cmd_tz(m: Message):
    await m.answer(f"Текущий TZ: {TZ_NAME}")

@router.message(Command("now"))
async def cmd_now(m: Message):
    now_utc = datetime.now(UTC)
    now_local = now_utc.astimezone(LOCAL_TZ)
    await m.answer(f"UTC: {now_utc:%Y-%m-%d %H:%M:%S %Z}\n{TZ_NAME}: {now_local:%Y-%m-%d %H:%M:%S %Z}")

@router.message(Command("test_morning"))
async def cmd_test_morning(m: Message):
    """
    Принудительно запустить утренний опрос (для тестов).
    Доступно только head.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
        if not me or me["role"] != "head":
            await m.answer("❌ Нет доступа.")
            return

    await m.answer("🚀 Запускаю утренний опрос вручную…")
    await daily_morning_broadcast()

# =========================
# Регистрация
# =========================
@router.message(Command("register"))
async def cmd_register(m: Message, state: FSMContext):
    # запрещаем повторную регистрацию, если уже зарегистрирован (кроме developer — он и так не проходит форму)
    async with aiosqlite.connect(DB_PATH) as db:
        u = await ensure_user(db, m.from_user.id, m.from_user.full_name or m.from_user.username or "unknown")

    if u.get("registered") == 1 and not is_dev_tg(m.from_user.id):
        await m.answer("Вы уже зарегистрированы ✅. Если нужна перерегистрация — обратитесь к разработчику.")
        return

    await state.set_state(RegisterForm.waiting_fullname)
    await m.answer("Введите ваши Фамилию и Имя (например: Иванов Иван).")

@router.message(RegisterForm.waiting_fullname)
async def do_register(m: Message, state: FSMContext):
    full = (m.text or "").strip()
    if len(full.split()) < 2:
        await m.answer("Нужно две части: Фамилия и Имя. Попробуйте ещё раз.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        # сохраним ФИО, регистрацию пока не закрываем
        u = await ensure_user(db, m.from_user.id, full)
        await db.execute("UPDATE users SET full_name=? WHERE id=?", (full, u["id"]))
        await db.commit()

    await state.set_state(RegisterForm.waiting_dept)
    await m.answer(
        "Укажите ваш отдел (например: <code>SMM</code> или <code>Дизайн/Графика</code>). "
        "Напишите одним словом или короткой фразой.",
        parse_mode="HTML"
    )

@router.message(RegisterForm.waiting_dept)
async def do_register_dept(m: Message, state: FSMContext):
    dept = (m.text or "").strip()
    if not dept:
        await m.answer("Отдел не распознан. Напишите название отдела текстом (например: SMM).")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        u = await ensure_user(db, m.from_user.id, None)
        await db.execute("UPDATE users SET dept=?, registered=1 WHERE id=?", (dept, u["id"]))
        await db.commit()

    await state.clear()
    await m.answer("Готово. Регистрация завершена ✅. Доступ к функциям открыт.", reply_markup=main_menu_kb())

@router.message(DeptAssign.waiting_dept)
async def dept_assign_apply(m: Message, state: FSMContext):
    dept = (m.text or "").strip()
    if not dept:
        await m.answer("Отдел не распознан. Введите короткое название."); 
        return

    data = await state.get_data()
    target_user_id = data.get("dept_target_user_id")
    if not target_user_id:
        await state.clear()
        await m.answer("Сессия потеряна, начните заново через «🏷 Определить отдел».")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
        if me["role"] not in ("head", "developer"):
            await m.answer("Нет доступа"); await state.clear(); return

        await db.execute("UPDATE users SET dept=? WHERE id=?", (dept, target_user_id))
        await db.commit()

        tgt = await get_user_by_id(db, target_user_id)

    await state.clear()
    await m.answer(
        f"✅ Отдел назначен.\n"
        f"<b>{H(tgt['full_name'] or f'user_{tgt['tg_id']}')}</b> → <b>{H(dept)}</b>",
        parse_mode="HTML"
    )

# =========================
# Добавление задачи СЕБЕ
# =========================
@router.message(Command("task"))
async def cmd_task(m: Message, state: FSMContext):
    async with aiosqlite.connect(DB_PATH) as db:
        u = await ensure_user(db, m.from_user.id, m.from_user.full_name or m.from_user.username or "unknown")
        if not u["full_name"] or u["full_name"] == "unknown" or len(u["full_name"].split()) < 2:
            await m.answer("⚠️ Сначала зарегистрируйтесь: «📝 Регистрация».")
            return

    await state.set_state(TaskForm.waiting_desc)
    # ⬇️ сохраняем id сообщения, чтобы дальше редактировать его же
    msg = await m.answer("Опишите задачу (кратко):")
    await state.update_data(add_msg_id=msg.message_id, add_chat_id=m.chat.id)

# Кнопки главного меню

# ====== DEV: "📈 Зарегистрировались" (reply-кнопка) ======
@router.message(F.text == "📈 Зарегистрировались")
async def admin_stats_reply(m: Message):
    # доступ только разработчику
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
        if not me or me.get("role") != "developer":
            await m.answer("Нет доступа."); return

        async def _count(sql: str, params=()):
            cur = await db.execute(sql, params)
            r = await cur.fetchone()
            return r[0] if r else 0

        total      = await _count("SELECT COUNT(*) FROM users WHERE is_active=1")
        employees  = await _count("SELECT COUNT(*) FROM users WHERE role='employee' AND is_active=1")
        leads      = await _count("SELECT COUNT(*) FROM users WHERE role='lead' AND is_active=1")
        heads      = await _count("SELECT COUNT(*) FROM users WHERE role='head' AND is_active=1")
        devs       = await _count("SELECT COUNT(*) FROM users WHERE role='developer' AND is_active=1")

        # Активность за 7 дней (если поле есть — не падаем, если нет)
        active_7d = 0
        try:
            since = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)).isoformat()
            active_7d = await _count("SELECT COUNT(*) FROM users WHERE created_at >= ?", (since,))
        except Exception:
            pass

    text = (
        "📈 Статистика пользователей:\n"
        f"• Всего: {total}\n"
        f"• Сотрудники: {employees}\n"
        f"• Лиды: {leads}\n"
        f"• Хеды: {heads}\n"
        f"• Девелоперы: {devs}\n"
    )
    if active_7d:
        text += f"• Активны за 7 дней: {active_7d}\n"

    await m.answer(text)


# ====== DEV: "👥 Сотрудники (удаление)" (reply-кнопка) ======
@router.message(F.text == "👥 Сотрудники (удаление)")
async def admin_users_reply(m: Message):
    # доступ только разработчику
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
        if not me or me.get("role") != "developer":
            await m.answer("Нет доступа."); return

        cur = await db.execute("""
            SELECT id, full_name, tg_id, role
            FROM users
            WHERE role!='developer' AND is_active=1
            ORDER BY role DESC, full_name COLLATE NOCASE
        """)
        rows = await cur.fetchall()

    if not rows:
        await m.answer("Нет пользователей для отображения.")
        return

    PAGE = 8
    page = 0
    total = len(rows)
    pages = max(1, (total + PAGE - 1) // PAGE)
    start, end = page * PAGE, page * PAGE + PAGE
    chunk = rows[start:end]

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    lines = ["Сотрудники (для удаления):"]

    # Используем уже существующие callback-генераторы из кода:
    # _admin_fire_cb(user_id), _admin_role_menu_cb(user_id), _admin_users_page_cb(page)
    for (uid, name, tg, role) in chunk:
        safe_name = (name or f"user_{tg}")
        lines.append(f"• {safe_name} (tg_id: {tg}, role: {role})")
        kb.button(text=f"👢 Уволить: {safe_name[:20]}", callback_data=_admin_fire_cb(uid))
        kb.button(text=f"⚙ Роль: {safe_name[:20]}", callback_data=_admin_role_menu_cb(uid))

    if page < pages - 1:
        kb.button(text="Далее »", callback_data=_admin_users_page_cb(page + 1))

    kb.adjust(1)
    await m.answer("\n".join(lines), reply_markup=kb.as_markup())

# === FULL RESET: показать подтверждение ===
@router.message(F.text == "🧨 FULL RESET")
async def admin_full_reset_prompt(m: Message, state: FSMContext):
    # на всякий случай — выходим из любых состояний
    try:
        await state.clear()
    except Exception:
        pass

    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Подтвердить сброс", callback_data="admin:reset_go")
    kb.button(text="❌ Отмена", callback_data="admin:reset_cancel")
    kb.adjust(1)

    await m.answer(
        "⚠️ Полный сброс бота.\n"
        "Будут удалены ВСЕ пользователи, задачи, проекты и события.\n\n"
        "Вы уверены?",
        reply_markup=kb.as_markup(),
    )

@router.message(F.text == "➕ Добавить задачу")
async def on_btn_add_task(m: Message, state: FSMContext):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
    if not me or not me.get("is_active", 1):
        await m.answer("❌ Вы больше не активны в системе. Обратитесь к руководителю.")
        return
    await cmd_task(m, state)

from aiogram import F
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

@router.message(F.text.in_({"Мои задачи", "📋 Мои задачи"}))
async def cmd_my_tasks(m: Message):
    """
    Показывает список активных задач сотрудника, отсортированных по дедлайну,
    каждая задача — отдельная карточка с кнопками управления.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        u = await ensure_user(db, m.from_user.id, m.from_user.full_name or "")

        # Получаем задачи, отсортированные по ближайшему дедлайну
        cur = await db.execute("""
            SELECT id, description, status, deadline
            FROM tasks
            WHERE user_id=? AND status!='done'
            ORDER BY COALESCE(deadline, '9999-12-31') ASC, id ASC
        """, (u["id"],))
        rows = await cur.fetchall()

    if not rows:
        await m.answer("✅ У вас нет активных задач.", parse_mode="HTML")
        return

    # Отправляем сообщение с количеством активных задач
    await m.answer(f"Активных задач: {len(rows)}", parse_mode="HTML")

    # Отправляем карточки по одной задаче
    for rid, desc, status, dl in rows:
        # Формируем красивую HTML-карточку
        text = (
            f"#{rid}: <b>{H(desc)}</b> | <u>{'Ожидает' if status=='new' else ('В работе' if status=='in_progress' else status)}</u>\n"
            f"{Q('Дедлайн: ' + fmt_dt_local(dl) if dl else 'Без дедлайна')}"
        )

        # Формируем кнопки под задачу
        kb = InlineKeyboardBuilder()
        if status == "new":
            kb.button(text="🚀 Начать задачу", callback_data=f"start_task_from_list:{rid}")
        elif status == "in_progress":
            kb.button(text="✅ Завершить задачу", callback_data=f"task_done:{rid}")
        kb.button(text="⏰ Сдвинуть срок", callback_data=f"task_extend:{rid}")
        kb.adjust(1)

        # Отправляем карточку
        await m.answer(text, parse_mode="HTML", reply_markup=kb.as_markup())

@router.message(F.text == "🔗 Важные ссылки")
async def links_home(m: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await ensure_user(db, m.from_user.id, m.from_user.full_name or "")
    is_editor = me["role"] in ("head", "developer")
    txt = (
        "Хранилище ссылок:\n"
        "• «📂 Выбрать проект» — открыть список проектов.\n"
        + ("• «➕ Добавить проект» — создать карточку проекта.\n" if is_editor else "")
    )
    await m.answer(txt, reply_markup=_links_root_kb(is_editor).as_markup())

@router.message(F.text.in_({"🔐 Пароли", "Пароли"}))
async def cmd_creds_menu(m: Message):
    text = (
        "Хранилище паролей:\n"
        "• «📂 Выбрать сервис» — открыть список сохранённых сервисов.\n"
        "• «➕ Добавить сервис» — добавить запись (сервис / логин / пароль).\n\n"
        "⚠️ Доступ: developer/head/lead."
    )
    await m.answer(text, reply_markup=_creds_main_kb().as_markup())

@router.callback_query(F.data == "pl:add_project")
async def pl_add_project(cq: CallbackQuery, state: FSMContext):
    await _remove_kb_safe(cq.message)
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
    if me["role"] not in ("head", "developer"):
        await cq.answer("Нет прав", show_alert=True); return

    await state.set_state(LinkProjectCreate.waiting_name)
    await cq.message.answer("Название проекта (кратко):")
    await cq.answer()

@router.message(LinkProjectCreate.waiting_name)
async def pl_add_project_apply(m: Message, state: FSMContext):
    name = (m.text or "").strip()
    if len(name) < 2:
        await m.answer("Слишком коротко. Введите название проекта."); return

    async with aiosqlite.connect(DB_PATH) as db:
        me = await ensure_user(db, m.from_user.id, m.from_user.full_name or "")
        try:
            await db.execute(
                "INSERT INTO projects(name, created_by_id) VALUES(?,?)",
                (name, me["id"])
            )
            await db.commit()
        except Exception:
            await m.answer("Такой проект уже есть или ошибка БД."); await state.clear(); return

    await state.clear()
    await m.answer(f"✅ Проект «{H(name)}» создан.")

def _pl_open_cb(pid: int) -> str:
    return f"pl:open:{pid}"

@router.callback_query(F.data == "pl:choose")
async def pl_choose(cq: CallbackQuery):
    # снимаем инлайн-клавиатуру у вызвавшего сообщения
    await _remove_kb_safe(cq.message)

    async with aiosqlite.connect(DB_PATH) as db:
        # показываем ТОЛЬКО те проекты, по которым уже есть ссылки
        cur = await db.execute("""
            SELECT p.id, p.name
            FROM projects p
            WHERE EXISTS (
                SELECT 1 FROM project_links l
                WHERE l.project_id = p.id
            )
            ORDER BY p.name COLLATE NOCASE
        """)
        rows = await cur.fetchall()

        me = await get_user_by_tg(db, cq.from_user.id)
        is_editor = me["role"] in ("head", "developer")

    if not rows:
        text = "Пока нет проектов со ссылками."
        if is_editor:
            text += "\nДобавьте через «➕ Добавить проект»."
        await cq.message.answer(text)
        await cq.answer()
        return

    kb = InlineKeyboardBuilder()
    for pid, name in rows:
        kb.button(text=name, callback_data=_pl_open_cb(pid))
    kb.adjust(1)

    await cq.message.answer("Выберите проект:", reply_markup=kb.as_markup())
    await cq.answer()

@router.callback_query(F.data.startswith("pl:open:"))
async def pl_open(cq: CallbackQuery):
    await _remove_kb_safe(cq.message)
    pid = int(cq.data.split(":")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT name FROM projects WHERE id=?", (pid,))
        r = await cur.fetchone()
        if not r:
            await cq.answer("Проект не найден", show_alert=True); return
        name = r[0]

        cur = await db.execute("""
            SELECT id, title, url
            FROM project_links
            WHERE project_id=?
            ORDER BY id DESC
        """, (pid,))
        links = await cur.fetchall()

        me = await get_user_by_tg(db, cq.from_user.id)
        is_editor = me["role"] in ("head", "developer")

    # вывод: «Имя проекта — <a href="...">краткое название</a>»
    if not links:
        text = f"Проект: <b>{H(name)}</b>\nСсылок пока нет."
    else:
        lines = [f"Проект: <b>{H(name)}</b>", "Ссылки:"]
        for _, title, url in links:
            lines.append(f"• <a href=\"{H(url)}\">{H(title)}</a>")
        text = "\n".join(lines)

    await cq.message.answer(text, parse_mode="HTML",
                            reply_markup=_project_menu_kb(pid, is_editor).as_markup())
    await cq.answer()

def _pl_add_link_cb(pid: int) -> str:
    return f"pl:add_link:{pid}"

@router.callback_query(F.data.startswith("pl:add_link:"))
async def pl_add_link_start(cq: CallbackQuery, state: FSMContext):
    await _remove_kb_safe(cq.message)
    parts = cq.data.split(":")
    pid = int(parts[2])

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
    if me["role"] not in ("head", "developer"):
        await cq.answer("Нет прав", show_alert=True); return

    await state.update_data(pl_project_id=pid)
    await state.set_state(LinkAdd.waiting_title)
    await cq.message.answer("Как назвать ссылку? (например: «Фигма дизайн»)")
    await cq.answer()

@router.message(LinkAdd.waiting_title)
async def pl_add_link_title(m: Message, state: FSMContext):
    title = (m.text or "").strip()
    if len(title) < 2:
        await m.answer("Слишком коротко. Напиши, как назвать ссылку."); return
    await state.update_data(pl_title=title)
    await state.set_state(LinkAdd.waiting_url)
    await m.answer("Вставь сам URL (начинается с http:// или https://).")

@router.message(LinkAdd.waiting_url)
async def pl_add_link_url(m: Message, state: FSMContext):
    url = (m.text or "").strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        await m.answer("Похоже, это не URL. Вставь ссылку целиком (http/https)."); return

    data = await state.get_data()
    pid = int(data["pl_project_id"])
    title = data["pl_title"]

    async with aiosqlite.connect(DB_PATH) as db:
        me = await ensure_user(db, m.from_user.id, m.from_user.full_name or "")
        await db.execute(
            "INSERT INTO project_links(project_id, title, url, created_by_id) VALUES(?,?,?,?)",
            (pid, title, url, me["id"])
        )
        await db.commit()

        # достанем имя проекта, чтобы показать карточку
        cur = await db.execute("SELECT name FROM projects WHERE id=?", (pid,))
        r = await cur.fetchone()
        name = r[0] if r else "проект"

    await state.clear()
    await m.answer(
        f"✅ Ссылка сохранена.\n"
        f"{H(name)} — <a href=\"{H(url)}\">{H(title)}</a>",
        parse_mode="HTML"
    )

@router.callback_query(F.data == "my_tasks")
async def cq_my_tasks(cq: CallbackQuery):
    await cmd_my_tasks(cq.message)
    await cq.answer()

@router.message(F.text == "🛠 Изменить статус задачи")
async def on_btn_change_status(m: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
        if not me or not me.get("is_active", 1):
            await m.answer("❌ Вы больше не активны в системе.")
            return
        cur = await db.execute("""
            SELECT id, description, deadline, status
            FROM tasks
            WHERE user_id=? AND status!='done'
            ORDER BY COALESCE(deadline,'9999') ASC, id DESC
        """, (me["id"],))
        rows = await cur.fetchall()

    if not rows:
        await m.answer("Нет активных задач.")
        return

    for tid, desc, dl, st in rows:
        text = task_line_html(tid, desc, st, dl)
        kb = InlineKeyboardBuilder()
        kb.button(text="⏳ Ожидает",    callback_data=f"task_setstatus:{tid}:new")
        kb.button(text="🛠 В процессе", callback_data=f"task_setstatus:{tid}:in_progress")
        kb.button(text="✅ Завершена",  callback_data=f"task_done:{tid}")
        kb.adjust(1)
        await m.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML")

# Кнопки меню руководителя

# =========================
# Меню руководителя: Проекты
# =========================
@router.message(F.text == "Проекты")
async def mgr_projects_menu(m: Message, state: FSMContext):
    """
    Корневое меню раздела «Проекты».
    Важно: всегда редактируем/перерисовываем одно сообщение, не плодим новые.
    """
    kb = InlineKeyboardBuilder()
    kb.button(text="📂 Выбрать проект",  callback_data="mgrp:choose")
    kb.button(text="➕ Добавить проект", callback_data="mgrp:add_project")
    kb.button(text="📝 Добавить задачу", callback_data="mgrp:add_task")
    kb.adjust(1)

    # Показываем/обновляем одно сообщение
    await m.answer("Раздел «Проекты». Выберите действие:", reply_markup=kb.as_markup(), parse_mode="HTML")

# --- helper: определить колонку с названием проекта в таблице projects ---
async def _projects_title_col(db) -> str:
    """
    Возвращает имя колонки, где хранится название проекта.
    Поддерживает разные схемы: 'title', 'name', 'project_name'.
    """
    cur = await db.execute("PRAGMA table_info(projects)")
    rows = await cur.fetchall()
    cols = {r[1] for r in rows}  # r[1] — имя колонки

    for c in ("title", "name", "project_name"):
        if c in cols:
            return c

    # На всякий случай — если схема экзотическая.
    # Вернём первую НЕ id колонку, чтобы хоть что-то вывести.
    for r in rows:
        if r[1] not in ("id",):
            return r[1]

    # Фоллбэк — вернём 'id' (не упадём, но текст будет «id»).
    return "id"

# --- выбор проекта из списка (устойчиво к разным схемам таблицы) ---
@router.callback_query(F.data == "mgrp:choose")
async def mgrp_choose_project(cq: CallbackQuery, state: FSMContext):
    try:
        await cq.answer()
    except Exception:
        pass

    async with aiosqlite.connect(DB_PATH) as db:
        # узнаём фактические колонки таблицы projects
        cur = await db.execute("PRAGMA table_info(projects)")
        cols = {row[1] for row in await cur.fetchall()}

        # колонка с названием проекта
        if "name" in cols:
            title_col = "name"
        elif "title" in cols:
            title_col = "title"
        elif "gs_sheet_name" in cols:
            title_col = "gs_sheet_name"
        else:
            title_col = "id"  # безопасный фоллбэк

        # фильтр по «архивности», если такая колонка вообще есть
        where_parts = []
        if "is_archived" in cols:
            where_parts.append("COALESCE(is_archived,0)=0")
        elif "archived" in cols:
            where_parts.append("COALESCE(archived,0)=0")
        where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""

        # сортировка — created_at, если есть; иначе по id
        order_col = "created_at" if "created_at" in cols else "id"

        sql = f"""
            SELECT id, {title_col}
            FROM projects
            {where_sql}
            ORDER BY {order_col} DESC
        """
        cur = await db.execute(sql)
        rows = await cur.fetchall()

    # если проектов нет — предлагаем создать
    if not rows:
        kb = InlineKeyboardBuilder()
        kb.button(text="➕ Добавить проект", callback_data="mgrp:add_project")
        kb.adjust(1)
        await cq.message.edit_text(
            "Активных проектов не найдено.\nСначала нажмите «➕ Добавить проект».",
            reply_markup=kb.as_markup(),
        )
        return

    # показываем список проектов
    kb = InlineKeyboardBuilder()
    for pid, title in rows:
        caption = str(title or f"Проект #{pid}")
        kb.button(text=caption, callback_data=f"mgrp:open:{pid}")
    kb.adjust(1)

    await cq.message.edit_text("Выберите проект:", reply_markup=kb.as_markup())

# Кнопка «назад» в корневое меню раздела «Проекты»
@router.callback_query(F.data == "mgrp:menu")
async def mgrp_back_to_menu(cq: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardBuilder()
    kb.button(text="📂 Выбрать проект",  callback_data="mgrp:choose")
    kb.button(text="➕ Добавить проект", callback_data="mgrp:add_project")
    kb.button(text="📝 Добавить задачу", callback_data="mgrp:add_task")
    kb.adjust(1)

    await cq.message.edit_text("Раздел «Проекты». Выберите действие:", reply_markup=kb.as_markup(), parse_mode="HTML")
    await cq.answer()


# --- открыть выбранный проект ---
@router.callback_query(F.data.startswith("mgrp:open:"))
async def mgrp_open_project(cq: CallbackQuery, state: FSMContext):
    try:
        pid = int(cq.data.split(":")[2])
    except Exception:
        await cq.answer("Ошибка формата данных", show_alert=True)
        return

    async with aiosqlite.connect(DB_PATH) as db:
        # берём имя по той же логике, что и в списке
        cur = await db.execute("PRAGMA table_info(projects)")
        cols = {row[1] for row in await cur.fetchall()}
        if "name" in cols:
            title_sql = "name"
        elif "title" in cols:
            title_sql = "title"
        elif "gs_sheet_name" in cols:
            title_sql = "gs_sheet_name"
        else:
            title_sql = "id"

        cur = await db.execute(f"SELECT {title_sql} FROM projects WHERE id=?", (pid,))
        row = await cur.fetchone()

    title = row[0] if row else f"Проект #{pid}"

    kb = InlineKeyboardBuilder()
    kb.button(text="📋 Список задач",   callback_data=f"mgrp:list:{pid}")
    kb.button(text="➕ Добавить задачу", callback_data=f"proj:plan_add:{pid}")
    kb.button(text="⬅️ К проектам",     callback_data="mgrp:choose")
    kb.adjust(1)

    await cq.message.edit_text(
        f"📁 Открыт проект: <b>{H(str(title))}</b>",
        reply_markup=kb.as_markup(),
        parse_mode="HTML",
    )
    await cq.answer()

@router.callback_query(F.data.startswith("mgrp:list:"))
async def mgrp_list_tasks(cq: CallbackQuery, state: FSMContext):
    """
    Список задач выбранного проекта в «карточном» стиле:
    - Проект
      «Название»
    - Активные задачи проекта:
      #1: <описание> | Открыта
      > Дедлайн: dd.mm.yyyy hh:mm
    - Завершённые задачи проекта:
      #2: <описание> | Завершена
      > Дедлайн: dd.mm.yyyy hh:mm
    """
    try:
        pid = int(cq.data.split(":")[2])
    except Exception:
        await cq.answer("Ошибка формата данных", show_alert=True)
        return

    # 1) Название проекта
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT name FROM projects WHERE id=?", (pid,))
        row = await cur.fetchone()
        project_name = row[0] if row else f"Проект #{pid}"

        # 2) Задачи проекта
        #   Берём минимум полей, сортировка: активные -> по дате; затем завершённые -> по дате
        cur = await db.execute(
            """
            SELECT id, task_text, status, planned_date
            FROM project_tasks
            WHERE project_id=?
            ORDER BY
                CASE WHEN status='done' THEN 1 ELSE 0 END ASC,
                CASE WHEN planned_date IS NULL THEN 1 ELSE 0 END,
                datetime(planned_date) ASC,
                id ASC
            """,
            (pid,),
        )
        rows = await cur.fetchall()

    # 3) Разделяем на активные/завершённые
    active, done = [], []
    for tid, text, st, dl in rows:
        (done if (st or "").lower() == "done" else active).append((tid, text, st, dl))

    def render_line(idx: int, text: str, st: str, dl_iso: str | None) -> str:
        st_h = "Завершена" if (st or "").lower() == "done" else "Открыта"
        head = f"<b>#{idx}:</b> {H(text or '')} | <u>{st_h}</u>"
        dl = f"\n<blockquote>🕘 Дедлайн: {fmt_dt_local(dl_iso)}</blockquote>" if dl_iso else ""
        return head + dl

    # 4) Собираем сообщение в «твоём» стиле
    parts: list[str] = []
    parts.append("<b>Проект</b>")
    parts.append(f"<blockquote>{H(project_name)}</blockquote>")

    parts.append("🗂 <b>Активные задачи проекта:</b>")
    if active:
        for i, (_, text, st, dl_iso) in enumerate(active, 1):
            parts.append(render_line(i, text, st, dl_iso))
    else:
        parts.append("— нет активных задач.")

    parts.append("\n✅ <b>Завершённые задачи проекта:</b>")
    if done:
        start_idx = 1  # нумерацию в каждом блоке начинаем с 1
        for i, (_, text, st, dl_iso) in enumerate(done, start_idx):
            parts.append(render_line(i, text, st, dl_iso))
    else:
        parts.append("— нет завершённых задач.")

    text_out = "\n".join(parts).replace("\n\n\n", "\n\n")

    # 5) Кнопки
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ К проекту", callback_data=f"mgrp:open:{pid}")
    kb.adjust(1)

    try:
        await cq.message.edit_text(text_out, reply_markup=kb.as_markup(), parse_mode="HTML")
    except Exception:
        # если исходного сообщения уже нет — шлём новое
        await cq.message.answer(text_out, reply_markup=kb.as_markup(), parse_mode="HTML")
    await cq.answer()

@router.callback_query(F.data == "mgrp:add_project")
async def mgrp_add_project(cq: CallbackQuery, state: FSMContext):
    # удалить сообщение-меню «Проекты»
    try:
        await cq.message.delete()
    except Exception:
        pass

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
    if me["role"] not in ("head", "developer"):
        await cq.answer("Нет доступа", show_alert=True)
        return

    # старт мастера создания проекта (как раньше)
    await state.set_state(BigProjectCreate.waiting_name)
    await cq.message.answer("Название проекта?")
    await cq.answer()


@router.callback_query(F.data == "mgrp:add_task")
async def mgrp_add_task(cq: CallbackQuery):
    # удалить сообщение-меню «Проекты»
    try:
        await cq.message.delete()
    except Exception:
        pass

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
        if me["role"] not in ("head", "developer"):
            await cq.answer("Нет доступа", show_alert=True)
            return

        # ВАЖНО: берём только проекты, у которых есть запись в project_meta
        cur = await db.execute("""
            SELECT p.id, p.name
            FROM projects p
            JOIN project_meta pm ON pm.project_id = p.id
            ORDER BY p.name COLLATE NOCASE
        """)
        rows = await cur.fetchall()

    if not rows:
        kb = InlineKeyboardBuilder()
        kb.button(text="➕ Создать проект", callback_data="mgrp:add_project")
        kb.adjust(1)
        await cq.message.answer("Проектов пока нет. Сначала создайте проект.", reply_markup=kb.as_markup())
        await cq.answer()
        return

    kb = InlineKeyboardBuilder()
    for pid, name in rows:
        kb.button(text=(name or f"#{pid}")[:60], callback_data=f"proj:plan_add:{pid}")
    kb.adjust(1)

    await cq.message.answer("Выберите проект, к которому добавить задачу:", reply_markup=kb.as_markup())
    await cq.answer()

# Назначить задачу (reply-кнопка)
@router.message(F.text == "👤 Назначить задачу")
async def mgr_assign_reply(m: Message, state: FSMContext):
    await state.set_state(AssignPick.picking_user)
    await show_user_picker(m, 0, for_tg_id=m.from_user.id)

# Сводка по сотруднику
@router.message(F.text == "📊 Сводка по сотруднику")
async def mgr_summary_reply(m: Message):
    await show_user_picker_summary(m, 0, for_tg_id=m.from_user.id)

@router.message(F.text == "🏷 Определить отдел")
async def dept_assign_start(m: Message, state: FSMContext):
    # Доступ только head/developer
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
    if me["role"] not in ("head", "developer"):
        await m.answer("Нет доступа"); return

    await state.set_state(DeptAssign.picking_user)
    await show_user_picker_dept(m, 0, for_tg_id=m.from_user.id)

# Мои подчинённые (тот же вывод, что и callback)
@router.message(F.text == "👥 Мои подчинённые")
async def mgr_team_reply(m: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
        if me["role"] not in ("lead","head","developer"):
            await m.answer("Нет доступа"); return

        cur = await db.execute("""
          SELECT u.full_name, u.tg_id
          FROM users u
          WHERE u.is_active=1 AND u.role='employee' AND COALESCE(u.dept,'') = COALESCE(?, '')
          ORDER BY u.full_name COLLATE NOCASE
        """, (me.get("dept") or "",))
        rows = await cur.fetchall()

    dept = me.get("dept") or "—"
    if not rows:
        await m.answer(f"{Q('Отдел ' + dept)}\nТвоих подчинённых пока нет.", parse_mode="HTML")
    else:
        lines = [Q("Отдел " + dept), "Твои подчинённые:", ""]
        for i, (full_name, tg_id) in enumerate(rows, start=1):
            name = full_name or f"user_{tg_id}"
            lines.append(f"{i}. {name}")
        await m.answer("\n".join(lines), parse_mode="HTML")

# Руководители
@router.message(F.text == "📒 Руководители")
async def mgr_leads_reply(m: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
        if me["role"] not in ("head","developer"):
            await m.answer("Нет доступа"); return
        cur = await db.execute("SELECT full_name, tg_id FROM users WHERE role='lead' ORDER BY full_name")
        rows = await cur.fetchall()
    if not rows:
        await m.answer("Линейных руководителей пока нет.")
    else:
        text = "Линейные руководители:\n" + "\n".join([f"• {r[0]} (tg_id: {r[1]})" for r in rows])
        await m.answer(text)

# Назначить роль
@router.message(F.text == "🛠 Назначить роль")
async def mgr_setrole_reply(m: Message, state: FSMContext):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
    if me["role"] not in ("head", "developer"):
        await m.answer("Нет доступа")
        return

    await state.set_state(SetRoleState.waiting)
    await m.answer(
        "Введи: <code>&lt;tg_id&gt; &lt;role&gt;</code> где role: "
        "<code>employee</code>|<code>lead</code>|<code>head</code>\n"
        "Например: <code>123456789 lead</code>"
    )

# Связать иерархию
@router.message(F.text == "🔗 Связать иерархию")
async def mgr_link_reply(m: Message, state: FSMContext):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
    if me["role"] not in ("head", "developer"):
        await m.answer("Нет доступа")
        return

    await state.set_state(LinkState.waiting)
    await m.answer(
        "Введи: <code>&lt;manager_tg_id&gt; &lt;subordinate_tg_id&gt;</code>\n"
        "Например: <code>111111111 222222222</code>"
    )

# Запросить план
@router.message(F.text == "📨 Запросить план")
async def mgr_plan_req_reply(m: Message):
    await show_user_picker_planreq(m, 0, for_tg_id=m.from_user.id)

# Назад к главному меню
@router.message(F.text == "⬅️ В главное меню")
async def back_to_main_menu(m: Message):
    # вернём обычное главное меню
    await m.answer("Главное меню:", reply_markup=main_menu_kb())

import re
TIME_RE = re.compile(r"\b([01]\d|2[0-3]):([0-5]\d)\b")  # HH:MM

@router.message(F.reply_to_message)
async def handle_daily_plan_item(m: Message):
    """
    Если пользователь отвечает РЕПЛАЕМ на утреннее сообщение, принимаем пункт плана.
    Требуем наличие времени HH:MM. Иначе — просим отправить заново.
    """
    if not (m.text and m.text.strip()):
        return

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
        if not me:
            return

        # сверяем, что реплай именно на «утреннее» сообщение
        cur = await db.execute("SELECT last_plan_msg_id, last_plan_date FROM users WHERE id=?", (me["id"],))
        row = await cur.fetchone()
        if not row:
            return
        last_plan_msg_id, last_plan_date = row
        if not last_plan_msg_id or not last_plan_date:
            return
        if m.reply_to_message.message_id != last_plan_msg_id:
            # это реплай не к утреннему, отдадим дальше другим хэндлерам (например, отчёт по напоминанию)
            return

        # валидируем время
        txt = m.text.strip()
        mt = TIME_RE.search(txt)
        if not mt:
            await m.answer(
                "❌ Не принял пункт: в сообщении нет времени в формате `HH:MM`.\n"
                "Пример: `Сдать обложку в 15:45`.\n"
                "Отправьте пункт снова, ОБЯЗАТЕЛЬНО отвечая реплаем на моё утреннее сообщение.",
                parse_mode="Markdown"
            )
            return

        hhmm = mt.group(0)

        # сохраняем пункт
        await db.execute(
            "INSERT INTO daily_plan_items(user_id, plan_date, text, time_str) VALUES(?,?,?,?)",
            (me["id"], last_plan_date, txt, hhmm)
        )
        await db.commit()

    await m.answer(f"✅ Принято: {txt}\n(время {hhmm})")

# Фолбэк: принимать пункт плана даже без reply,
# ТОЛЬКО когда нет активного состояния FSM
@router.message(StateFilter(None), F.text & ~F.text.startswith("/"))
async def handle_daily_plan_item_fallback(m: Message):
    txt = (m.text or "").strip()
    if not txt:
        return

    mt = TIME_RE.search(txt)
    if not mt:
        return  # это не пункт плана

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
        if not me:
            return

        # активна ли «сессия плана»?
        cur = await db.execute("SELECT last_plan_msg_id, last_plan_date FROM users WHERE id=?", (me["id"],))
        row = await cur.fetchone()
        if not row:
            return
        last_plan_msg_id, last_plan_date = row
        if not last_plan_msg_id or not last_plan_date:
            return  # сессии нет → игнор

        # сохраняем пункт
        hhmm = mt.group(0)
        await db.execute(
            "INSERT INTO daily_plan_items(user_id, plan_date, text, time_str) VALUES(?,?,?,?)",
            (me["id"], last_plan_date, txt, hhmm)
        )
        await db.commit()

    await m.answer(f"✅ Принято: {txt}\n(время {hhmm})")

@router.message(F.reply_to_message)
async def handle_report_reply(m: Message):
    """
    Если пользователь ответил реплаем на напоминание/просрочку,
    отправляем его текст руководителям как отчёт по задаче.
    """
    # Нужен reply_to_message и непустой текст
    if not m.reply_to_message or not (m.text and m.text.strip()):
        return

    user_tg = m.from_user.id
    reply_msg_id = m.reply_to_message.message_id
    report_text = m.text.strip()

    # Найти задачу, для которой это напоминание было отправлено
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, user_tg)
        if not me:
            return

        cur = await db.execute("""
            SELECT id, description, user_id, deadline, status
            FROM tasks
            WHERE user_id=? AND last_reminder_msg_id=?
              AND status!='done'
            LIMIT 1
        """, (me["id"], reply_msg_id))
        row = await cur.fetchone()

        if not row:
            # Ничего не нашли — возможно, ответили не на то сообщение
            await m.answer("Не удалось сопоставить ответ с задачей. Ответьте прямо на сообщение-напоминание (реплаем).")
            return

        task_id, desc, user_id, deadline, status = row
        managers = await get_manager_tg_ids(db, user_id)

        # По желанию можно «очистить» last_reminder_msg_id, чтобы ответ приняли только один раз
        await db.execute("UPDATE tasks SET last_reminder_msg_id=NULL, updated_at=? WHERE id=?",
                         (datetime.now(UTC).isoformat(), task_id))
        await db.commit()

    # Сообщение сотруднику
    await m.answer("✅ Принял отчёт, отправляю руководителям.")

    # Шлём руководителям
    if managers:
        try:
            me_name = m.from_user.full_name or f"user_{user_tg}"
        except Exception:
            me_name = f"user_{user_tg}"

        text_mgr = (
            f"📝 Отчёт по задаче #{task_id} от {me_name} (tg_id: {user_tg}):\n"
            f"{desc}\n"
            f"Дедлайн: {fmt_dt_local(deadline)}\n"
            f"Текущий статус: {status}\n\n"
            f"Ответ: {report_text}"
        )
        for mid in managers:
            try:
                await bot.send_message(mid, text_mgr)
            except Exception as e:
                logging.warning(f"notify manager failed (report reply) tg_id={mid}: {e}")

@router.message(TaskForm.waiting_desc)
async def form_desc(m: Message, state: FSMContext):
    await state.update_data(description=(m.text or "").strip(), add_chat_id=m.chat.id)
    await state.set_state(TaskForm.waiting_deadline)

    txt = (
        "Укажите дедлайн (можно по-простому):\n"
        "• 10:00\n"
        "• в 19:00\n"
        "• завтра в 10:00\n"
        "• через 20 минут\n"
        "• 30.09 в 11\n"
    )

    data = await state.get_data()
    msg_id = data.get("add_msg_id")
    chat_id = data.get("add_chat_id") or m.chat.id

    if msg_id:
        try:
            await bot.edit_message_text(
                text=txt,
                chat_id=chat_id,
                message_id=msg_id,
                parse_mode="HTML"
            )
            return
        except Exception:
            pass

    # fallback — если редактирование не удалось
    msg = await m.answer(txt)
    await state.update_data(add_msg_id=msg.message_id)

@router.message(TaskForm.waiting_deadline)
async def form_deadline(m: Message, state: FSMContext):
    text = (m.text or "").strip()
    dt_utc = parse_human_time(text)
    if not dt_utc:
        await m.answer(
            "❌ Не удалось понять время. Время должно быть в будущем.\n\n"
            "Примеры: `21:43`, `2143`, `в 19`, `завтра в 10:00`, `через 20 минут`, `30.09 в 11`."
        )
        return

    data = await state.get_data()
    desc = data["description"]
    now = datetime.now(UTC)

    async with aiosqlite.connect(DB_PATH) as db:
        user = await ensure_user(db, m.from_user.id, m.from_user.full_name or "")
        # ВАЖНО: не часовой пинг, а один-единственный триггер в момент дедлайна
        next_rem = dt_utc.isoformat()

        cur = await db.execute("""
            INSERT INTO tasks(user_id, description, deadline, status, next_reminder_at,
                              assigned_by_user_id, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?,?)
        """, (user["id"], desc, dt_utc.isoformat(), 'new', next_rem, None, now.isoformat(), now.isoformat()))
        await db.commit()
        task_id = cur.lastrowid
        await log_task_event(db, task_id, "create", meta=f"deadline={dt_utc.isoformat()}")

        manager_tg_ids = await get_manager_tg_ids(db, user["id"])
        if manager_tg_ids:
            text_mgr = (
                f"🆕 Новая задача у {H(user['full_name'])}:\n"
                f"#{task_id} — <b>{H(desc)}</b>\n"
                f"{Q('Дедлайн: ' + fmt_dt_local(dt_utc.isoformat()))}"
            )
            for mid in manager_tg_ids:
                try:
                    await bot.send_message(mid, text_mgr, parse_mode="HTML")
                except Exception as e:
                    logging.warning(f"notify manager failed (create) tg_id={mid}: {e}")

        summary = await active_tasks_summary(db, user["id"])

    # === финальный вывод одной карточки в том же сообщении ===
    # собираем клавиатуру под карточкой
    kb = await build_task_buttons(task_id)

    # если у пользователя нет начатых задач — добавим «Выбрать другую»
    try:
        async with aiosqlite.connect(DB_PATH) as db2:
            has_active = await user_has_active_task(db2, user["id"])
    except Exception:
        has_active = True  # безопасно

    if not has_active:
        kb.button(text="📋 Выбрать другую", callback_data="my_tasks")
    kb.adjust(1)

    final_text = "Задача добавлена ✅\n\n" + _format_task_line(task_id, desc, 'new', dt_utc.isoformat())

    data = await state.get_data()
    msg_id = data.get("add_msg_id")
    chat_id = data.get("add_chat_id") or m.chat.id
    await state.clear()

    # редактируем исходный промпт → финальная карточка
    try:
        if msg_id:
            await bot.edit_message_text(
                text=final_text,
                chat_id=chat_id,
                message_id=msg_id,
                reply_markup=kb.as_markup(),
                parse_mode="HTML",
            )
        else:
            await m.answer(final_text, reply_markup=kb.as_markup(), parse_mode="HTML")
    except Exception:
        await m.answer(final_text, reply_markup=kb.as_markup(), parse_mode="HTML")

# =========================
# Мои задачи
# =========================
async def build_task_buttons(task_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT status, started_at FROM tasks WHERE id=?", (task_id,))
        row = await cur.fetchone()

    status, started_at = (row or ('new', None))

    kb = InlineKeyboardBuilder()
    if not started_at:  # ещё не стартовали
        kb.button(text="🚀 Начать задачу", callback_data=f"task_start_now:{task_id}")
        # перенос срока доступен всегда
        kb.button(text="🕒 Сдвинуть срок", callback_data=f"task_extend:{task_id}")
    else:               # уже в работе
        kb.button(text="✅ Завершить задачу", callback_data=f"task_done:{task_id}")
        kb.button(text="🕒 Сдвинуть срок", callback_data=f"task_extend:{task_id}")

    kb.adjust(1)
    return kb

@router.message(Command("my"))
async def cmd_my(m: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        user = await ensure_user(db, m.from_user.id, m.from_user.full_name or "")
        if not user or not user.get("is_active", 1):
            await m.answer("❌ Вы больше не активны в системе. Обратитесь к руководителю.")
            return
        cur = await db.execute("""
          SELECT id, description, deadline, status, last_postpone_reason, planned_start_at
          FROM tasks WHERE user_id=? AND status!='done'
          ORDER BY COALESCE(deadline, '9999') ASC, id DESC
        """, (user["id"],))
        rows = await cur.fetchall()

    if not rows:
        await m.answer("Активных задач нет. Используйте «➕ Добавить задачу».")
        return

    for (tid, desc, deadline, status, reason, planned_start_at) in rows:
        # карточка задачи
        text = render_task_card(tid, desc, status, deadline)

        # КНОПКИ: начать / завершить / сдвинуть срок
        kb = InlineKeyboardBuilder()
        if status != "in_progress":
            kb.button(text="🚀 Начать задачу", callback_data=f"task_start_now:{tid}")
        else:
            kb.button(text="✅ Завершить задачу", callback_data=f"task_done:{tid}")
        kb.button(text="⏱️ Сдвинуть срок", callback_data=f"task_extend:{tid}")
        kb.adjust(1)
        
        await m.answer(
            text,
            reply_markup=kb.as_markup(),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

# =========================
# Кнопки задач: старт/готово/перенос/статус
# =========================

@router.callback_query(F.data == "admin:reset")
async def admin_reset_prompt(cq: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
    if me["role"] != "developer":
        await cq.answer("Нет доступа", show_alert=True); return
    kb = InlineKeyboardBuilder()
    kb.button(text="⚠️ Подтвердить полный сброс", callback_data="admin:reset_confirm")
    kb.button(text="Отмена", callback_data="admin:reset_cancel")
    kb.adjust(1)
    await cq.message.answer("ВНИМАНИЕ: Полный сброс удалит всех пользователей, связи и задачи. Продолжить?", reply_markup=kb.as_markup())
    await cq.answer()

@router.callback_query(F.data == "admin:reset_cancel")
async def admin_reset_cancel(cq: CallbackQuery):
    await cq.answer("Отменено")
    await cq.message.edit_text("Сброс отменён.")

@router.callback_query(F.data == "admin:reset_confirm")
async def admin_reset_confirm(cq: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
        if me["role"] != "developer":
            await cq.answer("Нет доступа", show_alert=True); return

        # Удаляем все данные, кроме текущего разработчика
        # 1) tasks
        await db.execute("DELETE FROM tasks")
        # 2) связи
        await db.execute("DELETE FROM manager_links")
        # 3) элементы планов
        await db.execute("DELETE FROM daily_plan_items")
        # 4) пользователи, кроме разработчика
        await db.execute("DELETE FROM users WHERE tg_id != ?", (me["tg_id"],))
        # 5) почистим поля-пометки
        await db.execute("UPDATE users SET last_plan_msg_id=NULL, last_plan_date=NULL WHERE id=?", (me["id"],))
        if DEVELOPER_TG_ID:
            await db.execute("UPDATE users SET is_active=CASE WHEN tg_id=? THEN 1 ELSE 0 END, registered=CASE WHEN tg_id=? THEN 1 ELSE 0 END, role=CASE WHEN tg_id=? THEN 'developer' ELSE role END",
                             (DEVELOPER_TG_ID, DEVELOPER_TG_ID, DEVELOPER_TG_ID))
        else:
            await db.execute("UPDATE users SET is_active=0, registered=0")
        await db.commit()

    await cq.message.edit_text("✅ Полный сброс выполнен. В системе остался только Developer.")
    await cq.answer("Сброшено")

@router.callback_query(F.data.startswith("plan_done:"))
async def cb_plan_done(cq: CallbackQuery):
    plan_date = cq.data.split(":")[1]  # YYYY-MM-DD

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
        if not me:
            await cq.answer(); return

        # заберём пункты плана
        cur = await db.execute("""
            SELECT text, time_str
            FROM daily_plan_items
            WHERE user_id=? AND plan_date=?
            ORDER BY time_str ASC, id ASC
        """, (me["id"], plan_date))
        rows = await cur.fetchall()

        # закрываем «сессию плана»
        await db.execute("UPDATE users SET last_plan_msg_id=NULL, last_plan_date=NULL WHERE id=?", (me["id"],))
        await db.commit()

    if not rows:
        await disable_kb_and_optionally_edit(cq.message, "План закрыт, но пунктов не найдено.")
        await cq.answer("Нет пунктов плана."); 
        return

    # Сводка сотруднику
    lines = [f"🗓 План на {plan_date}:"]
    for txt, hhmm in rows:
        lines.append(f"• {hhmm} — {txt}")
    plan_text = "\n".join(lines)

    await disable_kb_and_optionally_edit(cq.message, "План закрыт ✅")
    await cq.message.answer(plan_text)
    await cq.answer("Отправляю руководителям.")

    # Руководителям
    async with aiosqlite.connect(DB_PATH) as db:
        mgrs = await get_manager_tg_ids(db, me["id"])
    if mgrs:
        hdr = f"📬 План {me['full_name']} (tg_id: {me['tg_id']}) на {plan_date}:\n"
        for mid in mgrs:
            try:
                await bot.send_message(mid, hdr + plan_text)
            except Exception as e:
                logging.warning(f"notify manager failed (daily plan) tg_id={mid}: {e}")

# === НАЧАТЬ РАБОТУ ПО ЗАДАЧЕ ===============================================
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

@router.callback_query(F.data.startswith("task_start_now:"))
async def cb_task_start_now(cq: CallbackQuery):
    """
    Кнопка ▶️ «начать сейчас»: меняем статус и РЕДАКТИРУЕМ текущее сообщение.
    """
    rid = int(cq.data.split(":")[1])

    async with aiosqlite.connect(DB_PATH) as db:
        me = await ensure_user(db, cq.from_user.id, cq.from_user.full_name or "")
        now = datetime.now(UTC).isoformat()

        await db.execute("""
            UPDATE tasks
               SET status='in_progress',
                   started_at=COALESCE(started_at, ?),
                   updated_at=?
             WHERE id=? AND user_id=?
        """, (now, now, rid, me["id"]))
        await db.commit()
        await log_task_event(db, rid, "status", meta="in_progress")

        cur = await db.execute("SELECT description, deadline FROM tasks WHERE id=?", (rid,))
        row = await cur.fetchone()

    desc = (row[0] if row else "") or ""
    dl   =  row[1] if row else None

    text = "Статус обновлён: 🚀 Начал работу.\n\n" + _format_task_line(rid, desc, "in_progress", dl)
    kb = await build_task_buttons(rid)
    kb.adjust(1)

    try:
        await cq.message.edit_text(text, parse_mode="HTML", reply_markup=kb.as_markup())
    except Exception:
        await disable_kb_and_optionally_edit(cq.message, text, parse_mode="HTML")

    await cq.answer()

@router.callback_query(F.data.startswith("task_done:"))
async def cb_task_done(cq: CallbackQuery):
    """
    ✅ Завершение: помечаем как done и РЕДАКТИРУЕМ текущее сообщение карточки
    на зелёный блок «Задача выполнена». Никаких новых сообщений.
    """
    task_id = int(cq.data.split(":")[1])

    async with aiosqlite.connect(DB_PATH) as db:
        me = await ensure_user(db, cq.from_user.id, cq.from_user.full_name or "")
        # достанем описание/дедлайн до апдейта — нужно для текста
        cur = await db.execute("SELECT description, deadline, last_reminder_msg_id FROM tasks WHERE id=?", (task_id,))
        r = await cur.fetchone()
        desc = (r[0] if r else "") or ""
        dl   =  r[1] if r else None
        last_rem_msg_id = r[2] if r else None

        # посчитаем просрочку (в минутах)
        delay_min = 0
        try:
            if dl:
                from math import floor
                dl_dt = dateparser.parse(dl)
                diff  = (datetime.now(UTC) - dl_dt).total_seconds()
                delay_min = max(0, floor(diff / 60))
        except Exception:
            pass

        now = datetime.now(UTC).isoformat()
        await db.execute("""
            UPDATE tasks
               SET status='done',
                   updated_at=?,
                   completed_at=?,
                   completed_by_user_id=?,
                   delay_minutes=?
             WHERE id=?
        """, (now, now, me["id"], delay_min, task_id))
        await db.commit()
        await log_task_event(db, task_id, "done")

    # Удалим возможное «сообщение о просрочке»
    if last_rem_msg_id:
        try:
            await bot.delete_message(chat_id=cq.message.chat.id, message_id=last_rem_msg_id)
        except Exception:
            pass

    # Редактируем текущую карточку на «выполнено»
    done_text = f"✅ «{H(desc or 'Задача')}»\n<u>Отмечена как выполненная.</u>"
    try:
        await cq.message.edit_text(done_text, parse_mode="HTML")
    except Exception:
        await disable_kb_and_optionally_edit(cq.message, done_text, parse_mode="HTML")

    await cq.answer()

def _next_later_cb() -> str:
    return "next_later"

@router.callback_query(F.data == "next_later")
async def cb_next_later(cq: CallbackQuery):
    # снимаем клавиатуру именно у того сообщения, где нажали кнопку
    await _remove_kb_safe(cq.message)
    await cq.answer("Ок, вернёмся к выбору позже.")
    await bot.send_message(cq.from_user.id, "🕗 Ок, вернёмся к выбору позже.")

from aiogram.utils.keyboard import InlineKeyboardBuilder

async def prompt_next_task_for_user(user_tg_id: int, chat_id: int, force: bool = False):
    """
    Показывает сотруднику список незавершённых задач (отсортированных по дедлайну)
    и кнопки «▶️ #id Название». Есть «🕗 Выберу позже».
    Если force=False и у пользователя есть задача в работе — ничего не шлём.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        u = await get_user_by_tg(db, user_tg_id)
        if not u:
            return

        if not force:
            cur = await db.execute("SELECT COUNT(1) FROM tasks WHERE user_id=? AND status='in_progress'", (u["id"],))
            if (await cur.fetchone())[0] > 0:
                return  # уже что-то в работе — не отвлекаем

        cur = await db.execute("""
            SELECT id, description, deadline
            FROM tasks
            WHERE user_id=? AND status!='done'
            ORDER BY COALESCE(deadline,'9999') ASC, id DESC
            LIMIT 10
        """, (u["id"],))
        rows = await cur.fetchall()

    if not rows:
        await bot.send_message(chat_id, "✅ Все задачи закрыты. Отличная работа!")
        return

    # В нашем стиле: без номеров, с цитатой «Дедлайн: …»
    lines = ["Какой задачей займёмся следующей?"]
    kb = InlineKeyboardBuilder()

    for tid, desc, dl in rows:
        title = H(desc or "Задача")
        dl_line = f"<blockquote><b>Дедлайн:</b> {fmt_dt_local(dl)}</blockquote>" if dl else ""
        # строка списка — без #id
        lines.append(f"• <b>{title}</b>\n{dl_line}")
        short = (desc or "Задача")[:40]
        kb.button(text=f"▶️ {short}", callback_data=f"task_start_now:{tid}")

    kb.button(text="⏸ Выберу позже", callback_data=_next_later_cb())
    kb.adjust(1)

    await bot.send_message(
        chat_id,
        "\n".join(lines),
        reply_markup=kb.as_markup(),
        parse_mode="HTML",
    )

@router.callback_query(F.data.startswith("task_extend:"))
async def cb_task_extend(cq: CallbackQuery, state: FSMContext):
    """Обновление дедлайна задачи + запись причины и события."""
    task_id = int(cq.data.split(":")[1])
    await state.update_data(task_id=task_id)
    await state.update_data(
        overdue_msg_id=cq.message.message_id,
        overdue_chat_id=cq.message.chat.id,
    )
    await cq.message.answer("✏️ Укажите причину переноса дедлайна:")
    await state.set_state(ExtendReason.waiting_for_reason)
    await cq.answer()


@router.message(ExtendReason.waiting_for_reason)
async def extend_reason_entered(m: Message, state: FSMContext):
    try:
        await bot.delete_message(m.chat.id, m.message_id)
    except Exception:
        pass

    """Пользователь указал причину переноса — теперь просим новое время."""
    reason = (m.text or "").strip()
    await state.update_data(reason=reason)
    await m.answer(
        "🕒 Укажите новый дедлайн (например: `2025-09-22 18:00` или `завтра 10:00`).",
        parse_mode="Markdown",
    )
    await state.set_state(ExtendReason.waiting_for_datetime)


@router.message(ExtendReason.waiting_for_datetime)
async def extend_datetime_entered(m: Message, state: FSMContext):
    """Пользователь указал новое время дедлайна."""
    raw = (m.text or "").strip()
    if not raw:
        await m.answer("⚠️ Нужно указать время (например: `сегодня в 19:00`).")
        return

    try:
        import dateparser
        from datetime import datetime, UTC, timedelta
        dt_utc = dateparser.parse(raw, settings={"TIMEZONE": "UTC"})
        if not dt_utc or dt_utc <= datetime.now(UTC):
            raise ValueError
    except Exception:
        await m.answer(
            "❌ Не удалось понять время. Пример: `завтра в 10:00`, `30.09 в 11:00`.",
            parse_mode="Markdown",
        )
        return

    data = await state.get_data()
    task_id = data.get("task_id")
    reason = data.get("reason")
    now = datetime.now(UTC)
    new_next = dt_utc.isoformat()
    old_dl = None

    async with aiosqlite.connect(DB_PATH) as db:
        # получаем старый дедлайн
        cur_old = await db.execute("SELECT deadline FROM tasks WHERE id=?", (task_id,))
        row = await cur_old.fetchone()
        if row:
            old_dl = row[0]

        # обновляем задачу
        await db.execute(
            """
            UPDATE tasks
            SET deadline=?,
                updated_at=?,
                next_reminder_at=?,
                last_postpone_reason=?
            WHERE id=?
            """,
            (dt_utc.isoformat(), now.isoformat(), new_next, reason, task_id),
        )

        # записываем событие в журнал
        try:
            await db.execute(
                """
                INSERT INTO task_events(task_id, event, meta)
                VALUES (?, ?, ?)
                """,
                (
                    task_id,
                    "postpone",
                    f"old={old_dl or 'None'}; new={dt_utc.isoformat()}; reason={reason or ''}",
                ),
            )
        except Exception as e:
            import logging
            logging.warning(f"task_events insert failed: {e}")

        await db.commit()

    await state.clear()

    await m.answer(
        f"✅ Дедлайн обновлён.\n\n"
        f"<b>Новый дедлайн:</b> {dt_utc.strftime('%d.%m.%Y %H:%M')}\n"
        f"<b>Причина:</b> {reason or '—'}",
        parse_mode="HTML",
    )

@router.message(ExtendReason.waiting_for_reason)
async def get_postpone_reason(msg: Message, state: FSMContext):
    reason = msg.text.strip()
    if len(reason) < 3:
        await msg.answer("Слишком коротко. Опишите причину переноса чуть подробнее.")
        return
    await state.update_data(reason=reason)
    await state.set_state(ExtendDeadline.waiting_for_deadline)
    await msg.answer("Укажите новый дедлайн (например: `2025-09-22 18:00` или `завтра 10:00`).")

# --- Кнопка «перенести N минут/час» в просрочке
@router.callback_query(F.data.startswith("overdue_snooze:"))
async def cb_overdue_snooze(cq: CallbackQuery):
    # формат: overdue_snooze:<task_id>:<minutes>
    _, tid, mins = (cq.data or "").split(":")
    task_id, minutes = int(tid), int(mins)

    next_at_utc = datetime.now(UTC) + timedelta(minutes=minutes)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE tasks SET next_reminder_at=?, updated_at=? WHERE id=?",
            (next_at_utc.isoformat(), datetime.now(UTC).isoformat(), task_id)
        )
        await db.commit()

    await cq.answer(f"Напомню через {minutes} мин.")
    # при желании можно убрать клавиатуру у старого сообщения:
    try:
        await cq.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

def _overdue_enter_time_cb(task_id: int) -> str:
    return f"overdue_enter_time:{task_id}"

@router.callback_query(F.data.startswith("overdue_enter_time:"))
async def overdue_enter_time(cq: CallbackQuery, state: FSMContext):
    parts = (cq.data or "").split(":")
    if len(parts) != 2 or not parts[1].isdigit():
        await cq.answer("Некорректный запрос.", show_alert=True)
        return
    task_id = int(parts[1])

    # Запоминаем в FSM
    await state.update_data(task_id=task_id)
    await state.set_state(OverdueForm.waiting_time)

    # Зачищаем кнопки у исходного сообщения, чтобы не было дублей
    await disable_kb_and_optionally_edit(
        cq.message,
        "➡️ Перенос дедлайна — опишите причину ниже."
    )

    await cq.message.answer(
        "Укажите новый дедлайн (например: `2025-09-22 18:00`  или `завтра 10:00`)."
    )
    await cq.answer()

@router.message(OverdueForm.waiting_time)
async def overdue_set_custom_time(m: Message, state: FSMContext):
    data = await state.get_data()
    task_id = data.get("task_id")
    if not task_id:
        # на всякий случай, чтобы не ловить KeyError
        await state.clear()
        await m.answer("Сессия переноса потеряна. Повторите действие с кнопки на последнем уведомлении.")
        return

    dt_utc = parse_human_time((m.text or "").strip())
    if not dt_utc:
        await m.answer(
            "❌ Не удалось понять время. Пожалуйста, попробуйте снова.\n"
            "Примеры: `в 19`, `завтра`, `через 20 минут`, `30.09 в 11:00`."
        )
        return

    next_at = next_reminder_after(dt_utc.isoformat())

    async with aiosqlite.connect(DB_PATH) as db:
        # 0) прочитаем старый дедлайн
        cur_old = await db.execute("SELECT deadline FROM tasks WHERE id=?", (task_id,))
        r_old = await cur_old.fetchone()
        old_dl = r_old[0] if r_old else None

        # --- подготовим значения для апдейта ---
        # текущий момент
        now_iso = datetime.now(UTC).isoformat()

        # когда присылать следующее напоминание:
        # ставим на новый дедлайн (или ближайшее рабочее время — внутри helper-а)
        new_next = next_reminder_after(dt_utc.isoformat())

        # причина переноса (если собирали её через FSM раньше)
        reason = ""
        try:
            data = await state.get_data()              # если хэндлер с FSMContext
            reason = (data.get("extend_reason") or data.get("reason") or "").strip()
        except Exception:
            # если state недоступен — оставим пустую причину
            pass

        # --- 1) применим обновление в БД ---
        await db.execute(
            """
            UPDATE tasks
            SET
                deadline            = ?,
                updated_at          = ?,
                next_reminder_at    = ?,
                last_postpone_reason= ?
            WHERE id = ?
            """,
            (dt_utc.isoformat(), now_iso, new_next, reason, task_id),
        )
        await db.commit()

        # если меняли deadline — записываем событие
        try:
            async with aiosqlite.connect(DB_PATH) as db2:
                cur_old = await db2.execute("SELECT deadline FROM tasks WHERE id=?", (task_id,))
                r_old = await cur_old.fetchone()
                old_dl = r_old[0] if r_old else None
                await log_task_event(db2, task_id, "postpone", meta=f"old={old_dl}; new={dt_utc.isoformat()}")
                await db2.commit()
        except Exception:
            pass

        # 2) журнал
        await log_task_event(
            db, task_id, "postpone",
            meta=f"old={old_dl}; new={dt_utc.isoformat()}; reason={reason}"
        )
        await db.commit()

    # обновляем исходную карточку просрочки вместо нового сообщения
    chat_id = data.get("overdue_chat_id")
    msg_id = data.get("overdue_msg_id")
    info = f"🔔 Напоминание перенесено на {fmt_dt_local(next_at)}"
    if chat_id and msg_id:
        await _refresh_overdue_card(db, chat_id, msg_id, task_id, info)
    else:
        await m.answer(info)
    await state.clear()

# --- «Ввести время» — спрашиваем у пользователя своё время
class SnoozeCustom(StatesGroup):
    waiting_time = State()

@router.callback_query(F.data.startswith("overdue_custom:"))
async def cb_overdue_custom(cq: CallbackQuery, state: FSMContext):
    # формат: overdue_custom:<task_id>
    _, tid = (cq.data or "").split(":")
    task_id = int(tid)

    # записываем task_id в FSM и переводим в состояние ввода
    await state.set_state(SnoozeCustom.waiting_time)
    await state.update_data(
        task_id=task_id,
        overdue_msg_id=cq.message.message_id,
        overdue_chat_id=cq.message.chat.id,
    )

    await cq.message.answer(
        "Введите новое время напоминания (например: 21:43, завтра 10:00, через 20 минут)."
    )
    await cq.answer()

@router.message(SnoozeCustom.waiting_time)
async def cb_overdue_custom_apply(m: Message, state: FSMContext):
    data = await state.get_data()
    task_id = data.get("task_id")
    if not task_id:
        await m.answer("Сессия истекла. Попробуйте снова."); await state.clear(); return

    dt_utc = parse_human_time(m.text.strip())
    if not dt_utc:
        await m.answer(
            "❌ Не удалось понять время. Время должно быть в будущем.\n\n"
            "Примеры: `21:43`, `2143`, `в 19`, `завтра в 10:00`, `через 20 минут`, `30.09 в 11`."
        )
        return

    # для напоминаний тоже уважаем рабочие часы
    next_at = clamp_to_work_hours(dt_utc)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE tasks SET next_reminder_at=?, updated_at=? WHERE id=?",
            (next_at.isoformat(), datetime.now(UTC).isoformat(), task_id)
        )
        await db.commit()

    await state.clear()
    await m.answer(f"🔔 Напомню в {fmt_dt_local(next_at.isoformat())}.")

    dt_utc = parse_human_time(m.text)
    if not dt_utc:
        await m.answer(
            "❌ Не удалось понять время. Время должно быть в будущем.\n"
            "Примеры: `21:43`, `завтра в 10:00`, `через 20 минут`, `30.09 в 11`.",
            parse_mode="Markdown"
        )
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE tasks SET next_reminder_at=?, updated_at=? WHERE id=?",
            (dt_utc.isoformat(), datetime.now(UTC).isoformat(), int(task_id))
        )

        # если меняли deadline — записываем событие
        try:
            async with aiosqlite.connect(DB_PATH) as db2:
                cur_old = await db2.execute("SELECT deadline FROM tasks WHERE id=?", (task_id,))
                r_old = await cur_old.fetchone()
                old_dl = r_old[0] if r_old else None
                await log_task_event(db2, task_id, "postpone", meta=f"old={old_dl}; new={dt_utc.isoformat()}")
                await db2.commit()
        except Exception:
            pass

        await db.commit()

    await m.answer(f"🔔 Напоминание перенесено на {fmt_dt_local(dt_utc.isoformat())}")
    await state.clear()

@router.message(ExtendDeadline.waiting_for_deadline)
async def set_new_deadline(msg: Message, state: FSMContext):
    data = await state.get_data()
    task_id = data["task_id"]
    reason = (data.get("reason", "") or "").strip()

    # Читаем «человеческое» время (ваш парсер)
    dt_utc = parse_human_time((msg.text or "").strip())
    if not dt_utc:
        await msg.answer(
            "❌ Не удалось понять время. Время должно быть в будущем.\n\n"
            "Примеры: `10:00`, `1045`, `в 19`, `завтра в 10:00`, `через 20 минут`, `30.09 в 11`."
        )
        return

    try:
        emp_full_name = None
        task_desc = None
        managers = []

        async with aiosqlite.connect(DB_PATH) as db:
            now_iso = datetime.now(UTC).isoformat()

            # ВАЖНО: next_reminder_at считаем с учётом грейса/часовой логики
            new_next = next_reminder_after(dt_utc.isoformat())

            await db.execute(
                "UPDATE tasks SET deadline=?, updated_at=?, next_reminder_at=?, last_postpone_reason=? WHERE id=?",
                (dt_utc.isoformat(), now_iso, new_next, reason, task_id)
            )

            cur = await db.execute("""
                SELECT t.user_id, t.description, u.full_name
                FROM tasks t
                JOIN users u ON u.id = t.user_id
                WHERE t.id=?
            """, (task_id,))
            row = await cur.fetchone()
            if row:
                user_id, task_desc, emp_full_name = row
                cur2 = await db.execute("""
                    WITH RECURSIVE chain(manager_id, subordinate_id) AS (
                      SELECT manager_user_id, subordinate_user_id FROM manager_links
                      UNION
                      SELECT ml.manager_user_id, c.subordinate_id
                      FROM manager_links ml
                      JOIN chain c ON ml.subordinate_user_id = c.manager_id
                    )
                    SELECT DISTINCT u.tg_id
                    FROM chain ch
                    JOIN users u ON u.id = ch.manager_id
                    WHERE ch.subordinate_id = ?;
                """, (user_id,))
                managers = [r[0] for r in await cur2.fetchall()]

            await db.commit()
        
        # перерисуем исходную карточку просрочки
        od_chat = data.get("overdue_chat_id")
        od_msg = data.get("overdue_msg_id")
        if od_chat and od_msg:
            info = f"🗓 Дедлайн обновлён: {fmt_dt_local(dt_utc.isoformat())}"
            await _refresh_overdue_card(db, od_chat, od_msg, task_id, info)

        if managers and emp_full_name and task_desc:
            note = (
                f"🕒 Перенос дедлайна у {emp_full_name}\n"
                f"#{task_id} — {task_desc}\n"
                f"Причина: {reason}\n"
                f"Новый дедлайн: {fmt_dt_local(dt_utc.isoformat())}"
            )
            for mid in managers:
                try:
                    await bot.send_message(mid, note)
                except Exception as e:
                    logging.warning(f"notify manager failed (postpone) tg_id={mid}: {e}")

    except Exception as e:
        logging.exception("set_new_deadline failed: %s", e)
        await msg.answer("Произошла ошибка при переносе дедлайна. Попробуйте ещё раз.")

@router.callback_query(F.data.startswith("task_setstatus:"))
async def cb_set_status(cq: CallbackQuery):
    _, task_id, new_status = cq.data.split(":")
    task_id = int(task_id)

    async with aiosqlite.connect(DB_PATH) as db:
        # обновим статус
        now_iso = datetime.now(UTC).isoformat()
        await db.execute(
            "UPDATE tasks SET status=?, updated_at=? WHERE id=?",
            (new_status, now_iso, task_id)
        )
        await db.commit()

        # достанем актуальную карточку задачи
        cur = await db.execute("SELECT id, description, status, deadline FROM tasks WHERE id=?", (task_id,))
        t = await cur.fetchone()
        if not t:
            await cq.answer("Задача не найдена", show_alert=True)
            return
        t = dict(zip([c[0] for c in cur.description], t))

    # красиво выводим карточку
    txt = render_task_card_html(t) + f"\n\nСтатус задачи #{task_id} изменён на <u>{STATUS_RU.get(new_status,new_status)}</u>."
    await cq.message.edit_text(txt, parse_mode="HTML")
    await cq.answer("Статус обновлён")

# =========================
# Назначение задач руководителем (скрытое меню)
# =========================
def assign_list_cb(page: int) -> str:
    return f"assign_list:{page}"

def assign_user_cb(user_id: int) -> str:
    return f"assign_user:{user_id}"

@router.message(Command("manager"))
async def cmd_manager(m: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        u = await ensure_user(db, m.from_user.id, m.from_user.full_name or "")
    role = (u.get("role") or "").lower()
    is_dev = (role == "developer")
    is_head = is_dev or (role == "head")

    # показываем reply-клавиатуру руководителя вместо основной
    await m.answer(
        "Меню руководителя:",
        reply_markup=manager_reply_kb(is_head=is_head, is_dev=is_dev)
    )

from aiogram.filters import Command

@router.message(Command("rehire"))
async def cmd_rehire(m: Message):
    parts = (m.text or "").split()
    if len(parts) < 2:
        await m.answer("Использование: /rehire <tg_id> [role]\nrole: employee|lead|head|developer (необязательно)")
        return

    # права
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
    if not me or not me.get("is_active", 1) or me.get("role") != "developer":
        await m.answer("❌ Нет доступа.")
        return

    try:
        target_tg = int(parts[1])
    except ValueError:
        await m.answer("tg_id должен быть числом."); return
    role = parts[2] if len(parts) >= 3 else None
    if role and role not in ("employee","lead","head","developer"):
        await m.answer("Недопустимая роль. Разрешено: employee|lead|head|developer"); return

    async with aiosqlite.connect(DB_PATH) as db:
        # По умолчанию поднимаем is_active=1, роль — как указали (или оставляем), registered — не трогаем
        updated = await rehire_user_by_tg(db, target_tg, role=role, set_registered=None)
        u = await get_user_by_tg(db, target_tg)

    if not updated or not u:
        await m.answer(f"Пользователь с tg_id={target_tg} не найден.")
        return

    await m.answer(
        "✅ Доступ восстановлен.\n"
        f"Пользователь: {u.get('full_name','(без имени)')} (tg_id: {target_tg})\n"
        f"Роль: {u.get('role')}\n"
        f"registered: {u.get('registered')} → при необходимости сотрудник сможет пройти /register."
    )

# --- Developer: разрешить сотруднику пройти регистрацию заново ---
from aiogram.filters import Command

@router.message(Command("resetreg"))
async def cmd_resetreg(m: Message):
    parts = (m.text or "").strip().split()

    if len(parts) != 2 or not parts[1].isdigit():
        await m.answer(
            "Использование: <code>/resetreg &lt;tg_id&gt;</code>",
            parse_mode="HTML",
        )
        return

    # проверяем права
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
        if not me or me.get("role") != "developer":
            await m.answer("❌ Нет доступа.")
            return

    target_tg = int(parts[1])
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id, full_name FROM users WHERE tg_id=?", (target_tg,))
        row = await cur.fetchone()
        if not row:
            await m.answer(
                f"Пользователь с tg_id <code>{target_tg}</code> не найден.",
                parse_mode="HTML",
            )
            return

        user_id, full_name = row

        # сбрасываем регистрацию
        await db.execute("UPDATE users SET registered=0, is_active=1 WHERE tg_id=?", (target_tg,))
        await db.commit()

    await m.answer(
        f"Регистрация пользователя <b>{full_name}</b> (tg_id: <code>{target_tg}</code>) сброшена.\n"
        "Ему снова доступна команда регистрации.",
        parse_mode="HTML",
    )

@router.message(Command("gsync"))
async def cmd_gsync(m: Message):
    # доступ только руководителям/разработчику — оставьте вашу проверку, если уже есть
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
    if not me or me.get("role") not in ("head", "developer"):
        await m.answer("⛔ Нет доступа.")
        return

    try:
        _require_gs_config()
    except Exception as e:
        await m.answer(f"⚠️ Конфигурация Google Sheets не задана:\n<code>{e}</code>")
        return

    await m.answer("🔄 Синхронизирую Google Sheet…")
    try:
        await gs_sync_all()
        link = os.getenv("GSHEET_URL", "").strip()
        await m.answer("✅ Готово. " + (f"Таблица: {link}" if link else "Проверь таблицу."))
    except Exception as e:
        logging.exception("gsync failed: %s", e)
        await m.answer(f"❌ Ошибка синхронизации:\n<code>{H(str(e))}</code>")

@router.message(Command("gsdebug"))
async def cmd_gsdebug(m: Message):
    # доступ как и был
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, m.from_user.id)
    if not me or me.get("role") not in ("head", "developer"):
        await m.answer("⛔ Нет доступа.")
        return

    from pathlib import Path
    gs_id = os.getenv("GSHEET_ID", "").strip()
    cred_env = os.getenv("GOOGLE_CREDENTIALS_FILE", "").strip()
    link = os.getenv("GSHEET_URL", "").strip()

    # Покажем, откуда взяли .env и где ищем json
    env_path = Path(__file__).resolve().with_name(".env")
    cred_path = Path(cred_env)
    if cred_env and not cred_path.is_absolute():
        cred_path = Path(__file__).resolve().parent / cred_env

    lines = [
        f".env path: {env_path}",
        f"GSHEET_ID: {'✅ задан' if gs_id else '❌ пуст'}",
        f"GOOGLE_CREDENTIALS_FILE: {('✅ найден' if cred_path.exists() else '❌ НЕ найден')} ({cred_path if cred_env else '—'})",
        f"GSHEET_URL: {link or '—'}",
    ]

    # Пробуем соединение
    try:
        sh = await _gs_open()
        ws_titles = [ws.title for ws in await sh.worksheets()]
        lines.append(f"Подключение: ✅ ок. Листы: {', '.join(ws_titles) or 'нет'}")
    except Exception as e:
        lines.append(f"Подключение: ❌ ошибка\n{H(str(e))}")

    await m.answer("\n".join(lines))

@router.callback_query(F.data == "mgr:assign")
async def mgr_assign(cq: CallbackQuery, state: FSMContext):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
    if me["role"] not in ("lead","head","developer"):
        await cq.answer("Нет доступа", show_alert=True); return
    await state.set_state(AssignPick.picking_user)
    await show_user_picker(cq, 0, for_tg_id=cq.from_user.id)

def summary_list_cb(page: int) -> str:
    return f"summary_list:{page}"

def summary_user_cb(user_id: int) -> str:
    return f"summary_user:{user_id}"

@router.callback_query(F.data == "mgr:summary")
async def mgr_summary(cq: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
    if me["role"] not in ("lead","head","developer"):
        await cq.answer("Нет доступа", show_alert=True); return
    await show_user_picker_summary(cq, 0, for_tg_id=cq.from_user.id)

@router.callback_query(F.data == "mgr:dept")
async def mgr_dept(cq: CallbackQuery, state: FSMContext):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
    if me["role"] not in ("head", "developer"):
        await cq.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(DeptAssign.picking_user)
    await show_user_picker_dept(cq, 0, for_tg_id=cq.from_user.id)

# ====== Перезапрос плана: выбор сотрудника ======

def planreq_list_cb(page: int) -> str:
    return f"planreq_list:{page}"

def planreq_user_cb(user_id: int) -> str:
    return f"planreq_user:{user_id}"

@router.callback_query(F.data == "mgr:plan_req")
async def mgr_plan_req(cq: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
    if me["role"] not in ("lead","head","developer"):
        await cq.answer("Нет доступа", show_alert=True); return
    await show_user_picker_planreq(cq, 0, for_tg_id=cq.from_user.id)

async def show_user_picker_planreq(m_or_cq, page: int, for_tg_id: int):
    is_callback = isinstance(m_or_cq, CallbackQuery)
    chat_id = m_or_cq.message.chat.id if is_callback else m_or_cq.chat.id

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, for_tg_id)

        # фильтрация по отделу
        if me["role"] == "developer":
            # дев — все активные (кроме developer)
            cur = await db.execute("""
                SELECT id, full_name, tg_id
                FROM users
                WHERE is_active=1 AND role!='developer'
                ORDER BY full_name COLLATE NOCASE
            """)
        else:
            # head/lead — только сотрудники их отдела
            cur = await db.execute("""
                SELECT id, full_name, tg_id
                FROM users
                WHERE is_active=1 AND role='employee' AND COALESCE(dept,'') = COALESCE(?, '')
                ORDER BY full_name COLLATE NOCASE
            """, (me.get("dept") or "",))
        candidates = await cur.fetchall()

    total = len(candidates)
    if total == 0:
        text = "Нет доступных сотрудников."
        if is_callback:
            await m_or_cq.message.edit_text(text)
            await m_or_cq.answer()
        else:
            await bot.send_message(chat_id, text)
        return

    pages = ceil(total / PAGE_SIZE)
    page = max(0, min(page, pages - 1))
    start, end = page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE
    chunk = candidates[start:end]

    kb = InlineKeyboardBuilder()
    for uid, full, tg in chunk:
        label = full if full and full != "unknown" else f"user_{tg}"
        kb.button(text=label, callback_data=planreq_user_cb(uid))
    if page > 0:
        kb.button(text="« Назад", callback_data=planreq_list_cb(page - 1))
    if page < pages - 1:
        kb.button(text="Далее »", callback_data=planreq_list_cb(page + 1))
    kb.adjust(1)

    text = f"Кому переотправить форму плана? (стр {page+1}/{pages})"
    if is_callback:
        await m_or_cq.message.edit_text(text, reply_markup=kb.as_markup())
        await m_or_cq.answer()
    else:
        await bot.send_message(chat_id, text, reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("planreq_list:"))
async def cb_planreq_list(cq: CallbackQuery):
    page = int(cq.data.split(":")[1])
    await show_user_picker_planreq(cq, page, for_tg_id=cq.from_user.id)

@router.callback_query(F.data.startswith("planreq_user:"))
async def cb_planreq_user(cq: CallbackQuery):
    target_user_id = int(cq.data.split(":")[1])

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
        if me["role"] not in ("lead","head","developer"):
            await cq.answer("Нет доступа", show_alert=True); return
        if me["role"] == "lead" and not await is_manager_of(db, me["id"], target_user_id):
            await cq.answer("Можно запрашивать план только у своих подчинённых.", show_alert=True); return

    # отправляем форму плана
    ok, err = await send_morning_plan_to_user(target_user_id)
    if ok:
        await cq.message.answer("✅ Форма плана отправлена сотруднику.")
        await cq.answer()
    else:
        await cq.answer(f"Не удалось отправить: {err or 'ошибка'}", show_alert=True)

async def show_user_picker_summary(m_or_cq, page: int, for_tg_id: int):
    is_callback = isinstance(m_or_cq, CallbackQuery)
    chat_id = m_or_cq.message.chat.id if is_callback else m_or_cq.chat.id

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, for_tg_id)
        # фильтрация по отделу
        if me["role"] == "developer":
            # дев — все активные (кроме developer)
            cur = await db.execute("""
                SELECT id, full_name, tg_id
                FROM users
                WHERE is_active=1 AND role!='developer'
                ORDER BY full_name COLLATE NOCASE
            """)
        else:
            # head/lead — только сотрудники их отдела
            cur = await db.execute("""
                SELECT id, full_name, tg_id
                FROM users
                WHERE is_active=1 AND role='employee' AND COALESCE(dept,'') = COALESCE(?, '')
                ORDER BY full_name COLLATE NOCASE
            """, (me.get("dept") or "",))
        candidates = await cur.fetchall()

    total = len(candidates)
    if total == 0:
        text = "Нет доступных сотрудников."
        if is_callback:
            await m_or_cq.message.edit_text(text)
            await m_or_cq.answer()
        else:
            await bot.send_message(chat_id, text)
        return

    pages = ceil(total / PAGE_SIZE)
    page = max(0, min(page, pages - 1))
    start, end = page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE
    chunk = candidates[start:end]

    kb = InlineKeyboardBuilder()
    for uid, full, tg in chunk:
        label = full if full and full != "unknown" else f"user_{tg}"
        kb.button(text=label, callback_data=summary_user_cb(uid))
    if page > 0:
        kb.button(text="« Назад", callback_data=summary_list_cb(page - 1))
    if page < pages - 1:
        kb.button(text="Далее »", callback_data=summary_list_cb(page + 1))
    kb.adjust(1)

    text = f"Выберите сотрудника (стр {page+1}/{pages}):"
    if is_callback:
        await m_or_cq.message.edit_text(text, reply_markup=kb.as_markup())
        await m_or_cq.answer()
    else:
        await bot.send_message(chat_id, text, reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("summary_list:"))
async def cb_summary_list(cq: CallbackQuery):
    page = int(cq.data.split(":")[1])
    await show_user_picker_summary(cq, page, for_tg_id=cq.from_user.id)

@router.callback_query(F.data.startswith("summary_user:"))
async def cb_summary_user(cq: CallbackQuery):
    # УДАЛЯЕМ сообщение со списком сотрудников
    await _delete_msg_safe(cq.message)

    target_user_id = int(cq.data.split(":")[1])

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
        if me["role"] not in ("lead","head","developer"):
            await cq.answer("Нет доступа", show_alert=True); 
            return
        if me["role"] == "lead" and not await is_manager_of(db, me["id"], target_user_id):
            await cq.answer("Можно смотреть только своих подчинённых.", show_alert=True); 
            return

        # 1) Сводка
        summary_text = await render_user_summary(db, target_user_id)

        # 2) Статистика по закрытым
        cur = await db.execute("""
            SELECT 
                SUM(CASE WHEN delay_minutes IS NULL OR delay_minutes<=0 THEN 1 ELSE 0 END) as ontime,
                SUM(CASE WHEN delay_minutes>0 THEN 1 ELSE 0 END) as late,
                COUNT(*) as total
            FROM tasks
            WHERE user_id=? AND status='done'
        """, (target_user_id,))
        row = await cur.fetchone()
        ontime, late, total = (row or (0,0,0))

        # 3) CSV
        cur = await db.execute("""
            SELECT t.id, t.description, t.status, t.deadline, t.completed_at, 
                   COALESCE(t.delay_minutes,0) as delay_minutes
            FROM tasks t
            WHERE t.user_id=?
            ORDER BY t.id DESC
            LIMIT 500
        """, (target_user_id,))
        rows = await cur.fetchall()

    # Блок статистики
    stat_block = (
        "\n\n<b>Статистика по закрытым задачам</b>\n"
        f"• В срок: <b>{ontime or 0}</b>\n"
        f"• С просрочкой: <b>{late or 0}</b>\n"
        f"• Всего закрыто: <b>{total or 0}</b>"
    )
    await cq.message.answer(summary_text + stat_block, parse_mode="HTML")

    # CSV
    import io, csv
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=';')
    w.writerow(["task_id","description","status","deadline","completed_at","delay_minutes"])
    for r in rows:
        w.writerow([r[0], r[1] or "", r[2] or "", r[3] or "", r[4] or "", r[5] or 0])
    csv_bytes = io.BytesIO(buf.getvalue().encode("utf-8"))
    csv_bytes.name = "tasks.csv"
    try:
        await cq.message.answer_document(document=csv_bytes, caption="Экспорт задач сотрудника (CSV)")
    except Exception:
        pass

    await cq.answer()

async def show_user_picker(m_or_cq, page: int, for_tg_id: int):
    is_callback = isinstance(m_or_cq, CallbackQuery)
    chat_id = m_or_cq.message.chat.id if is_callback else m_or_cq.chat.id

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, for_tg_id)
        if me["role"] == "developer":
            cur = await db.execute("""
                SELECT id, full_name, tg_id
                FROM users
                WHERE is_active=1 AND role='employee'
                ORDER BY full_name COLLATE NOCASE
            """)
        else:
            # head/lead — только их отдел
            cur = await db.execute("""
                SELECT id, full_name, tg_id
                FROM users
                WHERE is_active=1 AND role='employee' AND COALESCE(dept,'') = COALESCE(?, '')
                ORDER BY full_name COLLATE NOCASE
            """, (me.get("dept") or "",))
        candidates = await cur.fetchall()

    total = len(candidates)
    if total == 0:
        text = "Нет доступных сотрудников для назначения."
        if is_callback:
            await m_or_cq.message.edit_text(text)
            await m_or_cq.answer()
        else:
            await bot.send_message(chat_id, text)
        return

    pages = ceil(total / PAGE_SIZE)
    page = max(0, min(page, pages - 1))
    start, end = page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE
    chunk = candidates[start:end]

    kb = InlineKeyboardBuilder()
    for uid, full, tg in chunk:
        label = full if full and full != "unknown" else f"user_{tg}"
        kb.button(text=label, callback_data=assign_user_cb(uid))
    if page > 0:
        kb.button(text="« Назад", callback_data=assign_list_cb(page - 1))
    if page < pages - 1:
        kb.button(text="Далее »", callback_data=assign_list_cb(page + 1))
    kb.adjust(1)

    text = f"Выберите сотрудника (стр {page+1}/{pages}):"
    if is_callback:
        await m_or_cq.message.edit_text(text, reply_markup=kb.as_markup())
        await m_or_cq.answer()
    else:
        await bot.send_message(chat_id, text, reply_markup=kb.as_markup())

# ===== Назначение отдела: выбор сотрудника =====

def dept_list_cb(page: int) -> str:
    return f"dept_list:{page}"

def dept_user_cb(user_id: int) -> str:
    return f"dept_user:{user_id}"

async def show_user_picker_dept(m_or_cq, page: int, for_tg_id: int):
    is_callback = isinstance(m_or_cq, CallbackQuery)
    chat_id = m_or_cq.message.chat.id if is_callback else m_or_cq.chat.id

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, for_tg_id)
        # head/developer видят всех активных (кроме developer)
        cur = await db.execute("""
            SELECT id, full_name, tg_id
            FROM users
            WHERE is_active=1 AND role!='developer'
            ORDER BY full_name COLLATE NOCASE
        """)
        candidates = await cur.fetchall()

    total = len(candidates)
    if total == 0:
        text = "Нет доступных пользователей."
        if is_callback:
            await m_or_cq.message.edit_text(text)
            await m_or_cq.answer()
        else:
            await bot.send_message(chat_id, text)
        return

    pages = ceil(total / PAGE_SIZE)
    page = max(0, min(page, pages - 1))
    start, end = page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE
    chunk = candidates[start:end]

    kb = InlineKeyboardBuilder()
    for uid, full, tg in chunk:
        label = full if full and full != "unknown" else f"user_{tg}"
        kb.button(text=label, callback_data=dept_user_cb(uid))
    if page > 0:
        kb.button(text="« Назад", callback_data=dept_list_cb(page - 1))
    if page < pages - 1:
        kb.button(text="Далее »", callback_data=dept_list_cb(page + 1))
    kb.adjust(1)

    text = f"Кому назначить отдел? (стр {page+1}/{pages})"
    if is_callback:
        await m_or_cq.message.edit_text(text, reply_markup=kb.as_markup())
        await m_or_cq.answer()
    else:
        await bot.send_message(chat_id, text, reply_markup=kb.as_markup())

@router.callback_query(F.data.startswith("dept_list:"))
async def cb_dept_list(cq: CallbackQuery, state: FSMContext):
    page = int(cq.data.split(":")[1])
    # состояние остаётся DeptAssign.picking_user
    await show_user_picker_dept(cq, page, for_tg_id=cq.from_user.id)

@router.callback_query(F.data.startswith("dept_user:"))
async def cb_dept_user(cq: CallbackQuery, state: FSMContext):
    # удаляем сообщение со списком
    await _delete_msg_safe(cq.message)

    target_user_id = int(cq.data.split(":")[1])

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
        if me["role"] not in ("head", "developer"):
            await cq.answer("Нет доступа", show_alert=True); return
        tgt = await get_user_by_id(db, target_user_id)
        if not tgt:
            await cq.answer("Пользователь не найден", show_alert=True); return

    await state.update_data(dept_target_user_id=target_user_id)
    await state.set_state(DeptAssign.waiting_dept)
    await cq.message.answer(
        f"Назначение отдела для: <b>{H(tgt['full_name'] or f'user_{tgt['tg_id']}')}</b>\n"
        f"Текущий отдел: <b>{H(tgt.get('dept') or '—')}</b>\n\n"
        "Введите название отдела (например: <code>SMM</code> или <code>Дизайн/Графика</code>).",
        parse_mode="HTML"
    )
    await cq.answer()

@router.callback_query(F.data.startswith("assign_list:"))
async def cb_assign_list(cq: CallbackQuery, state: FSMContext):
    page = int(cq.data.split(":")[1])
    await state.set_state(AssignPick.picking_user)
    await show_user_picker(cq, page, for_tg_id=cq.from_user.id)

@router.callback_query(F.data.startswith("assign_user:"))
async def cb_assign_user(cq: CallbackQuery, state: FSMContext):
    target_user_id = int(cq.data.split(":")[1])
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
        if me["role"] not in ("lead","head","developer"):
            await cq.answer("Нет доступа", show_alert=True); return
        if me["role"] == "lead" and not await is_manager_of(db, me["id"], target_user_id):
            await cq.answer("Можно назначать только своим подчинённым.", show_alert=True); return
        tgt = await get_user_by_id(db, target_user_id)
        if not tgt:
            await cq.answer("Сотрудник не найден."); return
    await state.update_data(assign_target_user_id=target_user_id)
    await state.set_state(AssignTask.waiting_desc)
    await cq.message.edit_text(f"Назначение задачи для: {tgt['full_name']}\n\nОпишите задачу (кратко):")
    await cq.answer()

@router.message(AssignTask.waiting_desc)
async def assign_desc(m: Message, state: FSMContext):
    # сохраняем текст
    await state.update_data(assign_desc=(m.text or "").strip())
    # просим дедлайн в простом виде
    await state.set_state(AssignTask.waiting_deadline)
    await m.answer(
        "Укажите дедлайн (можно по-простому):\n"
        "• 10:00\n"
        "• в 19:00\n"
        "• завтра в 10:00\n"
        "• через 20 минут\n"
        "• 30.09 в 11"
    )

@router.message(AssignTask.waiting_deadline)
async def assign_deadline(m: Message, state: FSMContext):
    # 1) Парсим «человеческое» время -> aware UTC
    text = (m.text or "").strip()
    dt_utc = parse_human_time(text)
    if not dt_utc:
        await m.answer(
            "❌ Не удалось понять время. Время должно быть в будущем.\n\n"
            "Примеры: 21:43, 2143, «в 19», «завтра в 10:00», «через 20 минут», «30.09 в 11»."
        )
        return

    # 2) Достаём ранее сохранённые данные
    data = await state.get_data()
    target_user_id = data["assign_target_user_id"]   # id сотрудника (из вашей логики выбора)
    desc = data["assign_desc"]
    now = datetime.now(UTC)

    # 3) Создаём задачу: next_reminder_at ставим РОВНО НА ДЕДЛАЙН
    async with aiosqlite.connect(DB_PATH) as db:
        assigner = await ensure_user(db, m.from_user.id, m.from_user.full_name or "")

        next_rem = dt_utc.isoformat()  # <- ключ: событие в момент дедлайна

        cur = await db.execute("""
            INSERT INTO tasks (
                user_id, description, deadline, status,
                next_reminder_at, assigned_by_user_id,
                created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?)
        """, (
            target_user_id, desc, dt_utc.isoformat(), 'new',
            next_rem, assigner["id"],
            now.isoformat(), now.isoformat()
        ))
        await db.commit()
        task_id = cur.lastrowid
        await log_task_event(db, task_id, "create", meta=f"assigned_by={assigner['id']}; deadline={dt_utc.isoformat()}")

        tgt = await get_user_by_id(db, target_user_id)  # получим tg_id сотрудника

    # 4) Уведомляем сотрудника
    try:
        await bot.send_message(
            tgt["tg_id"],
            (
                f"📌 Вам назначена новая задача от {H(assigner['full_name'])}:\n"
                f"#{task_id} — <b>{H(desc)}</b>\n"
                f"{Q('Дедлайн: ' + fmt_dt_local(dt_utc.isoformat()))}\n"
                "Зайдите в «📋 Мои задачи» и нажмите «▶️ Начать задачу», когда приступите."
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        logging.warning(f"notify employee failed (assign): {e}")

    # 5) Ответ руководителю — сводка задач сотрудника
    async with aiosqlite.connect(DB_PATH) as db:
        summary = await active_tasks_summary(db, target_user_id)
    await state.clear()
    await m.answer(f"Задача назначена ✅\n\n{summary}", parse_mode="HTML")

@router.callback_query(F.data == "mgr:team")
async def mgr_team(cq: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
        if me["role"] not in ("lead","head","developer"):
            await cq.answer("Нет доступа", show_alert=True); return

        cur = await db.execute("""
          SELECT u.full_name, u.tg_id
          FROM users u
          WHERE u.is_active=1 AND u.role='employee' AND COALESCE(u.dept,'') = COALESCE(?, '')
          ORDER BY u.full_name COLLATE NOCASE
        """, (me.get("dept") or "",))
        rows = await cur.fetchall()

    dept = me.get("dept") or "—"
    if not rows:
        await cq.message.answer(f"{Q('Отдел ' + dept)}\nТвоих подчинённых пока нет.")
    else:
        lines = [Q("Отдел " + dept), "Твои подчинённые:", ""]
        for i, (full_name, tg_id) in enumerate(rows, start=1):
            name = full_name or f"user_{tg_id}"
            lines.append(f"{i}. {name}")
        await cq.message.answer("\n".join(lines), parse_mode="HTML")
    await cq.answer()

@router.callback_query(F.data == "mgr:leads")
async def mgr_leads(cq: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
        if me["role"] not in ("head","developer"):
            await cq.answer("Нет доступа", show_alert=True); return
        cur = await db.execute("SELECT full_name, tg_id FROM users WHERE role='lead' ORDER BY full_name")
        rows = await cur.fetchall()
    if not rows:
        await cq.message.answer("Линейных руководителей пока нет.")
    else:
        text = "Линейные руководители:\n" + "\n".join([f"• {r[0]} (tg_id: {r[1]})" for r in rows])
        await cq.message.answer(text)
    await cq.answer()

@router.callback_query(F.data == "mgr:setrole")
async def mgr_setrole(cq: CallbackQuery, state: FSMContext):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
    if me["role"] not in ("head", "developer"):
        await cq.answer("Нет доступа", show_alert=True)
        return

    await state.set_state(SetRoleState.waiting)
    await cq.message.answer(
        "Введи: <code>&lt;tg_id&gt; &lt;role&gt;</code> где role: "
        "<code>employee</code>|<code>lead</code>|<code>head</code>\n"
        "Например: <code>123456789 lead</code>"
    )
    await cq.answer()

@router.message(SetRoleState.waiting)
async def mgr_setrole_apply(m: Message, state: FSMContext):
    parts = m.text.strip().split()
    if len(parts) != 2 or (not parts[0].isdigit()) or parts[1] not in ("employee","lead","head"):
        await m.answer("Формат: <code>&lt;tg_id&gt; &lt;role&gt;</code> (role: employee|lead|head)")
        return
    target_tg_id = int(parts[0]); role = parts[1]
    async with aiosqlite.connect(DB_PATH) as db:
        me = await ensure_user(db, m.from_user.id, m.from_user.full_name or "")
        if me["role"] not in ("head","developer"):
            await m.answer("Нет доступа."); await state.clear(); return
        tgt = await get_user_by_tg(db, target_tg_id)
        if not tgt:
            await db.execute("INSERT INTO users(tg_id, full_name, role) VALUES(?,?,?)",
                             (target_tg_id, f"user_{target_tg_id}", role))
        else:
            await db.execute("UPDATE users SET role=? WHERE tg_id=?", (role, target_tg_id))
        await db.commit()
    await state.clear()
    await m.answer(f"Роль пользователя {target_tg_id} установлена: {role}")

@router.callback_query(F.data == "mgr:link")
async def mgr_link(cq: CallbackQuery, state: FSMContext):
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
    if me["role"] not in ("head", "developer"):
        await cq.answer("Нет доступа", show_alert=True)
        return

    await state.set_state(LinkState.waiting)
    await cq.message.answer(
        "Введи: <code>&lt;manager_tg_id&gt; &lt;subordinate_tg_id&gt;</code>\n"
        "Например: <code>111111111 222222222</code>"
    )
    await cq.answer()

@router.message(LinkState.waiting)
async def mgr_link_apply(m: Message, state: FSMContext):
    parts = m.text.strip().split()
    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
        await m.answer("Формат: `<manager_tg_id> <subordinate_tg_id>`"); return
    man_tg, sub_tg = int(parts[0]), int(parts[1])

    if man_tg == sub_tg:
        await m.answer("Нельзя связать пользователя сам с собой."); return

    async with aiosqlite.connect(DB_PATH) as db:
        me = await ensure_user(db, m.from_user.id, m.from_user.full_name or "")
        if me["role"] not in ("head","developer"):
            await m.answer("Нет доступа."); await state.clear(); return

        man = await get_user_by_tg(db, man_tg)
        sub = await get_user_by_tg(db, sub_tg)
        if not man or not sub:
            await m.answer("Оба пользователя должны хотя бы раз открыть бота (/start)."); return

        # Запрет дубликатов
        cur = await db.execute("""
            SELECT 1 FROM manager_links WHERE manager_user_id=? AND subordinate_user_id=? LIMIT 1
        """, (man["id"], sub["id"]))
        if await cur.fetchone():
            await m.answer("Такая связь уже существует."); await state.clear(); return

        # Запрет колец: нельзя сделать подчинённого руководителем своего начальника
        # проверим, что sub не является (прямо/косвенно) руководителем man
        cur = await db.execute("""
            WITH RECURSIVE chain(manager_id, subordinate_id) AS (
              SELECT manager_user_id, subordinate_user_id FROM manager_links
              UNION
              SELECT ml.manager_user_id, c.subordinate_id
              FROM manager_links ml
              JOIN chain c ON ml.subordinate_user_id = c.manager_id
            )
            SELECT 1 FROM chain WHERE manager_id=? AND subordinate_id=? LIMIT 1
        """, (sub["id"], man["id"]))
        if await cur.fetchone():
            await m.answer("Нельзя создавать циклическую иерархию."); await state.clear(); return

        await db.execute(
            "INSERT INTO manager_links(manager_user_id, subordinate_user_id) VALUES(?,?)",
            (man["id"], sub["id"])
        )
        await db.commit()

    await state.clear()
    await m.answer(f"Связь установлена: {man['full_name']} → {sub['full_name']}")

# =========================
# Диагностика
# =========================
@router.message(Command("taskinfo"))
async def cmd_taskinfo(m: Message):
    parts = m.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await m.answer("Использование: /taskinfo <task_id>")
        return
    tid = int(parts[1])
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT id, user_id, description, deadline, status, last_reminder_at, next_reminder_at, 
                   last_postpone_reason, started_at, planned_start_at, assigned_by_user_id
            FROM tasks WHERE id=?
        """, (tid,))
        row = await cur.fetchone()
    if not row:
        await m.answer("Задача не найдена.")
        return
    keys = ["id","user_id","description","deadline","status","last_reminder_at","next_reminder_at",
            "last_postpone_reason","started_at","planned_start_at","assigned_by_user_id"]
    data = dict(zip(keys, row))
    await m.answer("Инфо по задаче:\n" + "\n".join(f"{k}: {v}" for k,v in data.items()))

@router.message(Command("forcecheck"))
async def cmd_forcecheck(m: Message):
    await scheduler_job()
    await m.answer("Проверка напоминаний выполнена вручную.")

# =========================
# Утренний опрос (10:00) — «Нет задач сегодня»
# =========================
@router.callback_query(F.data.startswith("no_tasks_today:"))
async def cb_no_tasks_today(cq: CallbackQuery):
    parts = cq.data.split(":")
    if len(parts) != 2:
        await cq.answer(); return
    date_str = parts[1]

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
        if not me:
            await cq.answer(); return
        mgrs = await get_manager_tg_ids(db, me["id"])

    await disable_kb_and_optionally_edit(cq.message, f"Отмечено: нет задач на {date_str}.")
    await cq.answer("Отправил руководителям.")

    if mgrs:
        text = f"ℹ️ {me['full_name']} сообщил(а), что на {date_str} задач нет."
        for mid in mgrs:
            try:
                await bot.send_message(mid, text)
            except Exception as e:
                logging.warning(f"notify manager failed (no_tasks_today) tg_id={mid}: {e}")

# === План дня: меню создания задач из пунктов плана ===

def _plan_item_btn_cb(item_id: int) -> str:
    return f"plan_item_to_task:{item_id}"

@router.callback_query(F.data.startswith("plan_to_tasks_menu:"))
async def cb_plan_to_tasks_menu(cq: CallbackQuery):
    plan_date = cq.data.split(":")[1]
    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
        cur = await db.execute("""
            SELECT id, text, time_str, task_id
            FROM daily_plan_items
            WHERE user_id=? AND plan_date=?
            ORDER BY time_str ASC, id ASC
        """, (me["id"], plan_date))
        items = await cur.fetchall()

    if not items:
        await cq.answer("Нет пунктов плана для создания.", show_alert=True)
        return

    kb = InlineKeyboardBuilder()
    for iid, txt, hhmm, task_id in items:
        label = f"{'✅' if task_id else '📌'} {hhmm} — {txt}"
        kb.button(text=label[:64], callback_data=_plan_item_btn_cb(iid))
    kb.button(text="➕ Создать все", callback_data=f"plan_all_to_tasks:{plan_date}")
    kb.adjust(1)

    await cq.message.answer(f"Пункты плана на {plan_date}:", reply_markup=kb.as_markup())
    await cq.answer()

@router.callback_query(F.data.startswith("plan_item_to_task:"))
async def cb_plan_item_to_task(cq: CallbackQuery):
    item_id = int(cq.data.split(":")[1])
    now_utc = datetime.now(UTC)

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)

        cur = await db.execute(
            "SELECT plan_date, text, time_str, task_id FROM daily_plan_items WHERE id=? AND user_id=?",
            (item_id, me["id"])
        )
        row = await cur.fetchone()
        if not row:
            await cq.answer("Пункт не найден.", show_alert=True); return
        plan_date, raw_text, hhmm, task_id = row
        if task_id:
            await cq.answer("Уже создано.", show_alert=True); return

        # описание = текст без HH:MM
        desc = raw_text.replace(hhmm, "").strip(" -–.,;")

        # дедлайн: (plan_date + HH:MM локально) -> UTC
        from datetime import datetime as dtmod
        try:
            dl_local = dtmod.strptime(f"{plan_date} {hhmm}", "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TZ)
        except ValueError:
            await cq.answer("Некорректное время в пункте.", show_alert=True); return
        dl_utc = dl_local.astimezone(UTC)

        # если уже прошло — подвинем в рабочее окно
        if dl_utc <= now_utc:
            dl_utc = clamp_to_work_hours(
                now_utc.replace(hour=dl_local.hour, minute=dl_local.minute, second=0, microsecond=0)
            )

        next_rem = next_reminder_after(dl_utc.isoformat())

        # создаём задачу сразу в статусе «в работе»
        cur2 = await db.execute("""
            INSERT INTO tasks(user_id, description, deadline, status, next_reminder_at, started_at, updated_at, assigned_by_user_id)
            VALUES(?,?,?,?,?,?,?,?)
        """, (me["id"], desc, dl_utc.isoformat(), 'in_progress', next_rem, now_utc.isoformat(), now_utc.isoformat(), None))
        await db.commit()
        new_task_id = cur2.lastrowid
        await log_task_event(db, new_task_id, "create", meta=f"from_plan={plan_date} {hhmm}; deadline={dl_utc.isoformat()}")

        # связываем пункт плана с задачей
        await db.execute("UPDATE daily_plan_items SET task_id=? WHERE id=?", (new_task_id, item_id))
        await db.commit()

        mgrs = await get_manager_tg_ids(db, me["id"])

    await cq.answer("Задача создана и запущена.")
    await cq.message.answer(
        f"📌 Создана задача #{new_task_id}: {desc}\nДедлайн: {fmt_dt_local(dl_utc.isoformat())}\nСтатус: в работе"
    )

    if mgrs:
        note = f"🚀 {me['full_name']} начал(а) задачу #{new_task_id} из плана: {desc}\nДедлайн: {fmt_dt_local(dl_utc.isoformat())}"
        for mid in mgrs:
            try:
                await bot.send_message(mid, note)
            except Exception as e:
                logging.warning(f"notify mgr (plan->task) failed: {e}")

@router.callback_query(F.data.startswith("plan_all_to_tasks:"))
async def cb_plan_all_to_tasks(cq: CallbackQuery):
    plan_date = cq.data.split(":")[1]
    now_utc = datetime.now(UTC)
    created = 0

    async with aiosqlite.connect(DB_PATH) as db:
        me = await get_user_by_tg(db, cq.from_user.id)
        cur = await db.execute("""
            SELECT id, text, time_str, task_id
            FROM daily_plan_items
            WHERE user_id=? AND plan_date=?
            ORDER BY time_str ASC, id ASC
        """, (me["id"], plan_date))
        items = await cur.fetchall()

        for iid, raw_text, hhmm, task_id in items:
            if task_id:
                continue

            desc = raw_text.replace(hhmm, "").strip(" -–.,;")

            from datetime import datetime as dtmod
            dl_local = dtmod.strptime(f"{plan_date} {hhmm}", "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TZ)
            dl_utc = dl_local.astimezone(UTC)
            if dl_utc <= now_utc:
                dl_utc = clamp_to_work_hours(
                    now_utc.replace(hour=dl_local.hour, minute=dl_local.minute, second=0, microsecond=0)
                )

            next_rem = next_reminder_after(dl_utc.isoformat())

            cur2 = await db.execute("""
                INSERT INTO tasks(user_id, description, deadline, status, next_reminder_at, started_at, updated_at, assigned_by_user_id)
                VALUES(?,?,?,?,?,?,?,?)
            """, (me["id"], desc, dl_utc.isoformat(), 'in_progress', next_rem, now_utc.isoformat(), now_utc.isoformat(), None))
            await db.commit()
            new_task_id = cur2.lastrowid
            await log_task_event(db, new_task_id, "create", meta=f"from_plan={plan_date} {hhmm}; deadline={dl_utc.isoformat()}")

            await db.execute("UPDATE daily_plan_items SET task_id=? WHERE id=?", (new_task_id, iid))
            await db.commit()
            created += 1

        mgrs = await get_manager_tg_ids(db, me["id"])

    await cq.answer(f"Создано задач: {created}")
    if created:
        await cq.message.answer(f"📌 Создано задач из плана: {created}\nВсе поставлены в статус «в работе».")
        if mgrs:
            note = f"🚀 {me['full_name']} запустил(а) задачи из плана на {plan_date} (всего {created})."
            for mid in mgrs:
                try:
                    await bot.send_message(mid, note)
                except Exception as e:
                    logging.warning(f"notify mgr (plan all->tasks) failed: {e}")


async def daily_morning_broadcast():
    now_local = datetime.now(LOCAL_TZ)
    today_local = now_local.date()
    midnight_local = datetime.combine(today_local, datetime.min.time(), tzinfo=LOCAL_TZ)
    midnight_utc = midnight_local.astimezone(UTC).isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id, tg_id, full_name FROM users WHERE role='employee'")
        employees = await cur.fetchall()

        for uid, tg_id, full_name in employees:
            # «хвосты» со вчера
            cur2 = await db.execute("""
                SELECT id, description, deadline, status
                FROM tasks
                WHERE user_id=? AND status!='done' AND created_at < ?
                ORDER BY COALESCE(deadline,'9999') ASC, id DESC
            """, (uid, midnight_utc))
            tasks = await cur2.fetchall()

            if tasks:
                lines = [f"Доброе утро, {full_name}!"]
                lines.append("Остатки с прошлого дня:")
                for (tid, desc, deadline, status) in tasks[:15]:
                    lines.append(f"• #{tid}: {desc} | {status}, дедлайн: {fmt_dt_local(deadline)}")
                if len(tasks) > 15:
                    lines.append(f"… и ещё {len(tasks)-15}")
                lines.append("")
            else:
                lines = [f"Доброе утро, {full_name}!"]

            # Инструкция по плану
            lines += [
                "🗓 Сформируй план на сегодня.",
                "Напиши СВОИ ЗАДАЧИ — по ОДНОЙ в каждом сообщении — и обязательно укажи время окончания в формате `HH:MM`.",
                "Пример: `Подготовить отчёт 12:30`",
                "Когда перечислишь все пункты — нажми кнопку ниже «План заполнен».",
                "",
                "Если в сообщении нет времени — я НЕ приму пункт и попрошу отправить заново."
            ]
            text = "\n".join(lines)

            kb = InlineKeyboardBuilder()
            kb.button(text="Нет задач сегодня", callback_data=f"no_tasks_today:{today_local.isoformat()}")
            kb.button(text="📌 Создать задачи из плана", callback_data=f"plan_to_tasks_menu:{today_local.isoformat()}")
            kb.button(text="✅ План заполнен", callback_data=f"plan_done:{today_local.isoformat()}")
            kb.adjust(1)

            resp = None
            try:
                resp = await bot.send_message(tg_id, text, reply_markup=kb.as_markup(), parse_mode="Markdown")
            except Exception as e:
                logging.warning(f"morning send failed to {tg_id}: {e}")

            if resp:
                try:
                    # запомним «утреннее сообщение» для реплаев
                    await db.execute(
                        "UPDATE users SET last_plan_msg_id=?, last_plan_date=? WHERE id=?",
                        (resp.message_id, today_local.isoformat(), uid)
                    )
                    # почистим черновики плана на этот день (если вдруг есть)
                    await db.execute("DELETE FROM daily_plan_items WHERE user_id=? AND plan_date=?",
                                     (uid, today_local.isoformat()))
                    await db.commit()
                except Exception as e:
                    logging.warning(f"morning meta store failed for {tg_id}: {e}")

async def send_morning_plan_to_user(user_id: int) -> tuple[bool, str | None]:
    """
    Переотправка формы плана одному сотруднику.
    Возвращает (ok, error_message_or_None).
    """
    now_local = datetime.now(LOCAL_TZ)
    today_local = now_local.date()
    midnight_local = datetime.combine(today_local, datetime.min.time(), tzinfo=LOCAL_TZ)
    midnight_utc = midnight_local.astimezone(UTC).isoformat()

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            # берём пользователя
            cur = await db.execute("SELECT id, tg_id, full_name, role, is_active FROM users WHERE id=?", (user_id,))
            u = await cur.fetchone()
            if not u:
                return False, "Пользователь не найден"
            uid, tg_id, full_name, role, is_active = u
            if role == "developer":
                return False, "Для developer не требуется план"
            if is_active != 1:
                return False, "Пользователь не активен"

            # «хвосты» со вчера
            cur2 = await db.execute("""
                SELECT id, description, deadline, status
                FROM tasks
                WHERE user_id=? AND status!='done' AND created_at < ?
                ORDER BY COALESCE(deadline,'9999') ASC, id DESC
            """, (uid, midnight_utc))
            tasks = await cur2.fetchall()

            if tasks:
                lines = [f"Доброе утро, {full_name}!"]
                lines.append("Остатки с прошлого дня:")
                for (tid, desc, deadline, status) in tasks[:15]:
                    lines.append(f"• #{tid}: {desc} | {status}, дедлайн: {fmt_dt_local(deadline)}")
                if len(tasks) > 15:
                    lines.append(f"… и ещё {len(tasks)-15}")
                lines.append("")
            else:
                lines = [f"Доброе утро, {full_name}!"]

            lines += [
                "🗓 Сформируй план на сегодня.",
                "Напиши СВОИ ЗАДАЧИ — по ОДНОЙ в каждом сообщении — и обязательно укажи время окончания в формате `HH:MM`.",
                "Пример: `Подготовить отчёт 12:30`",
                "Когда перечислишь все пункты — нажми кнопку ниже «План заполнен».",
                "",
                "Если в сообщении нет времени — я НЕ приму пункт и попрошу отправить заново."
            ]
            text = "\n".join(lines)

            kb = InlineKeyboardBuilder()
            kb.button(text="Нет задач сегодня", callback_data=f"no_tasks_today:{today_local.isoformat()}")
            kb.button(text="📌 Создать задачи из плана", callback_data=f"plan_to_tasks_menu:{today_local.isoformat()}")
            kb.button(text="✅ План заполнен", callback_data=f"plan_done:{today_local.isoformat()}")
            kb.adjust(1)

            resp = None
            try:
                resp = await bot.send_message(tg_id, text, reply_markup=kb.as_markup(), parse_mode="Markdown")
            except Exception as e:
                logging.warning(f"plan resend failed to {tg_id}: {e}")
                return False, str(e)

            if resp:
                try:
                    # помечаем это сообщение как «утреннее» и сбрасываем черновики на сегодня
                    await db.execute(
                        "UPDATE users SET last_plan_msg_id=?, last_plan_date=? WHERE id=?",
                        (resp.message_id, today_local.isoformat(), uid)
                    )
                    await db.execute(
                        "DELETE FROM daily_plan_items WHERE user_id=? AND plan_date=?",
                        (uid, today_local.isoformat())
                    )
                    await db.commit()
                except Exception as e:
                    logging.warning(f"plan resend meta store failed for {tg_id}: {e}")
                    return False, str(e)

        return True, None
    except Exception as e:
        logging.exception("send_morning_plan_to_user error: %s", e)
        return False, str(e)

# =========================
# Google Sheets: тонкий синхронизатор (Gantt + KPI + листы по сотрудникам)
# =========================

from pathlib import Path
import os
import json
import gspread_asyncio
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

# --- Google Sheets: единый клиент и open ---
_GS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def _require_gs_config():
    """Валидируем env и возвращаем (gs_id, absolute_credentials_path)."""
    from pathlib import Path
    gs_id = os.getenv("GSHEET_ID", "").strip()
    cred_env = os.getenv("GOOGLE_CREDENTIALS_FILE", "").strip()
    errors = []
    if not gs_id:
        errors.append("GSHEET_ID пуст (нет ID таблицы).")
    if not cred_env:
        errors.append("GOOGLE_CREDENTIALS_FILE пуст (нет пути к service_account.json).")
    cred_path = Path(cred_env) if cred_env else None
    if cred_path and not cred_path.is_absolute():
        cred_path = Path(__file__).resolve().parent / cred_path
    if cred_path and not cred_path.exists():
        errors.append(f"Файл кредов не найден: {cred_path}")
    if errors:
        raise RuntimeError("Конфигурация Google Sheets не задана:\n- " + "\n- ".join(errors))
    return gs_id, str(cred_path)

def _agcm_builder(abs_credentials_path: str):
    from google.oauth2.service_account import Credentials
    def _creds():
        return Credentials.from_service_account_file(abs_credentials_path, scopes=_GS_SCOPES)
    return gspread_asyncio.AsyncioGspreadClientManager(_creds)

async def _gs_ensure_ws(sh, title: str, rows: int = 100, cols: int = 20):
    """Гарантированно получить лист по имени; создать при отсутствии."""
    from gspread.exceptions import WorksheetNotFound
    try:
        return await sh.worksheet(title)
    except WorksheetNotFound:
        return await sh.add_worksheet(title=title, rows=rows, cols=cols)

# кэш клиента
_agcm_cache = {"path": None, "mgr": None}

async def _gs_open():
    """
    Возвращает Spreadsheet. Всегда проверяем конфиг и резолвим путь к JSON.
    """
    gs_id, cred_abs = _require_gs_config()
    if _agcm_cache["mgr"] is None or _agcm_cache["path"] != cred_abs:
        _agcm_cache["mgr"] = _agcm_builder(cred_abs)
        _agcm_cache["path"] = cred_abs
    agc = await _agcm_cache["mgr"].authorize()
    sh = await agc.open_by_key(gs_id)
    return sh

# кешируем менеджер на процесс, чтобы не создавать заново каждый раз
_agcm_cache = {"path": None, "mgr": None}

async def _gs_ensure_ws(sh, title: str, rows: int = 100, cols: int = 20):
    """
    Гарантированно получить worksheet с именем `title`. Создать если нет.
    """
    try:
        ws = await sh.worksheet(title)
        return ws
    except Exception:
        pass
    # создаём
    try:
        ws = await sh.add_worksheet(title=title, rows=rows, cols=cols)
        return ws
    except Exception as e:
        # возможно, только что создали/гонка — пробуем ещё раз получить
        try:
            return await sh.worksheet(title)
        except Exception:
            raise e

async def _ws_clear_and_set_header(ws, header: list[str]):
    await ws.clear()
    if header:
        await ws.update('A1', [header], value_input_option="USER_ENTERED")

def _overdue_minutes(deadline_iso: str | None, completed_at_iso: str | None) -> int | None:
    """
    Просрочка в минутах: если задача завершена — по completed_at, иначе по now().
    Если дедлайна нет — None.
    """
    if not deadline_iso:
        return None
    try:
        dl = dateparser.parse(deadline_iso)
        end = dateparser.parse(completed_at_iso) if completed_at_iso else datetime.now(UTC)
        diff = int((end - dl).total_seconds() // 60)
        return max(0, diff)
    except Exception:
        return None

async def _fetch_gantt_rows(db) -> list[list]:
    """
    Подготовить строки для Gantt и персональных листов.
    Колонки: Сотр. | Задача | Дедлайн | Факт | Просрочка (мин) | Статус | Комментарий | Проект | Создана | Сдвиги
    """
    # count postpones per task
    postpone_counts = {}
    curp = await db.execute("""
        SELECT task_id, COUNT(*) 
        FROM task_events 
        WHERE event='postpone'
        GROUP BY task_id
    """)
    for tid, cnt in await curp.fetchall():
        postpone_counts[int(tid)] = int(cnt or 0)

    cur = await db.execute("""
        SELECT t.id, u.full_name, u.tg_id, t.description, t.deadline, t.completed_at,
               t.status, t.last_postpone_reason, t.created_at
        FROM tasks t
        JOIN users u ON u.id = t.user_id
        ORDER BY COALESCE(t.deadline, '9999'), t.id
    """)
    rows = await cur.fetchall()

    out = []
    for (tid, full_name, tg_id, desc, deadline, completed_at, status, last_reason, created_at) in rows:
        overdue = _overdue_minutes(deadline, completed_at)
        # "Проект" — у вас пока не связано; оставим пустым
        project = ""
        # Комментарий — последняя причина переноса (если была)
        comment = last_reason or ""
        employee = full_name or f"user_{tg_id}"
        out.append([
            employee,
            f"#{tid} {desc or ''}",
            fmt_dt_local(deadline) if deadline else "",
            fmt_dt_local(completed_at) if completed_at else "",
            overdue if overdue is not None else "",
            status_human(status or "new"),
            comment,
            project,
            fmt_dt_local(created_at) if created_at else "",
            postpone_counts.get(int(tid), 0),
        ])
    return out

async def _write_ws_table(ws, header: list[str], rows: list[list]):
    await _ws_clear_and_set_header(ws, header)
    if rows:
        # пишем пачкой начиная со 2-й строки
        rng = f"A2"
        await ws.update(rng, rows, value_input_option="USER_ENTERED")

async def _apply_task_cf(sh, ws):
    """
    Условное форматирование колонки B (Задача) по значениям колонки E (Просрочка, мин).
      E = 0        -> зелёный
      1 <= E <=120 -> жёлтый
      E > 120      -> красный

    Функция автоматически подбирает формат формул под локаль листа:
    1) сначала пробует EN-формы (AND, запятые),
    2) при ошибке INVALID_ARGUMENT — пробует RU-формы (И, точки с запятой).
    """
    sheet_id = ws.id

    rng_B = {
        "sheetId": sheet_id,
        "startRowIndex": 1,    # со 2-й строки
        "startColumnIndex": 1, # B (0-based)
        "endColumnIndex": 2    # только колонка B
    }

    def _rules(en: bool):
        # Формулы с ведущим '=' — это обязательно для CUSTOM_FORMULA
        if en:
            f_green  = "=$E2=0"
            f_yellow = "=AND($E2>0,$E2<=120)"
            f_red    = "=$E2>120"
        else:
            # RU локаль: локализованное имя функции и ';' как разделитель
            f_green  = "=$E2=0"
            f_yellow = "=И($E2>0;$E2<=120)"
            f_red    = "=$E2>120"

        return [
            {
                "addConditionalFormatRule": {
                    "index": 0,
                    "rule": {
                        "ranges": [rng_B],
                        "booleanRule": {
                            "condition": {
                                "type": "CUSTOM_FORMULA",
                                "values": [{"userEnteredValue": f_green}]
                            },
                            "format": {"backgroundColor": {"red": 0.85, "green": 0.97, "blue": 0.85}}
                        }
                    }
                }
            },
            {
                "addConditionalFormatRule": {
                    "index": 0,
                    "rule": {
                        "ranges": [rng_B],
                        "booleanRule": {
                            "condition": {
                                "type": "CUSTOM_FORMULA",
                                "values": [{"userEnteredValue": f_yellow}]
                            },
                            "format": {"backgroundColor": {"red": 1.0, "green": 0.97, "blue": 0.80}}
                        }
                    }
                }
            },
            {
                "addConditionalFormatRule": {
                    "index": 0,
                    "rule": {
                        "ranges": [rng_B],
                        "booleanRule": {
                            "condition": {
                                "type": "CUSTOM_FORMULA",
                                "values": [{"userEnteredValue": f_red}]
                            },
                            "format": {"backgroundColor": {"red": 1.0, "green": 0.80, "blue": 0.80}}
                        }
                    }
                }
            },
        ]

    # Попытка 1: EN (AND, запятые)
    try:
        await sh.batch_update({"requests": _rules(en=True)})
        return
    except Exception as e1:
        # если ошибка не про неверную формулу — пробрасываем дальше
        if "INVALID_ARGUMENT" not in str(e1) and "Invalid ConditionValue.userEnteredValue" not in str(e1):
            raise

    # Попытка 2: RU (И, точки с запятой)
    await sh.batch_update({"requests": _rules(en=False)})

async def _sync_gantt_and_personal(sh, db):
    header = ["Сотр.", "Задача", "Дедлайн", "Факт", "Просрочка (мин)", "Статус", "Комментарий", "Проект", "Создана", "Сдвиги"]
    all_rows = await _fetch_gantt_rows(db)

    # --- Gantt (общая)
    ws_gantt = await _gs_ensure_ws(sh, "Gantt", rows=max(100, len(all_rows)+10), cols=len(header)+2)
    await _write_ws_table(ws_gantt, header, all_rows)

    # --- Листы по сотрудникам
    # группируем
    by_emp = {}
    for r in all_rows:
        emp = r[0] or "—"
        by_emp.setdefault(emp, []).append(r)

    for emp, rows in by_emp.items():
        ws = await _gs_ensure_ws(sh, emp[:100], rows=max(50, len(rows)+5), cols=len(header)+2)
        await _write_ws_table(ws, header, rows)

async def _compute_kpi(db) -> list[list]:
    """
    KPI-таблица: по сотруднику за неделю и за месяц.
    Столбцы: Сотр. | Период | В срок | Просрочено | Медиана просрочки (мин) | % on-time | Streak (дней без просрочек)
    """
    now = datetime.now(UTC)
    week_ago = (now - timedelta(days=7)).isoformat()
    month_ago = (now - timedelta(days=30)).isoformat()

    # заберём завершённые с delay_minutes
    cur = await db.execute("""
        SELECT u.full_name, u.tg_id, t.completed_at, COALESCE(t.delay_minutes, 0)
        FROM tasks t
        JOIN users u ON u.id = t.user_id
        WHERE t.status='done' AND t.completed_at IS NOT NULL
    """)
    rows = await cur.fetchall()

    from statistics import median

    # аккумуляторы
    data = {}  # (emp, period) -> list[delay_minutes]
    for full, tg, completed_at, delay in rows:
        emp = (full or f"user_{tg}")
        delay = int(delay or 0)
        # месяц
        if completed_at >= month_ago:
            data.setdefault((emp, "месяц"), []).append(delay)
        # неделя
        if completed_at >= week_ago:
            data.setdefault((emp, "неделя"), []).append(delay)

    # streak «дней без просрочек»: считаем по последним дням, пока нет задач с delay>0
    # упрощённо: считаем подряд от вчера назад по датам completed_at
    streak_cache = {}
    for emp_period in list(data.keys()):
        emp = emp_period[0]
        if emp in streak_cache:
            continue
        cur2 = await db.execute("""
            SELECT DATE(t.completed_at), MAX(CASE WHEN COALESCE(t.delay_minutes,0)>0 THEN 1 ELSE 0 END)
            FROM tasks t
            JOIN users u ON u.id = t.user_id
            WHERE u.full_name=? OR u.full_name IS NULL
            GROUP BY DATE(t.completed_at)
            ORDER BY DATE(t.completed_at) DESC
            LIMIT 60
        """, (emp,))
        days = await cur2.fetchall()
        s = 0
        # считаем от сегодняшней даты назад: если сегодня нет записей — streak не сбиваем
        for d, has_late in days:
            if int(has_late or 0) == 0:
                s += 1
            else:
                break
        streak_cache[emp] = s

    out = []
    for (emp, period), delays in sorted(data.items()):
        total = len(delays)
        late = sum(1 for x in delays if x > 0)
        ontime = total - late
        med = (median([x for x in delays if x > 0]) if late else 0)
        pct = round(ontime / total * 100, 1) if total else 100.0
        out.append([
            emp, period, ontime, late, med, pct, streak_cache.get(emp, 0)
        ])
    return out

async def _sync_kpi(sh, db):
    header = ["Сотр.", "Период", "В срок", "Просрочено", "Медиана просрочки (мин)", "% on-time", "Streak (дней без просрочек)"]
    rows = await _compute_kpi(db)
    ws = await _gs_ensure_ws(sh, "KPI", rows=max(50, len(rows)+5), cols=len(header)+2)
    await _write_ws_table(ws, header, rows)

async def gs_sync_all():
    """
    Полная синхронизация:
      1) Общий лист "Gantt"
      2) KPI
      3) Персональные листы по сотрудникам на текущий месяц
    """
    _require_gs_config()
    sh = await _gs_open()

    # 1) Общий Gantt
    GANTT_HEADER = ["Сотр.", "Задача", "Дедлайн", "Факт", "Просрочка (мин)"]
    async with aiosqlite.connect(DB_PATH) as db:
        rows = await _fetch_gantt_rows(db)  # у тебя уже есть эта функция

    ws_gantt = await _gs_ensure_ws(
        sh, "Gantt",
        rows=max(2000, (len(rows) + 10) if rows else 2000),
        cols=len(GANTT_HEADER) + 2
    )
    await _ws_clear_and_set_header(ws_gantt, GANTT_HEADER)
    if rows:
        await ws_gantt.update("A2", rows, value_input_option="USER_ENTERED")

    # (опционально) применить CF для общей таблицы, если используешь
    if "_apply_task_cf" in globals():
        try:
            await _apply_task_cf(sh, ws_gantt)
        except Exception as _e:
            logging.warning("CF for Gantt skipped: %s", _e)

    # 2) KPI
    async with aiosqlite.connect(DB_PATH) as db:
        await _sync_kpi(sh, db)

    # 3) Персональные листы за текущий месяц
    await _sync_emp_gantts(sh)

# ===== Персональные листы Gantt по сотрудникам =====

def _month_days_header(year: int, month: int) -> list[str]:
    """Заголовок-ряд: 'Имя | Даты на весь месяц | 1.7 | 2.7 | ...'"""
    from calendar import monthrange
    days = monthrange(year, month)[1]
    # первая колонка — 'Проект/Задача'
    hdr = ["Проект/Задача"]
    for d in range(1, days + 1):
        hdr.append(f"{d}.{month}")
    return hdr

def _emp_ws_title(full_name: str) -> str:
    """Название листа = имя сотрудника (урезаем до 80 символов для Google Sheets)."""
    t = (full_name or "Employee").strip()
    return t[:80]

async def _ensure_emp_month_ws(sh, full_name: str, year: int, month: int):
    """Создать (если нет) и подготовить лист сотрудника на текущий месяц."""
    title = _emp_ws_title(full_name)
    ws = await _gs_ensure_ws(sh, title, rows=200, cols=40)

    # шапка: строки 1-2
    header = _month_days_header(year, month)
    await ws.clear()
    await ws.update("A1", [["Имя сотрудника", full_name]])
    await ws.update("A2", [header], value_input_option="USER_ENTERED")

    # заморозим верхние 2 строки и первый столбец
    await sh.batch_update({
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": ws.id,
                        "gridProperties": {"frozenRowCount": 2, "frozenColumnCount": 1},
                    },
                    "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
                }
            }
        ]
    })
    return ws

def _rgb(r: int, g: int, b: int) -> dict:
    return {"red": r/255.0, "green": g/255.0, "blue": b/255.0}

GREEN = _rgb(46, 204, 113)   # в срок
YELLOW = _rgb(255, 224, 102) # 1–120 мин
RED    = _rgb(244,  67,  54) # >120 мин
LIGHT  = _rgb(238, 238, 238)
# Светло-голубой для второго дня задачи
BLUE = {"red": 0.80, "green": 0.90, "blue": 1.00}


async def _fill_emp_month(sh, ws, rows_for_emp: list[dict], year: int, month: int):
    """
    rows_for_emp: список словарей с полями:
      task_id, project, description, deadline_iso, completed_at_iso, overdue_min
    Рисуем таблицу: строки — уникальные project/description, столбцы — дни месяца.
    Цвет ячейки по просрочке; в заметке — детали с временем завершения.
    """
    from calendar import monthrange
    days = monthrange(year, month)[1]

    # Собираем уникальные "проекты/задачи" в порядке появления
    lines: list[str] = []
    def _key(r):
        p = (r.get("project") or "").strip()
        d = (r.get("description") or "").strip()
        return p or d or f"Задача #{r.get('task_id')}"
    for r in rows_for_emp:
        k = _key(r)
        if k not in lines:
            lines.append(k)
    if not lines:
        return  # нет строк — оставляем только шапку

    # Запишем левый столбец со списком задач (начиная с A3)
    table = [[name] + [""] * days for name in lines]
    await ws.update("A3", table, value_input_option="USER_ENTERED")

    # Индексы для быстрого доступа
    line_index = {name: i for i, name in enumerate(lines)}  # строка (0..)
    start_row = 2  # zero-based (строка 3)
    start_col = 1  # zero-based (колонка B)

    requests = []
    notes = []

    for r in rows_for_emp:
        k = _key(r)
        i = line_index[k]
        dt_src = r.get("completed_at_iso") or r.get("deadline_iso")
        if not dt_src:
            continue
        try:
            dt = dateparser.parse(dt_src)
        except Exception:
            continue
        if dt.year != year or dt.month != month:
            continue
        d = dt.day  # 1..days

        row_index = start_row + i
        col_index = start_col + (d - 1)

        overdue = r.get("overdue_min")
        color = GREEN
        if overdue is None:
            color = LIGHT
        elif overdue > 120:
            color = RED
        elif overdue > 0:
            color = YELLOW

        # Красим одну ячейку
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": row_index,
                    "endRowIndex": row_index + 1,
                    "startColumnIndex": col_index,
                    "endColumnIndex": col_index + 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": color,
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment)"
            }
        })

        # Заметка с деталями
        note = (
            f"#{r.get('task_id')} — {r.get('description') or r.get('project')}\n"
            f"Дедлайн: {fmt_dt_local(r.get('deadline_iso'))}\n"
            f"Факт:    {fmt_dt_local(r.get('completed_at_iso'))}\n"
            f"Просрочка: {overdue if overdue is not None else '—'} мин"
        )
        notes.append({
            "updateCells": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": row_index,
                    "endRowIndex": row_index + 1,
                    "startColumnIndex": col_index,
                    "endColumnIndex": col_index + 1,
                },
                "rows": [{"values": [{"note": note}]}],
                "fields": "note"
            }
        })

    if requests:
        await sh.batch_update({"requests": requests})
    if notes:
        await sh.batch_update({"requests": notes})

async def _collect_emp_rows_for_month(year: int, month: int) -> dict[str, list[dict]]:
    """
    Достаём из БД завершённые задачи за месяц по сотрудникам.
    Возвращаем: { 'ФИО': [ {...}, ... ], ... }
    Работает без поля t.project (которого нет в схеме).
    """
    start = datetime(year, month, 1, tzinfo=UTC)
    end = datetime(year + (1 if month == 12 else 0), 1 if month == 12 else month + 1, 1, tzinfo=UTC)

    # Выбираем только существующие столбцы: id, user_id, description, deadline, completed_at
    q = """
        SELECT t.id,
               u.full_name,
               t.description,
               t.deadline,
               t.completed_at
        FROM tasks t
        JOIN users u ON u.id = t.user_id
        WHERE t.completed_at IS NOT NULL
          AND t.completed_at >= ?
          AND t.completed_at <  ?
        ORDER BY u.full_name, t.completed_at
    """

    out: dict[str, list[dict]] = {}
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(q, (start.isoformat(), end.isoformat()))
        rows = await cur.fetchall()

    # rows: (task_id, full_name, description, deadline_iso, completed_at_iso)
    for (task_id, full, descr, deadline_iso, completed_at_iso) in rows:
        overdue = _overdue_minutes(deadline_iso, completed_at_iso)
        emp = (full or "Employee").strip() or "Employee"

        out.setdefault(emp, []).append({
            "task_id": task_id,
            # В схеме нет project — используем пустую строку (или можно извлечь из description по своим правилам)
            "project": "",
            "description": descr or "",
            "deadline_iso": deadline_iso,
            "completed_at_iso": completed_at_iso,
            "overdue_min": overdue,
        })

    return out

async def _sync_emp_gantts(sh):
    """Построить/обновить персональные листы за текущий месяц по всем сотрудникам."""
    now = datetime.now(UTC).astimezone(LOCAL_TZ)
    year, month = now.year, now.month
    emp_rows = await _collect_emp_rows_for_month(year, month)
    for full_name, rows in emp_rows.items():
        ws = await _ensure_emp_month_ws(sh, full_name, year, month)
        await _fill_emp_month(sh, ws, rows, year, month)

# =========================
# Планировщик напоминаний
# =========================
async def fetch_due_tasks(db):
    now_iso = datetime.now(UTC).isoformat()
    # ВАЖНО: только по next_reminder_at, без OR deadline<=now — иначе спам каждую минуту при просрочке
    q = """
    SELECT t.id, t.user_id, t.description, t.deadline, t.status, t.started_at,
           t.next_reminder_at, u.tg_id
    FROM tasks t
    JOIN users u ON u.id = t.user_id
    WHERE t.status != 'done'
      AND (t.next_reminder_at IS NULL OR t.next_reminder_at <= ?)
    """
    cur = await db.execute(q, (now_iso,))
    rows = await cur.fetchall()
    keys = ["id","user_id","description","deadline","status","started_at","next_reminder_at","tg_id"]
    return [dict(zip(keys, r)) for r in rows]

async def mark_reminded(db, task_id: int, next_iso: str | None = None, hours: int = 1):
    now = datetime.now(UTC)
    if next_iso:
        next_at = dateparser.parse(next_iso)
    else:
        next_at = now + timedelta(hours=hours)
    next_at = clamp_to_work_hours(next_at)
    await db.execute(
        "UPDATE tasks SET last_reminder_at=?, next_reminder_at=? WHERE id=?",
        (now.isoformat(), next_at.isoformat(), task_id)
    )
    await db.commit()

async def scheduler_job():
    logging.info("Scheduler tick")
    now_utc = datetime.now(UTC)

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT t.id, t.user_id, t.description, t.status, t.deadline,
                   t.next_reminder_at, u.tg_id, u.full_name
            FROM tasks t
            JOIN users u ON u.id = t.user_id
            WHERE t.status!='done'
              AND (
                    (t.next_reminder_at IS NOT NULL AND t.next_reminder_at <= ?)
                 OR (t.deadline IS NOT NULL AND t.deadline <= ? AND t.next_reminder_at IS NULL)
              )
            ORDER BY t.id ASC
        """, (now_utc.isoformat(), now_utc.isoformat()))
        tasks = await cur.fetchall()

        for tid, user_id, desc, status, dl_iso, next_iso, tg_id, emp_name in tasks:
            try:
                dl_dt = None
                if dl_iso:
                    dl_dt = dateparser.parse(dl_iso)

                # 1) Ровно в дедлайн: сообщение «время вышло»
                if dl_dt and abs((now_utc - dl_dt).total_seconds()) < 60:
                    text_emp = text_deadline_reached(tid, desc or "", dl_iso)
                    resp = await bot.send_message(
                        tg_id, text_emp, parse_mode="HTML", reply_markup=_kb_overdue(tid).as_markup()
                    )
                    # Планируем проверку просрочки через 5 минут
                    next_check = (dl_dt + timedelta(minutes=5)).isoformat()
                    await db.execute(
                        "UPDATE tasks SET last_reminder_msg_id=?, next_reminder_at=? WHERE id=?",
                        (resp.message_id, next_check, tid),
                    )
                    await db.commit()
                    continue

                # 2) Просрочка (прошло 5+ минут после дедлайна)
                if dl_dt and (now_utc - dl_dt) > timedelta(minutes=5):
                    text_emp = text_overdue_emp(emp_name, tid, desc or "", dl_iso)
                    await bot.send_message(
                        tg_id, text_emp, parse_mode="HTML", reply_markup=_kb_overdue(tid).as_markup()
                    )

                    # Руководителям — оповещение
                    mgr_ids = await get_manager_tg_ids(db, user_id)
                    if mgr_ids:
                        mgr_text = text_overdue_mgr(emp_name, tid, desc or "", dl_iso)
                        for mid in mgr_ids:
                            try:
                                await bot.send_message(mid, mgr_text, parse_mode="HTML")
                            except Exception as e:
                                logging.warning(f"notify manager failed (overdue) tg_id={mid}: {e}")

                    # Следующая проверка через час
                    next_check = (now_utc + timedelta(hours=1)).isoformat()
                    await db.execute("UPDATE tasks SET next_reminder_at=? WHERE id=?", (next_check, tid))
                    await db.commit()

            except Exception as e:
                logging.warning(f"scheduler loop failed for task {tid}: {e}")

# ——— шедулер
from apscheduler.schedulers.asyncio import AsyncIOScheduler

def start_scheduler():
    """
    Планировщик для асинхронного окружения бота.
    Запускаем:
      • reminders_job — проверка дедлайнов/просрочек (каждую минуту);
      • gsync_job     — синхронизация Google Sheets (период из .env);
      • proj_sync_job — синхронизация просрочек по проектам.
    """
    import os
    import logging

    # валидируем конфиг; если нет API/ID — не ставим gsync_job
    try:
        _require_gs_config()
        gs_ready = True
    except Exception as e:
        logging.warning("GS config is not ready: %s", e)
        gs_ready = False

    # период из .env (минуты), минимум 1
    try:
        period_min = max(1, int(os.getenv("GSYNC_PERIOD_MIN", "5")))
    except Exception:
        period_min = 5

    sched = AsyncIOScheduler()

    # 1) Проверка дедлайнов/просрочек — КАЖДУЮ МИНУТУ
    sched.add_job(
        scheduler_job,
        trigger="interval",
        seconds=60,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=30,
        id="reminders_job",
        replace_existing=True,
    )
    logging.info("Reminders job scheduled every 60 sec")

    # 2) Синхронизация Google Sheets — если конфиг готов
    if gs_ready:
        sched.add_job(
            gs_sync_all,
            trigger="interval",
            minutes=period_min,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=30,
            id="gsync_job",
            replace_existing=True,
        )
        logging.info("Google Sheets sync job scheduled every %s min", period_min)
    else:
        logging.info("Google Sheets sync job NOT scheduled (config not ready)")

    # 3) Синхронизация просрочек по проектам — ВСЕГДА
    sched.add_job(
        projects_sync_overdues,
        trigger="interval",
        minutes=period_min,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=30,
        id="proj_sync_job",
        replace_existing=True,
    )

    sched.start()
    logging.info("Scheduler started")
    return sched

# =========================
# Точка входа
# =========================

from aiogram.types import BotCommand, BotCommandScopeAllPrivateChats, BotCommandScopeChat

async def setup_bot_commands():
    # Базовые команды для всех
    base_cmds = [
        BotCommand(command="start", description="Старт"),
        BotCommand(command="help", description="Помощь"),
        BotCommand(command="id", description="Мой Telegram ID"),
        BotCommand(command="register", description="Регистрация"),
        BotCommand(command="manager", description="Меню руководителя"),
        BotCommand(command="my", description="Мои задачи (список)"),
        BotCommand(command="gsync", description="Синхронизация в Google Sheets"),
        BotCommand(command="gsdebug", description="Диагностика Google Sheets"),
    ]
    await bot.set_my_commands(base_cmds, scope=BotCommandScopeAllPrivateChats())

    # Отдельный набор для разработчика (добавим служебные)
    if DEVELOPER_TG_ID:
        dev_cmds = base_cmds + [
            BotCommand(command="rehire", description="Восстановить доступ пользователю"),
            BotCommand(command="resetreg", description="Сбросить регистрацию пользователю"),
            BotCommand(command="forcecheck", description="Проверить напоминания сейчас"),
            BotCommand(command="taskinfo", description="Диагностика задачи"),
        ]
        await bot.set_my_commands(dev_cmds, scope=BotCommandScopeChat(chat_id=DEVELOPER_TG_ID))

# =========================
# Точка входа и ловец ошибок
# =========================
from aiogram.types.error_event import ErrorEvent  # корректный импорт для aiogram v3

async def main():
    await init_db()
    await setup_bot_commands()
    start_scheduler()
    dp.update.middleware(AccessMiddleware())
    await bot.delete_webhook(drop_pending_updates=True)

    # Глобальный ловец ошибок, чтобы видеть исключения из callback-хэндлеров тоже
    @dp.errors()
    async def on_error(event: ErrorEvent):
        logging.error("Unhandled error: %s", event.exception, exc_info=event.exception)
        # Аккуратно сообщаем пользователю, если это callback/message из чата
        try:
            cq = getattr(event.update, "callback_query", None)
            if cq:
                try:
                    await cq.answer("Произошла ошибка. Уже чиним 🧰", show_alert=False)
                except Exception:
                    # если нельзя ответить — просто игнорируем
                    pass
        except Exception:
            pass

    # ВАЖНО: закрываем HTTP-сессию бота ПОСЛЕ polling — пока цикл ещё жив
    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
    finally:
        try:
            await bot.session.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Бот остановлен вручную.")

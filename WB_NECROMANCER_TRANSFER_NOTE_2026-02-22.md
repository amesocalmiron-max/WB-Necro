# WB Revival v2 «Некромант» — перенос в новый чат (без потерь)
Дата: 2026-02-22  
Таймзона: Europe/Oslo

> Цель файла: кинуть в новый чат **одним сообщением** вместе со скриптом/входными файлами и продолжить работу без “а что мы делали вообще”.

---

## 0) TL;DR
Мы строим пайплайн, который решает: **есть ли смысл оживлять мёртвый SKU на WB** (чехлы), НЕ по “нашей цене/позициям”, а по **фактам рынка**.

Для каждого SKU считаем **2 рынка**:
1) **phone-market**: жив ли спрос на чехлы под модель телефона.
2) **type-market**: жив ли спрос на конкретный тип: **TPU силиконовый чехол с карманом под карту**.

Итоговый вердикт:
- `REVIVE_FAST` / `REVIVE_REWORK` / `CLONE_NEW_CARD` / `DROP`
+ `risk_flags` (EN) + `backlog` (RU) + `rationale` (RU).

LLM — **не источник данных**. LLM только:
- (опционально) обогащает запросы (Stage D),
- (опционально) помогает на пограничной релевантности (Stage H),
- (опционально) делает **красивую выжимку** (exec summary) в Stage M по уже посчитанным фактам.

---

## 1) Границы мира (scope) и входные данные
- Работаем **строго по 64 SKU** из файла:
  - `WB_INPUT_64_FROM_POCKETS_POD.xlsx`, лист `INPUT_64`
- Колонки входа (минимум):
  - `nm_id`, `vendor_code`, `name`
  - `potential_qty` — опционально
- `nm_id` везде хранить как **строку**.

---

## 2) VPN и разделение “WB vs LLM”
Проблема: WB из “европейского VPN” может банить/резать доступ.  
Решение:
- **WB-стадии** гоняем **без VPN** (или РФ сеть).
- **LLM-стадии** можно гонять **под VPN** (Норвегия и т.п.), потому что LLM **не ходит в WB**.

UX-фичи:
- пауза перед первой LLM стадией: `--pause-before-llm`
- пауза между стадиями: `--pause-between-stages`
- автоподтверждение: `--yes`

---

## 3) Каноничные артефакты и кэш
Храним всё в `out_dir` запуска (например `RUN_001/`).

**Артефакты (JSONL/JSON):**
- `run_manifest.json`
- `own_norm.jsonl`, `own_errors.jsonl`
- `intent.jsonl`
- `queries_raw.jsonl`, `queries_valid.jsonl`
- `competitor_pool.jsonl`
- `competitor_lite.jsonl`
- `relevance.jsonl`
- `market_pulse.jsonl` (+ `*_errors.jsonl` где нужно)
- `supply_structure.jsonl`
- `cluster_verdicts.jsonl`
- `decisions.jsonl`

**Кэш WB:**
- `.wb_cache/` (own, serp, competitors, reviews и т.п.) — экономия запросов, ускорение `--resume`.

**Отчёты:**
- `WB_NECROMANCER_REPORT.xlsx`
- `WB_NECROMANCER_REPORT.html` (самодостаточный “дашборд”).

---

## 4) Матрица решений v2 (самое важное)
1) Если **phone-market DEAD** → `DROP`
2) Если **phone-market ALIVE**, но **type-market DEAD** → обычно `DROP`
   - (опционально флаг `ALT_STRATEGY`: “модель жива, но карман не востребован”)
3) Если **type-market ALIVE**:
   - если карма карточки токсична (низкий рейтинг при достаточном числе отзывов) → `CLONE_NEW_CARD`
   - иначе → `REVIVE_FAST` или `REVIVE_REWORK` (по контент-долгу/задачам)

Правило: **own_price в решениях игнорируем**.

---

## 5) Пайплайн A–M: что делает стадия и что ей нужно
Нотация:
- `WB` = ходим в Wildberries (желательно VPN OFF)
- `LLM` = ходим к провайдеру LLM (можно VPN ON)
- `LOCAL` = локальная обработка

| Stage | Тип | VPN | LLM | Суть | Выход |
|------:|-----|-----|-----|------|------|
| A | LOCAL | ANY | нет | читаем INPUT, фиксируем scope | run_manifest.json |
| B | WB | OFF | нет | тянем own-card (v4/v1), deep-card best-effort | own_norm.jsonl + own_errors.jsonl |
| C | LOCAL | ANY | нет | извлекаем intent: модель телефона, TPU, pocket/card, карма | intent.jsonl |
| D | LOCAL/LLM (опц) | ON (если LLM) | да (опц) | генерим запросы для phone/type, rules-first | queries_raw.jsonl + queries_valid.jsonl |
| E | WB | OFF | нет | SERP по запросам, валидация/отбор 2–5 запросов | (внутренние snapshots) + queries_valid.jsonl |
| F | LOCAL | ANY | нет | пул конкурентов (leaders + closest, дедуп, диверсификация) | competitor_pool.jsonl |
| G | WB | OFF | нет | lite fetch конкурентов (цены, рейтинг, продавец, стоки proxy) | competitor_lite.jsonl |
| H | LOCAL/LLM (опц) | ON (если LLM) | да (опц) | rules-first KEEP/DROP, LLM только на borderline | relevance.jsonl |
| I | WB | OFF | нет | Market Pulse по отзывам (30/90 дней, days_since_last) + кэш | market_pulse.jsonl |
| J | LOCAL | ANY | нет | Supply/Structure: робастная цена, outliers, продавцы, стоки proxy | supply_structure.jsonl |
| K | LOCAL | ANY | нет | вердикты ALIVE/SLOW/DEAD + confidence по кластерам | cluster_verdicts.jsonl |
| L | LOCAL | ANY | нет | применяем матрицу решений v2 + backlog | decisions.jsonl |
| M | LOCAL/LLM (опц) | ON (если LLM) | да (опц) | XLSX+HTML отчёты + exec summary (best-effort) | .xlsx + .html (+ exec_summary.json если делаем) |

---

## 6) Что случилось в этом чате (история проблем и фиксов)
### 6.1 Зачем был rewrite
Старый файл был “франкенштейн”: дубли утилит, конфликты имён, неявные зависимости (функции ниже по файлу), разный парсинг в разных стадиях.

Пошли по пути: **переписать с нормальной структурой**.

### 6.2 Баги, которые реально всплыли на прогоне
1) **Меню/паузы**: первая rewrite-версия не включала паузы по умолчанию → “пролетает всё говно”.
   - Решение: `MENUIX` ветка, где меню реально включает паузы, плюс `--pause-before-llm`, `--yes`.

2) **Stage B: own_card_missing_or_empty**, хотя HTTP 200:
   - Причина: WB иногда отдаёт карточку как `payload.products` (а не `data.products`).
   - Решение: расширили `parse_card_v4()` (учёт `payload.products` / `payload.data.products`).

3) Ранний “патч” сломал синтаксис (ошибка типа `products = js["products"]if not products:`) — признано и исправлено.

4) Resume-нюанс:
   - `--resume` пропускает **nm_id**, которые уже записаны в JSONL.
   - Если Stage B записал пустые записи, простой `--resume` их не перезапишет.
   - Лечение: новый `out_dir` или удалить Stage B артефакты и перезапустить с B.

### 6.3 Exec summary в финале
В старом подходе (`wb_revive`) была LLM-выжимка для “человеческого” отчёта. В rewrite мы это вернули:
- Stage M: **опционально** LLM делает exec summary строго по FACTS (best-effort).

---

## 7) Текущие файлы/версии (что реально использовать)
### Основной “rewrite + меню + паузы + B-parse fix + stage meta + exec summary”
- **Рекомендуемый текущий файл:**  
  `wb_necromancer_v2_REWRITE_MENUFIX_FIXED_BPARSE_v4.py`

### Прочие файлы (история/бэкапы)
- `wb_necromancer_v2_REWRITE.py` — ранний rewrite (меню было слабее)
- `wb_necromancer_v2_REWRITE_MENUFIX.py` — первая правка меню
- `wb_necromancer_v2_REWRITE_MENUFIX_FIXED_BPARSE.py` / `_v2` / `_v3` — промежуточные фиксы парсинга/стадий
- Старый “франкен, но рабочий по ощущениям”:  
  `wb_necromancer_v2_FINAL_FIXED_REFACTORED_FIXED_RU.py`

---

## 8) Как запускать (типовые сценарии)
### 8.1 Через меню (рекомендуется)
```powershell
python wb_necromancer_v2_REWRITE_MENUFIX_FIXED_BPARSE_v4.py
```

### 8.2 Полный прогон rules-only (без LLM, VPN не нужен)
```powershell
python wb_necromancer_v2_REWRITE_MENUFIX_FIXED_BPARSE_v4.py --out RUN_001 --start-stage A --end-stage M --resume
```

### 8.3 VPN-split руками (с паузой перед LLM)
```powershell
python wb_necromancer_v2_REWRITE_MENUFIX_FIXED_BPARSE_v4.py --out RUN_001 --start-stage A --end-stage G --resume --pause-between-stages
# включил VPN
python wb_necromancer_v2_REWRITE_MENUFIX_FIXED_BPARSE_v4.py --out RUN_001 --start-stage H --end-stage M --resume --pause-before-llm --pause-between-stages
```

### 8.4 Продолжить с Stage I и дальше с паузами
```powershell
python wb_necromancer_v2_REWRITE_MENUFIX_FIXED_BPARSE_v4.py --out RUN_001 --start-stage I --end-stage M --resume --pause-between-stages
```

---

## 9) Зависимости
Минимум:
```bash
pip install requests openpyxl
```

---

## 10) Известные “дальше улучшить” (но это после проверки фактов)
1) Fallback: если после H keep пустой → брать топ-N из SERP (иначе метрики пустые).
2) Пороги токсичности/матрица (min_fb/min_rating) вынести в конфиг/manifest.
3) Lazy-import `openpyxl` (чтобы без Stage M не падало, если отчёт не нужен).
4) “Удобство запуска” (отложено, уже зафиксировано в памяти):
   - `RUN_NECROMANCER.ps1/.bat`, профили `profiles/*.json`, `--preflight`, `--auto-vpn-split`, позже модульная упаковка/EXE/GUI.

---

## 11) Что прикладывать в новый чат
1) Этот файл (текущий): `WB_NECROMANCER_TRANSFER_NOTE_2026-02-22.md`
2) План-конспект: `WB_Revival_v2_Necromancer_plan_transfer.txt`
3) Доп. перенос-заметка: `WB_Revival_v2_Necromancer_TRANSFER_NOTE.md`
4) Скрипт: `wb_necromancer_v2_REWRITE_MENUFIX_FIXED_BPARSE_v4.py`
5) Входной Excel: `WB_INPUT_64_FROM_POCKETS_POD.xlsx`
6) (опционально) пример ошибки: `own_errors.jsonl` (как “на что наступили”)

---
Конец файла.

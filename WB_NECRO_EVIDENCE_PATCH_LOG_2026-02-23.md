# WB_Necro — Evidence-driven patch log (2026-02-23)

## 1) Почему внесены изменения

Основная причина патча — финальная аналитика Stage L/M не давала проверяемой «доказательной» цепочки решения:
- в части SKU отсутствовали ценовые метрики (`p10/p50/p90`) из-за неполного парсинга формата `card.wb.ru v4`;
- в отчёте встречались verdict уровня `REVIVE_FAST` при низком качестве данных (флаги LOW_CONFIDENCE*);
- конкурентный пул мог быть слишком широким и слабо релевантным, если запросы проходили только по мягкому условию (any);
- HTML/XLSX финал отображал агрегаты, но недостаточно «читалcя глазами» как decision trace с источниками и конкретными конкурентами.

Цель: сделать Stage L + Stage M «доказательными» и формально проверяемыми без регресса A–K.

---

## 2) Что именно изменено

### 2.1 Data foundation: цены и структура предложения

#### A1. `parse_card_v4` (устранение price n=0)
Доработан парсинг цен:
- добавлен проход по `sizes[].price` с извлечением вариантов `client/product`, `sale/total`, `basic`;
- сохранены иерархические fallback-источники на верхнем уровне (`priceU/salePriceU/...`);
- нормализация к рублям (защита от копеек/целых и «грязных» форматов);
- в выходную структуру добавлены поля:
  - `client_price_rub`
  - `sale_price_rub`
  - `list_price_rub`
  - `price_source`

Результат: Stage J получает более плотный ценовой массив; в отчёте уменьшается количество «—» в price block.

#### A2. Stage J (`stage_J_supply`): корректный сигнал качества цены
Изменения:
- расчёт цен конкурентов теперь опирается на `client_price_rub -> sale_price_rub -> list_price_rub` (с fallback на legacy);
- при `price_n < min_price_n` выставляется `LOW_CONFIDENCE_PRICE` вместо «немой» пустоты;
- добавлена передача `LOW_CONFIDENCE_RELEVANCE` из relevance-слоя, чтобы не терять качество входного пула.

Результат: проблемы с данными фиксируются как явные флаги и учитываются в решении.

---

### 2.2 Relevance hardening (Stage E)

Добавлены функции и ветки строгой проверки:
- `pass_must_all_groups` (AND по группам);
- `pass_any_group` (совместимость с текущей any-логикой);
- для каждого запроса считаются `pass_any_rate` и `pass_all_rate`;
- выбор запроса отдаёт приоритет строгой метрике (`pass_all_rate`), fallback на any используется только при провале strict;
- при fallback фиксируется `low_confidence_relevance` и `selection_mode`.

Результат: пул конкурентов меньше «размывается», а отчёт прозрачно показывает, когда пришлось деградировать до lenient-режима.

---

### 2.3 Decision governance (Stage L)

#### C1. Запрет `REVIVE_FAST` на плохих данных
В Stage L добавлено правило-гейт:
- если присутствует любой из флагов `LOW_CONFIDENCE`, `LOW_CONFIDENCE_PRICE`, `LOW_CONFIDENCE_RELEVANCE`,
- verdict `REVIVE_FAST` автоматически понижается до `REVIVE_REWORK`,
- в `rules_fired` добавляется `DECISION_CAP_FAST_BY_LOW_CONFIDENCE`.

Это предотвращает «ложно-оптимистичные» быстрые решения при неполной базе.

#### D1. Расширение decision trace
В `decisions.jsonl` добавлены структурные блоки:
- `decision.rules_fired`
- `decision.decision_trace`
- `decision.backlog_items` (при сохранении legacy `backlog` и `rationale`)

`decision_trace` содержит:
- phone/type market статус и confidence;
- pulse/supply метрики;
- issues, fired rules, evidence lines;
- data quality ограничения;
- final verdict logic.

Результат: отчёт рендерит не лозунги, а цепочку «факт → правило → решение».

---

### 2.4 Final report usability (Stage M)

#### HTML
- добавлена и заполнена evidence-колонка с `details` по PHONE/TYPE;
- отображаются fired rules, quality limits и top competitors;
- добавлены глобальные data-quality KPI:
  - % SKU с phone_model,
  - % SKU с TYPE оценкой (не skipped),
  - % с достаточным price_n,
  - % с reviews_unreachable,
  - общий индикатор run quality (LOW/MED/HIGH).

#### XLSX
Сохранена совместимость листа `REPORT`, и добавлены новые листы:
- `EVIDENCE_MARKET`
- `EVIDENCE_SUPPLY`
- `THRESHOLDS`

#### Validator
В конце формирования отчёта добавлен mini-validator, предупреждающий о типовых дефектах:
- verdict без evidence;
- UNKNOWN без unreachable-share;
- DUMPING_PRESSURE без p10/p50.

---

### 2.5 Встроенные регрессионные самотесты

Добавлен `--selftest` и `run_selftests()`:
1. парсинг цены из `sizes.price`;
2. различимость strict/all и any relevance;
3. запрет FAST при LOW_CONFIDENCE*.

Цель — ловить критичные регрессы без внешнего стенда.

---

## 3) Ошибки в ходе работ и как исправлены

### Ошибка 1: неверный импорт `jdump` в ad-hoc harness
- Симптом: `ImportError: cannot import name 'jdump' from 'WB_Necro'`.
- Причина: вспомогательная функция не экспортируется как публичный API.
- Исправление: удалён импорт `jdump`, использован стандартный `json`.

### Ошибка 2: неверная сигнатура `stage_L_decisions`
- Симптом: `TypeError: got an unexpected keyword argument 'manifest_path'`.
- Причина: функция принимает `out_dir`, а не путь к файлам поштучно.
- Исправление: harness перестроен под файловую структуру каталога и вызов `stage_L_decisions(out_dir)`.

### Ошибка 3: неверная структура `run_manifest.json` в synthetic run
- Симптом: `KeyError: 'meta'`.
- Причина: Stage L ожидает `manifest["meta"]["run_id"]` и полный формат манифеста.
- Исправление: synthetic прогон приведён к реальному контракту манифеста.

### Ошибка 4: частые предупреждения “apply_patch requested via exec_command”
- Симптом: системные предупреждения о некорректном способе patch-редактирования.
- Причина: в прошлых итерациях редактирование инициировалось shell-командой.
- Исправление: правки выполнены напрямую в рабочем файле и проверены компиляцией/selftest.

---

## 4) Что теперь делает скрипт в финале (Stage L/M)

После патча финальная часть работает как «доказательный конвейер»:
1. Stage L формирует решение и одновременно складывает decision trace с числовыми фактами и сработавшими правилами.
2. Если качество данных низкое, aggressive verdict автоматически ограничивается (без ручного контроля).
3. Stage M рендерит HTML/XLSX так, чтобы оператор видел:
   - статусы PHONE/TYPE,
   - конкретные метрики pulse/supply,
   - issues + thresholds,
   - top competitors,
   - глобальный health качества запуска.
4. Встроенный validator предупреждает о дырах доказательной базы.

Итог: решения становятся проверяемыми, воспроизводимыми и пригодными к аудиту под релиз.

---

## 5) Остаточные риски и рекомендации

1. Stage E strict-группы зависят от наполнения query-пака; при недостаточно явной разметке token-групп желательно зафиксировать унифицированный генератор strict-групп (модель + intent case + type-атрибуты).
2. Для production-run полезно хранить versioned snapshot порогов внутри отчётного пакета (частично закрыто листом THRESHOLDS).
3. Рекомендуется nightly cron с `--selftest` + smoke-генерацией одного synthetic отчёта и проверкой наличия evidence-листов.


---

## 6) Дополнительный релизный патч: Stage G anti-sticky cache + UX пауз

### Что добавлено
- Исправлены подсказки артефактов Stage G:
  - `STAGE_IO["G"]` теперь указывает на `competitor_lite.jsonl (+ .wb_cache/comp_lite/*.json)`;
  - `STAGE_CACHES["G"]` теперь указывает на `.wb_cache/comp_lite`.
- В Stage G внедрён анти-залипающий refetch-контур:
  - если кэш `status!=ok` или `http!=200` — при default-настройке выполняется повторный запрос;
  - если payload невалиден (нет ни `ids.nm_id`, ни `content.title` в parsed v4) — выполняется повторный запрос;
  - если ok-кэш старше TTL (`--lite-cache-ttl-hours`, по умолчанию 36) — выполняется повторный запрос.
- Добавлены CLI-рычаги:
  - `--no-refetch-failed-lite` (отключить рефетч fail-кэша);
  - `--lite-cache-ttl-hours` (управление TTL ok-кэша).
- Улучшен UX пауз:
  - `pause_between_stages` теперь показывает VPN hint именно следующей стадии;
  - `pause_before_llm` паузит перед LLM-стадией с конкретным hint этой стадии;
  - убран двусмысленный текст про «включил VPN для LLM» в общем pause-сообщении.
- Усилен кап вердикта в Stage L:
  - кроме `LOW_CONFIDENCE*`, кап на `FAST` теперь срабатывает и при `REVIEWS_UNREACHABLE`/флагах с `UNKNOWN`.

### Зачем
- Главный эксплуатационный баг: после неудачного Stage G bad-кэш оставался «липким», и повторный запуск продолжал давать `ok=0`.
- Операторы часто чистили не тот каталог кэша из-за устаревшей подсказки (`.wb_cache/cards`).
- Паузы иногда провоцировали неверный VPN режим перед WB-стадиями.

### Как проверено
- `--selftest` расширен тестами Stage G:
  - fail-кэш обязан рефетчиться по умолчанию;
  - устаревший ok-кэш (TTL=0) обязан рефетчиться;
  - cap FAST->REWORK сохраняется.

---

## 7) Дополнительные доработки по комментариям ревью (A/B/C/D)

### A) Диагностика фейлов Stage G без гадания

Добавлена агрегированная сводка на уровне стадии:
- HTTP bins: `200 / 429 / 403 / 5xx / timeout / other`;
- Top fail reasons (топ-3 причин по счётчику).

Теперь после Stage G в логе сразу видно природу проблем, а не только `ok_n/fail_n`.

### B) Force refresh для lite

Добавлен флаг:
- `--refetch-lite-all`

Эффект: Stage G игнорирует кэш и принудительно обновляет все lite-карточки (цены/остатки/метрики), без удаления папок вручную.

### C) Управление `requests.Session(trust_env)`

Добавлен флаг:
- `--no-trust-env`

Эффект: для WB-стадий (B/E/G/I) `Session.trust_env=False`, чтобы случайные системные proxy/env-настройки не ломали сетевое поведение.

### D) Красивый рендер `backlog_items` в HTML

В Stage M обновлён блок “Что делать”:
- если есть `decision.backlog_items`, то рендер идёт структурно;
- группировка по `tag` (`price/seo/content/data/social/other`);
- сортировка внутри групп по `prio` (возрастание), затем по `task`;
- fallback на старый плоский `backlog` сохранён.

### Проверка

- `python -m py_compile WB_Necro.py`
- `python WB_Necro.py --selftest`
- `python WB_Necro.py --help | rg "refetch-lite-all|no-trust-env|lite-cache-ttl-hours|no-refetch-failed-lite"`

Все проверки успешно пройдены в текущей итерации.

---

## 8) Hotfix: совместимость run_stage ↔ stage_E/stage_I по trust_env

### Инцидент
- Pipeline падал на стадии E с ошибкой:
  - `TypeError: stage_E_serp() got an unexpected keyword argument 'trust_env'`
- Причина: `run_stage(...)` уже передавал `trust_env`, но сигнатура `stage_E_serp` не принимала этот аргумент.
- Аналогичный риск был у `stage_I_pulse` (runner передаёт `trust_env`, сигнатура не принимала).

### Исправление
- Добавлен kw-only аргумент `trust_env: bool = True` в:
  - `stage_E_serp(...)`
  - `stage_I_pulse(...)`
- Внутри обеих стадий продолжено использование `req_session(trust_env=trust_env)`.
- Обратная совместимость сохранена: все существующие аргументы остаются, добавлен только новый optional kw-arg.

### Антирегресс
- В `--selftest` добавлена проверка через `inspect.signature(...)`, что:
  - `stage_E_serp` содержит параметр `trust_env`;
  - `stage_I_pulse` содержит параметр `trust_env`.

### Проверка
- `python -m py_compile WB_Necro.py`
- `python WB_Necro.py --selftest`
- `python WB_Necro.py --help | rg "no-trust-env|refetch-lite-all|no-refetch-failed-lite"`
- `python WB_Necro.py --list-stages`

Все команды завершились успешно.

---

## 9) Hotfix Stage G: корректный критерий успешного кэша (`ok_v4`/`ok_v1`)

### Проблема
- Stage G учитывал успех только при `status == "ok"` и `http == 200`.
- Реальные успешные записи часто имеют `status="ok_v4"` или `status="ok_v1"`, из-за чего:
  - корректный кэш мог ошибочно считаться fail;
  - refetch-логика могла перезапрашивать валидный кэш;
  - `ok_n/fail_n` и диагностика искажались.

### Исправление
- В Stage G введён единый предикат `_is_fetch_ok(card)`:
  - успех = `http == 200` и `status.startswith("ok")`.
- Этот предикат используется:
  - в `_should_refetch(...)` (решение рефетчить/не рефетчить кэш),
  - в подсчётах `ok_n/fail_n`.

### Эффект
- `ok_v4`/`ok_v1` больше не считаются ошибками.
- Refetch работает корректно и не трогает валидный 200+ok* кэш.
- Статистика Stage G ближе к фактическому состоянию сети/данных.

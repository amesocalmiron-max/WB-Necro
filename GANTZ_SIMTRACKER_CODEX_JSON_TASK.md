# GANTZ SimTracker JSON Preset Task for GPT Codex

## 0. Назначение документа

Этот документ является **implementation handoff** для GPT Codex.

Это не место для архитектурных экспериментов.
Это не приглашение "улучшить модель данных".
Это не задача на UI.

Цель документа:
- дать Codex точную задачу на реализацию нового `SimTracker` JSON preset для GANTZ GM-бота;
- зафиксировать каноническую state-schema;
- зафиксировать allowed fields и allowed semantics;
- зафиксировать updater contract;
- исключить schema drift, field invention и путаницу между state и display.

Этот пакет касается **только JSON preset**.
HTML, CSS, JS и визуальный рендер в эту задачу не входят.

---

## 1. Контекст и исходная логика

Новый GANTZ SimTracker preset должен быть не декоративным UI-блоком, а **authoritative runtime state layer**.

Это соответствует базовым правилам ядра и SimTracker adapter:
- tracker является continuity interface and state carrier;
- активный preset определяет schema, field meanings, meter semantics и display behavior;
- самый последний валидный tracker block читается как authoritative state input;
- narrative должен идти первым, а final tracker block последним, без текста после него;
- display conventions не равны state conventions.

Следовательно, новый preset обязан:
- хранить текущее состояние мира, игрока и значимых NPC;
- служить continuity anchor для GM;
- явно фиксировать scene mode;
- позволять conservative secondary updater корректно продолжать state от хода к ходу;
- не смешивать display-логику с underlying data schema.

---

## 2. Что именно должен сделать Codex

Codex должен создать **один новый preset JSON file**:

- `gantz-simtracker-preset.json`

Файл должен быть пригоден как основа для нового GANTZ SimTracker preset.

В рамках этого файла Codex должен:
1. оформить preset metadata в стиле, совместимом с существующими SimTracker preset-файлами;
2. описать canonical schema нового tracker state;
3. зафиксировать updater-facing instructions так, чтобы secondary model обновляла state консервативно;
4. не использовать старую dating-sim schema как semantic basis;
5. не трогать unrelated presets и не переписывать старые шаблоны.

---

## 3. Что Codex **не имеет права делать**

### 3.1 Нельзя менять agreed root model

Нельзя заменять:

```json
{
  "worldData": {},
  "playerState": {},
  "characters": []
}
```

на что-либо иное.

Нельзя:
- убирать `playerState`;
- прятать игрока внутрь `characters[]`;
- разбивать state на отдельные peaceful/combat arrays;
- делать отдельные schemas для male/female characters.

### 3.2 Нельзя тащить legacy fields из старого dating-sim preset

Старый preset использует семантику вроде:
- `affinity`
- `desire`
- `health`
- `reactionIcon`
- `connections`
- `belongings`
- `goals`
- `bg`
- `inactive`
- `inactiveReason`

Эти legacy поля **не являются основой новой GANTZ schema**.
Нельзя механически переносить их в новый preset.

### 3.3 Нельзя смешивать state и display

Нельзя:
- добавлять UI-only поля в JSON schema;
- кодировать card colors, animation states, tab layout или decorative flags как сюжетные state-поля;
- менять field semantics ради удобства HTML.

### 3.4 Нельзя придумывать поля "на всякий случай"

Запрещено добавлять без явного требования:
- inventory system;
- ammo system;
- exact coordinates;
- giant faction matrix;
- deep history log;
- porn-specific meter zoo;
- chemistry, mood, morale, stance, corruption, lust-stage и любую похожую самодеятельность.

---

## 4. Диагностические исправления относительно прошлых черновиков

Это важный раздел. Codex должен реализовывать **уже исправлённую** версию.

### 4.1 Исправление: `bond_label` не должен дублироваться

Ранее обсуждался вариант, где `bond_label` мог существовать и в `social`, и в `relations.to_player`.

Это ошибка.

**Правильно:**
- `social` хранит текущее сценическое/психологическое состояние персонажа;
- `relations.to_player` хранит устойчивую relational summary к игроку;
- `bond_label` существует **только** в `relations.to_player`.

### 4.2 Исправление: `presence` оставляем ближе к исходной задумке

Была попытка заменить `room` / `mission` на `in_room` / `deployed`.
Семантически это возможно, но для этой реализации это создаёт ненужную миграцию.

**Правильно для текущего пакета:**
использовать исходный enum `presence`:
- `onscreen`
- `offscreen`
- `room`
- `mission`
- `location_unknown`

### 4.3 Исправление: `injuries` только структурные

Ранее было справедливо замечено, что plain string list для травм породит кашу из синонимов.

**Правильно:**
`injuries` должны быть массивом structured objects.

### 4.4 Исправление: `hard_suit` не является weapon

Нельзя держать `hard_suit` в `combat.weapon`, потому что suit уже отдельно представлен через `suit_integrity`.

### 4.5 Исправление: `mask` не должен быть перегружен фетишной семантикой

Для v1 не нужно тащить туда полуэротические или overly-specialized labels.
Оставить компактный психологический словарь.

---

## 5. Каноническая root schema

Корень JSON state должен быть **ровно таким**:

```json
{
  "worldData": {},
  "playerState": {},
  "characters": []
}
```

Это не рекомендация. Это обязательный контракт.

---

## 6. Блок `worldData`

### 6.1 Назначение

`worldData` хранит глобальное состояние сцены, режима и давления мира.

### 6.2 Обязательные поля

- `mode`
- `submode`
- `story_date`
- `story_time`
- `location`
- `sublocation`
- `active_cast`
- `call_status`
- `countdown_visible`
- `countdown_min`
- `mission_active`
- `mission_id`
- `mission_target`
- `mission_timer_min`
- `threat_level`
- `public_heat`
- `last_mission_result`
- `scene_focus`

### 6.3 Enum values

#### `mode`
- `daily`
- `room`
- `mission`
- `aftermath`

#### `call_status`
- `silent`
- `foreshadowed`
- `called`
- `in_room`
- `deployed`
- `returning`

#### `threat_level`
- `low`
- `medium`
- `high`
- `extreme`

#### `public_heat`
- `low`
- `medium`
- `high`
- `critical`

#### `last_mission_result`
- `none`
- `survived`
- `injured`
- `failed`
- `partial_success`
- `wiped`

#### `scene_focus`
- `social`
- `combat`
- `mixed`

### 6.4 Спецправила

- `story_date` и `story_time` относятся к **in-world chronology**, а не к системной дате.
- `active_cast` содержит только прямо присутствующих или активно влияющих на сцену сущностей.
- `submode` это **controlled open string** в `snake_case`, а не бесконечный enum.

---

## 7. Блок `playerState`

### 7.1 Назначение

Игрок должен быть tracked explicitly.
Это сознательный отход от старых NPC-only presets.

### 7.2 Обязательные поля верхнего уровня

- `id`
- `name`
- `sex`
- `ui_variant`
- `icon_key`
- `tracked_tier`
- `status`
- `presence`
- `social`
- `combat`
- `gantz`
- `story`
- `relations`

### 7.3 Жёсткие инварианты

- `id` игрока всегда `player`
- `tracked_tier` игрока всегда `primary`
- `playerState` существует всегда
- игрок не дублируется внутри `characters[]`

### 7.4 Enum values

#### `sex`
- `male`
- `female`
- `unknown`

#### `ui_variant`
- `male`
- `female`
- `neutral`
- `custom`

#### `status`
- `active`
- `inactive`
- `missing`
- `down`
- `dead`
- `revived`

#### `presence`
- `onscreen`
- `offscreen`
- `room`
- `mission`
- `location_unknown`

---

## 8. `playerState.social`

### Поля
- `stress`
- `exposure`
- `comfort`
- `isolation`
- `arousal`
- `bond_need`
- `mask`

### `mask` enum
- `closed`
- `guarded`
- `neutral`
- `open`
- `friendly`
- `performative`
- `cold`
- `detached`
- `composed`
- `shaken`
- `cracked`

### Смысл

Это личный социально-психологический слой игрока.
Не путать с внешней relation-summary NPC к игроку.

---

## 9. `playerState.combat`

### Поля
- `health`
- `suit_integrity`
- `combat_readiness`
- `panic`
- `aggression`
- `role`
- `weapon`
- `position`
- `injuries`
- `downed`

### `role` enum
- `unassigned`
- `civilian`
- `leader`
- `frontliner`
- `shooter`
- `support`
- `scout`
- `protector`
- `wildcard`
- `objective`

### `weapon` enum
- `none`
- `x_gun`
- `x_shotgun`
- `y_gun`
- `gantz_sword`
- `improvised`
- `unknown`

### `position` enum
- `safe`
- `exposed`
- `behind_cover`
- `melee_range`
- `mid_range`
- `long_range`
- `flanking`
- `elevated`
- `retreating`
- `cornered`
- `restrained`
- `downed`
- `lost_contact`

---

## 10. `injuries`

`injuries` должны быть **только structured objects**.

Форма записи:

```json
{
  "key": "rib_fracture",
  "label": "трещина ребра",
  "severity": "moderate",
  "treated": false
}
```

### Поля
- `key`
- `label`
- `severity`
- `treated`

### `severity` enum
- `minor`
- `moderate`
- `severe`
- `critical`

### Правило

Никаких plain string entries вроде:
- `"сломано ребро"`
- `"повреждена грудь"`
- `"кажется больно"`

Потому что это превратит continuity в свалку синонимов.

---

## 11. `playerState.gantz`

### Поля
- `participant`
- `score`
- `room_status`
- `revivable`

### `room_status` enum
- `inactive`
- `summoned`
- `briefed`
- `deployed`
- `returned`
- `eliminated`

---

## 12. `playerState.story`

### Поля
- `goal_short`
- `goal_long`
- `internal_thought`
- `flags`

### Allowed `flags`
- `under_watch`
- `keeping_secret`
- `identity_strain`
- `trauma_active`
- `mission_shaken`
- `grief_locked`
- `vengeful`
- `fixated`
- `avoidant`
- `group_anchor`
- `leader_strain`
- `death_marked`
- `unstable`
- `isolating`
- `protective_drive`

### Ограничения
- максимум 3 активных flags на сущность
- `goal_short` можно менять по сцене
- `goal_long` очень инертен
- `internal_thought` должен быть коротким и не превращаться в литературный монолог

---

## 13. `playerState.relations`

### Поля
- `notable_links`

### `notable_links` entry

```json
{
  "target_id": "rei_yoshino",
  "bond": "teammate",
  "trust": 58,
  "tension": 21
}
```

### Правила
- хранить только реально значимые связи
- максимум 1-3 links
- не строить огромный граф отношений

---

## 14. `characters[]`

### 14.1 Назначение

Массив tracked NPC.
Только значимые, recurring или scene-relevant сущности.

### 14.2 Обязательные поля верхнего уровня

Для каждого NPC:
- `id`
- `name`
- `sex`
- `ui_variant`
- `icon_key`
- `tracked_tier`
- `status`
- `presence`
- `social`
- `combat`
- `gantz`
- `story`
- `relations`

### 14.3 `tracked_tier` enum
- `major`
- `support`
- `temporary`

---

## 15. `characters[].social`

### Поля
- `trust`
- `attraction`
- `tension`
- `jealousy`
- `comfort`
- `intimacy`
- `mask`

### Принцип

`social` для NPC хранит **текущее сценическое состояние** персонажа, а не его финальный relationship summary к игроку.

`bond_label` сюда **не добавлять**.

---

## 16. `characters[].relations`

### Обязательные поля
- `to_player`
- `notable_links`

### `to_player`

```json
{
  "trust": 24,
  "attraction": 7,
  "tension": 38,
  "jealousy": 0,
  "bond_label": "acquaintance"
}
```

### `bond_label` enum
- `stranger`
- `acquaintance`
- `classmate`
- `teammate`
- `uneasy_ally`
- `ally`
- `trusted`
- `protective`
- `dependent`
- `romantic_tension`
- `intimate`
- `rival`
- `hostile`
- `fractured`
- `obsessed`

### Правило приоритета

- `social` = текущее сценическое состояние NPC
- `relations.to_player` = долгоживущая relational memory к игроку

Если значения расходятся, это не баг. Это нормально.
Персонаж может доверять игроку как человеку, но при этом быть в текущей сцене на пределе и иметь высокий `tension` и низкий `comfort`.

---

## 17. Языковая политика

### Должно быть так
- JSON keys: English
- enum values: English
- short human-facing text fields: Russian
- `id`: English snake_case
- `icon_key`: English snake_case

### Не надо делать
- русские JSON keys
- смешанные enum catalogs
- случайную смесь RU/EN без правил

---

## 18. Числовые шкалы

### 18.1 Общая логика

Большинство meters используют диапазон `0..100`.

Интерпретация:
- `0-10` почти отсутствует
- `11-30` слабый уровень
- `31-55` заметный/рабочий
- `56-75` высокий
- `76-90` очень высокий
- `91-100` экстремальный

### 18.2 Для `health` и `suit_integrity`
- `100` полностью рабочее состояние
- `75-99` лёгкие повреждения
- `50-74` заметные повреждения
- `25-49` тяжёлая деградация
- `1-24` критическое состояние
- `0` мёртв / разрушен / выведен из строя

---

## 19. Lifecycle rules

### 19.1 Когда создавать новую карточку NPC

Только если NPC:
- имеет устойчивую идентичность;
- вошёл в активную сцену;
- может повториться;
- влияет на игрока, группу или арку;
- не является disposable extra.

### 19.2 Когда не создавать

Не создавать карточки для:
- случайных прохожих;
- фоновых школьников;
- безымянной толпы;
- одноразовых extras без ongoing relevance.

### 19.3 Повышение важности
- `temporary -> support -> major`

если NPC:
- повторился;
- пережил сцену;
- влияет на сюжет;
- влияет на отношения;
- закрепился в группе.

### 19.4 Уход в фон

Если NPC временно не в кадре:
- карточка сохраняется;
- `presence` обычно становится `offscreen`;
- `status` может стать `inactive`.

### 19.5 Смерть

Если NPC умер:
- `status = dead`
- `combat.health = 0`
- карточка сохраняется

### 19.6 Revive

Если канонически произошёл возврат:
- `status = revived`
- health > 0 только при явной сюжетной базе
- история сущности не стирается

---

## 20. Updater contract, который должен быть встроен в preset

Preset должен явно инструктировать updater model.

### 20.1 Базовая роль updater-а

Updater это:
- conservative state updater
- continuity worker
- schema preserver

Updater не является:
- narrator
- lore writer
- improvisational designer

### 20.2 Обязательные правила updater-а

Updater обязан:
- читать most recent valid `disp` block как authoritative state input;
- продолжать значения, если сцена не оправдывает изменение;
- self-correct missing keys консервативно;
- не invent new keys;
- не удалять важные dead/offscreen entities;
- трекать игрока explicitly, потому что активный preset этого требует;
- обновлять только реально затронутые поля;
- выдавать один strict JSON `disp` block.

### 20.3 `user input` и role adapter

Updater **не должен сам решать**, насколько `{{user}}` является authoritative source.
Это определяется active build и role adapter.

Если build трактует authored user prose как authoritative scene input, updater следует этому.
Если нет, updater работает через уже резолвленную narrative scene.

Иначе получится типичная нейросетевая грязь, где один модуль изображает бога, а второй потом чинит последствия.

---

## 21. Conservative update discipline

### 21.1 Базовый принцип

**Preserve unless justified.**

Если сцена не даёт достаточного основания:
- не менять
- не обнулять
- не восстанавливать
- не улучшать автоматически
- не устраивать декоративные микро-сдвиги ради красоты

### 21.2 Быстрые поля
Могут меняться быстрее:
- `panic`
- `aggression`
- `stress`
- `exposure`
- `combat_readiness`
- `position`
- `health`
- `suit_integrity`

### 21.3 Медленные поля
Меняются умеренно:
- `trust`
- `intimacy`
- `bond_need`
- `isolation`
- `jealousy`
- `relations.to_player.*`

### 21.4 Редкие категориальные поля
Меняются только при явной сценической базе:
- `mask`
- `bond_label`
- `tracked_tier`
- `status`
- `goal_long`

### 21.5 Запрещённые примеры

Нельзя без scene justification:
- `trust +20` за одну приятную беседу
- `intimacy +25` за одно случайное касание
- `panic -> 0` сразу после бойни
- `health +30` без лечения
- `suit_integrity +40` без ремонта
- удалять важного dead NPC

---

## 22. Validation rules

Codex должен встроить схему и инструкции так, чтобы реализация допускала валидаторную проверку.

### Нужно проверять:
- наличие `worldData`
- наличие `playerState`
- наличие `characters`
- отсутствие duplicate IDs
- player присутствует ровно один раз
- enums валидны
- numeric values capped
- нет forbidden keys вне схемы

### Fallback policy

Если новый tracker block оказался битым:
1. safe normalize / repair, если это надёжно
2. иначе вернуть previous valid state

Лучше старый валидный state, чем новый красивый мусор.

---

## 23. Acceptance criteria

Задача считается выполненной только если:

1. root schema ровно `worldData + playerState + characters`
2. player tracked explicitly
3. player не дублируется внутри `characters[]`
4. и player, и NPC используют `social + combat`
5. `bond_label` существует только в `relations.to_player`
6. `injuries` являются structured objects
7. dead/offscreen relevant entities сохраняются
8. preset instructions задают conservative updater behavior
9. JSON не содержит legacy dating-sim garbage
10. `disp` output contract остаётся совместимым с SimTracker

---

## 24. Финальная инструкция для Codex

Implement the new preset exactly as specified.

When a detail is underspecified:
- preserve schema stability,
- preserve continuity discipline,
- preserve player-explicit tracking,
- preserve strict separation of state and display,
- choose the most conservative implementation consistent with the active SimTracker contract.

Do not redesign the model.
Do not invent fields.
Do not "improve" the schema beyond this specification.

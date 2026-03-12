# GANTZ SimTracker HTML Template Task for GPT Codex

## 0. Назначение документа

Этот документ является **implementation handoff** для GPT Codex по HTML-шаблону нового GANTZ SimTracker.

Это не задача на JSON schema.
Это не задача на updater logic.
Это не лицензия переделывать state model под вкус фронтендера.

Цель документа:
- дать Codex точную задачу на реализацию нового `HTML template`;
- зафиксировать UX и visual contract;
- добиться красивого, современного, технологичного, но читаемого UI;
- сохранить жёсткое разделение между display и state semantics.

Этот пакет касается **только HTML template**.
JSON preset проектируется отдельно и не должен меняться в рамках этой задачи.

---

## 1. Контекст и исходная логика

В проекте уже есть reference template с полезным interaction pattern:
- right-side placement;
- tab-like icon rail;
- card drawer / slide behavior;
- animated transforms;
- layered card view.

Это хорошая механическая база, но плохая semantic basis.
Старый template заточен под dating-sim disposition tracker со старыми полями вроде `affinity`, `desire`, `health`, `reactionIcon`, `belongings`, `goals`, `bg` и прочей ролевой археологией.

Для GANTZ это не подходит.

Новый HTML template должен:
- читать **новую GANTZ schema**, а не старую;
- быть красивым и современным;
- давать one-icon-per-entity interaction model;
- открывать красивую карточку по нажатию на иконку;
- иметь main tabs `Social` и `Combat`;
- иметь красивые collapsible sections для secondary data;
- поддерживать плавные анимации без дешёвого RGB-цирка.

---

## 2. Что именно должен сделать Codex

Codex должен создать **один новый HTML template file**:

- `gantz-simtracker-template.html`

Файл должен:
- быть совместимым с новой GANTZ JSON schema;
- использовать правый rail с иконками сущностей;
- открывать соответствующую entity card при нажатии на иконку;
- поддерживать красивое переключение вкладок и секций;
- оставаться читаемым и функциональным при разном размере active cast.

Минимальный inline JS допустим, если он действительно нужен для tab switching / card opening / section toggles.

---

## 3. Что Codex **не имеет права делать**

### 3.1 Нельзя менять JSON schema из HTML-задачи

Нельзя:
- переименовывать JSON keys;
- тащить в шаблон новые state fields;
- выдумывать fallback fantasy fields;
- заменять agreed semantics старыми label mappings.

HTML template читает state. Он его не проектирует.

### 3.2 Нельзя возвращаться к legacy dating-sim логике

Запрещено:
- использовать `affinity`, `desire`, `reactionIcon`, `belongings`, `bg`, `inactiveReason` как основу нового шаблона;
- рендерить GANTZ tracker как re-skinned dating sim;
- навешивать на новый UI старую field semantics.

### 3.3 Нельзя превращать UI в новогоднюю ёлку

Пользователь хочет красивый, современный, почти tech-pron уровень презентации.
Но это не значит:
- кислотный неон everywhere;
- детский gaming-RGB circus;
- перегруженные glow-эффекты;
- анимации ради анимаций.

Нужен тёмный, дорогой, собранный интерфейс.

### 3.4 Нельзя раздувать main tab system

Top-level tabs должны быть **ровно две**:
- `Social`
- `Combat`

Нельзя добавлять `Story`, `Gantz`, `Links` как отдельные главные tabs.
Они должны быть collapsible sections внутри карточки.

---

## 4. Диагностические исправления относительно прошлых черновиков

Этот HTML task уже учитывает исправления, сделанные после ревизии логики.

### 4.1 Исправление: не делать 5 main tabs

Ранее обсуждалась перегруженная схема с отдельными tabs для `Story`, `Gantz`, `Links`.
Это признано ошибкой.

**Правильно:**
- только `Social` и `Combat` как main tabs;
- `Story`, `Gantz`, `Links` как collapsible sections.

### 4.2 Исправление: UI не должен диктовать state model

Ранее была опасность начать подгонять JSON под удобство фронта.
Это запрещено.

**Правильно:**
HTML работает поверх agreed schema и не меняет её.

### 4.3 Исправление: one icon per entity

Идея пользователя зафиксирована как обязательная:
- одна иконка на сущность;
- нажатие по иконке открывает карточку;
- выбранная карточка красиво выезжает;
- дальше внутри карточки работают красивые tabs и sections.

---

## 5. Главная UX-концепция

### 5.1 Основной принцип

**One tracked entity = one icon in the side rail.**

Это обязательный UX-контракт.

Для каждой сущности:
- есть ровно одна иконка/аватар badge;
- клик по иконке открывает соответствующую карточку;
- карточка выезжает как современная панель;
- иконка получает active state;
- внутри карточки доступны `Social` и `Combat`;
- дополнительные секции можно раскрывать.

### 5.2 Ощущение интерфейса

UI должен ощущаться как:
- premium dark sci-fi panel;
- stylish but disciplined tech UI;
- clean glass / steel / obsidian composition;
- expensive and modern, not loud and vulgar.

---

## 6. Общая layout-структура

### 6.1 Global container

Требования:
- right-side placement;
- rail и card drawer визуально связаны;
- layered layout без дёрганья;
- card area не должна ломать чат.

### 6.2 World HUD

Должен существовать отдельный блок world summary.
Он должен быть всегда видим при открытом template.

### 6.3 Icon rail

Должен быть вертикальный rail иконок сущностей.

### 6.4 Card drawer

Должна быть выезжающая карточка выбранной сущности.

---

## 7. World HUD

### 7.1 Обязательные элементы

HUD должен отображать:
- `mode`
- `submode`
- `story_date`
- `story_time`
- `location`
- `sublocation`
- `call_status`
- `countdown_min` если countdown visible
- `mission_timer_min` если mission active
- `threat_level`
- `public_heat`
- `scene_focus`

### 7.2 Визуальная логика

HUD должен быть:
- компактным;
- современным;
- легко читаемым;
- визуально отделённым от entity cards;
- не слишком высоким.

### 7.3 Цветовые акценты

Нормальная логика:
- calm/stable info: muted cyan / teal
- pressure/tension: amber
- danger/critical: crimson
- neutral metadata: steel/gray

---

## 8. Icon rail

### 8.1 Обязательные правила

- одна иконка на сущность;
- player тоже имеет иконку;
- rail расположен справа;
- rail должен быть scrollable при большом количестве сущностей;
- у активной иконки должен быть понятный active state;
- hover state обязателен;
- dead/down/offscreen statuses должны читаться визуально.

### 8.2 Sorting order

Иконки должны рендериться в таком порядке:
1. player
2. major onscreen NPCs
3. support onscreen NPCs
4. temporary onscreen NPCs
5. offscreen active NPCs
6. inactive NPCs
7. dead NPCs

### 8.3 Icon content

Иконка может быть реализована как:
- initials badge;
- icon_key-based glyph;
- stylized minimal avatar badge.

Но есть ограничения:
- нельзя полагаться на внешние кастомные изображения;
- решение должно работать из коробки;
- player должен визуально отличаться от NPC;
- dead/down states должны менять внешний вид иконки.

---

## 9. Card drawer

### 9.1 Обязательное поведение

При нажатии на иконку:
- соответствующая карточка открывается;
- карточка плавно выезжает;
- прежняя активная карточка скрывается без дёрганья;
- активная иконка синхронизируется с карточкой.

### 9.2 Стиль карточки

Карточка должна ощущаться как:
- layered panel;
- modern dark glass card;
- crisp readable UI surface;
- subtly animated drawer.

### 9.3 Требования к motion

Движение должно быть:
- плавным;
- быстрым, но не истеричным;
- clean;
- без дешёвого bounce;
- без кислотной перегрузки.

---

## 10. Структура карточки сущности

Каждая entity card должна содержать:

### 10.1 Header
- name
- icon/avatar badge
- tier badge
- status badge
- presence badge

### 10.2 Meta row
Компактная строка метаданных, где можно показывать:
- role summary
- room status
- short bond summary
- crisis hint, если это уместно

### 10.3 Main tabs
Ровно две:
- `Social`
- `Combat`

### 10.4 Collapsible sections
Ниже табов:
- `Story`
- `Gantz`
- `Links`

---

## 11. Main tabs

### 11.1 Social tab

#### Для player должны отображаться
- `stress`
- `exposure`
- `comfort`
- `isolation`
- `arousal`
- `bond_need`
- `mask`

#### Для NPC должны отображаться
- `trust`
- `attraction`
- `tension`
- `jealousy`
- `comfort`
- `intimacy`
- `mask`

### 11.2 Combat tab

Должен отображать:
- `health`
- `suit_integrity`
- `combat_readiness`
- `panic`
- `aggression`
- `role`
- `weapon`
- `position`
- `downed`
- `injuries`

### 11.3 Визуальный формат meters

Допустимые варианты:
- horizontal bars;
- segmented bars;
- clean stat rows with progress lines.

Требования:
- readable value labels;
- одинаковая визуальная система;
- без перегруженной инфографики;
- без ощущения spreadsheet hell.

---

## 12. Collapsible sections

### 12.1 Story section

Должна отображать:
- `goal_short`
- `goal_long`
- `internal_thought`
- `flags`

### 12.2 Gantz section

Должна отображать:
- `participant`
- `score`
- `room_status`
- `revivable`

### 12.3 Links section

#### Для player
- `notable_links`

#### Для NPC
- `to_player`
- `notable_links`

### 12.4 Правила секций

- sections должны быть collapsible;
- анимация раскрытия должна быть гладкой;
- по умолчанию секции могут быть свернуты;
- содержимое должно быть компактным, а не стеной текста.

---

## 13. Default opening logic

HTML template должен учитывать `scene_focus`.

### Если `scene_focus = social`
- по умолчанию открывать `Social`

### Если `scene_focus = combat`
- по умолчанию открывать `Combat`

### Если `scene_focus = mixed`
- если `downed = true` или есть severe/critical injury, предпочесть `Combat`
- иначе открыть `Social`

---

## 14. Player card special treatment

Player card должен ощущаться особенным, но не нарушать layout consistency.

Обязательные требования:
- player всегда первый в rail;
- player card имеет чуть более высокий визуальный приоритет;
- player обозначен яснее, чем обычные NPC;
- при этом используется та же schema-driven rendering logic, а не отдельная schema.

---

## 15. Compact mode

HTML должен поддерживать compact rendering для сущностей, у которых:
- `tracked_tier = temporary`
- или `presence = offscreen`
- или `status = inactive`
- или `status = dead`

### Compact mode должен показывать:
- icon
- name
- status/presence
- несколько critical indicators
- уменьшенное пространство

### Что запрещено
- полностью скрывать такие сущности;
- выкидывать dead/offscreen entities из rail.

---

## 16. Visual state mapping

### 16.1 По `status`

- `active`: normal readable state
- `inactive`: dimmed but readable
- `missing`: muted / uncertain
- `down`: danger emphasis
- `dead`: grayscale / low-saturation / memorial feeling
- `revived`: special accent, но без цирка

### 16.2 По `presence`

- `onscreen`: full emphasis
- `offscreen`: reduced emphasis
- `room`: room-accent
- `mission`: combat-accent
- `location_unknown`: obscured / uncertain

### 16.3 По `scene_focus`

Использовать только для initial tab choice и subtle emphasis.
Не делать из этого новый визуальный балаган.

---

## 17. Animation contract

Это важный раздел. Пользователь прямо хочет красивый и современный интерфейс.

### 17.1 Обязательные анимации

Должны быть:
- icon hover animation
- icon active-state transition
- card slide-in / slide-out
- tab switch animation
- collapsible section expand/collapse animation
- subtle opacity / transform transitions

### 17.2 Требования к стилю анимаций

Анимации должны быть:
- smooth;
- clean;
- premium-feeling;
- restrained;
- slightly futuristic.

### 17.3 Что запрещено

- навязчивые bounce-эффекты;
- excessive glow spam;
- кислотный sci-fi carnival;
- тряска интерфейса;
- визуальный шум, который убивает читаемость.

---

## 18. Visual design direction

### 18.1 Theme direction

Рекомендуемая базовая палитра:
- graphite
- obsidian
- muted steel
- subtle glass
- controlled accent colors

### 18.2 Mood target

Интерфейс должен ощущаться как:
- dark premium HUD
- techwear interface
- sci-fi without childish excess
- modern ST sidebar module with taste

### 18.3 Typography

Требования:
- легко читаемый основной текст;
- чистые labels;
- внятные hierarchy levels;
- не делать overly decorative typography.

---

## 19. Data mapping rules

HTML template должен читать agreed JSON schema **ровно как она описана в JSON package**.

### Нельзя:
- выдумывать fallback fields;
- ремапить `relations.to_player` в старые romance meters;
- подставлять старые поля, если новых нет;
- трактовать old dating-sim semantics как актуальные.

### Нужно:
- gracefully hide truly missing optional sections;
- работать с playerState отдельно от characters;
- сохранять strict separation of state and display.

---

## 20. Forbidden implementation moves

Codex не имеет права:
- добавлять больше двух main tabs;
- превращать `Story`, `Gantz`, `Links` в отдельные top-level tabs;
- скрывать player card;
- скрывать dead entities completely;
- делать layout громоздким и шумным;
- превращать UI в кислотный "киберпанк для школьников";
- менять JSON semantics ради фронта;
- использовать старую dating-sim data schema как источник правды.

---

## 21. Responsiveness and density behavior

Template должен оставаться работоспособным при:
- малом количестве сущностей;
- среднем количестве сущностей;
- большом количестве сущностей;
- длинных именах;
- нескольких injuries;
- нескольких flags.

### Допустимые меры
- rail scrolling
- stable card width
- constrained text blocks
- compact chips for injuries and flags

### Недопустимо
- layout jumpiness
- неконтролируемое растягивание карточек
- исчезновение части UI при росте cast size

---

## 22. Acceptance criteria

Задача считается выполненной только если:

1. rendered exactly one icon per tracked entity
2. clicking an icon opens that entity card
3. player card exists and is visually privileged
4. main tabs are only `Social` and `Combat`
5. `Story`, `Gantz`, `Links` are collapsible sections
6. World HUD is visible and readable
7. dead/offscreen/inactive entities remain visible
8. compact mode works
9. animations look modern and smooth
10. template reads agreed schema without redefining it

---

## 23. Финальная инструкция для Codex

Implement the HTML template exactly as specified.

When a detail is underspecified:
- preserve schema compatibility,
- preserve right-side icon rail UX,
- preserve one-icon-per-entity interaction,
- preserve clean modern readability,
- preserve strict separation of state and display,
- choose the most conservative elegant implementation.

Do not redesign the state model.
Do not turn the UI into a gimmick showcase.
Make it beautiful, modern, animated, and controlled.

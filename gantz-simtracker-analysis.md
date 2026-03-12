# GANTZ SimTracker — analysis report

## Context
Запрошенные reference-файлы:
- `disposition-card-template-sidebar-tabs.html`
- `disposition-card-template-sidebar-tabs.json`

не найдены в текущем репозитории и файловой системе контейнера, поэтому анализ выполнен по фактическим файлам:
- `gantz-simtracker-preset.json`
- `gantz-simtracker-template.html`

## What was checked
- соответствие root state contract (`worldData`, `playerState`, `characters`)
- соответствие schema mapping в UI (без legacy dating-sim remap)
- UX-контракт (right rail, one icon per entity, card drawer)
- top-level tabs contract (только `Social` и `Combat`)
- collapsible secondary sections (`Story`, `Gantz`, `Links`)
- сортировка и видимость non-active сущностей
- motion/visual restraint

## Findings

### 1) Preset contract quality
Preset в целом реализован консервативно и близко к handoff:
- root model зафиксирован как `worldData + playerState + characters`
- player tracked explicitly
- `bond_label` ограничен `relations.to_player`
- `injuries` структурированы как object entries
- updater contract и fallback policy присутствуют

Итог: preset пригоден как authoritative continuity state base.

### 2) Template architecture quality
Template уже реализует ключевые UX-контракты:
- right-side shell с HUD + rail + drawer
- one icon per entity
- click-to-open card
- две main tabs (`Social`, `Combat`)
- `Story/Gantz/Links` как collapsible sections

Итог: базовая архитектура корректная.

### 3) Improvements applied after analysis
Для более читаемой визуальной диагностики состояния сущностей добавлено:
- rail-style mapping для `down` состояния
- компактный rail signal (`stable/offscreen/down/critical/dead`) на каждой иконке

Это усиливает читаемость pressure/danger статусов без перегруза интерфейса.

## Result
Текущая версия соответствует основному контракту handoff и дополнительно улучшена по визуальной диагностике статусов в rail.

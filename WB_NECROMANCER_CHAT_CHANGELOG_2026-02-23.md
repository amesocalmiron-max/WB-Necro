# WB Necromancer — Chat Change Log (this conversation)
Date range: 2026-02-22 → 2026-02-23 (UTC)
Scope: menu/UX, verbosity/logging, LLM token controls, Stage H keep-fallback, Stage I reviews (critical), regression fixes, stability.

> Context: pipeline A–M. WB stages must run VPN OFF; LLM stages can run VPN ON. Project goal: decide revive/drop based on two markets (phone model market vs type market) with Market Pulse driven primarily by competitor review velocity.

---

## 0) Files referenced in this chat
- Base / working files (mounted):
  - wb_necromancer_v2_REWRITE_MENUFIX_FIXED_BPARSE_v4.py
  - WB_Revival_v2_Necromancer_plan_transfer.txt
  - WB_NECROMANCER_TRANSFER_NOTE_2026-02-22.md
  - WB_Revival_v2_Necromancer_TRANSFER_NOTE.md

- Run artifacts provided by user (used for diagnosis):
  - run_manifest.json
  - own_norm.jsonl
  - queries_raw.jsonl, queries_valid.jsonl
  - competitor_pool.jsonl
  - intent.jsonl
  - relevance.jsonl
  - market_pulse.jsonl
  - supply_structure.jsonl
  - cluster_verdicts.jsonl
  - decisions.jsonl
  - exec_summary.json
  - WB_NECROMANCER_REPORT.html
  - WB_NECROMANCER_REPORT.xlsx

---

## 1) Problem statements (what user requested / what was broken)
### 1.1 Stage banners + pauses (UX)
User requirement: stage title/description must appear BEFORE pause (like wb_revive), so operator knows what’s next and whether VPN/LLM is needed.

Observed issue (screenshot):
- Pause shown after Stage completion without showing the next stage’s banner first.
- Operator cannot know “Stage C needs LLM?” before pressing Enter.

### 1.2 Token controls (LLM max_tokens per stage)
User requirement: manual max_tokens controls per LLM stage (like wb_revive). Needed for cost control and stability.

### 1.3 Verbose / logging
User requirement: “normal verbose” that prints line-by-line results (not just a progress bar toggle). Missing visibility caused a wasted day of runs.

Observed issue:
- “verbose” behaved as progress bar on/off, not per-SKU/per-imt operational logs.
- Failures in reviews collection were not visible in-console during run.

### 1.4 Stage H cluster keep can become empty (data integrity)
DeepSeek note: no validation for minimal number of competitors kept in Stage H. Empty keep breaks downstream stages (Pulse/Structure/Decisions).

### 1.5 Critical: Review collection (Stage I) failing
Run artifacts showed:
- 64/64 SKUs ended up DROP with PHONE_MARKET_DEAD in exec_summary.json.
- market_pulse.jsonl indicated review fetch failure for all competitor imt_id (warnings “no_reviews_or_unreachable” across the board).
Root cause: review endpoint / access path (public-feedbacks / HTML feedbacks pages) blocked or unstable (403/498/anti-bot), combined with insufficient logs.

---

## 2) Version timeline (in this chat)
> Note: naming below is the exact filenames produced in chat. Some intermediate versions were later superseded due to regressions.

### v4 → v5: pause banner + verbose toggle (first fix set)
File: wb_necromancer_v2_REWRITE_MENUFIX_PAUSEBANNER_VERBOSE_v5.py
Changes:
- Stage banner printed BEFORE pause (so operator sees what comes next).
- Pause logic deduped (pause_before_llm + pause_between_stages avoids double pauses).
- Added --verbose (default ON) and --no-verbose to control progress bars.
Issue found later:
- Stage H keep-fallback logic accidentally removed vs earlier “stable” v4-style behavior.

### v6: restore Stage H fallback + restore STAGE_IO + global max_tokens
File: wb_necromancer_v2_REWRITE_MENUFIX_PAUSEBANNER_VERBOSE_v6.py
Changes:
- Restored Stage H keep-fallback:
  - --min-keep-competitors (default 6), --max-keep-competitors (default 18)
  - fallback: fill from borderline then from any items (ban_terms filter), add stats fallback_added/trimmed_n.
- Restored STAGE_IO (in/out file hints) in stage_banner.
- Restored global --llm-max-tokens with priority logic (global fills stage values unless explicitly set).
Status:
- Considered “stable baseline” at that time.

### v7: “settings menu” + verbosity levels + per-stage token prompts
File: wb_necromancer_v2_REWRITE_MENU_SETTINGS_VERBOSITY_v7.py
Changes:
- Introduced verbosity concept:
  - --verbosity 0/1/2, -v alias for debug.
  - --quiet / --no-verbose for verbosity 0.
- Menu: added separate “Настройка” section to edit key parameters.
- Menu now asks max_tokens separately per LLM stage (D/H/M) instead of single triplet input.

### v8: operational “OK/FAIL” lines per stage (requested ‘построчно’ style)
File: wb_necromancer_v2_REWRITE_MENU_SETTINGS_VERBOSITY_v8.py
Changes:
- Added per-stage completion lines:
  - OK: duration + output file line count/size summary.
- verbosity=2 adds deeper debug lines for fallback, config summary, etc.

### v9: persistence attempt (where settings saved)
File: wb_necromancer_v2_REWRITE_MENU_SETTINGS_VERBOSITY_v9.py
Changes:
- Added profile save/load to ./profiles/necromancer_menu_last.json (or env NECROMANCER_MENU_PROFILE).
Problem discovered:
- MenuConfig dataclass in v9 contained only 3 fields (input/sheet/out).
- Result: most settings were not actually saved; menu could throw AttributeError on missing fields.

### v10: fix persistence dataclass + full config in MenuConfig
File: wb_necromancer_v2_REWRITE_MENU_SETTINGS_PERSIST_VERBOSITY_v10.py
Changes:
- MenuConfig expanded to include all used settings (resume, pauses, yes, verbosity, LLM toggles, tokens, keep thresholds).
- cfg_reset fixed to reset actual fields.
- Identified UX trap: global llm_max_tokens could override per-stage values depending on argv construction.

### v11: guardrails + token precedence trap fixed + cfg_sanitize
File: wb_necromancer_v2_REWRITE_MENU_SETTINGS_PERSIST_VERBOSITY_v11.py
Changes:
- Added import traceback explicitly.
- Added guard: start_stage must be <= end_stage; else error (no silent “empty plan” run).
- Fixed token precedence trap: per-stage tokens should not be ignored when global exists.
- Added cfg_sanitize on load/reset/save (clamp verbosity, normalize types, ensure min_keep <= max_keep, tokens <=0 → None).

### v12: DeepSeek-driven reliability fixes (imt_id + Stage E/F/H/L etc.)
File: wb_necromancer_v2_REWRITE_MENU_SETTINGS_PERSIST_VERBOSITY_v12.py
Changes:
- Stage I: imt_id fallback seed.imt_id if v4 ids missing.
- Stage E: avoid selecting queries without data (status!=ok / no cache_path).
- Stage D: disable type-market if phone model not extracted (avoid cross-model SERP).
- Stage H: fallback respects must_any_groups first; if forced to broaden, set low_confidence.
- parse_card_v4: include imt_id key fallback.
- karma toxicity thresholds moved to manifest config.
- Stage L: add ALT_STRATEGY flag/backlog for “type market dead”.
- Stage F: process only selected_queries; warn if selected query cache missing.

### v13: real logging + UNKNOWN semantics (attempt to stop “DROP 64/64” from unreachable reviews)
File: wb_necromancer_v2_REWRITE_MENU_SETTINGS_PERSIST_VERBOSITY_v13.py
Changes:
- Introduced always-on logs:
  - out/logs/run.log (human)
  - out/logs/events.jsonl (machine)
- Stage K/L: if reviews unreachable, mark UNKNOWN + flags (REVIEWS_UNREACHABLE, LOW_CONFIDENCE) instead of DEAD → DROP.

### v14: switch Stage I to feedbacks1/feedbacks2 endpoint (key architecture fix)
File: wb_necromancer_v2_REWRITE_MENU_SETTINGS_PERSIST_VERBOSITY_v14.py
Changes:
- Stage I switched away from HTML/public-feedbacks to:
  - GET https://feedbacks1.wb.ru/feedbacks/v1/{imt_id}
  - fallback feedbacks2
- Added granular per-imt logs (r30/r90/days_since_last).
Regression:
- Stage E contained stray copy-paste referencing undefined verdict/risk_flags → NameError.

### v15: remove Stage E NameError regression
File: wb_necromancer_v2_REWRITE_MENU_SETTINGS_PERSIST_VERBOSITY_v15.py
Change:
- Removed the stray Stage E line causing NameError.
- Maintained feedbacks1/2 reviews approach + logs.

### v16: log nm_id field + parse_search_items imt_id fallback
File: wb_necromancer_v2_REWRITE_MENU_SETTINGS_PERSIST_VERBOSITY_v16.py
Changes:
- Logging fixed: nm_id stored as a proper field (not only inside message text).
- parse_search_items: also consider p.get("imt_id") alongside root/imtId/rootId.

---

## 3) What went wrong (root causes) and how it was resolved
### 3.1 “All DROP / market DEAD” run
Symptoms:
- exec_summary.json showed 64/64 DROP with PHONE_MARKET_DEAD.
- market_pulse.jsonl showed review fetch failure for all competitor imt_ids (no real dates).
Root cause:
- Review extraction path blocked/unstable (HTML feedbacks pages / public-feedbacks endpoint).
- Logging didn’t surface per-imt failures in real time.
Resolution:
- Stage I moved to feedbacks1/feedbacks2 JSON endpoint.
- Added per-imt logs (recent_30/90, days_since_last, warnings, source).
- Added UNKNOWN semantics for unreachable reviews.

### 3.2 Stage H empty keep
Root cause:
- No hard floor on keep count; cluster could end up empty.
Resolution:
- Added min/max keep competitors + fallback fill logic.
- Added stats fallback_added/trimmed_n + low_confidence tagging.

### 3.3 Menu/persistence regressions
Root cause:
- v9 persisted only 3 cfg fields.
Resolution:
- v10 expanded MenuConfig; v11 sanitized and fixed token precedence.

### 3.4 Stage E NameError regression
Root cause:
- Copy-paste line referencing non-existent variables in Stage E.
Resolution:
- v15 removed the line; script compiles/runs.

---

## 4) Operational notes (how to rerun correctly with resume)
To rerun from Stage I without redoing A–H:
- Delete artifacts produced by I and downstream:
  - market_pulse.jsonl
  - supply_structure.jsonl (optional but recommended for clean rerun)
  - cluster_verdicts.jsonl
  - decisions.jsonl
  - exec_summary.json
  - WB_NECROMANCER_REPORT.xlsx
  - WB_NECROMANCER_REPORT.html
- Delete review cache if present:
  - out/.wb_cache/reviews/ (or equivalent)
- Run:
  - --start-stage I --end-stage M --resume -v --strict
- Keep WB stages (including Stage I) on VPN OFF; LLM stages can use VPN ON via pause.

---

## 5) Current status (end of this chat)
- Latest script successfully:
  - read competitor reviews (feedbacks1/2),
  - produced report outputs (HTML and XLSX),
  - generated final jsonl artifacts.
- Note: last stage output/decision behavior is not satisfactory and will be changed later (planned work).

---

## 6) Pending work (explicitly postponed)
- Redesign/adjust “last stage” (final decision / report shaping).
- Possible improvements:
  - preflight/probe for reviews endpoint before long run
  - stronger guardrails for query selection vs missing cache
  - expose more decision thresholds in Settings menu

---

## 7) Quick index: versions produced in this chat
- v4: wb_necromancer_v2_REWRITE_MENUFIX_FIXED_BPARSE_v4.py (starting point)
- v5: wb_necromancer_v2_REWRITE_MENUFIX_PAUSEBANNER_VERBOSE_v5.py
- v6: wb_necromancer_v2_REWRITE_MENUFIX_PAUSEBANNER_VERBOSE_v6.py
- v7: wb_necromancer_v2_REWRITE_MENU_SETTINGS_VERBOSITY_v7.py
- v8: wb_necromancer_v2_REWRITE_MENU_SETTINGS_VERBOSITY_v8.py
- v9: wb_necromancer_v2_REWRITE_MENU_SETTINGS_VERBOSITY_v9.py (persistence attempt, flawed)
- v10: wb_necromancer_v2_REWRITE_MENU_SETTINGS_PERSIST_VERBOSITY_v10.py
- v11: wb_necromancer_v2_REWRITE_MENU_SETTINGS_PERSIST_VERBOSITY_v11.py
- v12: wb_necromancer_v2_REWRITE_MENU_SETTINGS_PERSIST_VERBOSITY_v12.py
- v13: wb_necromancer_v2_REWRITE_MENU_SETTINGS_PERSIST_VERBOSITY_v13.py
- v14: wb_necromancer_v2_REWRITE_MENU_SETTINGS_PERSIST_VERBOSITY_v14.py (reviews endpoint switch; had Stage E NameError)
- v15: wb_necromancer_v2_REWRITE_MENU_SETTINGS_PERSIST_VERBOSITY_v15.py (fixed Stage E)
- v16: wb_necromancer_v2_REWRITE_MENU_SETTINGS_PERSIST_VERBOSITY_v16.py (nm_id logging + SERP imt_id fallback)

---

End of log.
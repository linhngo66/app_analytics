# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`app_analytics` is a dbt project for **video engagement analytics** on a short-video platform. It transforms raw user-video exposure events and a category reference table into a dimensional model ready for analysis.

- **Data warehouse**: DuckDB (implied by `iff()`, `div0()`, `count_if()` syntax in SQL)
- **Profile**: `app_analytics` (configured in `~/.dbt/profiles.yml`)
- **Output schema**: `dbt_lngo` (staging views and marts tables)
- **Package dependency**: `dbt-labs/dbt_utils >= 1.3.0`

## Common Commands

```bash
# Install packages
dbt deps

# Run all models
dbt run

# Run a specific model and its downstream dependencies
dbt run --select stg_interaction+

# Run a single model
dbt run --select fct_video_engagement

# Run tests
dbt test

# Test a specific model
dbt test --select fct_video_engagement

# Build (run + test) everything
dbt build

# Preview query results (no materialisation)
dbt show --select fct_video_engagement --limit 10

# Compile SQL without running
dbt compile --select fct_video_engagement
```

## Editing Guidelines

When making edits or debugging, minimize rewrites and renames. Prefer targeted changes — add or modify only what is necessary, leave existing structure and naming intact.

## Data Architecture

### Sources (raw layer)
Defined in `models/staging/__sources.yml`, sourced from database `app_analytics`, schema `source`:
- `interaction` — raw user-video exposure events (one row per impression)
- `categories_cn_en` — category reference with three-level hierarchy (root → parent → leaf)

### Staging layer (`models/staging/`, materialized as views)
Cleans, casts, and renames source columns. Produces `is_*` boolean flags and derived fields (e.g. `is_effective_view` = watch_time > 3s). **Preserves user demographic columns** (gender, age, city, etc.) that are intentionally stripped in downstream fact tables.

- `stg_interaction` — typed and renamed interaction events; the only model that reads from `source.interaction`
- `stg_category` — typed category rows with a `dbt_utils.generate_surrogate_key` surrogate key (`category_sk`)

### Marts layer (`models/marts/`, materialized as tables)

| Model | Grain | Purpose |
|---|---|---|
| `fct_interaction` | One row per exposure event | Atomic fact table; strips demographic columns from staging |
| `fct_video_engagement` | One row per video | Aggregated engagement metrics (rates, counts); primary model for "what makes a video engaging?" |
| `fct_user_engagement` | One row per user | Aggregated engagement metrics per user |
| `dim_video` | One row per video | Video attributes (title, duration); deduped by most recent `exposed_at` |
| `dim_author` | One row per author | Latest `author_fans_count`; deduped by most recent `exposed_at` |
| `dim_category` | One row per category node | Three-level hierarchy (category1/2/3); each row is a leaf node with its ancestor IDs |

### Key design decisions
- **`fct_interaction` is the single source of truth** for all aggregation — both `fct_video_engagement` and `fct_user_engagement` reference it via `ref()`.
- **`dim_category` is self-joined in `fct_video_engagement`** three times (on `category3_id`) to resolve category1 and category2 names without a separate hierarchy table.
- **User demographics are not in `fct_interaction`**. When segmentation by gender, age, city, etc. is needed, join back to `stg_interaction`.
- **`div0()`** is used throughout for safe division (returns 0 instead of error on division by zero).
- **Effective view** threshold is watch_time > 3 seconds, defined in `stg_interaction` and propagated as `is_effective_view`.

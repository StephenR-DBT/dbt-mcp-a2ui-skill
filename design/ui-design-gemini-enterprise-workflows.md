# UI/UX Design Specification: dbt MCP Workflows for Gemini Enterprise
**Target Surface:** Gemini Enterprise (GE) Right-Hand Canvas (`A2UI v0.9`) & Mobile App  
**Reference Document:** [docs/gemini-enterprise-workflows.md](file:///Users/stephenrobb/Dev/MCP/dbt-mcp/docs/gemini-enterprise-workflows.md)  
**Theme:** dbt Cloud (`https://xa897.us1.dbt.com/`) & dbt Labs Official Design Tokens  
**Interactive Prototype:** [dbt_enterprise_workbench.html](file:///Users/stephenrobb/.gemini/antigravity/brain/5b053b02-0787-47d2-aa42-cb825ee75e6e/dbt_enterprise_workbench.html)

---

## 1. Executive Summary & Design System

This document specifies the responsive user interface designs for all **5 enterprise workflows** powered by `dbt-mcp` inside **Gemini Enterprise (GE)**.

### Global UX Tenets
1. **Less Busy is Better**: Zero cognitive clutter. Lead with high-contrast summary verdicts and metric cards; progressively disclose SQL expressions, raw error traces, and lineage graphs on demand.
2. **Adaptive Lineage Orientation**:
   * **Desktop / Web Canvas (`>= 768px`)**: Lineage renders **Left-to-Right (`⇄ LR`)** with horizontal connectors (`→`).
   * **Tiny Devices (Mobile `320px–430px`)**: Lineage automatically reconfigures to **Top-Down (`⇅ TD`)** with vertical connectors (`↓`) and full-width stacked cards, eliminating horizontal scrolling and clipped text.
3. **Formfactor Ergonomics**:
   * All interactive buttons maintain a minimum tap target of `44px × 44px`.
   * Form inputs and select elements enforce `16px` font size on mobile to prevent iOS Safari auto-zoom.
4. **Authentic dbt Cloud Theme**:
   * dbt signature orange (`#ff694b`), dark navy headers (`#040a14`), slate canvas (`#f8fafc`), and official Explorer checklist & badge palettes.

---

## 2. Shared Visual Tokens

```text
┌────────────────────────────────────────────────────────────────────────┐
│  dbt Labs Brand Tokens                                                 │
│  ■ dbt Orange: #ff694b    ■ Header Navy: #040a14 / #0b111e             │
│  ■ Slate Canvas: #f8fafc  ■ Surface Card: #ffffff                      │
└────────────────────────────────────────────────────────────────────────┘
```

### 2.1 Health & Status Palette (From dbt Explorer Screenshots)
| Token Name | Hex Code | Background Tint | Meaning |
| :--- | :--- | :--- | :--- |
| `--status-healthy` | `#16a34a` / `#22c55e` | `#f0fdf4` / `#dcfce7` | All models built, all tests passed, sources healthy |
| `--status-caution` | `#b45309` / `#d97706` | `#fffbeb` / `#fef3c7` | Upstream source sync delayed or approaching SLA limit |
| `--status-degraded` | `#dc2626` / `#ef4444` | `#fef2f2` / `#fee2e2` | Test failure or compile/runtime error detected |
| `--popover-border` | `#2563eb` | `#ffffff` | Active 2px blue boundary for the health checkpoint card |

### 2.2 Lineage Node Pill Badges (From Column Lineage Screenshot)
| Pill Badge | Background | Text Color | Border | Node Category |
| :--- | :--- | :--- | :--- | :--- |
| **`RAW`** | `#dcfce7` | `#15803d` | `#bbf7d0` | Raw ingestion table |
| **`RENAME`** | `#f3e8ff` | `#6b21a8` | `#e9d5ff` | Aliased / staging column |
| **`PASSTHROUGH`** | `#e0f2fe` | `#0369a1` | `#bae6fd` | Passthrough transformation |
| **`TRANSFORMATION`**| `#ffedd5` | `#c2410c` | `#fed7aa` | Business logic calculation |
| **`SOURCE`** | `#ccfbf1` | `#0f766e` | `#99f6e4` | Source warehouse table |
| **`MODEL`** | `#dbeafe` | `#1d4ed8` | `#bfdbfe` | dbt SQL model |
| **`EXPOSURE`** | `#fef3c7` | `#92400e` | `#fde68a` | Downstream BI dashboard |

---

## 3. Detailed UI Design: Workflow by Workflow

```
┌────────────────────────────────────────────────────────────────────────┐
│ WORKFLOW 1: Governed Metric Explorer (Semantic Layer)                  │
├────────────────────────────────────────────────────────────────────────┤
│ WORKFLOW 2: Data Trust & Freshness Audit (Metrics, Queries, Exposures) │
├────────────────────────────────────────────────────────────────────────┤
│ WORKFLOW 3: Column Lineage (CLL) & Pre-Migration Impact Analysis       │
├────────────────────────────────────────────────────────────────────────┤
│ WORKFLOW 4: Autonomous Pipeline Incident Triage & Safe Retry           │
├────────────────────────────────────────────────────────────────────────┤
│ WORKFLOW 5: Cross-Project dbt Mesh Contract Catalog                    │
└────────────────────────────────────────────────────────────────────────┘
```

---

### Workflow 1: Governed Metric Explorer (Semantic Layer)
* **Persona**: Executives, C-Suite, Business Decision-Makers, Product Leads
* **Tools**: `list_metrics`, `get_dimensions`, `get_dimension_values`, `query_metrics`, `list_saved_queries`, `get_metrics_compiled_sql`

#### UI Layout & Components
1. **Governed Query Summary Bar**:
   * Header displaying metric name, formula definition, and the green `✓ Governed Metric` seal.
2. **Interactive Grain & Dimension Controls**:
   * Horizontal segmented button bar: Time Grains (`Monthly`, `Quarterly`, `Weekly`) and filter chips (`customer_region: NA, EMEA`).
3. **Metric Scorecards**:
   * High-impact aggregate numbers (e.g. `142,500 MAU`) with MoM growth trend pills (`↑ +8.4% MoM`).
4. **Dimensional Breakdown Table & Compiled SQL Accordion**:
   * Clean table with verified status badges for each slice.
   * Expandable `[View Compiled SQL]` drawer rendering syntax-highlighted SQL from `get_metrics_compiled_sql`.
5. **Guided Action Bar**:
   * `[Export to Sheets]` and `[Post to Chat]` buttons.

---

### Workflow 2: Data Trust & Freshness Audit (Metrics, Queries & Exposures)
* **Persona**: Business & Financial Analysts, BI Developers
* **Tools**: `query_metrics`, `execute_sql`, `get_model_health`, `get_all_sources`, `get_lineage`, `get_exposures`

#### UI Layout & Components
1. **Audit Scope Selector**:
   * Three primary entry points: `BI Exposure (Looker)` / `Semantic Metric` / `Ad-Hoc SQL Query`.
2. **1-Second Verdict Card**:
   * Prominent traffic-light status pill (`Healthy (Safe)` / `Caution (Stale)` / `Degraded (Error)`).
3. **3-Checkpoint Health Card** (Directly matching dbt Explorer screenshot):
   * `✓ All upstream models built successfully` (with node build count)
   * `✓ All upstream models passed their tests` (with passed test count)
   * `✓ All upstream sources healthy` (with latency SLA check)
4. **Adaptive Provenance Lineage**:
   * Left-to-Right (`→`) on desktop; Top-Down (`↓`) on mobile.
5. **Guided Action Bar**:
   * `[Copy Trust Brief]` and `[Re-check Freshness]`.

---

### Workflow 3: Pre-Migration Column Lineage (CLL) & Impact Analysis
* **Persona**: Analytics Engineers, Data Developers
* **Tools**: `get_node_details`, `fusion.get_column_lineage`, `get_exposures`, `generate_model_yaml`

#### UI Layout & Components
1. **Column Detail Header** (Directly matching Screenshot #1):
   * Column name in monospace: `COUNT_LIFETIME_ORDERS NUMBER`.
   * Action badge: `⌘ CLL`.
   * Subtitle: *"Total number of orders a customer has ever placed."*
   * Tags: `events`, `staging`.
   * Banner: *"Column lineage may be missing columns (i)"*.
2. **Column Lineage DAG**:
   * **Node 1 (Raw Source)**: `ecom.raw_orders` (`RAW` green pill) → Column: `ID` (`SOURCE` teal badge).
   * **Node 2 (Staging Model)**: `stg_orders` (`RENAME` purple pill) → Column: `ORDER_ID 🔑PK` (`MODEL` blue badge).
   * **Node 3 (Intermediate)**: `orders` (`PASSTHROUGH` sky blue pill) → Column: `ORDER_ID 🔑PK` (`MODEL` blue badge).
   * **Node 4 (Mart Model)**: `customers` (`TRANSFORMATION` orange pill) → Column: `COUNT_LIFETIME_ORDERS` with SQL expression `COALESCE(COUNT(DISTINCT...))`.
3. **Orientation Handling**:
   * **Desktop Canvas**: Horizontal flow `→` across a dotted canvas.
   * **Mobile (Tiny Screen)**: Vertical flow `↓` with 100% width cards and downward arrows.
4. **Guided Action Bar**:
   * `[Generate Model YAML]` to scaffold updated schema definitions with inherited descriptions.

---

### Workflow 4: Autonomous Pipeline Incident Triage & Safe Retry
* **Persona**: DataOps, On-Call Engineers, Analytics Leads
* **Tools**: `list_jobs_runs`, `get_job_run_error`, `get_job_run_details`, `get_job_run_artifacts`, `retry_job_run`

#### UI Layout & Components
1. **Incident Header**:
   * Red error banner: Job Run `#98124` — *Nightly Production Build (03:14 AM)*.
   * Environment and duration tags.
2. **Extracted Error & Root Cause Card** (via `get_job_run_error`):
   * Monospace code block extracting the exact failure:
     `SQL compilation error: Lock wait timeout exceeded (300 seconds) while awaiting table lock on raw_events.`
   * Eliminates digging through megabytes of raw CLI logs.
3. **Safe Retry Assessment Matrix**:
   * Three summary tiles:
     * `142 Models Succeeded` (Green)
     * `1 Model Failed` (Red)
     * `2 Models Skipped` (Amber)
4. **Prominent Safe Retry Action**:
   * Big green button: `⚡ Trigger Targeted Safe Retry (3 Models)`.
   * Re-runs only the failed model and 2 skipped dependencies, saving warehouse compute.

---

### Workflow 5: Cross-Project dbt Mesh Contract Catalog
* **Persona**: Domain Data Teams, Cross-Project Consumers
* **Tools**: `search_public_mesh_models`, `get_node_details`, `get_model_performance`

#### UI Layout & Components
1. **Mesh Search & Domain Filter Bar**:
   * Keyword input search bar (`"customer contract"`, `"revenue"`) + Domain selector dropdown (`Finance Core`, `Product`, `Marketing`).
2. **Contracted Model Card**:
   * Model identifier: `finance_core.dim_customer_contracts`.
   * Governance pills: `Access: Public` (Green) and `Contract: Enforced` (Blue).
   * Performance badge: `✓ 99.8% SLA Adherence`.
   * Column specification pills with data types and primary key indicators (`🔑`).
3. **One-Click Jinja Snippet Box**:
   * Monospace snippet box with `[Copy Ref]` button:
     ```sql
     {{ ref('finance_core', 'dim_customer_contracts', v=1) }}
     ```

---

## 4. Multi-Formfactor Responsive Matrix

| Breakpoint | Formfactor | Lineage Layout | Touch & Inputs | Card Ergonomics |
| :--- | :--- | :--- | :--- | :--- |
| **`>= 840px`** | Desktop (GE Canvas 50% split) | **Left-to-Right (`⇄ LR`)** with horizontal connectors `→` | Mouse hover tooltips, 14px body font | Single-column calm layout (`max-width: 840px`) |
| **`768px`** | Tablet (iPad portrait/landscape) | **Left-to-Right (`⇄ LR`)** with touch swipe | `≥ 44px` touch targets | Fluid width with collapsible sections |
| **`320px–430px`**| Mobile (iPhone / Android) | **Top-Down (`⇅ TD`)** with vertical connectors `↓` | `16px` form inputs (no Safari zoom), full-width buttons | Vertical stacked card sequence, zero horizontal scrolling |

---

## 5. Prototype & Artifact Index

* **Master Workbench HTML Prototype**: [dbt_enterprise_workbench.html](file:///Users/stephenrobb/.gemini/antigravity/brain/5b053b02-0787-47d2-aa42-cb825ee75e6e/dbt_enterprise_workbench.html)
* **Workspace Design Specification**: [docs/ui-design-gemini-enterprise-workflows.md](file:///Users/stephenrobb/Dev/MCP/dbt-mcp/docs/ui-design-gemini-enterprise-workflows.md)
* **Workflow Catalog**: [docs/gemini-enterprise-workflows.md](file:///Users/stephenrobb/Dev/MCP/dbt-mcp/docs/gemini-enterprise-workflows.md)

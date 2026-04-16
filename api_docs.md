# 📊 Dashboard API Documentation

> **Base URL**: `http://localhost:8000`
> **Auth**: Bearer Token required on all endpoints

---

## 🔑 Common Query Parameters

These filters are accepted by **every** dashboard endpoint.

| Parameter | Type | Format | Required | Description |
|---|---|---|---|---|
| `branch` | string (repeatable) | UUID / ID | No | Filter by branch. Use multiple times: `?branch=1&branch=2` |
| `from_date` | string | YYYY-MM-DD | No* | Start date. Required if `to_date` is given |
| `to_date` | string | YYYY-MM-DD | No* | End date (inclusive). Required if `from_date` is given |
| `period` | string | 30D / 3M / 1Y | No | Relative date shortcut. Overrides from/to dates |

> [!NOTE]
> `from_date` and `to_date` must always be provided **together**. Providing only one returns HTTP 400.

> [!TIP]
> **Date Boundary**: `to_date` is inclusive. Internally the system adds 1 day, so `to_date=2026-04-15` captures all records up to and including April 15.

---

## 🚀 Unified Dashboard Route (Recommended)

### GET `/api/dashboard/{dashboard_name}`

Loads KPIs + Charts in one call. Optionally load specific paginated table data.

#### Path Parameter

| Parameter | Description |
|---|---|
| `dashboard_name` | One of the valid dashboard identifiers listed below |

#### All Query Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `branch` | string (repeatable) | — | Branch filter |
| `from_date` | string | — | Start date `YYYY-MM-DD` |
| `to_date` | string | — | End date `YYYY-MM-DD` |
| `period` | string | — | Relative period: `30D`, `3M`, `1Y` |
| `table` | string (repeatable) | — | Request specific table(s) by ID — bypasses KPI+Charts entirely |
| `page` | integer | 1 | Global page number for table pagination |
| `size` | integer | 10 | Global page size for table pagination |
| `{table_id}_page` | integer | 1 | Per-table page override — e.g. `recent_orders_page=2` |
| `{table_id}_size` | integer | 10 | Per-table size override — e.g. `recent_orders_size=25` |

#### Valid `dashboard_name` Values

| dashboard_name | Dashboard |
|---|---|
| `financial` | Financial |
| `hrm` | Human Resource Management |
| `petty_cash` | Petty Cash |
| `employee_management` | Employee Management |
| `inventory` | Inventory & Stock |
| `vendor` | Vendor Management |
| `purchase` | Purchase Orders |
| `lead_followup` | Leads & Follow-ups |
| `quotation` | Quotations |
| `gma` | Gross Margin Analysis |
| `customer_management` | Customer Management |
| `contract_management` | Contract Management |
| `sales_order` | Sales Orders |
| `task_management` | Task Management |
| `customer_support` | Customer Support |
| `branch_management` | Branch Management |

#### Example URLs

- Load Financial dashboard by date range:
  `GET /api/dashboard/financial?from_date=2026-01-01&to_date=2026-04-15`

- Load HRM filtered by 2 branches and a period:
  `GET /api/dashboard/hrm?branch=1&branch=2&period=30D`

- Load a specific paginated table:
  `GET /api/dashboard/sales_order?table=recent_orders&page=1&size=25`

- Load multiple tables with per-table pagination:
  `GET /api/dashboard/task_management?table=recent_tasks&table=material_usage&recent_tasks_page=1&recent_tasks_size=10&material_usage_size=5`

---

## 📋 Available `table` IDs per Dashboard

Use these values in the `?table=` parameter.

### `financial`

| Table ID | Description |
|---|---|
| `revenue_summary` | Revenue grouped by branch |
| `task_summary` | Tasks grouped by status |
| `customer_revenue` | Revenue per customer |
| `invoice_details` | Recent invoice list |
| `product_consumption` | Stock consumption by product |
| `vendor_outstanding` | Vendor payment outstanding |

### `hrm`

| Table ID | Description |
|---|---|
| `employee_list` | Full employee roster |
| `salary_slips` | Salary slip records |

### `petty_cash`

| Table ID | Description |
|---|---|
| `recent_requests` | Latest petty cash requests |
| `approved_payments` | Paid / approved requests |

### `employee_management`

| Table ID | Description |
|---|---|
| `critical_hiring` | Hiring requests with more than 5 positions |
| `compensation_audit` | Salary breakdown per employee |

### `inventory`

| Table ID | Description |
|---|---|
| `low_stock_products` | Products with LOW status |
| `out_of_stock_products` | Products with OUT status |
| `branch_stock` | Stock levels per branch |
| `central_stock_entries` | Central warehouse entries |
| `recent_stock_movements` | Recent movement log |
| `stock_transfers` | Inter-branch stock transfers |

### `vendor`

| Table ID | Description |
|---|---|
| `recent_vendor_additions` | Newly added vendors |
| `active_contract_list` | Vendors with active contracts |

### `purchase`

| Table ID | Description |
|---|---|
| `recent_po` | Recent purchase orders |
| `vendor_summary` | PO summary per vendor |

### `lead_followup`

| Table ID | Description |
|---|---|
| `recent_leads` | Latest lead records |
| `upcoming_followups` | Leads with upcoming follow-up dates |

### `quotation`

| Table ID | Description |
|---|---|
| `high_value_quotes` | Quotes with total over 25,000 |
| `expiring_quotes` | SENT quotes expiring in next 3 days |

### `gma`

| Table ID | Description |
|---|---|
| `recent_gma` | Recent GMA sheets |
| `approved_summary` | Approved GMA financial details |

### `customer_management`

| Table ID | Description |
|---|---|
| `recent_customers` | Latest customer additions |
| `active_contracts` | Customers with active contracts |

### `contract_management`

| Table ID | Description |
|---|---|
| `recent_contracts` | Recently created contracts |
| `expiring_contracts` | Contracts expiring within 30 days |

### `sales_order`

| Table ID | Description |
|---|---|
| `recent_orders` | Recent sales orders |

### `task_management`

| Table ID | Description |
|---|---|
| `recent_tasks` | Recent tasks with customer info |
| `material_usage` | Material consumption per task |

### `customer_support`

| Table ID | Description |
|---|---|
| `recent_tickets` | Recently created support tickets |
| `open_high_priority` | Open HIGH priority tickets |

---

## 🗂️ Legacy Module-Specific Routes

Each module has its own set of endpoints for KPI, Charts, Tables, and Alerts. All accept the same common query parameters.

> [!WARNING]
> These are legacy routes. Use `/api/dashboard/{name}` for new integrations.

---

### 💰 Financial

| Endpoint | Description |
|---|---|
| `GET /api/financial/charts` | Charts data |
| `GET /api/financial/tables` | Tables data |
| `GET /api/financial/download` | Download report |

Filters: `branch`, `from_date`, `to_date`, `period`
Branch column: `sales_invoices.branch_id`

---

### 👥 HRM

| Endpoint | Description |
|---|---|
| `GET /api/hrm/kpi` | KPI metrics |
| `GET /api/hrm/charts` | Charts data |
| `GET /api/hrm/tables` | Tables data |
| `GET /api/hrm/alerts` | Alert data |

Filters: `branch`, `from_date`, `to_date`, `period`
Branch column: via `user_branches` junction table
Date column: `date_of_joining`, `hrm_salary_month.created_at`, `hrm_holidays.holiday_date`

---

### 💵 Petty Cash

| Endpoint | Description |
|---|---|
| `GET /api/petty-cash/kpi` | KPI metrics |
| `GET /api/petty-cash/charts` | Charts data |
| `GET /api/petty-cash/tables` | Tables data |
| `GET /api/petty-cash/alerts` | Alert data |

Filters: `branch`, `from_date`, `to_date`, `period`
Branch column: `petty_cash_requests.requester_branch_id`
Date column: `petty_cash_requests.submitted_at`

---

### 🏭 Inventory

| Endpoint | Description |
|---|---|
| `GET /api/inventory/kpi` | KPI metrics |
| `GET /api/inventory/charts` | Charts data |
| `GET /api/inventory/tables` | Tables data |
| `GET /api/inventory/alerts` | Alert data |

Filters: `branch`, `from_date`, `to_date`, `period`
Branch column: `stock_ledger.branch_id`
Date column: `stock_ledger.created_at`

---

### 🏪 Vendor Management

| Endpoint | Description |
|---|---|
| `GET /api/vendor-management/kpi` | KPI metrics |
| `GET /api/vendor-management/charts` | Charts data |
| `GET /api/vendor-management/tables` | Tables data |
| `GET /api/vendor-management/alerts` | Alert data |

Filters: `branch`, `from_date`, `to_date`, `period`
Date column: `vendors.created_at`

---

### 📦 Purchase

| Endpoint | Description |
|---|---|
| `GET /api/purchase/kpi` | KPI metrics |
| `GET /api/purchase/charts` | Charts data |
| `GET /api/purchase/tables` | Tables data |
| `GET /api/purchase/alerts` | Alert data |

Filters: `branch`, `from_date`, `to_date`, `period`
Branch column: `purchase_order.branch_id`
Date column: `purchase_order.created_at`

---

### 📣 Lead Follow-up

| Endpoint | Description |
|---|---|
| `GET /api/lead-followup/kpi` | KPI metrics |
| `GET /api/lead-followup/charts` | Charts data |
| `GET /api/lead-followup/tables` | Tables data |
| `GET /api/lead-followup/alerts` | Alert data |

Filters: `branch`, `from_date`, `to_date`, `period`
Date column: `leads.created_at`
RBAC Modules: `LEADS_MANAGEMENT`, `FOLLOW_UP_MANAGEMENT`

---

### 📝 Quotation

| Endpoint | Description |
|---|---|
| `GET /api/quotation/kpi` | KPI metrics |
| `GET /api/quotation/charts` | Charts data |
| `GET /api/quotation/tables` | Tables data |
| `GET /api/quotation/alerts` | Alert data |

Filters: `branch`, `from_date`, `to_date`, `period`
Date column: `quotations.created_at`

---

### 📈 GMA

| Endpoint | Description |
|---|---|
| `GET /api/gma/kpi` | KPI metrics |
| `GET /api/gma/charts` | Charts data |
| `GET /api/gma/tables` | Tables data |
| `GET /api/gma/alerts` | Alert data |

Filters: `branch`, `from_date`, `to_date`, `period`
Branch column: `gma_sheets.branch_id`
Date column: `gma_sheets.created_at`

---

### 👷 Employee Management

| Endpoint | Description |
|---|---|
| `GET /api/employee-management/kpi` | KPI metrics |
| `GET /api/employee-management/charts` | Charts data |
| `GET /api/employee-management/tables` | Tables data |
| `GET /api/employee-management/alerts` | Alert data |

Filters: `branch`, `from_date`, `to_date`, `period`
Branch column: via `user_branches` junction table
Date column: `users.date_of_joining`

---

### 🤝 Customer Management

| Endpoint | Description |
|---|---|
| `GET /api/customer-management/kpi` | KPI metrics |
| `GET /api/customer-management/charts` | Charts data |
| `GET /api/customer-management/tables` | Tables data |
| `GET /api/customer-management/alerts` | Alert data |

Filters: `branch`, `from_date`, `to_date`, `period`
Branch column: `customers.branch_id`
Date column: `customers.created_at`

---

### 📃 Contract Management

| Endpoint | Description |
|---|---|
| `GET /api/contract-management/kpi` | KPI metrics |
| `GET /api/contract-management/charts` | Charts data |
| `GET /api/contract-management/tables` | Tables data |
| `GET /api/contract-management/alerts` | Alert data |

Filters: `branch`, `from_date`, `to_date`, `period`
Branch column: `contracts.branch_id`
Date column: `contracts.created_at`, `contracts.start_date`

---

### 🛒 Sales Order

| Endpoint | Description |
|---|---|
| `GET /api/sales-order/kpi` | KPI metrics |
| `GET /api/sales-order/charts` | Charts data |
| `GET /api/sales-order/tables` | Tables data |
| `GET /api/sales-order/alerts` | Alert data |

Filters: `branch`, `from_date`, `to_date`, `period`
Branch column: `sales_orders.branch_id`
Date column: `sales_orders.created_at`

---

### ✅ Task Management

| Endpoint | Description |
|---|---|
| `GET /api/task-management/kpi` | KPI metrics |
| `GET /api/task-management/charts` | Charts data |
| `GET /api/task-management/tables` | Tables data |
| `GET /api/task-management/alerts` | Alert data |

Filters: `branch`, `from_date`, `to_date`, `period`
Date column: `tasks.created_at`, `tasks.scheduled_date`

---

### 🎧 Customer Support

| Endpoint | Description |
|---|---|
| `GET /api/customer-support/kpi` | KPI metrics |
| `GET /api/customer-support/charts` | Charts data |
| `GET /api/customer-support/tables` | Tables data |
| `GET /api/customer-support/alerts` | Alert data |

Filters: `branch`, `from_date`, `to_date`, `period`
Branch column: `support_tickets.branch_id`
Date column: `support_tickets.created_at`

---

### 🏢 Branch Management

| Endpoint | Description |
|---|---|
| `GET /api/branch-management/kpi` | KPI metrics |
| `GET /api/branch-management/charts` | Charts data |
| `GET /api/branch-management/tables` | Tables data |
| `GET /api/branch-management/alerts` | Alert data |

Filters: `branch`, `from_date`, `to_date`, `period`

---

## ⚠️ Error Responses

All errors are handled centrally and return consistent JSON.

| HTTP Code | `status` | Cause |
|---|---|---|
| `400` | `fail` | Invalid/missing date parameters |
| `401` | `fail` | Missing or expired Bearer token |
| `403` | `fail` | Role lacks dashboard access |
| `404` | `fail` | Dashboard name not found |
| `500` | `error` | Unhandled server-side exception |
| `500` | `db_error` | Database / SQLAlchemy exception |

---

## 📅 Filter Quick Reference

| Goal | URL Pattern |
|---|---|
| Date range | `?from_date=2026-01-01&to_date=2026-03-31` |
| Relative period | `?period=30D` |
| Single branch | `?branch=abc123` |
| Multiple branches | `?branch=abc123&branch=def456` |
| Branch + date | `?branch=1&from_date=2026-01-01&to_date=2026-03-31` |
| Paginated table | `?table=recent_orders&page=2&size=20` |
| Multiple tables | `?table=recent_tasks&table=material_usage` |
| Per-table pagination | `?table=recent_tasks&recent_tasks_page=1&recent_tasks_size=15` |

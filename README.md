# 📊 Enterprise Resource Planning (ERP) Dashboard System

![Project Status](https://img.shields.io/badge/status-active-brightgreen.svg)
![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100%2B-009688.svg)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0%2B-red.svg)

---

## 📑 Table of Contents

1. [Overview](#-overview)
2. [Key Features](#-key-features)
3. [Technology Stack](#-technology-stack)
4. [System Modules](#-system-modules)
5. [Architecture & Design](#-architecture--design)
6. [Getting Started](#-getting-started)
   - [Prerequisites](#prerequisites)
   - [Installation](#installation)
   - [Environment Configuration](#environment-configuration)
   - [Running the Server](#running-the-server)
7. [API & Dashboard Structure](#-api--dashboard-structure)
8. [Security & RBAC](#-security--rbac)
9. [Documentation Reference](#-documentation-reference)
10. [Footer & Support](#-footer--support)

---

## 🚀 Overview

This repository contains a highly scalable, multi-tenant backend system designed for a comprehensive Enterprise Resource Planning (ERP) dashboard. Built with **FastAPI** and **Python**, it offers lightning-fast performance, granular Role-Based Access Control (RBAC), and schema-aware database architecture to ensure data isolation across different branches and tenants.

---

## ✨ Key Features

- **Multi-Tenant Architecture**: Supports multiple branches with strict data isolation and schema-aware queries.
- **Granular RBAC**: Role-Based Access Control enforcing permissions at the module, table, and row level.
- **Unified API Routing**: Centralized `/api/dashboard/{dashboard_name}` endpoints serving KPIs, charts, and paginated table data dynamically.
- **Dynamic Filtering**: Robust data filtering out of the box (`branch`, `from_date`, `to_date`, `period`).
- **Data Export & Reporting**: Built-in support for generating charts and PDF exports for various business modules.
- **Highly Modular**: Decoupled service and repository layers making it extremely easy to add new modules.

---

## 🛠 Technology Stack

- **Framework:** [FastAPI](https://fastapi.tiangolo.com/) - High-performance web framework.
- **Language:** Python 3.10+
- **Database ORM:** [SQLAlchemy](https://www.sqlalchemy.org/) - Robust database abstraction.
- **Authentication:** JWT (JSON Web Tokens) with module-specific permissions.
- **Server:** Uvicorn - ASGI web server implementation.

---

## 📦 System Modules

The ERP is broken down into interconnected business modules, each possessing its own KPIs, tables, and analytical charts. 

| Module | Description | Core Entities |
| :--- | :--- | :--- |
| **💰 Financial** | Tracks revenue, invoices, and expenses. | `sales_invoices`, `revenue_summary` |
| **🏭 Inventory** | Manages stock, movements, and transfers. | `stock_ledger`, `inventory_products` |
| **👥 HRM & Employee** | Employee rosters, salaries, and critical hiring. | `users`, `hrm_salary_month` |
| **💵 Petty Cash** | Manages branch-level cash requests. | `petty_cash_requests` |
| **🏪 Vendor & Purchase** | Vendor contracts and purchase orders (POs). | `vendors`, `purchase_order` |
| **📣 Leads & Quotation** | CRM functions tracking leads to quotes. | `leads`, `quotations` |
| **📈 GMA** | Gross Margin Analysis reporting. | `gma_sheets` |
| **🤝 Customer & Contracts** | Client database and active contracts. | `customers`, `contracts` |
| **🛒 Sales Order** | Tracking multi-branch sales pipelines. | `sales_orders` |
| **✅ Task & Support** | Internal task delegation and client tickets. | `tasks`, `support_tickets` |
| **🏢 Branch Management** | Administrative oversight across business units. | `branches` |

---

## 🏗 Architecture & Design

The application follows a clean architectural pattern to ensure maintainability:

1. **Routers (`app/api/`)**: Defines the API endpoints, validates incoming HTTP requests, and enforces authentication/RBAC.
2. **Services (`app/services/`)**: Contains the core business logic. Transforms and orchestrates data fetched from repositories.
3. **Repositories (`app/repositories/`)**: Manages direct database interactions, housing all SQLAlchemy queries and joins.
4. **Models (`app/models/`)**: Defines the database schema and relationships.
5. **Filters (`app/filters/`)**: Dedicated components for handling complex dynamic query parameters.

---

## 🏁 Getting Started

### Prerequisites

- Python 3.10 or higher
- Git
- Access to the target SQL database.

### Installation

1. **Clone the repository:**
   ```bash
   git clone <repository_url>
   cd New_erp
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   # On Windows
   .\venv\Scripts\activate
   # On macOS/Linux
   source venv/bin/activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

### Environment Configuration

Create a `.env` file in the root directory (if not already present). This file must contain your core configuration:

```env
DATABASE_URL=postgresql://user:password@localhost/erp_db
SECRET_KEY=your_super_secret_jwt_key
ALGORITHM=HS256
DEBUG=True
```

### Running the Server

Start the development server using Uvicorn:

```bash
python -m uvicorn app.main:app --reload
```

The application will be available at `http://localhost:8000`.
- Interactive API docs (Swagger UI): `http://localhost:8000/docs`
- Redoc API docs: `http://localhost:8000/redoc`

---

## 🌐 API & Dashboard Structure

The core principle of the frontend-backend communication relies on a **Unified Dashboard Route**. 

**Endpoint:** `GET /api/dashboard/{dashboard_name}`

Instead of maintaining hundreds of discrete endpoints, the frontend queries a specific module and passes filters. 
Example Request:
```http
GET /api/dashboard/inventory?from_date=2026-01-01&to_date=2026-04-15&branch=1
```

*(For deep-dive instructions on API payloads, pagination, and legacy routes, please see `api_docs.md`).*

---

## 🛡 Security & RBAC

The system employs a strict **Role-Based Access Control (RBAC)** architecture. 

- **Token Parsing:** Users authenticate via standard OAuth2 JWT. The token contains the user's allowed modules and branch scopes.
- **Service Layer Enforcement:** The `app/services/` layer injects permissions into repository calls, ensuring a user from "Branch A" cannot query "Branch B" data unless explicitly authorized.
- **Module Access:** Accessing a dashboard (e.g., `/api/dashboard/hrm`) will result in a `403 Forbidden` if the user's role lacks the `HRM_MANAGEMENT` claim.

---

## 📚 Documentation Reference

For a complete breakdown of API parameters, available table IDs, and module endpoints, please refer to the dedicated API documentation file:

👉 **[View API Documentation](./api_docs.md)**

---

<br>

<div align="center">
  <b>Enterprise Resource Planning Core</b><br>
  <i>Built with ❤️ by the Development Team</i>
</div>

---

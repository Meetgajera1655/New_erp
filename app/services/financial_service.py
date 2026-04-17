import time
from sqlalchemy import text
from app.filters.financial_filter import apply_financial_filters

class FinancialService:

    @staticmethod
    def get_kpis(db, schema, **kwargs):
        # We can add financial KPIs here in the future
        return {}

    @staticmethod
    def get_charts(db, schema, **kwargs):
        start_time = time.time()
        branch = kwargs.get("branch")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        period = kwargs.get("period")

        # 1. Filters
        where_si, params_si = apply_financial_filters("si", "invoice_date", branch, from_date, to_date, period)
        where_t, params_t = apply_financial_filters("t", "created_at", branch, from_date, to_date, period)
        
        # 2. Employee Filter via subquery
        u_sub = f"(SELECT _u.*, _ub.branch_id FROM \"{schema}\".users _u LEFT JOIN \"{schema}\".user_branches _ub ON _u.id = _ub.user_id)"
        where_u, params_u = apply_financial_filters("u", "date_of_joining", branch, from_date, to_date, period)

        # OPTIMIZATION: Combine multiple executions and add timing
        results = {
            "country_revenue": [
                {"date": str(r[0]), "revenue": float(r[1] or 0)}
                for r in db.execute(text(f"""
                    SELECT si.invoice_date, SUM(si.grand_total)
                    FROM "{schema}".sales_invoices si
                    WHERE {where_si}
                    GROUP BY si.invoice_date
                    ORDER BY si.invoice_date
                """), params_si).fetchall()
            ],

            "branch_revenue": [
                {"branch_id": r[0], "date": str(r[1]), "revenue": float(r[2] or 0)}
                for r in db.execute(text(f"""
                    SELECT si.branch_id, si.invoice_date, SUM(si.grand_total)
                    FROM "{schema}".sales_invoices si
                    WHERE {where_si}
                    GROUP BY si.branch_id, si.invoice_date
                    ORDER BY si.invoice_date
                """), params_si).fetchall()
            ],

            "revenue_breakup": [
                {"type": r[0], "value": float(r[1] or 0)}
                for r in db.execute(text(f"""
                    SELECT sil.item_type, SUM(sil.line_total)
                    FROM "{schema}".sales_invoice_lines sil
                    JOIN "{schema}".sales_invoices si ON sil.invoice_id = si.id
                    WHERE {where_si}
                    GROUP BY sil.item_type
                """), params_si).fetchall()
            ],

            "technician_productivity": [
                {
                    "technician_id": r[0], "technician_name": r[1],
                    "total_tasks": r[2], "total_revenue": float(r[3] or 0),
                    "productivity": float(r[4] or 0)
                }
                for r in db.execute(text(f"""
                    SELECT tt.user_id, tt.employee_name, COUNT(t.id),
                           COALESCE(SUM(si.grand_total), 0),
                           CASE WHEN COUNT(t.id)=0 THEN 0 ELSE COALESCE(SUM(si.grand_total),0)/COUNT(t.id) END
                    FROM "{schema}".task_technicians tt
                    JOIN "{schema}".tasks t ON tt.task_id = t.id
                    JOIN "{schema}".user_branches ub ON tt.user_id = ub.user_id
                    LEFT JOIN "{schema}".sales_invoices si ON si.sales_order_id = t.sales_order_id
                    WHERE tt.is_primary = TRUE AND t.status = 'COMPLETED'
                    AND ub.branch_id = ANY(:branches)
                    AND {where_t}
                    GROUP BY tt.user_id, tt.employee_name
                """), {**params_t, "branches": branch}).fetchall()
            ],

            "collection_vs_outstanding": {}, # Populated below
            "employee_growth": [
                {"date": str(r[0]), "count": r[1]}
                for r in db.execute(text(f"""
                    SELECT u.date_of_joining, COUNT(u.id)
                    FROM {u_sub} u
                    WHERE {where_u}
                    GROUP BY u.date_of_joining
                    ORDER BY u.date_of_joining
                """), params_u).fetchall()
            ],

            "invoice_status": [
                {"status": r[0], "count": r[1]}
                for r in db.execute(text(f"""
                    SELECT si.status, COUNT(*)
                    FROM "{schema}".sales_invoices si
                    WHERE {where_si}
                    GROUP BY si.status
                """), params_si).fetchall()
            ],
        }

        # COMBINED KPI Query for Collection vs Outstanding
        coll_metrics = db.execute(text(f"""
            SELECT 
                COALESCE(SUM(received_amount),0) AS collected,
                COALESCE(SUM(pending_amount),0) AS outstanding
            FROM "{schema}".sales_invoices si 
            WHERE {where_si}
        """), params_si).fetchone()
        
        results["collection_vs_outstanding"] = {
            "collected": float(coll_metrics[0]),
            "outstanding": float(coll_metrics[1])
        }

        print(f"Financial Charts Query Time: {time.time() - start_time:.4f}s")
        return results

    @staticmethod
    def get_tables(db, schema, **kwargs):
        start_time = time.time()
        branch = kwargs.get("branch")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        period = kwargs.get("period")

        where_si, params_si = apply_financial_filters("si", "invoice_date", branch, from_date, to_date, period)
        where_t, params_t = apply_financial_filters("t", "created_at", branch, from_date, to_date, period)
        
        where_po, params_po = apply_financial_filters("po", "created_at", branch, from_date, to_date, period)
        po_sub = f"(SELECT _po.*, _ub.branch_id FROM \"{schema}\".purchase_order _po LEFT JOIN \"{schema}\".users _u ON _po.created_by = _u.email LEFT JOIN \"{schema}\".user_branches _ub ON _u.id = _ub.user_id)"

        where_sl, params_sl = apply_financial_filters("sl", "created_at", branch, from_date, to_date, period)
        sl_sub = f"(SELECT _sl.*, _ub.branch_id FROM \"{schema}\".stock_ledger _sl LEFT JOIN \"{schema}\".users _u ON _sl.created_by = _u.email LEFT JOIN \"{schema}\".user_branches _ub ON _u.id = _ub.user_id)"

        results = {
            "revenue_summary": [
                {"branch_id": r[0], "branch_name": r[1], "total_revenue": float(r[2] or 0)}
                for r in db.execute(text(f"""
                    SELECT si.branch_id, b.branch_name, SUM(si.grand_total)
                    FROM "{schema}".sales_invoices si
                    LEFT JOIN "{schema}".branches b ON si.branch_id = b.id
                    WHERE {where_si}
                    GROUP BY si.branch_id, b.branch_name
                """), params_si).fetchall()
            ],

            "task_summary": [
                {"status": r[0], "total_tasks": r[1]}
                for r in db.execute(text(f"""
                    SELECT t.status, COUNT(*)
                    FROM "{schema}".tasks t
                    JOIN "{schema}".users _u_filter ON t.created_by = _u_filter.email
                    JOIN "{schema}".user_branches ub ON _u_filter.id = ub.user_id
                    WHERE {where_t.replace("t.", "ub.") if branch else "1=1"}
                    AND {where_t}
                    GROUP BY t.status
                """), params_t).fetchall()
            ],

            "customer_revenue": [
                {"customer_id": r[0], "customer_name": r[1], "total_revenue": float(r[2] or 0)}
                for r in db.execute(text(f"""
                    SELECT si.customer_id, c.full_name, SUM(si.grand_total)
                    FROM "{schema}".sales_invoices si
                    LEFT JOIN "{schema}".customers c ON si.customer_id = c.id
                    WHERE {where_si}
                    GROUP BY si.customer_id, c.full_name
                    LIMIT 20
                """), params_si).fetchall()
            ],

            "invoice_details": [
                {
                    "invoice_number": r[0], "customer_id": r[1], "customer_name": r[2],
                    "invoice_date": str(r[3]), "grand_total": float(r[4] or 0), "status": r[5]
                }
                for r in db.execute(text(f"""
                    SELECT si.invoice_number, si.customer_id, c.full_name, si.invoice_date, si.grand_total, si.status
                    FROM "{schema}".sales_invoices si
                    LEFT JOIN "{schema}".customers c ON si.customer_id = c.id
                    WHERE {where_si}
                    LIMIT 20
                """), params_si).fetchall()
            ],

            "product_consumption": [
                {"product_code": r[0], "product_name": r[1], "consumable_qty": float(r[2] or 0)}
                for r in db.execute(text(f"""
                    SELECT sl.product_code, sl.product_name, SUM(sl.consumable_qty)
                    FROM {sl_sub} sl
                    WHERE {where_sl}
                    GROUP BY sl.product_code, sl.product_name
                    LIMIT 20
                """), params_sl).fetchall()
            ],

            "vendor_outstanding": [
                {"vendor_name": r[0], "paid_amount": float(r[1] or 0), "pending_amount": float(r[2] or 0), "grand_total": float(r[3] or 0)}
                for r in db.execute(text(f"""
                    SELECT v.vendor_name, 0, SUM(po.grand_total), SUM(po.grand_total)
                    FROM "{schema}".vendors v
                    LEFT JOIN {po_sub} po ON po.vendor_id = v.id AND po.is_deleted = FALSE
                    WHERE {where_po}
                    GROUP BY v.vendor_name
                    ORDER BY v.vendor_name
                    LIMIT 20
                """), params_po).fetchall()
            ]
        }

        print(f"Financial Tables Query Time: {time.time() - start_time:.4f}s")
        return results
import time
from sqlalchemy import text
from app.filters.petty_cash_filter import apply_petty_cash_filters

class PettyCashService:

    @staticmethod
    def get_kpi(db, schema, **kwargs):
        start_time = time.time()
        branch = kwargs.get("branch")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        period = kwargs.get("period")

        # 1. Filters
        where_pc, params = apply_petty_cash_filters("pc", "submitted_at", branch, from_date, to_date, period)

        # OPTIMIZATION: Combine 4 queries into 1 single aggregate query
        metrics = db.execute(text(f"""
            SELECT 
                COALESCE(SUM(pc.amount_requested), 0),
                COALESCE(SUM(pc.approved_amount), 0),
                COUNT(*) FILTER (WHERE pc.status = 'PENDING'),
                COUNT(*) FILTER (WHERE pc.status = 'PAID')
            FROM "{schema}".petty_cash_requests pc 
            WHERE 1=1 AND {where_pc}
        """), params).fetchone()

        print(f"Petty Cash KPI Query Time: {time.time() - start_time:.4f}s")

        return {
            "total_requested": float(metrics[0]),
            "total_approved": float(metrics[1]),
            "pending_requests": metrics[2],
            "paid_requests": metrics[3],
        }

    @staticmethod
    def get_charts(db, schema, **kwargs):
        start_time = time.time()
        branch = kwargs.get("branch")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        period = kwargs.get("period")

        where_pc, params = apply_petty_cash_filters("pc", "submitted_at", branch, from_date, to_date, period)

        results = {
            "status_chart": [
                {"status": r[0], "count": r[1]}
                for r in db.execute(text(f"""
                    SELECT pc.status, COUNT(*) 
                    FROM "{schema}".petty_cash_requests pc
                    WHERE {where_pc}
                    GROUP BY pc.status
                """), params).fetchall()
            ],
            "branch_expense": [
                {"branch": r[0], "amount": r[1]}
                for r in db.execute(text(f"""
                    SELECT b.branch_name, COALESCE(SUM(pc.approved_amount), 0)
                    FROM "{schema}".petty_cash_requests pc
                    JOIN "{schema}".branches b ON pc.requester_branch_id = b.id
                    WHERE {where_pc}
                    GROUP BY b.branch_name
                    ORDER BY SUM(pc.approved_amount) DESC
                """), params).fetchall()
            ],
            "monthly_requests": [
                {"date": str(r[0]), "count": r[1]}
                for r in db.execute(text(f"""
                    SELECT DATE(pc.submitted_at), COUNT(*)
                    FROM "{schema}".petty_cash_requests pc
                    WHERE {where_pc}
                    GROUP BY DATE(pc.submitted_at)
                    ORDER BY DATE(pc.submitted_at)
                """), params).fetchall()
            ],
        }

        print(f"Petty Cash Charts Query Time: {time.time() - start_time:.4f}s")
        return results

    @staticmethod
    def get_tables(db, schema, **kwargs):
        start_time = time.time()
        branch = kwargs.get("branch")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        period = kwargs.get("period")

        where_pc, params = apply_petty_cash_filters("pc", "submitted_at", branch, from_date, to_date, period)

        results = {
            "recent_requests": [
                {
                    "request_id": r[0], "employee": r[1], "branch": r[2], "category": r[3],
                    "requested_amount": float(r[4]), "approved_amount": float(r[5]), "status": r[6], "submitted_at": str(r[7]),
                }
                for r in db.execute(text(f"""
                    SELECT 
                        pc.id, u.first_name, b.branch_name, pc.category,
                        pc.amount_requested, pc.approved_amount, pc.status, pc.submitted_at
                    FROM "{schema}".petty_cash_requests pc
                    JOIN "{schema}".users u ON pc.requester_user_id = u.id
                    JOIN "{schema}".branches b ON pc.requester_branch_id = b.id
                    WHERE {where_pc}
                    ORDER BY pc.submitted_at DESC
                    LIMIT 20
                """), params).fetchall()
            ],
            "approved_payments": [
                {
                    "request_id": r[0], "paid_by": r[1], "category": r[2], "approved_amount": float(r[3]),
                    "payment_mode": r[4], "transaction_ref": r[5], "payment_date": str(r[6]), "status": r[7],
                }
                for r in db.execute(text(f"""
                    SELECT 
                        pc.id, u.first_name, pc.category, pc.approved_amount,
                        pc.payment_mode_processed, pc.transaction_ref, pc.payment_date, pc.status
                    FROM "{schema}".petty_cash_requests pc
                    JOIN "{schema}".users u ON pc.paid_by_user_id = u.id
                    WHERE pc.status = 'PAID' AND {where_pc}
                    ORDER BY pc.payment_date DESC
                    LIMIT 20
                """), params).fetchall()
            ],
        }

        print(f"Petty Cash Tables Query Time: {time.time() - start_time:.4f}s")
        return results

    @staticmethod
    def get_alerts(db, schema, **kwargs):
        start_time = time.time()
        branch = kwargs.get("branch")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        period = kwargs.get("period")

        where_pc, params = apply_petty_cash_filters("pc", "submitted_at", branch, from_date, to_date, period)

        results = {
            "high_amount": [
                {
                    "id": r[0], "category": r[1], "amount": float(r[2]), "status": r[3], "submitted_at": str(r[4]),
                }
                for r in db.execute(text(f"""
                    SELECT id, category, amount_requested, status, submitted_at
                    FROM "{schema}".petty_cash_requests pc
                    WHERE pc.amount_requested > 10000 AND {where_pc}
                    ORDER BY pc.amount_requested DESC
                    LIMIT 20
                """), params).fetchall()
            ],
            "pending_old": [
                {
                    "id": r[0], "category": r[1], "amount": float(r[2]), "status": r[3], "submitted_at": str(r[4]),
                }
                for r in db.execute(text(f"""
                    SELECT id, category, amount_requested, status, submitted_at
                    FROM "{schema}".petty_cash_requests pc
                    WHERE pc.status = 'PENDING'
                    AND pc.submitted_at < CURRENT_DATE - INTERVAL '3 days'
                    AND {where_pc}
                    ORDER BY pc.submitted_at
                """), params).fetchall()
            ],
        }

        print(f"Petty Cash Alerts Query Time: {time.time() - start_time:.4f}s")
        return results
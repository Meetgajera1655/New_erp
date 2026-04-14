from sqlalchemy import text

class SalesOrderRepository:

    # ---------------- KPI ----------------
    @staticmethod
    def total_orders(db, schema):
        return db.execute(text(f"SELECT COUNT(id) FROM {schema}.sales_orders")).scalar()

    @staticmethod
    def total_amount(db, schema):
        return db.execute(text(f"SELECT COALESCE(SUM(grand_total),0) FROM {schema}.sales_orders")).scalar()

    @staticmethod
    def open_orders(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM {schema}.sales_orders
            WHERE status IN ('DRAFT','OPEN')
        """)).scalar()

    @staticmethod
    def completed_orders(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM {schema}.sales_orders
            WHERE status IN ('FULFILLED','BILLED')
        """)).scalar()

    # ---------------- CHARTS ----------------
    @staticmethod
    def status_chart(db, schema):
        return db.execute(text(f"""
            SELECT status, COUNT(id)
            FROM {schema}.sales_orders
            GROUP BY status
        """)).fetchall()

    @staticmethod
    def monthly_revenue(db, schema):
        return db.execute(text(f"""
            SELECT DATE_TRUNC('month', created_at), SUM(grand_total)
            FROM {schema}.sales_orders
            GROUP BY 1
            ORDER BY 1
        """)).fetchall()

    @staticmethod
    def branch_sales(db, schema):
        return db.execute(text(f"""
            SELECT b.branch_name, SUM(s.grand_total)
            FROM {schema}.sales_orders s
            JOIN {schema}.branches b ON s.branch_id = b.id
            GROUP BY b.branch_name
        """)).fetchall()

    # ---------------- TABLES ----------------
    @staticmethod
    def recent_orders(db, schema):
        return db.execute(text(f"""
            SELECT s.so_number, c.full_name, s.order_type, s.grand_total, s.status, b.branch_name, s.created_at
            FROM {schema}.sales_orders s
            JOIN {schema}.customers c ON s.customer_id = c.id
            JOIN {schema}.branches b ON s.branch_id = b.id
            ORDER BY s.created_at DESC
            LIMIT 20
        """)).fetchall()

    # ---------------- ALERTS ----------------
    @staticmethod
    def high_value_orders(db, schema):
        return db.execute(text(f"""
            SELECT so_number, customer_id, grand_total, status, created_at
            FROM {schema}.sales_orders
            WHERE grand_total > 50000
            AND status IN ('DRAFT','OPEN')
        """)).fetchall()

    @staticmethod
    def pending_orders(db, schema):
        return db.execute(text(f"""
            SELECT so_number, customer_id, status, created_at
            FROM {schema}.sales_orders
            WHERE status IN ('DRAFT','OPEN')
            AND created_at < CURRENT_DATE - INTERVAL '7 days'
        """)).fetchall()
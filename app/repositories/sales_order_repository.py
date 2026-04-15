from sqlalchemy import text
from app.filters.sales_order_filter import apply_sales_order_filters

class SalesOrderRepository:

    # ---------------- KPI ----------------
    @staticmethod
    def total_orders(db, schema, **kwargs):
        where_clause, params = apply_sales_order_filters(schema=schema, alias="so", **kwargs)
        query_sql = f"""
            SELECT COUNT(so.id)
            FROM {schema}.sales_orders so
            WHERE 1=1 AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def total_amount(db, schema, **kwargs):
        where_clause, params = apply_sales_order_filters(schema=schema, alias="so", **kwargs)
        query_sql = f"""
            SELECT COALESCE(SUM(so.grand_total), 0)
            FROM {schema}.sales_orders so
            WHERE 1=1 AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def open_orders(db, schema, **kwargs):
        where_clause, params = apply_sales_order_filters(schema=schema, alias="so", **kwargs)
        query_sql = f"""
            SELECT COUNT(so.id)
            FROM {schema}.sales_orders so
            WHERE so.status IN ('DRAFT', 'OPEN')
            AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def completed_orders(db, schema, **kwargs):
        where_clause, params = apply_sales_order_filters(schema=schema, alias="so", **kwargs)
        query_sql = f"""
            SELECT COUNT(so.id)
            FROM {schema}.sales_orders so
            WHERE so.status IN ('FULFILLED', 'BILLED')
            AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    # ---------------- CHARTS ----------------
    @staticmethod
    def status_chart(db, schema, **kwargs):
        where_clause, params = apply_sales_order_filters(schema=schema, alias="so", **kwargs)
        query_sql = f"""
            SELECT so.status, COUNT(so.id)
            FROM {schema}.sales_orders so
            WHERE 1=1 AND {where_clause}
            GROUP BY so.status
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def monthly_revenue(db, schema, **kwargs):
        where_clause, params = apply_sales_order_filters(schema=schema, alias="so", **kwargs)
        query_sql = f"""
            SELECT DATE_TRUNC('month', so.created_at), SUM(so.grand_total)
            FROM {schema}.sales_orders so
            WHERE 1=1 AND {where_clause}
            GROUP BY 1
            ORDER BY 1
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def branch_sales(db, schema, **kwargs):
        where_clause, params = apply_sales_order_filters(schema=schema, alias="so", **kwargs)
        query_sql = f"""
            SELECT b.branch_name, SUM(so.grand_total)
            FROM {schema}.sales_orders so
            JOIN {schema}.branches b ON so.branch_id = b.id
            WHERE 1=1 AND {where_clause}
            GROUP BY b.branch_name
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    # ---------------- TABLES ----------------
    @staticmethod
    def recent_orders(db, schema, **kwargs):
        where_clause, params = apply_sales_order_filters(schema=schema, alias="so", **kwargs)
        query_sql = f"""
            SELECT so.so_number, c.full_name, so.order_type, so.grand_total, so.status, b.branch_name, so.created_at
            FROM {schema}.sales_orders so
            JOIN {schema}.customers c ON so.customer_id = c.id
            JOIN {schema}.branches b ON so.branch_id = b.id
            WHERE 1=1 AND {where_clause}
            ORDER BY so.created_at DESC
            LIMIT 20
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    # ---------------- ALERTS ----------------
    @staticmethod
    def high_value_orders(db, schema, **kwargs):
        where_clause, params = apply_sales_order_filters(schema=schema, alias="so", **kwargs)
        query_sql = f"""
            SELECT so.so_number, so.customer_id, so.grand_total, so.status, so.created_at
            FROM {schema}.sales_orders so
            WHERE so.grand_total > 50000
            AND so.status IN ('DRAFT', 'OPEN')
            AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def pending_orders(db, schema, **kwargs):
        where_clause, params = apply_sales_order_filters(schema=schema, alias="so", **kwargs)
        query_sql = f"""
            SELECT so.so_number, so.customer_id, so.status, so.created_at
            FROM {schema}.sales_orders so
            WHERE so.status IN ('DRAFT', 'OPEN')
            AND so.created_at < CURRENT_DATE - INTERVAL '7 days'
            AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()
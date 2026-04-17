from sqlalchemy import text
from app.filters.customer_filter import apply_customer_filters

class CustomerRepository:

    # ================= KPI =================

    @staticmethod
    def total_customers(db, schema, **kwargs):
        where_clause, params = apply_customer_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT COUNT(c.id)
            FROM "{schema}".customers c
            WHERE c.status IN ('ACTIVE', 'INACTIVE')
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def active_customers(db, schema, **kwargs):
        where_clause, params = apply_customer_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT COUNT(c.id)
            FROM "{schema}".customers c
            WHERE c.status = 'ACTIVE'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def contract_customers(db, schema, **kwargs):
        where_clause, params = apply_customer_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT COUNT(c.id)
            FROM "{schema}".customers c
            WHERE c.customer_type = 'Contract'
            AND c.status IN ('ACTIVE', 'INACTIVE')
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def total_revenue(db, schema, **kwargs):
        # Using sales_orders (so) for revenue
        where_clause, params = apply_customer_filters(alias="so", **kwargs)
        query_sql = f"""
            SELECT COALESCE(SUM(so.grand_total),0)
            FROM "{schema}".sales_orders so
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    # ================= CHARTS =================

    @staticmethod
    def customer_type(db, schema, **kwargs):
        where_clause, params = apply_customer_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT c.customer_type, COUNT(c.id)
            FROM "{schema}".customers c
            WHERE c.status IN ('ACTIVE', 'INACTIVE')
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY c.customer_type"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def branch_customers(db, schema, **kwargs):
        where_clause, params = apply_customer_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT b.branch_name, COUNT(c.id)
            FROM "{schema}".customers c
            JOIN "{schema}".branches b
            ON c.branch_id = b.id
            WHERE c.status IN ('ACTIVE', 'INACTIVE')
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += """
            GROUP BY b.branch_name
            ORDER BY COUNT(c.id) DESC
        """

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def monthly_customers(db, schema, **kwargs):
        where_clause, params = apply_customer_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT DATE_TRUNC('month', c.created_at), COUNT(c.id)
            FROM "{schema}".customers c
            WHERE c.status IN ('ACTIVE', 'INACTIVE')
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += """
            GROUP BY DATE_TRUNC('month', c.created_at)
            ORDER BY DATE_TRUNC('month', c.created_at)
        """

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    # ================= TABLES =================

    @staticmethod
    def recent_customers(db, schema, **kwargs):
        where_clause, params = apply_customer_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT c.id, c.full_name, c.customer_type,
                   c.phone, b.branch_name, c.status, c.created_at
            FROM "{schema}".customers c
            JOIN "{schema}".branches b
            ON c.branch_id = b.id
            WHERE c.status IN ('ACTIVE', 'INACTIVE')
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += """
            ORDER BY c.created_at DESC
            LIMIT 10
        """

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def active_contracts(db, schema, **kwargs):
        where_clause, params = apply_customer_filters(alias="c", branch_alias="c", **kwargs)
        query_sql = f"""
            SELECT c.id, c.full_name,
                   con.id, con.start_date,
                   con.end_date, con.total_sale_value, con.status
            FROM "{schema}".customers c
            JOIN "{schema}".contracts con
            ON c.id = con.customer_id
            WHERE con.status = 'ACTIVE'
            AND c.status IN ('ACTIVE', 'INACTIVE')
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def inactive_customers(db, schema, **kwargs):
        where_clause, params = apply_customer_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT c.id, c.full_name, c.customer_type, c.branch_id, c.status
            FROM "{schema}".customers c
            WHERE c.status = 'INACTIVE'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def no_contract_customers(db, schema, **kwargs):
        where_clause, params = apply_customer_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT c.id, c.full_name, c.customer_type, c.created_at
            FROM "{schema}".customers c
            LEFT JOIN "{schema}".contracts con
            ON c.id = con.customer_id
            WHERE con.customer_id IS NULL
            AND c.status IN ('ACTIVE', 'INACTIVE')
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()
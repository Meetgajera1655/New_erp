from sqlalchemy import text
from app.filters.contract_filter import apply_contract_filters

class ContractRepository:

    # ================= KPI =================

    @staticmethod
    def total_contracts(db, schema, **kwargs):
        where_clause, params = apply_contract_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT COUNT(c.id)
            FROM "{schema}".contracts c
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def active_contracts(db, schema, **kwargs):
        where_clause, params = apply_contract_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT COUNT(c.id)
            FROM "{schema}".contracts c
            WHERE c.status = 'ACTIVE'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def total_value(db, schema, **kwargs):
        where_clause, params = apply_contract_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT COALESCE(SUM(c.total_sale_value),0)
            FROM "{schema}".contracts c
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def expiring_soon(db, schema, **kwargs):
        where_clause, params = apply_contract_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT COUNT(c.id)
            FROM "{schema}".contracts c
            WHERE c.end_date <= CURRENT_DATE + INTERVAL '30 days'
            AND c.status = 'ACTIVE'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    # ================= CHARTS =================

    @staticmethod
    def status_distribution(db, schema, **kwargs):
        where_clause, params = apply_contract_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT c.status, COUNT(c.id)
            FROM "{schema}".contracts c
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY c.status"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def branch_contracts(db, schema, **kwargs):
        where_clause, params = apply_contract_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT b.branch_name, COUNT(c.id)
            FROM "{schema}".contracts c
            JOIN "{schema}".branches b
            ON c.branch_id = b.id
            WHERE 1=1
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
    def monthly_value(db, schema, **kwargs):
        # Using start_date for trend if appropriate, but user said use created_at for FILTER.
        # So filter applies to created_at, but we SELECT by start_date for the chart.
        where_clause, params = apply_contract_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT DATE_TRUNC('month', c.start_date), SUM(c.total_sale_value)
            FROM "{schema}".contracts c
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += """
            GROUP BY DATE_TRUNC('month', c.start_date)
            ORDER BY DATE_TRUNC('month', c.start_date)
        """

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    # ================= TABLES =================

    @staticmethod
    def recent_contracts(db, schema, **kwargs):
        where_clause, params = apply_contract_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT c.id,
                   cu.full_name,
                   c.gma_sheet_id,
                   c.total_sale_value,
                   c.start_date,
                   c.end_date,
                   c.status,
                   b.branch_name
            FROM "{schema}".contracts c
            JOIN "{schema}".customers cu
            ON c.customer_id = cu.id
            JOIN "{schema}".branches b
            ON c.branch_id = b.id
            WHERE 1=1
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
    def expiring_list(db, schema, **kwargs):
        where_clause, params = apply_contract_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT c.id,
                   cu.full_name,
                   c.total_sale_value,
                   c.end_date,
                   c.status,
                   b.branch_name
            FROM "{schema}".contracts c
            JOIN "{schema}".customers cu
            ON c.customer_id = cu.id
            JOIN "{schema}".branches b
            ON c.branch_id = b.id
            WHERE c.end_date <= CURRENT_DATE + INTERVAL '30 days'
            AND c.status = 'ACTIVE'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " ORDER BY c.end_date"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def expiry_alert(db, schema, **kwargs):
        where_clause, params = apply_contract_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT c.id,
                   cu.full_name,
                   c.end_date,
                   c.total_sale_value,
                   c.status
            FROM "{schema}".contracts c
            JOIN "{schema}".customers cu
            ON c.customer_id = cu.id
            WHERE c.end_date <= CURRENT_DATE + INTERVAL '7 days'
            AND c.status = 'ACTIVE'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def no_sales_order(db, schema, **kwargs):
        where_clause, params = apply_contract_filters(alias="c", **kwargs)
        query_sql = f"""
            SELECT c.id,
                   c.total_sale_value,
                   c.start_date,
                   c.status
            FROM "{schema}".contracts c
            LEFT JOIN "{schema}".sales_orders so
            ON c.id = so.contract_id
            WHERE so.contract_id IS NULL
            AND c.status = 'ACTIVE'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()
from sqlalchemy import text
from app.filters.gma_filter import apply_gma_filters

class GMARepository:

    # ================= KPI =================

    @staticmethod
    def total_gma(db, schema, **kwargs):
        where_clause, params = apply_gma_filters(alias="g", **kwargs)
        query_sql = f"""
            SELECT COUNT(g.id)
            FROM "{schema}".gma_sheets g
            WHERE g.is_deleted = FALSE
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def approved_gma(db, schema, **kwargs):
        where_clause, params = apply_gma_filters(alias="g", **kwargs)
        query_sql = f"""
            SELECT COUNT(g.id)
            FROM "{schema}".gma_sheets g
            WHERE g.status = 'APPROVED'
            AND g.is_deleted = FALSE
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def pending_gma(db, schema, **kwargs):
        where_clause, params = apply_gma_filters(alias="g", **kwargs)
        query_sql = f"""
            SELECT COUNT(g.id)
            FROM "{schema}".gma_sheets g
            WHERE g.status = 'PENDING'
            AND g.is_deleted = FALSE
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def avg_margin(db, schema, **kwargs):
        where_clause, params = apply_gma_filters(alias="g", **kwargs)
        query_sql = f"""
            SELECT COALESCE(AVG(g.overall_gross_margin),0)
            FROM "{schema}".gma_sheets g
            WHERE g.is_deleted = FALSE
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
        where_clause, params = apply_gma_filters(alias="g", **kwargs)
        query_sql = f"""
            SELECT g.status, COUNT(g.id)
            FROM "{schema}".gma_sheets g
            WHERE g.is_deleted = FALSE
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY g.status"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def branch_gma(db, schema, **kwargs):
        where_clause, params = apply_gma_filters(alias="g", **kwargs)
        query_sql = f"""
            SELECT b.branch_name, COUNT(g.id)
            FROM "{schema}".gma_sheets g
            JOIN "{schema}".branches b
            ON g.branch_id = b.id
            WHERE g.is_deleted = FALSE
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += """
            GROUP BY b.branch_name
            ORDER BY COUNT(g.id) DESC
        """

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def monthly_gma(db, schema, **kwargs):
        where_clause, params = apply_gma_filters(alias="g", **kwargs)
        query_sql = f"""
            SELECT DATE_TRUNC('month', g.created_at), COUNT(g.id)
            FROM "{schema}".gma_sheets g
            WHERE g.is_deleted = FALSE
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += """
            GROUP BY DATE_TRUNC('month', g.created_at)
            ORDER BY DATE_TRUNC('month', g.created_at)
        """

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    # ================= TABLES =================

    @staticmethod
    def recent_gma(db, schema, **kwargs):
        where_clause, params = apply_gma_filters(alias="g", **kwargs)
        query_sql = f"""
            SELECT g.id, g.source_type, g.contract_duration,
                   g.proposed_start_date, b.branch_name,
                   g.total_annual_price, g.status, g.created_at
            FROM "{schema}".gma_sheets g
            JOIN "{schema}".branches b
            ON g.branch_id = b.id
            WHERE g.is_deleted = FALSE
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += """
            ORDER BY g.created_at DESC
            LIMIT 10
        """

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def approved_summary(db, schema, **kwargs):
        where_clause, params = apply_gma_filters(alias="g", **kwargs)
        query_sql = f"""
            SELECT g.id, g.total_annual_cost, g.total_annual_price,
                   g.overall_gross_margin, g.gm_with_doc,
                   g.total_visits_per_month, g.approved_on
            FROM "{schema}".gma_sheets g
            WHERE g.status = 'APPROVED'
            AND g.is_deleted = FALSE
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " ORDER BY g.approved_on DESC"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def pending_alert(db, schema, **kwargs):
        where_clause, params = apply_gma_filters(alias="g", **kwargs)
        query_sql = f"""
            SELECT g.id, g.source_type, g.status, g.deadline, g.created_at
            FROM "{schema}".gma_sheets g
            WHERE g.status = 'PENDING'
            AND g.deadline < CURRENT_TIMESTAMP
            AND g.is_deleted = FALSE
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def low_margin(db, schema, **kwargs):
        where_clause, params = apply_gma_filters(alias="g", **kwargs)
        query_sql = f"""
            SELECT g.id, g.total_annual_price, g.total_annual_cost,
                   g.overall_gross_margin, g.status
            FROM "{schema}".gma_sheets g
            WHERE g.overall_gross_margin < 10
            AND g.is_deleted = FALSE
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()
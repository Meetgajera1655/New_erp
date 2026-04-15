from sqlalchemy import text
from app.filters.quotation_filter import apply_quotation_filters

class QuotationRepository:

    # ================= KPI =================

    @staticmethod
    def total_quotes(db, schema, **kwargs):
        where_clause, params = apply_quotation_filters(alias="q", **kwargs)
        query_sql = f"""
            SELECT COUNT(q.id)
            FROM "{schema}".quotations q
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def accepted_rate(db, schema, **kwargs):
        where_clause, params = apply_quotation_filters(alias="q", **kwargs)
        query_sql = f"""
            SELECT 
                COUNT(*) FILTER (WHERE q.status = 'ACCEPTED')::float /
                NULLIF(COUNT(*),0)
            FROM "{schema}".quotations q
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def total_value(db, schema, **kwargs):
        where_clause, params = apply_quotation_filters(alias="q", **kwargs)
        query_sql = f"""
            SELECT COALESCE(SUM(q.grand_total),0)
            FROM "{schema}".quotations q
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def avg_value(db, schema, **kwargs):
        where_clause, params = apply_quotation_filters(alias="q", **kwargs)
        query_sql = f"""
            SELECT COALESCE(AVG(q.grand_total),0)
            FROM "{schema}".quotations q
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
    def status_chart(db, schema, **kwargs):
        where_clause, params = apply_quotation_filters(alias="q", **kwargs)
        query_sql = f"""
            SELECT q.status, COUNT(q.id)
            FROM "{schema}".quotations q
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY q.status"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def monthly_trend(db, schema, **kwargs):
        where_clause, params = apply_quotation_filters(alias="q", **kwargs)
        query_sql = f"""
            SELECT DATE_TRUNC('month', q.created_at), SUM(q.grand_total)
            FROM "{schema}".quotations q
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += """
            GROUP BY DATE_TRUNC('month', q.created_at)
            ORDER BY DATE_TRUNC('month', q.created_at)
        """

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def branch_performance(db, schema, **kwargs):
        where_clause, params = apply_quotation_filters(alias="q", **kwargs)
        query_sql = f"""
            SELECT b.branch_name, SUM(q.grand_total)
            FROM "{schema}".quotations q
            JOIN "{schema}".quotation_locations ql ON q.id = ql.quotation_id
            JOIN "{schema}".branches b ON ql.branch_id = b.id
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY b.branch_name"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def source_mix(db, schema, **kwargs):
        where_clause, params = apply_quotation_filters(alias="q", **kwargs)
        query_sql = f"""
            SELECT q.source_type, COUNT(q.id)
            FROM "{schema}".quotations q
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY q.source_type"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    # ================= TABLES =================

    @staticmethod
    def high_value_quotes(db, schema, **kwargs):
        where_clause, params = apply_quotation_filters(alias="q", **kwargs)
        query_sql = f"""
            SELECT q.quotation_number, q.source_type, q.grand_total, q.status, q.created_at
            FROM "{schema}".quotations q
            WHERE q.grand_total > 25000
            AND q.is_deleted = FALSE
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += """
            ORDER BY q.created_at DESC
            LIMIT 10
        """

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def expiring_quotes(db, schema, **kwargs):
        where_clause, params = apply_quotation_filters(alias="q", **kwargs)
        query_sql = f"""
            SELECT q.quotation_number, q.valid_till, q.grand_total, u.first_name
            FROM "{schema}".quotations q
            JOIN "{schema}".users u ON q.created_by::bigint = u.id
            WHERE q.valid_till BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '3 days'
            AND q.status = 'SENT'
            AND q.is_deleted = FALSE
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " ORDER BY q.valid_till"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def low_acceptance(db, schema, **kwargs):
        where_clause, params = apply_quotation_filters(alias="q", **kwargs)
        query_sql = f"""
            SELECT 
                COUNT(*) FILTER (WHERE q.status = 'ACCEPTED')::float /
                NULLIF(COUNT(*),0)
            FROM "{schema}".quotations q
            WHERE q.created_at >= CURRENT_DATE - INTERVAL '30 days'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def high_value_pending(db, schema, **kwargs):
        where_clause, params = apply_quotation_filters(alias="q", **kwargs)
        query_sql = f"""
            SELECT q.quotation_number, q.grand_total
            FROM "{schema}".quotations q
            WHERE q.status = 'SENT'
            AND q.grand_total > 50000
            AND CURRENT_DATE - q.sent_at::date > 7
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def critical_expiry(db, schema, **kwargs):
        where_clause, params = apply_quotation_filters(alias="q", **kwargs)
        query_sql = f"""
            SELECT q.quotation_number, q.valid_till
            FROM "{schema}".quotations q
            WHERE q.status = 'SENT'
            AND q.valid_till = CURRENT_DATE + 1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()
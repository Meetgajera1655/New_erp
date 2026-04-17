from sqlalchemy import text
from app.filters.support_filter import apply_support_filters

class SupportRepository:

    # ================= KPI =================

    @staticmethod
    def total_tickets(db, schema, **kwargs):
        where_clause, params = apply_support_filters(alias="st", **kwargs)
        query_sql = f"""
            SELECT COUNT(st.id) FROM "{schema}".support_tickets st
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def open_tickets(db, schema, **kwargs):
        where_clause, params = apply_support_filters(alias="st", **kwargs)
        query_sql = f"""
            SELECT COUNT(st.id)
            FROM "{schema}".support_tickets st
            WHERE st.status = 'OPEN'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def closed_tickets(db, schema, **kwargs):
        where_clause, params = apply_support_filters(alias="st", **kwargs)
        query_sql = f"""
            SELECT COUNT(st.id)
            FROM "{schema}".support_tickets st
            WHERE st.status = 'CLOSED'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def high_priority(db, schema, **kwargs):
        where_clause, params = apply_support_filters(alias="st", **kwargs)
        query_sql = f"""
            SELECT COUNT(st.id)
            FROM "{schema}".support_tickets st
            WHERE st.priority = 'HIGH'
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
        where_clause, params = apply_support_filters(alias="st", **kwargs)
        query_sql = f"""
            SELECT st.status, COUNT(st.id)
            FROM "{schema}".support_tickets st
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY st.status"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def daily_tickets(db, schema, **kwargs):
        where_clause, params = apply_support_filters(alias="st", **kwargs)
        query_sql = f"""
            SELECT DATE(st.created_at), COUNT(st.id)
            FROM "{schema}".support_tickets st
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += """
            GROUP BY DATE(st.created_at)
            ORDER BY DATE(st.created_at)
        """

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def priority_chart(db, schema, **kwargs):
        where_clause, params = apply_support_filters(alias="st", **kwargs)
        query_sql = f"""
            SELECT st.priority, COUNT(st.id)
            FROM "{schema}".support_tickets st
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY st.priority"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    # ================= TABLES =================

    @staticmethod
    def recent_tickets(db, schema, **kwargs):
        where_clause, params = apply_support_filters(alias="st", **kwargs)
        query_sql = f"""
            SELECT 
                st.ticket_number,
                c.full_name,
                st.subject,
                st.priority,
                st.status,
                st.created_at,
                b.branch_name
            FROM "{schema}".support_tickets st
            JOIN "{schema}".customers c ON st.customer_id = c.id
            JOIN "{schema}".branches b ON st.branch_id = b.id
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += """
            ORDER BY st.created_at DESC
            LIMIT 20
        """

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def open_high_priority(db, schema, **kwargs):
        where_clause, params = apply_support_filters(alias="st", **kwargs)
        query_sql = f"""
            SELECT 
                st.ticket_number,
                c.full_name,
                st.subject,
                st.priority,
                st.status,
                st.created_at
            FROM "{schema}".support_tickets st
            JOIN "{schema}".customers c ON st.customer_id = c.id
            WHERE st.priority = 'HIGH'
            AND st.status = 'OPEN'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " ORDER BY st.created_at DESC"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def high_priority_alert(db, schema, **kwargs):
        where_clause, params = apply_support_filters(alias="st", **kwargs)
        query_sql = f"""
            SELECT 
                st.ticket_number,
                st.subject,
                st.priority,
                st.status,
                st.created_at
            FROM "{schema}".support_tickets st
            WHERE st.priority = 'HIGH'
            AND st.status = 'OPEN'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def old_open_tickets(db, schema, **kwargs):
        where_clause, params = apply_support_filters(alias="st", **kwargs)
        query_sql = f"""
            SELECT 
                st.ticket_number,
                st.subject,
                st.priority,
                st.status,
                st.created_at
            FROM "{schema}".support_tickets st
            WHERE st.status = 'OPEN'
            AND st.created_at < CURRENT_DATE - INTERVAL '3 days'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()
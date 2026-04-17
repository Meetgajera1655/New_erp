from sqlalchemy import text

class LeadRepository:

    # ================= KPI =================

    @staticmethod
    def total_active_leads(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT COUNT(l.id)
            FROM "{schema}".leads l
            WHERE l.status NOT IN ('Lost','Converted')
            AND {where_clause}
        """
        return db.execute(text(query_sql), params or {}).scalar()

    @staticmethod
    def qualified_leads(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT COUNT(l.id)
            FROM "{schema}".leads l
            WHERE l.status = 'Qualified'
            AND {where_clause}
        """
        return db.execute(text(query_sql), params or {}).scalar()

    @staticmethod
    def pending_followups(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT COUNT(f.id)
            FROM "{schema}".follow_ups f
            JOIN "{schema}".leads l ON f.lead_id = l.id
            WHERE f.next_follow_up_date >= CURRENT_DATE
            AND {where_clause}
        """
        return db.execute(text(query_sql), params or {}).scalar()

    @staticmethod
    def conversion_rate(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT 
                COUNT(*) FILTER (WHERE l.status = 'Converted')::float /
                NULLIF(COUNT(*) FILTER (WHERE l.status IN ('Converted','Lost')), 0)
            FROM "{schema}".leads l
            WHERE 1=1 AND {where_clause}
        """
        return db.execute(text(query_sql), params or {}).scalar()

    # ================= CHARTS =================

    # ----------------------------------------------------

    @staticmethod
    def status_wise_leads(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT l.status, COUNT(l.id)
            FROM "{schema}".leads l
            WHERE 1=1 AND {where_clause}
            GROUP BY l.status
        """
        return db.execute(text(query_sql), params or {}).fetchall()

    @staticmethod
    def leads_by_source(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT l.source, COUNT(l.id)
            FROM "{schema}".leads l
            WHERE 1=1 AND {where_clause}
            GROUP BY l.source
        """
        return db.execute(text(query_sql), params or {}).fetchall()

    @staticmethod
    def daily_followups(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT DATE(f.created_at), COUNT(f.id)
            FROM "{schema}".follow_ups f
            JOIN "{schema}".leads l ON f.lead_id = l.id
            WHERE 1=1 AND {where_clause}
            GROUP BY DATE(f.created_at)
            ORDER BY DATE(f.created_at)
        """
        return db.execute(text(query_sql), params or {}).fetchall()

    @staticmethod
    def leads_by_priority(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT l.priority, COUNT(l.id)
            FROM "{schema}".leads l
            WHERE 1=1 AND {where_clause}
            GROUP BY l.priority
        """
        return db.execute(text(query_sql), params or {}).fetchall()

    # ================= TABLES =================

    @staticmethod
    def recent_leads(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT l.id, l.lead_name, l.mobile_number,
                l.lead_type, l.source, l.priority,
                l.status, l.created_at
            FROM "{schema}".leads l
            WHERE 1=1 AND {where_clause}
            ORDER BY l.created_at DESC
            LIMIT 20
        """
        return db.execute(text(query_sql), params or {}).fetchall()

    @staticmethod
    def upcoming_followups(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT l.lead_name, l.next_follow_up_date, l.priority
            FROM "{schema}".leads l
            WHERE l.next_follow_up_date >= CURRENT_DATE
            AND {where_clause}
        """
        return db.execute(text(query_sql), params or {}).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def overdue_followups(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT l.id, l.lead_name, l.next_follow_up_date, l.status
            FROM "{schema}".leads l
            WHERE l.next_follow_up_date < CURRENT_DATE
            AND l.status NOT IN ('Converted','Lost')
            AND {where_clause}
        """
        return db.execute(text(query_sql), params or {}).fetchall()

    @staticmethod
    def urgent_leads(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT l.id, l.lead_name, l.mobile_number,
                l.priority, l.status, l.created_at
            FROM "{schema}".leads l
            WHERE l.priority = 'Urgent'
            AND l.status = 'New'
            AND {where_clause}
        """
        return db.execute(text(query_sql), params or {}).fetchall()

    @staticmethod
    def pending_leads(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT l.id, l.lead_name, l.priority, l.status, l.updated_at
            FROM "{schema}".leads l
            WHERE (
                (
                    l.priority = 'Urgent'
                    AND l.status = 'Negotiation'
                    AND CURRENT_DATE - l.updated_at::date > 2
                )
                OR (
                    l.priority = 'Normal'
                    AND l.status = 'Negotiation'
                    AND CURRENT_DATE - l.updated_at::date > 7
                )
            )
            AND {where_clause}
        """
        return db.execute(text(query_sql), params or {}).fetchall()
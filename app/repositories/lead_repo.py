from sqlalchemy import text

class LeadRepository:

    # ================= KPI =================

    @staticmethod
    def total_active_leads(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT COUNT(l.id)
            FROM "{schema}".leads l
            WHERE l.status NOT IN ('LOST','CONVERTED')
            AND {where_clause}
        """
        return db.execute(text(query_sql), params or {}).scalar()

    @staticmethod
    def qualified_leads(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT COUNT(l.id)
            FROM "{schema}".leads l
            WHERE l.status = 'QUALIFIED'
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
                COUNT(*) FILTER (WHERE l.status = 'CONVERTED')::float /
                NULLIF(COUNT(*) FILTER (WHERE l.status IN ('CONVERTED','LOST')), 0)
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
            SELECT l.lead_name, l.next_follow_up_date, l.priority, 
                   COALESCE((SELECT contact_mode FROM "{schema}".follow_ups WHERE lead_id = l.id ORDER BY created_at DESC LIMIT 1), 'Phone') as contact_mode
            FROM "{schema}".leads l
            WHERE l.next_follow_up_date >= CURRENT_DATE
            AND UPPER(l.status) NOT IN ('CONVERTED', 'LOST')
            AND {where_clause}
            ORDER BY l.next_follow_up_date ASC
        """
        return db.execute(text(query_sql), params or {}).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def overdue_followups(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT l.id, l.lead_name, l.next_follow_up_date, l.status
            FROM "{schema}".leads l
            WHERE l.next_follow_up_date < CURRENT_DATE
            AND UPPER(l.status) NOT IN ('CONVERTED', 'LOST')
            AND {where_clause}
            ORDER BY l.next_follow_up_date ASC
            LIMIT 20
        """
        return db.execute(text(query_sql), params or {}).fetchall()

    @staticmethod
    def urgent_leads(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT l.id, l.lead_name, l.mobile_number,
                l.priority, l.status, l.created_at
            FROM "{schema}".leads l
            WHERE UPPER(l.priority) = 'URGENT'
            AND UPPER(l.status) = 'NEW'
            AND {where_clause}
            ORDER BY l.created_at DESC
            LIMIT 20
        """
        return db.execute(text(query_sql), params or {}).fetchall()

    @staticmethod
    def pending_leads(db, schema, where_clause="1=1", params=None):
        query_sql = f"""
            SELECT l.id, l.lead_name, l.priority, l.status, l.updated_at
            FROM "{schema}".leads l
            WHERE (
                (
                    UPPER(l.priority) = 'URGENT'
                    AND UPPER(l.status) = 'NEGOTIATION'
                    AND CURRENT_DATE - l.updated_at::date > 2
                )
                OR (
                    UPPER(l.priority) = 'NORMAL'
                    AND UPPER(l.status) = 'NEGOTIATION'
                    AND CURRENT_DATE - l.updated_at::date > 7
                )
            )
            AND {where_clause}
            ORDER BY l.updated_at ASC
            LIMIT 20
        """
        return db.execute(text(query_sql), params or {}).fetchall()
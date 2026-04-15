from sqlalchemy import text

class LeadRepository:

    # ================= KPI =================

    @staticmethod
    def total_active_leads(db, schema):
        query = text(f"""
            SELECT COUNT(id)
            FROM "{schema}".leads
            WHERE status NOT IN ('Lost','Converted')
        """)
        return db.execute(query).scalar()

    @staticmethod
    def qualified_leads(db, schema):
        query = text(f"""
            SELECT COUNT(id)
            FROM "{schema}".leads
            WHERE status = 'Qualified'
        """)
        return db.execute(query).scalar()

    @staticmethod
    def pending_followups(db, schema):
        query = text(f"""
            SELECT COUNT(*)
            FROM "{schema}".follow_ups
            WHERE next_follow_up_date >= CURRENT_DATE
        """)
        return db.execute(query).scalar()

    @staticmethod
    def conversion_rate(db, schema):
        query = text(f"""
            SELECT 
                COUNT(*) FILTER (WHERE status = 'Converted')::float /
                NULLIF(COUNT(*) FILTER (WHERE status IN ('Converted','Lost')), 0)
            FROM "{schema}".leads
        """)
        return db.execute(query).scalar()

    # ================= CHARTS =================

    # ----------------------------------------------------

    @staticmethod
    def status_wise_leads(db, schema):
        query = text(f"""
            SELECT status, COUNT(id)
            FROM "{schema}".leads
            GROUP BY status
        """)
        return db.execute(query).fetchall()

    @staticmethod
    def leads_by_source(db, schema):
        query = text(f"""
            SELECT source, COUNT(id)   -- ✅ FIXED HERE
            FROM "{schema}".leads
            GROUP BY source
        """)
        return db.execute(query).fetchall()

    @staticmethod
    def daily_followups(db, schema):
        query = text(f"""
            SELECT DATE(created_at), COUNT(id)
            FROM "{schema}".follow_ups
            GROUP BY DATE(created_at)
            ORDER BY DATE(created_at)
        """)
        return db.execute(query).fetchall()

    @staticmethod
    def leads_by_priority(db, schema):
        query = text(f"""
            SELECT priority, COUNT(id)
            FROM "{schema}".leads
            GROUP BY priority
        """)
        return db.execute(query).fetchall()

    # ================= TABLES =================

    @staticmethod
    def recent_leads(db, schema):
        query = text(f"""
        SELECT id, lead_name, mobile_number,
               lead_type, source, priority,
               status, created_at   -- ✅ FIXED
        FROM "{schema}".leads
        ORDER BY created_at DESC
        LIMIT 20
    """)
        return db.execute(query).fetchall()

    @staticmethod
    def upcoming_followups(db, schema):
        query = text(f"""
        SELECT lead_name, next_follow_up_date, priority
        FROM "{schema}".leads
        WHERE next_follow_up_date >= CURRENT_DATE
    """)
        return db.execute(query).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def overdue_followups(db, schema):
        query = text(f"""
        SELECT id, lead_name, next_follow_up_date, status
        FROM "{schema}".leads
        WHERE next_follow_up_date < CURRENT_DATE
        AND status NOT IN ('Converted','Lost')
    """)
        return db.execute(query).fetchall()

    @staticmethod
    def urgent_leads(db, schema):
        query = text(f"""
        SELECT id, lead_name, mobile_number,
               priority, status, created_at
        FROM "{schema}".leads
        WHERE priority = 'Urgent'
        AND status = 'New'
    """)
        return db.execute(query).fetchall()

    @staticmethod
    def pending_leads(db, schema):
        query = text(f"""
        SELECT id, lead_name, priority, status, updated_at
        FROM "{schema}".leads
        WHERE (
            priority = 'Urgent'
            AND status = 'Negotiation'
            AND CURRENT_DATE - updated_at::date > 2
        )
        OR (
            priority = 'Normal'
            AND status = 'Negotiation'
            AND CURRENT_DATE - updated_at::date > 7
        )
    """)
        return db.execute(query).fetchall()
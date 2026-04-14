from sqlalchemy import text

class SupportRepository:

    # ================= KPI =================

    @staticmethod
    def total_tickets(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id) FROM {schema}.support_tickets
        """)).scalar()

    @staticmethod
    def open_tickets(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM {schema}.support_tickets
            WHERE status = 'OPEN'
        """)).scalar()

    @staticmethod
    def closed_tickets(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM {schema}.support_tickets
            WHERE status = 'CLOSED'
        """)).scalar()

    @staticmethod
    def high_priority(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM {schema}.support_tickets
            WHERE priority = 'HIGH'
        """)).scalar()

    # ================= CHARTS =================

    @staticmethod
    def status_chart(db, schema):
        return db.execute(text(f"""
            SELECT status, COUNT(id)
            FROM {schema}.support_tickets
            GROUP BY status
        """)).fetchall()

    @staticmethod
    def daily_tickets(db, schema):
        return db.execute(text(f"""
            SELECT DATE(created_at), COUNT(id)
            FROM {schema}.support_tickets
            GROUP BY DATE(created_at)
            ORDER BY DATE(created_at)
        """)).fetchall()

    @staticmethod
    def priority_chart(db, schema):
        return db.execute(text(f"""
            SELECT priority, COUNT(id)
            FROM {schema}.support_tickets
            GROUP BY priority
        """)).fetchall()

    # ================= TABLES =================

    @staticmethod
    def recent_tickets(db, schema):
        return db.execute(text(f"""
            SELECT 
                st.ticket_number,
                c.full_name,
                st.subject,
                st.priority,
                st.status,
                st.created_at,
                b.branch_name
            FROM {schema}.support_tickets st
            JOIN {schema}.customers c ON st.customer_id = c.id
            JOIN {schema}.branches b ON st.branch_id = b.id
            ORDER BY st.created_at DESC
            LIMIT 20
        """)).fetchall()

    @staticmethod
    def open_high_priority(db, schema):
        return db.execute(text(f"""
            SELECT 
                st.ticket_number,
                c.full_name,
                st.subject,
                st.priority,
                st.status,
                st.created_at
            FROM {schema}.support_tickets st
            JOIN {schema}.customers c ON st.customer_id = c.id
            WHERE st.priority = 'HIGH'
            AND st.status = 'OPEN'
            ORDER BY st.created_at DESC
        """)).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def high_priority_alert(db, schema):
        return db.execute(text(f"""
            SELECT 
                ticket_number,
                subject,
                priority,
                status,
                created_at
            FROM {schema}.support_tickets
            WHERE priority = 'HIGH'
            AND status = 'OPEN'
        """)).fetchall()

    @staticmethod
    def old_open_tickets(db, schema):
        return db.execute(text(f"""
            SELECT 
                ticket_number,
                subject,
                priority,
                status,
                created_at
            FROM {schema}.support_tickets
            WHERE status = 'OPEN'
            AND created_at < CURRENT_DATE - INTERVAL '3 days'
        """)).fetchall()
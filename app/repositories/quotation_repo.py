from sqlalchemy import text

class QuotationRepository:

    # ================= KPI =================

    @staticmethod
    def total_quotes(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM "{schema}".quotations
        """)).scalar()

    @staticmethod
    def accepted_rate(db, schema):
        return db.execute(text(f"""
            SELECT 
                COUNT(*) FILTER (WHERE status = 'ACCEPTED')::float /
                NULLIF(COUNT(*),0)
            FROM "{schema}".quotations
        """)).scalar()

    @staticmethod
    def total_value(db, schema):
        return db.execute(text(f"""
            SELECT COALESCE(SUM(grand_total),0)
            FROM "{schema}".quotations
        """)).scalar()

    @staticmethod
    def avg_value(db, schema):
        return db.execute(text(f"""
            SELECT COALESCE(AVG(grand_total),0)
            FROM "{schema}".quotations
        """)).scalar()

    # ================= CHARTS =================

    @staticmethod
    def status_chart(db, schema):
        return db.execute(text(f"""
            SELECT status, COUNT(id)
            FROM "{schema}".quotations
            GROUP BY status
        """)).fetchall()

    @staticmethod
    def monthly_trend(db, schema):
        return db.execute(text(f"""
            SELECT DATE_TRUNC('month', created_at), SUM(grand_total)
            FROM "{schema}".quotations
            GROUP BY DATE_TRUNC('month', created_at)
            ORDER BY DATE_TRUNC('month', created_at)
        """)).fetchall()

    @staticmethod
    def branch_performance(db, schema):
        return db.execute(text(f"""
            SELECT b.branch_name, SUM(q.grand_total)
            FROM "{schema}".quotations q
            JOIN "{schema}".quotation_locations ql ON q.id = ql.quotation_id
            JOIN "{schema}".branches b ON ql.branch_id = b.id
            GROUP BY b.branch_name
        """)).fetchall()

    @staticmethod
    def source_mix(db, schema):
        return db.execute(text(f"""
            SELECT source_type, COUNT(id)
            FROM "{schema}".quotations
            GROUP BY source_type
        """)).fetchall()

    # ================= TABLES =================

    @staticmethod
    def high_value_quotes(db, schema):
        return db.execute(text(f"""
            SELECT quotation_number, source_type, grand_total, status, created_at
            FROM "{schema}".quotations
            WHERE grand_total > 25000
            AND is_deleted = FALSE
            ORDER BY created_at DESC
            LIMIT 10
        """)).fetchall()

    @staticmethod
    def expiring_quotes(db, schema):
        return db.execute(text(f"""
        SELECT q.quotation_number, q.valid_till, q.grand_total, u.first_name
        FROM "{schema}".quotations q
        JOIN "{schema}".users u 
        ON q.created_by::bigint = u.id   -- ✅ FIX HERE
        WHERE q.valid_till BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '3 days'
        AND q.status = 'SENT'
        AND q.is_deleted = FALSE
        ORDER BY q.valid_till
    """)).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def low_acceptance(db, schema):
        return db.execute(text(f"""
            SELECT 
                COUNT(*) FILTER (WHERE status = 'ACCEPTED')::float /
                NULLIF(COUNT(*),0)
            FROM "{schema}".quotations
            WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
        """)).scalar()

    @staticmethod
    def high_value_pending(db, schema):
        return db.execute(text(f"""
            SELECT quotation_number, grand_total
            FROM "{schema}".quotations
            WHERE status = 'SENT'
            AND grand_total > 50000
            AND CURRENT_DATE - sent_at::date > 7
        """)).fetchall()

    @staticmethod
    def critical_expiry(db, schema):
        return db.execute(text(f"""
            SELECT quotation_number, valid_till
            FROM "{schema}".quotations
            WHERE status = 'SENT'
            AND valid_till = CURRENT_DATE + 1
        """)).fetchall()
from sqlalchemy import text

class GMARepository:

    # ================= KPI =================

    @staticmethod
    def total_gma(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM "{schema}".gma_sheets
            WHERE is_deleted = FALSE
        """)).scalar()

    @staticmethod
    def approved_gma(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM "{schema}".gma_sheets
            WHERE status = 'APPROVED'
            AND is_deleted = FALSE
        """)).scalar()

    @staticmethod
    def pending_gma(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM "{schema}".gma_sheets
            WHERE status = 'PENDING'
            AND is_deleted = FALSE
        """)).scalar()

    @staticmethod
    def avg_margin(db, schema):
        return db.execute(text(f"""
            SELECT COALESCE(AVG(overall_gross_margin),0)
            FROM "{schema}".gma_sheets
            WHERE is_deleted = FALSE
        """)).scalar()

    # ================= CHARTS =================

    @staticmethod
    def status_distribution(db, schema):
        return db.execute(text(f"""
            SELECT status, COUNT(id)
            FROM "{schema}".gma_sheets
            WHERE is_deleted = FALSE
            GROUP BY status
        """)).fetchall()

    @staticmethod
    def branch_gma(db, schema):
        return db.execute(text(f"""
            SELECT b.branch_name, COUNT(g.id)
            FROM "{schema}".gma_sheets g
            JOIN "{schema}".branches b
            ON g.branch_id = b.id
            WHERE g.is_deleted = FALSE
            GROUP BY b.branch_name
            ORDER BY COUNT(g.id) DESC
        """)).fetchall()

    @staticmethod
    def monthly_gma(db, schema):
        return db.execute(text(f"""
            SELECT DATE_TRUNC('month', created_at), COUNT(id)
            FROM "{schema}".gma_sheets
            WHERE is_deleted = FALSE
            GROUP BY DATE_TRUNC('month', created_at)
            ORDER BY DATE_TRUNC('month', created_at)
        """)).fetchall()

    # ================= TABLES =================

    @staticmethod
    def recent_gma(db, schema):
        return db.execute(text(f"""
            SELECT g.id, g.source_type, g.contract_duration,
                   g.proposed_start_date, b.branch_name,
                   g.total_annual_price, g.status, g.created_at
            FROM "{schema}".gma_sheets g
            JOIN "{schema}".branches b
            ON g.branch_id = b.id
            WHERE g.is_deleted = FALSE
            ORDER BY g.created_at DESC
            LIMIT 10
        """)).fetchall()

    @staticmethod
    def approved_summary(db, schema):
        return db.execute(text(f"""
            SELECT id, total_annual_cost, total_annual_price,
                   overall_gross_margin, gm_with_doc,
                   total_visits_per_month, approved_on
            FROM "{schema}".gma_sheets
            WHERE status = 'APPROVED'
            AND is_deleted = FALSE
            ORDER BY approved_on DESC
        """)).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def pending_alert(db, schema):
        return db.execute(text(f"""
            SELECT id, source_type, status, deadline, created_at
            FROM "{schema}".gma_sheets
            WHERE status = 'PENDING'
            AND deadline < CURRENT_TIMESTAMP
            AND is_deleted = FALSE
        """)).fetchall()

    @staticmethod
    def low_margin(db, schema):
        return db.execute(text(f"""
            SELECT id, total_annual_price, total_annual_cost,
                   overall_gross_margin, status
            FROM "{schema}".gma_sheets
            WHERE overall_gross_margin < 10
            AND is_deleted = FALSE
        """)).fetchall()
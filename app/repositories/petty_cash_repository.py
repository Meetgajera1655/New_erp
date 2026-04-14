from sqlalchemy.orm import Session
from sqlalchemy import text


class PettyCashRepository:

    # ================= KPI =================
    @staticmethod
    def total_requested(db: Session, schema: str):
        return db.execute(text(f"""
            SELECT COALESCE(SUM(amount_requested),0)
            FROM {schema}.petty_cash_requests
        """)).scalar()

    @staticmethod
    def total_approved(db: Session, schema: str):
        return db.execute(text(f"""
            SELECT COALESCE(SUM(approved_amount),0)
            FROM {schema}.petty_cash_requests
            WHERE status = 'APPROVED'
        """)).scalar()

    @staticmethod
    def pending_requests(db: Session, schema: str):
        return db.execute(text(f"""
            SELECT COUNT(*)
            FROM {schema}.petty_cash_requests
            WHERE status = 'PENDING'
        """)).scalar()

    @staticmethod
    def paid_requests(db: Session, schema: str):
        return db.execute(text(f"""
            SELECT COUNT(*)
            FROM {schema}.petty_cash_requests
            WHERE status = 'PAID'
        """)).scalar()

    # ================= CHARTS =================
    @staticmethod
    def status_chart(db: Session, schema: str):
        return db.execute(text(f"""
            SELECT status, COUNT(*) 
            FROM {schema}.petty_cash_requests
            GROUP BY status
        """)).fetchall()

    @staticmethod
    def branch_expense(db: Session, schema: str):
        return db.execute(text(f"""
            SELECT b.branch_name, COALESCE(SUM(pc.approved_amount),0)
            FROM {schema}.petty_cash_requests pc
            JOIN {schema}.branches b
            ON pc.requester_branch_id = b.id
            GROUP BY b.branch_name
            ORDER BY SUM(pc.approved_amount) DESC
        """)).fetchall()

    @staticmethod
    def monthly_requests(db: Session, schema: str):
        return db.execute(text(f"""
            SELECT DATE(submitted_at), COUNT(*)
            FROM {schema}.petty_cash_requests
            GROUP BY DATE(submitted_at)
            ORDER BY DATE(submitted_at)
        """)).fetchall()

    # ================= TABLES =================
    @staticmethod
    def recent_requests(db: Session, schema: str):
        return db.execute(text(f"""
            SELECT 
                pc.id,
                u.first_name,
                b.branch_name,
                pc.category,
                pc.amount_requested,
                pc.approved_amount,
                pc.status,
                pc.submitted_at
            FROM {schema}.petty_cash_requests pc
            JOIN {schema}.users u
            ON pc.requester_user_id = u.id
            JOIN {schema}.branches b
            ON pc.requester_branch_id = b.id
            ORDER BY pc.submitted_at DESC
            LIMIT 20
        """)).fetchall()

    @staticmethod
    def approved_payments(db: Session, schema: str):
        return db.execute(text(f"""
            SELECT 
                pc.id,
                u.first_name,
                pc.category,
                pc.approved_amount,
                pc.payment_mode_processed,
                pc.transaction_ref,
                pc.payment_date,
                pc.status
            FROM {schema}.petty_cash_requests pc
            JOIN {schema}.users u
            ON pc.paid_by_user_id = u.id
            WHERE pc.status = 'PAID'
            ORDER BY pc.payment_date DESC
        """)).fetchall()

    # ================= ALERTS =================
    @staticmethod
    def high_amount(db: Session, schema: str):
        return db.execute(text(f"""
            SELECT 
                id,
                category,
                amount_requested,
                status,
                submitted_at
            FROM {schema}.petty_cash_requests
            WHERE amount_requested > 10000
            ORDER BY amount_requested DESC
        """)).fetchall()

    @staticmethod
    def pending_old(db: Session, schema: str):
        return db.execute(text(f"""
            SELECT 
                id,
                category,
                amount_requested,
                status,
                submitted_at
            FROM {schema}.petty_cash_requests
            WHERE status = 'PENDING'
            AND submitted_at < CURRENT_DATE - INTERVAL '3 days'
            ORDER BY submitted_at
        """)).fetchall()
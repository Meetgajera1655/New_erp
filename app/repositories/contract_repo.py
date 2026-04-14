from sqlalchemy import text

class ContractRepository:

    # ================= KPI =================

    @staticmethod
    def total_contracts(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM "{schema}".contracts
        """)).scalar()

    @staticmethod
    def active_contracts(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM "{schema}".contracts
            WHERE status = 'ACTIVE'
        """)).scalar()

    @staticmethod
    def total_value(db, schema):
        return db.execute(text(f"""
            SELECT COALESCE(SUM(total_sale_value),0)
            FROM "{schema}".contracts
        """)).scalar()

    @staticmethod
    def expiring_soon(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM "{schema}".contracts
            WHERE end_date <= CURRENT_DATE + INTERVAL '30 days'
            AND status = 'ACTIVE'
        """)).scalar()

    # ================= CHARTS =================

    @staticmethod
    def status_distribution(db, schema):
        return db.execute(text(f"""
            SELECT status, COUNT(id)
            FROM "{schema}".contracts
            GROUP BY status
        """)).fetchall()

    @staticmethod
    def branch_contracts(db, schema):
        return db.execute(text(f"""
            SELECT b.branch_name, COUNT(c.id)
            FROM "{schema}".contracts c
            JOIN "{schema}".branches b
            ON c.branch_id = b.id
            GROUP BY b.branch_name
            ORDER BY COUNT(c.id) DESC
        """)).fetchall()

    @staticmethod
    def monthly_value(db, schema):
        return db.execute(text(f"""
            SELECT DATE_TRUNC('month', start_date), SUM(total_sale_value)
            FROM "{schema}".contracts
            GROUP BY DATE_TRUNC('month', start_date)
            ORDER BY DATE_TRUNC('month', start_date)
        """)).fetchall()

    # ================= TABLES =================

    @staticmethod
    def recent_contracts(db, schema):
        return db.execute(text(f"""
            SELECT c.id,
                   cu.full_name,
                   c.gma_sheet_id,
                   c.total_sale_value,
                   c.start_date,
                   c.end_date,
                   c.status,
                   b.branch_name
            FROM "{schema}".contracts c
            JOIN "{schema}".customers cu
            ON c.customer_id = cu.id
            JOIN "{schema}".branches b
            ON c.branch_id = b.id
            ORDER BY c.created_at DESC
            LIMIT 10
        """)).fetchall()

    @staticmethod
    def expiring_list(db, schema):
        return db.execute(text(f"""
            SELECT c.id,
                   cu.full_name,
                   c.total_sale_value,
                   c.end_date,
                   c.status,
                   b.branch_name
            FROM "{schema}".contracts c
            JOIN "{schema}".customers cu
            ON c.customer_id = cu.id
            JOIN "{schema}".branches b
            ON c.branch_id = b.id
            WHERE c.end_date <= CURRENT_DATE + INTERVAL '30 days'
            AND c.status = 'ACTIVE'
            ORDER BY c.end_date
        """)).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def expiry_alert(db, schema):
        return db.execute(text(f"""
            SELECT c.id,
                   cu.full_name,
                   c.end_date,
                   c.total_sale_value,
                   c.status
            FROM "{schema}".contracts c
            JOIN "{schema}".customers cu
            ON c.customer_id = cu.id
            WHERE c.end_date <= CURRENT_DATE + INTERVAL '7 days'
            AND c.status = 'ACTIVE'
        """)).fetchall()

    @staticmethod
    def no_sales_order(db, schema):
        return db.execute(text(f"""
            SELECT c.id,
                   c.total_sale_value,
                   c.start_date,
                   c.status
            FROM "{schema}".contracts c
            LEFT JOIN "{schema}".sales_orders so
            ON c.id = so.contract_id
            WHERE so.contract_id IS NULL
            AND c.status = 'ACTIVE'
        """)).fetchall()
from sqlalchemy import text

class CustomerRepository:

    # ================= KPI =================

    @staticmethod
    def total_customers(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM "{schema}".customers
            WHERE is_deleted = FALSE
        """)).scalar()

    @staticmethod
    def active_customers(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM "{schema}".customers
            WHERE status = 'Active'
        """)).scalar()

    @staticmethod
    def contract_customers(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM "{schema}".customers
            WHERE customer_type = 'Contract'
        """)).scalar()

    @staticmethod
    def total_revenue(db, schema):
        return db.execute(text(f"""
        SELECT COALESCE(SUM(grand_total),0)
        FROM "{schema}".sales_orders
    """)).scalar()

    # ================= CHARTS =================

    @staticmethod
    def customer_type(db, schema):
        return db.execute(text(f"""
            SELECT customer_type, COUNT(id)
            FROM "{schema}".customers
            WHERE is_deleted = FALSE
            GROUP BY customer_type
        """)).fetchall()

    @staticmethod
    def branch_customers(db, schema):
        return db.execute(text(f"""
            SELECT b.branch_name, COUNT(c.id)
            FROM "{schema}".customers c
            JOIN "{schema}".branches b
            ON c.branch_id = b.id
            WHERE c.is_deleted = FALSE
            GROUP BY b.branch_name
            ORDER BY COUNT(c.id) DESC
        """)).fetchall()

    @staticmethod
    def monthly_customers(db, schema):
        return db.execute(text(f"""
            SELECT DATE_TRUNC('month', created_at), COUNT(id)
            FROM "{schema}".customers
            WHERE is_deleted = FALSE
            GROUP BY DATE_TRUNC('month', created_at)
            ORDER BY DATE_TRUNC('month', created_at)
        """)).fetchall()

    # ================= TABLES =================

    @staticmethod
    def recent_customers(db, schema):
        return db.execute(text(f"""
        SELECT c.id, c.full_name, c.customer_type,
               c.phone, b.branch_name, c.status, c.created_at
        FROM "{schema}".customers c
        JOIN "{schema}".branches b
        ON c.branch_id = b.id
        WHERE c.is_deleted = FALSE
        ORDER BY c.created_at DESC
        LIMIT 10
    """)).fetchall()

    @staticmethod
    def active_contracts(db, schema):
        return db.execute(text(f"""
        SELECT c.id, c.full_name,
               con.id, con.start_date,
               con.end_date, con.total_sale_value, con.status
        FROM "{schema}".customers c
        JOIN "{schema}".contracts con
        ON c.id = con.customer_id
        WHERE con.status = 'ACTIVE'
        AND c.is_deleted = FALSE
    """)).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def inactive_customers(db, schema):
        return db.execute(text(f"""
        SELECT id, full_name, customer_type, branch_id, status
        FROM "{schema}".customers
        WHERE status = 'INACTIVE'
        AND is_deleted = FALSE
    """)).fetchall()

    @staticmethod
    def no_contract_customers(db, schema):
        return db.execute(text(f"""
        SELECT c.id, c.full_name, c.customer_type, c.created_at
        FROM "{schema}".customers c
        LEFT JOIN "{schema}".contracts con
        ON c.id = con.customer_id
        WHERE con.customer_id IS NULL
        AND c.is_deleted = FALSE
    """)).fetchall()
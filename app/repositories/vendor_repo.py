from sqlalchemy import text

class VendorRepository:

    # ================= KPI =================

    @staticmethod
    @staticmethod
    def total_active_vendors(db, schema):
        query = text(f"""
        SELECT COUNT(id)
        FROM "{schema}".vendors
        WHERE LOWER(vendor_status) = 'active'
    """)
        return db.execute(query).scalar()

    @staticmethod
    def avg_vendor_rating(db, schema):
        query = text(f"""
            SELECT COALESCE(AVG(vendor_rating), 0)
            FROM "{schema}".vendors
        """)
        return db.execute(query).scalar()

    @staticmethod
    def expiring_contracts(db, schema):
        query = text(f"""
            SELECT COUNT(id)
            FROM "{schema}".vendors
            WHERE has_contract = TRUE
            AND contract_end_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '30 days'
        """)
        return db.execute(query).scalar()

    @staticmethod
    def avg_delivery_time(db, schema):
        query = text(f"""
            SELECT COALESCE(AVG(delivery_lead_time_days), 0)
            FROM "{schema}".vendor_product_supplies
        """)
        return db.execute(query).scalar()


    # ================= CHARTS =================

    @staticmethod
    def vendors_by_category(db, schema):
        query = text(f"""
            SELECT vendor_category, COUNT(id)
            FROM "{schema}".vendors
            GROUP BY vendor_category
        """)
        return db.execute(query).fetchall()

    @staticmethod
    def contract_status_split(db, schema):
        query = text(f"""
            SELECT has_contract, COUNT(id)
            FROM "{schema}".vendors
            GROUP BY has_contract
        """)
        return db.execute(query).fetchall()

    @staticmethod
    def rating_distribution(db, schema):
        query = text(f"""
            SELECT vendor_rating, COUNT(id)
            FROM "{schema}".vendors
            GROUP BY vendor_rating
            ORDER BY vendor_rating
        """)
        return db.execute(query).fetchall()


    # ================= TABLES =================

    @staticmethod
    def recent_vendors(db, schema):
        query = text(f"""
            SELECT id, vendor_name, vendor_type, created_at, vendor_status
            FROM "{schema}".vendors
            ORDER BY created_at DESC
            LIMIT 10
        """)
        return db.execute(query).fetchall()

    @staticmethod
    def active_contracts(db, schema):
        query = text(f"""
            SELECT id, vendor_name, contract_type, contract_end_date, payment_terms
            FROM "{schema}".vendors
            WHERE has_contract = TRUE
        """)
        return db.execute(query).fetchall()


    # ================= ALERTS =================

    @staticmethod
    def low_vendor_rating(db, schema):
        query = text(f"""
            SELECT id, vendor_name, vendor_rating
            FROM "{schema}".vendors
            WHERE vendor_rating < 2
        """)
        return db.execute(query).fetchall()

    @staticmethod
    def expiring_contracts_alert(db, schema):
        query = text(f"""
            SELECT id, vendor_name, contract_end_date
            FROM "{schema}".vendors
            WHERE has_contract = TRUE
            AND contract_end_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '30 days'
        """)
        return db.execute(query).fetchall()
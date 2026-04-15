from sqlalchemy import text
from app.filters.vendor_filter import apply_vendor_filters

class VendorRepository:

    # ================= KPI =================

    @staticmethod
    def total_active_vendors(db, schema, **kwargs):
        where_clause, params = apply_vendor_filters(alias="v", **kwargs)
        query_sql = f"""
            SELECT COUNT(v.id)
            FROM "{schema}".vendors v
            WHERE LOWER(v.vendor_status) = 'active'
            AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def avg_vendor_rating(db, schema, **kwargs):
        where_clause, params = apply_vendor_filters(alias="v", **kwargs)
        query_sql = f"""
            SELECT COALESCE(AVG(v.vendor_rating), 0)
            FROM "{schema}".vendors v
            WHERE 1=1 AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def expiring_contracts(db, schema, **kwargs):
        where_clause, params = apply_vendor_filters(alias="v", **kwargs)
        query_sql = f"""
            SELECT COUNT(v.id)
            FROM "{schema}".vendors v
            WHERE v.has_contract = TRUE
            AND v.contract_end_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '30 days'
            AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def avg_delivery_time(db, schema, **kwargs):
        where_clause, params = apply_vendor_filters(alias="vps", **kwargs)
        query_sql = f"""
            SELECT COALESCE(AVG(vps.delivery_lead_time_days), 0)
            FROM "{schema}".vendor_product_supplies vps
            WHERE 1=1 AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()


    # ================= CHARTS =================

    @staticmethod
    def vendors_by_category(db, schema, **kwargs):
        where_clause, params = apply_vendor_filters(alias="v", **kwargs)
        query_sql = f"""
            SELECT v.vendor_category, COUNT(v.id)
            FROM "{schema}".vendors v
            WHERE 1=1 AND {where_clause}
            GROUP BY v.vendor_category
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def contract_status_split(db, schema, **kwargs):
        where_clause, params = apply_vendor_filters(alias="v", **kwargs)
        query_sql = f"""
            SELECT v.has_contract, COUNT(v.id)
            FROM "{schema}".vendors v
            WHERE 1=1 AND {where_clause}
            GROUP BY v.has_contract
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def rating_distribution(db, schema, **kwargs):
        where_clause, params = apply_vendor_filters(alias="v", **kwargs)
        query_sql = f"""
            SELECT v.vendor_rating, COUNT(v.id)
            FROM "{schema}".vendors v
            WHERE 1=1 AND {where_clause}
            GROUP BY v.vendor_rating
            ORDER BY v.vendor_rating
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()


    # ================= TABLES =================

    @staticmethod
    def recent_vendors(db, schema, **kwargs):
        where_clause, params = apply_vendor_filters(alias="v", **kwargs)
        query_sql = f"""
            SELECT v.id, v.vendor_name, v.vendor_type, v.created_at, v.vendor_status
            FROM "{schema}".vendors v
            WHERE 1=1 AND {where_clause}
            ORDER BY v.created_at DESC
            LIMIT 10
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def active_contracts(db, schema, **kwargs):
        where_clause, params = apply_vendor_filters(alias="v", **kwargs)
        query_sql = f"""
            SELECT v.id, v.vendor_name, v.contract_type, v.contract_end_date, v.payment_terms
            FROM "{schema}".vendors v
            WHERE v.has_contract = TRUE
            AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()


    # ================= ALERTS =================

    @staticmethod
    def low_vendor_rating(db, schema, **kwargs):
        where_clause, params = apply_vendor_filters(alias="v", **kwargs)
        query_sql = f"""
            SELECT v.id, v.vendor_name, v.vendor_rating
            FROM "{schema}".vendors v
            WHERE v.vendor_rating < 2
            AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def expiring_contracts_alert(db, schema, **kwargs):
        where_clause, params = apply_vendor_filters(alias="v", **kwargs)
        query_sql = f"""
            SELECT v.id, v.vendor_name, v.contract_end_date
            FROM "{schema}".vendors v
            WHERE v.has_contract = TRUE
            AND v.contract_end_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '30 days'
            AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()
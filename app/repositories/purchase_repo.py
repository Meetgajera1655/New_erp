from sqlalchemy import text
from app.filters.purchase_filter import apply_purchase_filters

class PurchaseRepository:

    @staticmethod
    def total_purchase_money(db, schema, **kwargs):
        where_clause, params = apply_purchase_filters(schema=schema, alias="po", **kwargs)
        query_sql = f"""
            SELECT COALESCE(SUM(po.grand_total),0)
            FROM "{schema}".purchase_order po
            WHERE po.is_deleted = FALSE
            AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def pending_orders(db, schema, **kwargs):
        where_clause, params = apply_purchase_filters(schema=schema, alias="po", **kwargs)
        query_sql = f"""
            SELECT COUNT(po.id)
            FROM "{schema}".purchase_order po
            WHERE po.status IN ('Pending Approval','Approved','Ordered','Partially Received')
            AND po.is_deleted = FALSE
            AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def total_items(db, schema, **kwargs):
        where_clause, params = apply_purchase_filters(schema=schema, alias="po", **kwargs)
        query_sql = f"""
            SELECT COALESCE(SUM(poi.quantity),0)
            FROM "{schema}".purchase_order_item poi
            JOIN "{schema}".purchase_order po ON poi.purchase_order_id = po.id
            WHERE poi.is_deleted = FALSE AND po.is_deleted = FALSE
            AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def late_orders(db, schema, **kwargs):
        where_clause, params = apply_purchase_filters(schema=schema, alias="po", **kwargs)
        query_sql = f"""
            SELECT COUNT(po.id)
            FROM "{schema}".purchase_order po
            WHERE po.delivery_date < CURRENT_DATE
            AND po.status != 'Received'
            AND po.is_deleted = FALSE
            AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()
    
    @staticmethod
    def po_status_chart(db, schema, **kwargs):
        where_clause, params = apply_purchase_filters(schema=schema, alias="po", **kwargs)
        query_sql = f"""
            SELECT po.status, COUNT(po.id)
            FROM "{schema}".purchase_order po
            WHERE po.is_deleted = FALSE
            AND {where_clause}
            GROUP BY po.status
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def vendor_spending(db, schema, **kwargs):
        where_clause, params = apply_purchase_filters(schema=schema, alias="po", **kwargs)
        query_sql = f"""
            SELECT v.vendor_name, SUM(po.grand_total)
            FROM "{schema}".purchase_order po
            JOIN "{schema}".vendors v ON po.vendor_id = v.id
            WHERE po.is_deleted = FALSE
            AND {where_clause}
            GROUP BY v.vendor_name
            ORDER BY SUM(po.grand_total) DESC
            LIMIT 10
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def daily_po(db, schema, **kwargs):
        where_clause, params = apply_purchase_filters(schema=schema, alias="po", **kwargs)
        query_sql = f"""
            SELECT po.po_date, COUNT(po.id)
            FROM "{schema}".purchase_order po
            WHERE po.is_deleted = FALSE
            AND {where_clause}
            GROUP BY po.po_date
            ORDER BY po.po_date
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()
    
    @staticmethod
    def recent_po(db, schema, **kwargs):
        where_clause, params = apply_purchase_filters(schema=schema, alias="po", **kwargs)
        query_sql = f"""
            SELECT po.po_number, v.vendor_name, po.po_date, po.delivery_date,
                   po.grand_total, po.status, b.branch_name
            FROM "{schema}".purchase_order po
            JOIN "{schema}".vendors v ON po.vendor_id = v.id
            JOIN "{schema}".branches b ON po.branch_id = b.id
            WHERE po.is_deleted = FALSE
            AND {where_clause}
            ORDER BY po.created_at DESC
            LIMIT 20
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def vendor_summary(db, schema, **kwargs):
        where_clause, params = apply_purchase_filters(schema=schema, alias="po", **kwargs)
        query_sql = f"""
            SELECT v.vendor_name, b.branch_name,
                   COUNT(po.id), SUM(po.grand_total), MAX(po.po_date)
            FROM "{schema}".purchase_order po
            JOIN "{schema}".vendors v ON po.vendor_id = v.id
            JOIN "{schema}".branches b ON po.branch_id = b.id
            WHERE po.is_deleted = FALSE
            AND {where_clause}
            GROUP BY v.vendor_name, b.branch_name
            ORDER BY SUM(po.grand_total) DESC
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()
    

    @staticmethod
    def late_delivery_alert(db, schema, **kwargs):
        where_clause, params = apply_purchase_filters(schema=schema, alias="po", **kwargs)
        query_sql = f"""
            SELECT po.po_number, po.delivery_date
            FROM "{schema}".purchase_order po
            WHERE po.delivery_date < CURRENT_DATE
            AND po.status != 'Received'
            AND po.is_deleted = FALSE
            AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def high_value_alert(db, schema, **kwargs):
        where_clause, params = apply_purchase_filters(schema=schema, alias="po", **kwargs)
        query_sql = f"""
            SELECT po.po_number, po.grand_total
            FROM "{schema}".purchase_order po
            WHERE po.grand_total > 50000
            AND po.is_deleted = FALSE
            AND {where_clause}
        """
        print(f"WHERE: {where_clause}\nPARAMS: {params}\nQUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()
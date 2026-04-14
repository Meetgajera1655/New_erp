from sqlalchemy import text

class PurchaseRepository:

    @staticmethod
    def total_purchase_money(db, schema):
        query = text(f"""
            SELECT COALESCE(SUM(grand_total),0)
            FROM "{schema}".purchase_order
            WHERE is_deleted = FALSE
        """)
        return db.execute(query).scalar()

    @staticmethod
    def pending_orders(db, schema):
        query = text(f"""
            SELECT COUNT(id)
            FROM "{schema}".purchase_order
            WHERE status IN ('Pending Approval','Approved','Ordered','Partially Received')
            AND is_deleted = FALSE
        """)
        return db.execute(query).scalar()

    @staticmethod
    def total_items(db, schema):
        query = text(f"""
            SELECT COALESCE(SUM(quantity),0)
            FROM "{schema}".purchase_order_item
            WHERE is_deleted = FALSE
        """)
        return db.execute(query).scalar()

    @staticmethod
    def late_orders(db, schema):
        query = text(f"""
            SELECT COUNT(id)
            FROM "{schema}".purchase_order
            WHERE delivery_date < CURRENT_DATE
            AND status != 'Received'
            AND is_deleted = FALSE
        """)
        return db.execute(query).scalar()
    
    @staticmethod
    def po_status_chart(db, schema):
        query = text(f"""
            SELECT status, COUNT(id)
            FROM "{schema}".purchase_order
            WHERE is_deleted = FALSE
            GROUP BY status
        """)
        return db.execute(query).fetchall()

    @staticmethod
    def vendor_spending(db, schema):
        query = text(f"""
            SELECT v.vendor_name, SUM(p.grand_total)
            FROM "{schema}".purchase_order p
            JOIN "{schema}".vendors v ON p.vendor_id = v.id
            WHERE p.is_deleted = FALSE
            GROUP BY v.vendor_name
            ORDER BY SUM(p.grand_total) DESC
            LIMIT 10
        """)
        return db.execute(query).fetchall()

    @staticmethod
    def daily_po(db, schema):
        query = text(f"""
            SELECT po_date, COUNT(id)
            FROM "{schema}".purchase_order
            WHERE is_deleted = FALSE
            GROUP BY po_date
            ORDER BY po_date
        """)
        return db.execute(query).fetchall()
    
    @staticmethod
    def recent_po(db, schema):
        query = text(f"""
            SELECT p.po_number, v.vendor_name, p.po_date, p.delivery_date,
                   p.grand_total, p.status, b.branch_name
            FROM "{schema}".purchase_order p
            JOIN "{schema}".vendors v ON p.vendor_id = v.id
            JOIN "{schema}".branches b ON p.branch_id = b.id
            WHERE p.is_deleted = FALSE
            ORDER BY p.created_at DESC
            LIMIT 20
        """)
        return db.execute(query).fetchall()

    @staticmethod
    def vendor_summary(db, schema):
        query = text(f"""
            SELECT v.vendor_name, b.branch_name,
                   COUNT(p.id), SUM(p.grand_total), MAX(p.po_date)
            FROM "{schema}".purchase_order p
            JOIN "{schema}".vendors v ON p.vendor_id = v.id
            JOIN "{schema}".branches b ON p.branch_id = b.id
            WHERE p.is_deleted = FALSE
            GROUP BY v.vendor_name, b.branch_name
            ORDER BY SUM(p.grand_total) DESC
        """)
        return db.execute(query).fetchall()
    

    @staticmethod
    def late_delivery_alert(db, schema):
        query = text(f"""
            SELECT po_number, delivery_date
            FROM "{schema}".purchase_order
            WHERE delivery_date < CURRENT_DATE
            AND status != 'Received'
            AND is_deleted = FALSE
        """)
        return db.execute(query).fetchall()

    @staticmethod
    def high_value_alert(db, schema):
        query = text(f"""
            SELECT po_number, grand_total
            FROM "{schema}".purchase_order
            WHERE grand_total > 50000
            AND is_deleted = FALSE
        """)
        return db.execute(query).fetchall()
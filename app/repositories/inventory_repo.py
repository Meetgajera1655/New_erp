from sqlalchemy import text

class InventoryRepository:

    @staticmethod
    def total_products(db, schema):
        query = text(f"""
            SELECT COUNT(id)
            FROM "{schema}".inventory_products
            WHERE deleted_at IS NULL
        """)
        return db.execute(query).scalar()

    @staticmethod
    def active_products(db, schema):
        query = text(f"""
            SELECT COUNT(id)
            FROM "{schema}".inventory_products
            WHERE status = 'ACTIVE' AND deleted_at IS NULL
        """)
        return db.execute(query).scalar()

    @staticmethod
    def total_stock(db, schema):
        query = text(f"""
            SELECT COALESCE(SUM(assets_qty + consumable_qty + resell_qty), 0)
            FROM "{schema}".stock_ledger
            WHERE deleted_at IS NULL
        """)
        return db.execute(query).scalar()

    @staticmethod
    def total_inventory_value(db, schema):
        query = text(f"""
            SELECT COALESCE(SUM(
                (sl.assets_qty + sl.consumable_qty + sl.resell_qty) * ip.purchase_price
            ), 0)
            FROM "{schema}".stock_ledger sl
            JOIN "{schema}".inventory_products ip
            ON sl.product_id = ip.id
        """)
        return db.execute(query).scalar()

    @staticmethod
    def total_assets(db, schema):
        query = text(f"""
            SELECT COALESCE(SUM(assets_qty), 0)
            FROM "{schema}".stock_ledger
        """)
        return db.execute(query).scalar()

    @staticmethod
    def total_consumables(db, schema):
        query = text(f"""
            SELECT COALESCE(SUM(consumable_qty), 0)
            FROM "{schema}".stock_ledger
        """)
        return db.execute(query).scalar()

    @staticmethod
    def total_resell(db, schema):
        query = text(f"""
            SELECT COALESCE(SUM(resell_qty), 0)
            FROM "{schema}".stock_ledger
        """)
        return db.execute(query).scalar()

    @staticmethod
    def in_transit_stock(db, schema):
        query = text(f"""
            SELECT COALESCE(SUM(in_transit_qty), 0)
            FROM "{schema}".stock_ledger
        """)
        return db.execute(query).scalar()

    @staticmethod
    def reserved_stock(db, schema):
        query = text(f"""
            SELECT COALESCE(SUM(reserved_qty), 0)
            FROM "{schema}".stock_ledger
        """)
        return db.execute(query).scalar()

    @staticmethod
    def low_stock_products(db, schema):
        query = text(f"""
            SELECT COUNT(*)
            FROM "{schema}".stock_ledger
            WHERE status = 'LOW'
        """)
        return db.execute(query).scalar()

    @staticmethod
    def out_of_stock(db, schema):
        query = text(f"""
            SELECT COUNT(*)
            FROM "{schema}".stock_ledger
            WHERE status = 'OUT'
        """)
        return db.execute(query).scalar()
    
    # 1️⃣ Stock by Category (Pie)
    @staticmethod
    def stock_by_category(db, schema):
        query = text(f"""
            SELECT category, 
                   SUM(assets_qty + consumable_qty + resell_qty) AS total_stock
            FROM "{schema}".stock_ledger
            GROUP BY category
        """)
        return db.execute(query).fetchall()

    # 2️⃣ Stock by Type (Donut)
    @staticmethod
    def stock_by_type(db, schema):
        query = text(f"""
            SELECT 
                COALESCE(SUM(assets_qty),0) AS assets,
                COALESCE(SUM(consumable_qty),0) AS consumables,
                COALESCE(SUM(resell_qty),0) AS resell
            FROM "{schema}".stock_ledger
        """)
        return db.execute(query).fetchone()

    # 3️⃣ Branch-wise Stock (Bar)
    @staticmethod
    def branch_stock(db, schema):
        query = text(f"""
            SELECT branch_id,
                   SUM(assets_qty + consumable_qty + resell_qty) AS total_stock
            FROM "{schema}".stock_ledger
            GROUP BY branch_id
        """)
        return db.execute(query).fetchall()

    # 4️⃣ Stock Movement Trend (Line)
    @staticmethod
    def stock_movement_trend(db, schema):
        query = text(f"""
            SELECT DATE(created_at) AS date,
                   SUM(quantity_delta) AS movement
            FROM "{schema}".stock_movement_logs
            GROUP BY DATE(created_at)
            ORDER BY date
        """)
        return db.execute(query).fetchall()

    # 5️⃣ Inventory Value by Category
    @staticmethod
    def inventory_value_by_category(db, schema):
        query = text(f"""
            SELECT sl.category,
                   SUM(
                       (sl.assets_qty + sl.consumable_qty + sl.resell_qty)
                       * ip.purchase_price
                   ) AS value
            FROM "{schema}".stock_ledger sl
            JOIN "{schema}".inventory_products ip
            ON sl.product_id = ip.id
            GROUP BY sl.category
        """)
        return db.execute(query).fetchall()
    

    # 1️⃣ Low Stock Products
    @staticmethod
    def low_stock_table(db, schema):
        query = text(f"""
            SELECT product_name, product_code, branch_id, category,
                   assets_qty, consumable_qty, resell_qty, status
            FROM "{schema}".stock_ledger
            WHERE status = 'LOW'
        """)
        return db.execute(query).fetchall()

    # 2️⃣ Out of Stock Products
    @staticmethod
    def out_of_stock_table(db, schema):
        query = text(f"""
            SELECT product_name, product_code, branch_id, category
            FROM "{schema}".stock_ledger
            WHERE status = 'OUT'
        """)
        return db.execute(query).fetchall()

    # 3️⃣ Branch Stock Table
    @staticmethod
    def branch_stock_table(db, schema):
        query = text(f"""
            SELECT branch_id, product_name, category,
                   assets_qty, consumable_qty, resell_qty,
                   in_transit_qty, reserved_qty, status
            FROM "{schema}".stock_ledger
        """)
        return db.execute(query).fetchall()

    # 4️⃣ Central Stock Entries
    @staticmethod
    def central_stock_entries(db, schema):
        query = text(f"""
            SELECT entry_id, product_name, supplier_name, invoice_number,
                   invoice_date, total_qty, assets_qty, consumable_qty,
                   resell_qty, total_with_tax, created_at
            FROM "{schema}".central_stock_entries
        """)
        return db.execute(query).fetchall()

    # 5️⃣ Recent Stock Movements
    @staticmethod
    def stock_movements(db, schema):
        query = text(f"""
            SELECT reference_type, reference_id, product_id, branch_id,
                   stock_type, quantity_delta, action, created_by, created_at
            FROM "{schema}".stock_movement_logs
            ORDER BY created_at DESC
            LIMIT 20
        """)
        return db.execute(query).fetchall()

    # 6️⃣ Stock Transfers
    @staticmethod
    def stock_transfers_table(db, schema):
        query = text(f"""
            SELECT product_name, assets_qty, consumable_qty,
                   resell_qty, source_branch_id
            FROM "{schema}".stock_transfer_items
        """)
        return db.execute(query).fetchall()
    

    # 1️⃣ Low Stock Alert
    @staticmethod
    def low_stock_alert(db, schema):
        query = text(f"""
            SELECT product_name, branch_id
            FROM "{schema}".stock_ledger
            WHERE status = 'LOW'
        """)
        return db.execute(query).fetchall()

    # 2️⃣ Out of Stock Alert
    @staticmethod
    def out_of_stock_alert(db, schema):
        query = text(f"""
            SELECT product_name
            FROM "{schema}".stock_ledger
            WHERE status = 'OUT'
        """)
        return db.execute(query).fetchall()

    # 3️⃣ High Reserved Stock
    @staticmethod
    def high_reserved_stock(db, schema):
        query = text(f"""
            SELECT product_name, reserved_qty
            FROM "{schema}".stock_ledger
            WHERE reserved_qty > 50
        """)
        return db.execute(query).fetchall()

    # 4️⃣ High In Transit Stock
    @staticmethod
    def high_in_transit(db, schema):
        query = text(f"""
            SELECT product_name, in_transit_qty
            FROM "{schema}".stock_ledger
            WHERE in_transit_qty > 100
        """)
        return db.execute(query).fetchall()

    # 5️⃣ Expired Consumables
    @staticmethod
    def expired_consumables(db, schema):
        query = text(f"""
            SELECT product_name, expiry_date
            FROM "{schema}".central_stock_entries
            WHERE expiry_date < CURRENT_DATE
        """)
        return db.execute(query).fetchall()
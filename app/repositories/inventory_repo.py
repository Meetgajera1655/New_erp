from sqlalchemy import text
from app.filters.inventory_filter import apply_dashboard_filters

class InventoryRepository:

    @staticmethod
    def total_products(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["ip"], **kwargs)
        query_sql = f"""
            SELECT COUNT(ip.id)
            FROM "{schema}".inventory_products ip
            WHERE ip.deleted_at IS NULL
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def active_products(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["ip"], **kwargs)
        query_sql = f"""
            SELECT COUNT(ip.id)
            FROM "{schema}".inventory_products ip
            WHERE ip.status = 'ACTIVE' AND ip.deleted_at IS NULL
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def total_stock(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT COALESCE(SUM(sl.assets_qty + sl.consumable_qty + sl.resell_qty), 0)
            FROM "{schema}".stock_ledger sl
            WHERE sl.deleted_at IS NULL
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def total_inventory_value(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl", "ip"], **kwargs)
        query_sql = f"""
            SELECT COALESCE(SUM(
                (sl.assets_qty + sl.consumable_qty + sl.resell_qty) * ip.purchase_price
            ), 0)
            FROM "{schema}".stock_ledger sl
            JOIN "{schema}".inventory_products ip ON sl.product_id = ip.id
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def total_assets(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT COALESCE(SUM(sl.assets_qty), 0)
            FROM "{schema}".stock_ledger sl
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def total_consumables(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT COALESCE(SUM(sl.consumable_qty), 0)
            FROM "{schema}".stock_ledger sl
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def total_resell(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT COALESCE(SUM(sl.resell_qty), 0)
            FROM "{schema}".stock_ledger sl
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def in_transit_stock(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT COALESCE(SUM(sl.in_transit_qty), 0)
            FROM "{schema}".stock_ledger sl
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def reserved_stock(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT COALESCE(SUM(sl.reserved_qty), 0)
            FROM "{schema}".stock_ledger sl
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def low_stock_products(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT COUNT(*)
            FROM "{schema}".stock_ledger sl
            WHERE sl.status = 'LOW'
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def out_of_stock(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT COUNT(*)
            FROM "{schema}".stock_ledger sl
            WHERE sl.status = 'OUT'
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()
    
    # 1️⃣ Stock by Category (Pie)
    @staticmethod
    def stock_by_category(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT sl.category, 
                   SUM(sl.assets_qty + sl.consumable_qty + sl.resell_qty) AS total_stock
            FROM "{schema}".stock_ledger sl
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY sl.category"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    # 2️⃣ Stock by Type (Donut)
    @staticmethod
    def stock_by_type(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT 
                COALESCE(SUM(sl.assets_qty),0) AS assets,
                COALESCE(SUM(sl.consumable_qty),0) AS consumables,
                COALESCE(SUM(sl.resell_qty),0) AS resell
            FROM "{schema}".stock_ledger sl
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchone()

    # 3️⃣ Branch-wise Stock (Bar)
    @staticmethod
    def branch_stock(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT sl.branch_id,
                   SUM(sl.assets_qty + sl.consumable_qty + sl.resell_qty) AS total_stock
            FROM "{schema}".stock_ledger sl
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY sl.branch_id"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    # 4️⃣ Stock Movement Trend (Line)
    @staticmethod
    def stock_movement_trend(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sml"], **kwargs)
        query_sql = f"""
            SELECT DATE(sml.created_at) AS date,
                   SUM(sml.quantity_delta) AS movement
            FROM "{schema}".stock_movement_logs sml
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY DATE(sml.created_at) ORDER BY date"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    # 5️⃣ Inventory Value by Category
    @staticmethod
    def inventory_value_by_category(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl", "ip"], **kwargs)
        query_sql = f"""
            SELECT sl.category,
                   SUM(
                       (sl.assets_qty + sl.consumable_qty + sl.resell_qty)
                       * ip.purchase_price
                   ) AS value
            FROM "{schema}".stock_ledger sl
            JOIN "{schema}".inventory_products ip ON sl.product_id = ip.id
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY sl.category"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()
    

    # 1️⃣ Low Stock Products
    @staticmethod
    def low_stock_table(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT sl.product_name, sl.product_code, sl.branch_id, sl.category,
                   sl.assets_qty, sl.consumable_qty, sl.resell_qty, sl.status
            FROM "{schema}".stock_ledger sl
            WHERE sl.status = 'LOW'
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    # 2️⃣ Out of Stock Products
    @staticmethod
    def out_of_stock_table(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT sl.product_name, sl.product_code, sl.branch_id, sl.category
            FROM "{schema}".stock_ledger sl
            WHERE sl.status = 'OUT'
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    # 3️⃣ Branch Stock Table
    @staticmethod
    def branch_stock_table(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT sl.branch_id, sl.product_name, sl.category,
                   sl.assets_qty, sl.consumable_qty, sl.resell_qty,
                   sl.in_transit_qty, sl.reserved_qty, sl.status
            FROM "{schema}".stock_ledger sl
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    # 4️⃣ Central Stock Entries
    @staticmethod
    def central_stock_entries(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["cse"], **kwargs)
        query_sql = f"""
            SELECT cse.entry_id, cse.product_name, cse.supplier_name, cse.invoice_number,
                   cse.invoice_date, cse.total_qty, cse.assets_qty, cse.consumable_qty,
                   cse.resell_qty, cse.total_with_tax, cse.created_at
            FROM "{schema}".central_stock_entries cse
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    # 5️⃣ Recent Stock Movements
    @staticmethod
    def stock_movements(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sml"], **kwargs)
        query_sql = f"""
            SELECT sml.reference_type, sml.reference_id, sml.product_id, sml.branch_id,
                   sml.stock_type, sml.quantity_delta, sml.action, sml.created_by, sml.created_at
            FROM "{schema}".stock_movement_logs sml
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        query_sql += " ORDER BY sml.created_at DESC LIMIT 20"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    # 6️⃣ Stock Transfers
    @staticmethod
    def stock_transfers_table(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=[], **kwargs)
        query_sql = f"""
            SELECT sti.product_name, sti.assets_qty, sti.consumable_qty,
                   sti.resell_qty, sti.source_branch_id
            FROM "{schema}".stock_transfer_items sti
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()
    

    # 1️⃣ Low Stock Alert
    @staticmethod
    def low_stock_alert(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT sl.product_name, sl.branch_id
            FROM "{schema}".stock_ledger sl
            WHERE sl.status = 'LOW'
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    # 2️⃣ Out of Stock Alert
    @staticmethod
    def out_of_stock_alert(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT sl.product_name
            FROM "{schema}".stock_ledger sl
            WHERE sl.status = 'OUT'
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    # 3️⃣ High Reserved Stock
    @staticmethod
    def high_reserved_stock(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT sl.product_name, sl.reserved_qty
            FROM "{schema}".stock_ledger sl
            WHERE sl.reserved_qty > 50
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    # 4️⃣ High In Transit Stock
    @staticmethod
    def high_in_transit(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["sl"], **kwargs)
        query_sql = f"""
            SELECT sl.product_name, sl.in_transit_qty
            FROM "{schema}".stock_ledger sl
            WHERE sl.in_transit_qty > 100
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    # 5️⃣ Expired Consumables
    @staticmethod
    def expired_consumables(db, schema, **kwargs):
        where_clause, params = apply_dashboard_filters(schema=schema, aliases=["cse"], **kwargs)
        query_sql = f"""
            SELECT cse.product_name, cse.expiry_date
            FROM "{schema}".central_stock_entries cse
            WHERE cse.expiry_date < CURRENT_DATE
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()
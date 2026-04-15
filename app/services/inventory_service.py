from app.repositories.inventory_repo import InventoryRepository

class InventoryService:
    """
    Service layer for inventory dashboard following RBAC rules.
    Partially filters data between PRODUCT_MANAGEMENT and STOCK_MANAGEMENT.
    """

    @staticmethod
    def get_inventory_kpi(db, schema, allowed_modules: list = None):
        # Default to full access if none specified
        am = [m.upper() for m in allowed_modules] if allowed_modules else ["PRODUCT_MANAGEMENT", "STOCK_MANAGEMENT"]
        
        data = {}
        
        # 1. Product Management Scope
        if "PRODUCT_MANAGEMENT" in am:
            data.update({
                "total_products": InventoryRepository.total_products(db, schema),
                "active_products": InventoryRepository.active_products(db, schema),
            })
            
        # 2. Stock Management Scope
        if "STOCK_MANAGEMENT" in am:
            data.update({
                "total_stock": InventoryRepository.total_stock(db, schema),
                "total_inventory_value": InventoryRepository.total_inventory_value(db, schema),
                "total_assets": InventoryRepository.total_assets(db, schema),
                "total_consumables": InventoryRepository.total_consumables(db, schema),
                "total_resell_stock": InventoryRepository.total_resell(db, schema),
                "in_transit_stock": InventoryRepository.in_transit_stock(db, schema),
                "reserved_stock": InventoryRepository.reserved_stock(db, schema),
                "low_stock_products": InventoryRepository.low_stock_products(db, schema),
                "out_of_stock_products": InventoryRepository.out_of_stock(db, schema)
            })
            
        return data
    
    @staticmethod
    def get_inventory_charts(db, schema, allowed_modules: list = None):
        am = [m.upper() for m in allowed_modules] if allowed_modules else ["PRODUCT_MANAGEMENT", "STOCK_MANAGEMENT"]
        
        data = {}
        
        # Charts are largely stock-dependent in this dashboard
        if "STOCK_MANAGEMENT" in am:
            data.update({
                "stock_by_category": [
                    {"category": r[0], "total_stock": r[1]}
                    for r in InventoryRepository.stock_by_category(db, schema)
                ],
                "stock_by_type": {
                    "assets": InventoryRepository.stock_by_type(db, schema)[0],
                    "consumables": InventoryRepository.stock_by_type(db, schema)[1],
                    "resell": InventoryRepository.stock_by_type(db, schema)[2],
                },
                "branch_stock": [
                    {"branch_id": r[0], "total_stock": r[1]}
                    for r in InventoryRepository.branch_stock(db, schema)
                ],
                "stock_movement_trend": [
                    {"date": str(r[0]), "movement": r[1]}
                    for r in InventoryRepository.stock_movement_trend(db, schema)
                ],
                "inventory_value_by_category": [
                    {"category": r[0], "value": r[1]}
                    for r in InventoryRepository.inventory_value_by_category(db, schema)
                ]
            })
            
        return data
    
    @staticmethod
    def get_inventory_tables(db, schema, allowed_modules: list = None):
        am = [m.upper() for m in allowed_modules] if allowed_modules else ["PRODUCT_MANAGEMENT", "STOCK_MANAGEMENT"]
        
        data = {}

        # Tables are largely related to stock movements and quantities
        if "STOCK_MANAGEMENT" in am:
            data.update({
                "low_stock_products": [
                    {
                        "product_name": r[0],
                        "product_code": r[1],
                        "branch_id": r[2],
                        "category": r[3],
                        "assets_qty": r[4],
                        "consumable_qty": r[5],
                        "resell_qty": r[6],
                        "status": r[7]
                    }
                    for r in InventoryRepository.low_stock_table(db, schema)
                ],
                "out_of_stock_products": [
                    {
                        "product_name": r[0],
                        "product_code": r[1],
                        "branch_id": r[2],
                        "category": r[3]
                    }
                    for r in InventoryRepository.out_of_stock_table(db, schema)
                ],
                "branch_stock": [
                    {
                        "branch_id": r[0],
                        "product_name": r[1],
                        "category": r[2],
                        "assets_qty": r[3],
                        "consumable_qty": r[4],
                        "resell_qty": r[5],
                        "in_transit_qty": r[6],
                        "reserved_qty": r[7],
                        "status": r[8]
                    }
                    for r in InventoryRepository.branch_stock_table(db, schema)
                ],
                "central_stock_entries": [
                    {
                        "entry_id": r[0],
                        "product_name": r[1],
                        "supplier_name": r[2],
                        "invoice_number": r[3],
                        "invoice_date": str(r[4]),
                        "total_qty": r[5],
                        "assets_qty": r[6],
                        "consumable_qty": r[7],
                        "resell_qty": r[8],
                        "total_with_tax": r[9],
                        "created_at": str(r[10])
                    }
                    for r in InventoryRepository.central_stock_entries(db, schema)
                ],
                "recent_stock_movements": [
                    {
                        "reference_type": r[0],
                        "reference_id": r[1],
                        "product_id": r[2],
                        "branch_id": r[3],
                        "stock_type": r[4],
                        "quantity_delta": r[5],
                        "action": r[6],
                        "created_by": r[7],
                        "created_at": str(r[8])
                    }
                    for r in InventoryRepository.stock_movements(db, schema)
                ],
                "stock_transfers": [
                    {
                        "product_name": r[0],
                        "assets_qty": r[1],
                        "consumable_qty": r[2],
                        "resell_qty": r[3],
                        "source_branch_id": r[4]
                    }
                    for r in InventoryRepository.stock_transfers_table(db, schema)
                ]
            })
            
        return data
    
    @staticmethod
    def get_inventory_alerts(db, schema, allowed_modules: list = None):
        am = [m.upper() for m in allowed_modules] if allowed_modules else ["PRODUCT_MANAGEMENT", "STOCK_MANAGEMENT"]
        
        data = {}

        if "STOCK_MANAGEMENT" in am:
            data.update({
                "low_stock_alerts": [
                    {"product_name": r[0], "branch_id": r[1]}
                    for r in InventoryRepository.low_stock_alert(db, schema)
                ],
                "out_of_stock_alerts": [
                    {"product_name": r[0]}
                    for r in InventoryRepository.out_of_stock_alert(db, schema)
                ],
                "high_reserved_stock": [
                    {"product_name": r[0], "reserved_qty": r[1]}
                    for r in InventoryRepository.high_reserved_stock(db, schema)
                ],
                "high_in_transit_stock": [
                    {"product_name": r[0], "in_transit_qty": r[1]}
                    for r in InventoryRepository.high_in_transit(db, schema)
                ],
                "expired_consumables": [
                    {"product_name": r[0], "expiry_date": str(r[1])}
                    for r in InventoryRepository.expired_consumables(db, schema)
                ]
            })
            
        return data
from app.repositories.purchase_repo import PurchaseRepository

class PurchaseService:

    @staticmethod
    def get_kpi(db, schema):
        return {
            "total_purchase_money": PurchaseRepository.total_purchase_money(db, schema),
            "pending_purchase_orders": PurchaseRepository.pending_orders(db, schema),
            "total_items_purchased": PurchaseRepository.total_items(db, schema),
            "late_delivery_orders": PurchaseRepository.late_orders(db, schema)
        }
    

    @staticmethod
    def get_charts(db, schema):
        return {
            "po_status": [{"status": r[0], "count": r[1]} for r in PurchaseRepository.po_status_chart(db, schema)],

            "vendor_spending": [{"vendor": r[0], "amount": r[1]} for r in PurchaseRepository.vendor_spending(db, schema)],

            "daily_po": [{"date": str(r[0]), "count": r[1]} for r in PurchaseRepository.daily_po(db, schema)]
        }
    

    @staticmethod
    def get_tables(db, schema):
        return {
            "recent_po": [
                {
                    "po_number": r[0], "vendor": r[1],
                    "po_date": str(r[2]), "delivery_date": str(r[3]),
                    "amount": r[4], "status": r[5], "branch": r[6]
                } for r in PurchaseRepository.recent_po(db, schema)
            ],

            "vendor_summary": [
                {
                    "vendor": r[0], "branch": r[1],
                    "total_orders": r[2], "total_amount": r[3],
                    "last_purchase": str(r[4])
                } for r in PurchaseRepository.vendor_summary(db, schema)
            ]
        }
    
    @staticmethod
    def get_alerts(db, schema):
        return {
            "late_delivery": [
                {"po_number": r[0], "delivery_date": str(r[1])}
                for r in PurchaseRepository.late_delivery_alert(db, schema)
            ],
            "high_value": [
                {"po_number": r[0], "amount": r[1]}
                for r in PurchaseRepository.high_value_alert(db, schema)
            ]
        }
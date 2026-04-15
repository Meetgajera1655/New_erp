from app.repositories.purchase_repo import PurchaseRepository

class PurchaseService:

    @staticmethod
    def get_kpi(db, schema, **kwargs):
        return {
            "total_purchase_money": PurchaseRepository.total_purchase_money(db, schema, **kwargs),
            "pending_purchase_orders": PurchaseRepository.pending_orders(db, schema, **kwargs),
            "total_items_purchased": PurchaseRepository.total_items(db, schema, **kwargs),
            "late_delivery_orders": PurchaseRepository.late_orders(db, schema, **kwargs)
        }
    

    @staticmethod
    def get_charts(db, schema, **kwargs):
        return {
            "po_status": [{"status": r[0], "count": r[1]} for r in PurchaseRepository.po_status_chart(db, schema, **kwargs)],

            "vendor_spending": [{"vendor": r[0], "amount": r[1]} for r in PurchaseRepository.vendor_spending(db, schema, **kwargs)],

            "daily_po": [{"date": str(r[0]), "count": r[1]} for r in PurchaseRepository.daily_po(db, schema, **kwargs)]
        }
    

    @staticmethod
    def get_tables(db, schema, **kwargs):
        return {
            "recent_po": [
                {
                    "po_number": r[0], "vendor": r[1],
                    "po_date": str(r[2]), "delivery_date": str(r[3]),
                    "amount": r[4], "status": r[5], "branch": r[6]
                } for r in PurchaseRepository.recent_po(db, schema, **kwargs)
            ],

            "vendor_summary": [
                {
                    "vendor": r[0], "branch": r[1],
                    "total_orders": r[2], "total_amount": r[3],
                    "last_purchase": str(r[4])
                } for r in PurchaseRepository.vendor_summary(db, schema, **kwargs)
            ]
        }
    
    @staticmethod
    def get_alerts(db, schema, **kwargs):
        return {
            "late_delivery": [
                {"po_number": r[0], "delivery_date": str(r[1])}
                for r in PurchaseRepository.late_delivery_alert(db, schema, **kwargs)
            ],
            "high_value": [
                {"po_number": r[0], "amount": r[1]}
                for r in PurchaseRepository.high_value_alert(db, schema, **kwargs)
            ]
        }
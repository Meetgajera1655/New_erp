from app.repositories.vendor_repo import VendorRepository

class VendorService:

    @staticmethod
    def get_vendor_kpi(db, schema, **kwargs):
        return {
            "total_active_vendors": VendorRepository.total_active_vendors(db, schema, **kwargs),

            "average_vendor_rating": round(
                VendorRepository.avg_vendor_rating(db, schema, **kwargs) or 0, 2
            ),

            "expiring_vendor_contracts": VendorRepository.expiring_contracts(db, schema, **kwargs),

            "average_delivery_time_days": round(
                VendorRepository.avg_delivery_time(db, schema, **kwargs) or 0, 2
            )
        }
    
    @staticmethod
    def get_vendor_charts(db, schema, **kwargs):
        return {

            "vendors_by_category": [
                {"vendor_category": r[0], "count": r[1]}
                for r in VendorRepository.vendors_by_category(db, schema, **kwargs)
            ],

            "contract_status_split": [
                {"has_contract": r[0], "count": r[1]}
                for r in VendorRepository.contract_status_split(db, schema, **kwargs)
            ],

            "rating_distribution": [
                {"vendor_rating": r[0], "count": r[1]}
                for r in VendorRepository.rating_distribution(db, schema, **kwargs)
            ]
        }
    

    @staticmethod
    def get_vendor_tables(db, schema, **kwargs):
        return {

            "recent_vendor_additions": [
                {
                    "id": r[0],
                    "vendor_name": r[1],
                    "vendor_type": r[2],
                    "created_at": str(r[3]),
                    "vendor_status": r[4]
                }
                for r in VendorRepository.recent_vendors(db, schema, **kwargs)
            ],

            "active_contract_list": [
                {
                    "id": r[0],
                    "vendor_name": r[1],
                    "contract_type": r[2],
                    "contract_end_date": str(r[3]),
                    "payment_terms": r[4]
                }
                for r in VendorRepository.active_contracts(db, schema, **kwargs)
            ]
        }
    
    @staticmethod
    def get_vendor_alerts(db, schema, **kwargs):
        return {

            "low_vendor_rating_alerts": [
                {
                    "id": r[0],
                    "vendor_name": r[1],
                    "vendor_rating": r[2]
                }
                for r in VendorRepository.low_vendor_rating(db, schema, **kwargs)
            ],

            "expiring_contract_alerts": [
                {
                    "id": r[0],
                    "vendor_name": r[1],
                    "contract_end_date": str(r[2])
                }
                for r in VendorRepository.expiring_contracts_alert(db, schema, **kwargs)
            ]
        }
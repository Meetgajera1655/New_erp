from app.repositories.contract_repo import ContractRepository

class ContractService:

    @staticmethod
    def get_kpi(db, schema, **kwargs):
        return {
            "total_contracts": ContractRepository.total_contracts(db, schema, **kwargs),
            "active_contracts": ContractRepository.active_contracts(db, schema, **kwargs),
            "total_value": ContractRepository.total_value(db, schema, **kwargs),
            "expiring_soon": ContractRepository.expiring_soon(db, schema, **kwargs)
        }

    @staticmethod
    def get_charts(db, schema, **kwargs):
        return {
            "status_distribution": [
                {"status": r[0], "count": r[1]}
                for r in ContractRepository.status_distribution(db, schema, **kwargs)
            ],
            "branch_contracts": [
                {"branch": r[0], "count": r[1]}
                for r in ContractRepository.branch_contracts(db, schema, **kwargs)
            ],
            "monthly_value": [
                {"month": str(r[0]), "value": r[1]}
                for r in ContractRepository.monthly_value(db, schema, **kwargs)
            ]
        }

    @staticmethod
    def get_tables(db, schema, **kwargs):
        return {
            "recent_contracts": [
                {
                    "contract_id": r[0],
                    "customer_name": r[1],
                    "gma_id": r[2],
                    "value": r[3],
                    "start": str(r[4]),
                    "end": str(r[5]),
                    "status": r[6],
                    "branch": r[7]
                }
                for r in ContractRepository.recent_contracts(db, schema, **kwargs)
            ],
            "expiring_contracts": [
                {
                    "contract_id": r[0],
                    "customer_name": r[1],
                    "value": r[2],
                    "end": str(r[3]),
                    "status": r[4],
                    "branch": r[5]
                }
                for r in ContractRepository.expiring_list(db, schema, **kwargs)
            ]
        }

    @staticmethod
    def get_alerts(db, schema, **kwargs):
        return {
            "expiry_alert": [
                {
                    "contract_id": r[0],
                    "customer_name": r[1],
                    "end_date": str(r[2]),
                    "value": r[3],
                    "status": r[4]
                }
                for r in ContractRepository.expiry_alert(db, schema, **kwargs)
            ],
            "no_sales_order": [
                {
                    "contract_id": r[0],
                    "value": r[1],
                    "start_date": str(r[2]),
                    "status": r[3]
                }
                for r in ContractRepository.no_sales_order(db, schema, **kwargs)
            ]
        }
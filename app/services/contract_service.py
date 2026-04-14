from app.repositories.contract_repo import ContractRepository

class ContractService:

    @staticmethod
    def get_kpi(db, schema):
        return {
            "total_contracts": ContractRepository.total_contracts(db, schema),
            "active_contracts": ContractRepository.active_contracts(db, schema),
            "total_value": ContractRepository.total_value(db, schema),
            "expiring_soon": ContractRepository.expiring_soon(db, schema)
        }

    @staticmethod
    def get_charts(db, schema):
        return {
            "status_distribution": [
                {"status": r[0], "count": r[1]}
                for r in ContractRepository.status_distribution(db, schema)
            ],
            "branch_contracts": [
                {"branch": r[0], "count": r[1]}
                for r in ContractRepository.branch_contracts(db, schema)
            ],
            "monthly_value": [
                {"month": str(r[0]), "value": r[1]}
                for r in ContractRepository.monthly_value(db, schema)
            ]
        }

    @staticmethod
    def get_tables(db, schema):
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
                for r in ContractRepository.recent_contracts(db, schema)
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
                for r in ContractRepository.expiring_list(db, schema)
            ]
        }

    @staticmethod
    def get_alerts(db, schema):
        return {
            "expiry_alert": [
                {
                    "contract_id": r[0],
                    "customer_name": r[1],
                    "end_date": str(r[2]),
                    "value": r[3],
                    "status": r[4]
                }
                for r in ContractRepository.expiry_alert(db, schema)
            ],
            "no_sales_order": [
                {
                    "contract_id": r[0],
                    "value": r[1],
                    "start_date": str(r[2]),
                    "status": r[3]
                }
                for r in ContractRepository.no_sales_order(db, schema)
            ]
        }
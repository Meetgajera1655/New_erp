from app.repositories.customer_repo import CustomerRepository

class CustomerService:

    # KPI
    @staticmethod
    def get_kpi(db, schema):
        return {
            "total_customers": CustomerRepository.total_customers(db, schema),
            "active_customers": CustomerRepository.active_customers(db, schema),
            "contract_customers": CustomerRepository.contract_customers(db, schema),
            "total_revenue": CustomerRepository.total_revenue(db, schema)
        }

    # Charts
    @staticmethod
    def get_charts(db, schema):
        return {
            "customer_type": [
                {"type": r[0], "count": r[1]}
                for r in CustomerRepository.customer_type(db, schema)
            ],
            "branch_customers": [
                {"branch": r[0], "count": r[1]}
                for r in CustomerRepository.branch_customers(db, schema)
            ],
            "monthly_trend": [
                {"month": str(r[0]), "count": r[1]}
                for r in CustomerRepository.monthly_customers(db, schema)
            ]
        }

    # Tables
    @staticmethod
    def get_tables(db, schema):
        return {
            "recent_customers": [
                {
                    "customer_id": r[0],
                    "name": r[1],
                    "type": r[2],
                    "phone": r[3],
                    "branch": r[4],
                    "status": r[5],
                    "created_at": str(r[6])
                }
                for r in CustomerRepository.recent_customers(db, schema)
            ],
            "active_contracts": [
                {
                    "customer_id": r[0],
                    "name": r[1],
                    "contract_id": r[2],
                    "start": str(r[3]),
                    "end": str(r[4]),
                    "value": r[5],
                    "status": r[6]
                }
                for r in CustomerRepository.active_contracts(db, schema)
            ]
        }

    # Alerts
    @staticmethod
    def get_alerts(db, schema):
        return {
            "inactive_customers": [
                {
                    "customer_id": r[0],
                    "name": r[1],
                    "type": r[2],
                    "branch_id": r[3],
                    "status": r[4]
                }
                for r in CustomerRepository.inactive_customers(db, schema)
            ],
            "no_contract_customers": [
                {
                    "customer_id": r[0],
                    "name": r[1],
                    "type": r[2],
                    "created_at": str(r[3])
                }
                for r in CustomerRepository.no_contract_customers(db, schema)
            ]
        }
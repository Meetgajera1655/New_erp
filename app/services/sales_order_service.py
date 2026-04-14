from app.repositories.sales_order_repository import SalesOrderRepository

class SalesOrderService:

    @staticmethod
    def get_kpi(db, schema):
        return {
          "total_orders": SalesOrderRepository.total_orders(db, schema),
            "total_amount": float(SalesOrderRepository.total_amount(db, schema)),
            "open_orders": SalesOrderRepository.open_orders(db, schema),
            "completed_orders": SalesOrderRepository.completed_orders(db, schema),
        }

    @staticmethod
    def get_charts(db, schema):
        return {
            "status_chart": [
                {"status": r[0], "count": r[1]}
                for r in SalesOrderRepository.status_chart(db, schema)
            ],
            "monthly_revenue": [
                {"month": str(r[0]), "revenue": float(r[1] or 0)}
                for r in SalesOrderRepository.monthly_revenue(db, schema)
            ],
            "branch_sales": [
                {"branch": r[0], "amount": float(r[1] or 0)}
                for r in SalesOrderRepository.branch_sales(db, schema)
            ]
        }

    @staticmethod
    def get_tables(db, schema):
        return {
            "recent_orders": [
                {
                    "so_number": r[0],
                    "customer_name": r[1],
                    "order_type": r[2],
                    "amount": float(r[3]),
                    "status": r[4],
                    "branch": r[5],
                    "created_at": str(r[6])
                }
                for r in SalesOrderRepository.recent_orders(db, schema)
            ]
        }

    @staticmethod
    def get_alerts(db, schema):
        return {
            "high_value_orders": [
                {
                    "so_number": r[0],
                    "customer_id": r[1],
                    "amount": float(r[2]),
                    "status": r[3],
                    "created_at": str(r[4])
                }
                for r in SalesOrderRepository.high_value_orders(db, schema)
            ],
            "pending_orders": [
                {
                    "so_number": r[0],
                    "customer_id": r[1],
                    "status": r[2],
                    "created_at": str(r[3])
                }
                for r in SalesOrderRepository.pending_orders(db, schema)
            ]
        }
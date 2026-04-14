from app.repositories.financial_repository import FinancialRepository

class FinancialService:

    @staticmethod
    def get_charts(db, schema):
        return {

            "country_revenue": [
                {"date": str(r[0]), "revenue": float(r[1] or 0)}
                for r in FinancialRepository.country_revenue(db, schema)
            ],

            "branch_revenue": [
                {
                    "branch_id": r[0],
                    "date": str(r[1]),
                    "revenue": float(r[2] or 0)
                }
                for r in FinancialRepository.branch_revenue_trend(db, schema)
            ],

            "revenue_breakup": [
                {"type": r[0], "value": float(r[1] or 0)}
                for r in FinancialRepository.revenue_breakup(db, schema)
            ],

            "technician_productivity": [
                {
                    "technician_id": r[0],
                    "technician_name": r[1],
                    "total_tasks": r[2],
                    "total_revenue": float(r[3] or 0),
                    "productivity": float(r[4] or 0)
                }
                for r in FinancialRepository.technician_productivity(db, schema)
            ],

            "chemical_consumption": [
                {"product_code": r[0], "consumption": float(r[1] or 0)}
                for r in FinancialRepository.chemical_consumption(db, schema)
            ],

            "collection_vs_outstanding": {
                "collected": float(
                    FinancialRepository.collection_vs_outstanding(db, schema)[0] or 0
                ),
                "outstanding": float(
                    FinancialRepository.collection_vs_outstanding(db, schema)[1] or 0
                ),
            },

            "employee_growth": [
                {"date": str(r[0]), "count": r[1]}
                for r in FinancialRepository.employee_growth(db, schema)
            ],

            "invoice_status": [
                {"status": r[0], "count": r[1]}
                for r in FinancialRepository.invoice_status(db, schema)
            ],
        }

    def get_tables(db, schema):
        return {
            "revenue_summary": [
                {"branch": r[0], "revenue": float(r[1])}
                for r in FinancialRepository.revenue_summary(db, schema)
            ],
            "task_summary": [
                {"status": r[0], "count": r[1]}
                for r in FinancialRepository.task_summary(db, schema)
            ],
            "customer_revenue": [
                {"customer": r[0], "revenue": float(r[1])}
                for r in FinancialRepository.customer_revenue(db, schema)
            ],
            "invoice_details": [
                dict(r._mapping)
                for r in FinancialRepository.invoice_details(db, schema)
            ],

            "product_consumption": [
                {
                    "product_code": r[0],
                    "consumable_qty": float(r[1] or 0)
                }
                for r in FinancialRepository.product_consumption(db, schema)
            ],

            "vendor_outstanding": [
                dict(r._mapping)
                for r in FinancialRepository.vendor_outstanding(db, schema)
            ]
        }
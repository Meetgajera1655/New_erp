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

    @staticmethod
    def get_tables(db, schema):
        return {

            # 📋 1. Revenue Summary Table
            "revenue_summary": [
                {
                    "branch_id": r[0],
                    "branch_name": r[1],
                    "total_revenue": float(r[2] or 0)
                }
                for r in FinancialRepository.revenue_summary(db, schema)
            ],

            # 📋 2. Task Summary Table
            "task_summary": [
                {
                    "status": r[0],
                    "total_tasks": r[1]
                }
                for r in FinancialRepository.task_summary(db, schema)
            ],

            # 📋 3. Customer Revenue Table
            "customer_revenue": [
                {
                    "customer_id": r[0],
                    "customer_name": r[1],
                    "total_revenue": float(r[2] or 0)
                }
                for r in FinancialRepository.customer_revenue(db, schema)
            ],

            # 📋 4. Invoice Detail Table
            "invoice_details": [
                {
                    "invoice_number": r[0],
                    "customer_id": r[1],
                    "customer_name": r[2],
                    "invoice_date": str(r[3]),
                    "grand_total": float(r[4] or 0),
                    "status": r[5]
                }
                for r in FinancialRepository.invoice_details(db, schema)
            ],

            # 📋 5. Product Consumption Table
            "product_consumption": [
                {
                    "product_code": r[0],
                    "product_name": r[1],
                    "consumable_qty": float(r[2] or 0)
                }
                for r in FinancialRepository.product_consumption(db, schema)
            ],

            # 📋 6. Vendor Outstanding Summary
            "vendor_outstanding": [
                {
                    "vendor_name": r[0],
                    "paid_amount": float(r[1] or 0),
                    "pending_amount": float(r[2] or 0),
                    "grand_total": float(r[3] or 0)
                }
                for r in FinancialRepository.vendor_outstanding(db, schema)
            ]

        }
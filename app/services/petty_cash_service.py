from app.repositories.petty_cash_repository import PettyCashRepository

class PettyCashService:

    @staticmethod
    def get_kpi(db, schema):
        return {
            "total_requested": PettyCashRepository.total_requested(db, schema),
            "total_approved": PettyCashRepository.total_approved(db, schema),
            "pending_requests": PettyCashRepository.pending_requests(db, schema),
            "paid_requests": PettyCashRepository.paid_requests(db, schema),
        }

    @staticmethod
    def get_charts(db, schema):
        return {
            "status_chart": [
                {"status": r[0], "count": r[1]}
                for r in PettyCashRepository.status_chart(db, schema)
            ],
            "branch_expense": [
                {"branch": r[0], "amount": r[1]}
                for r in PettyCashRepository.branch_expense(db, schema)
            ],
            "monthly_requests": [
                {"date": str(r[0]), "count": r[1]}
                for r in PettyCashRepository.monthly_requests(db, schema)
            ],
        }

    @staticmethod
    def get_tables(db, schema):
        return {
            "recent_requests": [
                {
                    "request_id": r[0],
                    "employee": r[1],
                    "branch": r[2],
                    "category": r[3],
                    "requested_amount": r[4],
                    "approved_amount": r[5],
                    "status": r[6],
                    "submitted_at": str(r[7]),
                }
                for r in PettyCashRepository.recent_requests(db, schema)
            ],
            "approved_payments": [
                {
                    "request_id": r[0],
                    "paid_by": r[1],
                    "category": r[2],
                    "approved_amount": r[3],
                    "payment_mode": r[4],
                    "transaction_ref": r[5],
                    "payment_date": str(r[6]),
                    "status": r[7],
                }
                for r in PettyCashRepository.approved_payments(db, schema)
            ],
        }

    @staticmethod
    def get_alerts(db, schema):
        return {
            "high_amount": [
                {
                    "id": r[0],
                    "category": r[1],
                    "amount": r[2],
                    "status": r[3],
                    "submitted_at": str(r[4]),
                }
                for r in PettyCashRepository.high_amount(db, schema)
            ],
            "pending_old": [
                {
                    "id": r[0],
                    "category": r[1],
                    "amount": r[2],
                    "status": r[3],
                    "submitted_at": str(r[4]),
                }
                for r in PettyCashRepository.pending_old(db, schema)
            ],
        }
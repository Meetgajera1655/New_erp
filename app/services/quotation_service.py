from app.repositories.quotation_repo import QuotationRepository

class QuotationService:

    # KPI
    @staticmethod
    def get_kpi(db, schema):
        return {
            "total_quotes": QuotationRepository.total_quotes(db, schema),
            "accepted_rate": round(QuotationRepository.accepted_rate(db, schema) or 0, 2),
            "total_value": QuotationRepository.total_value(db, schema),
            "average_value": round(QuotationRepository.avg_value(db, schema), 2)
        }

    # Charts
    @staticmethod
    def get_charts(db, schema):
        return {
            "status": [{"status": r[0], "count": r[1]} for r in QuotationRepository.status_chart(db, schema)],
            "monthly": [{"month": str(r[0]), "value": r[1]} for r in QuotationRepository.monthly_trend(db, schema)],
            "branch": [{"branch": r[0], "value": r[1]} for r in QuotationRepository.branch_performance(db, schema)],
            "source": [{"source": r[0], "count": r[1]} for r in QuotationRepository.source_mix(db, schema)]
        }

    # Tables
    @staticmethod
    def get_tables(db, schema):
        return {
            "high_value_quotes": [
                {
                    "quotation_number": r[0],
                    "source": r[1],
                    "amount": r[2],
                    "status": r[3],
                    "created_at": str(r[4])
                }
                for r in QuotationRepository.high_value_quotes(db, schema)
            ],
            "expiring_quotes": [
                {
                    "quotation_number": r[0],
                    "valid_till": str(r[1]),
                    "amount": r[2],
                    "created_by": r[3]
                }
                for r in QuotationRepository.expiring_quotes(db, schema)
            ]
        }

    # Alerts
    @staticmethod
    def get_alerts(db, schema):
        rate = QuotationRepository.low_acceptance(db, schema)

        return {
            "low_acceptance_alert": rate < 0.20 if rate else False,
            "high_value_pending": [
                {"quotation_number": r[0], "amount": r[1]}
                for r in QuotationRepository.high_value_pending(db, schema)
            ],
            "critical_expiry": [
                {"quotation_number": r[0], "valid_till": str(r[1])}
                for r in QuotationRepository.critical_expiry(db, schema)
            ]
        }
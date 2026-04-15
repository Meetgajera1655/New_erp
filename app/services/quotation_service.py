from app.repositories.quotation_repo import QuotationRepository

class QuotationService:

    # KPI
    @staticmethod
    def get_kpi(db, schema, **kwargs):
        return {
            "total_quotes": QuotationRepository.total_quotes(db, schema, **kwargs),
            "accepted_rate": round(QuotationRepository.accepted_rate(db, schema, **kwargs) or 0, 2),
            "total_value": QuotationRepository.total_value(db, schema, **kwargs),
            "average_value": round(QuotationRepository.avg_value(db, schema, **kwargs), 2)
        }

    # Charts
    @staticmethod
    def get_charts(db, schema, **kwargs):
        return {
            "status": [{"status": r[0], "count": r[1]} for r in QuotationRepository.status_chart(db, schema, **kwargs)],
            "monthly": [{"month": str(r[0]), "value": r[1]} for r in QuotationRepository.monthly_trend(db, schema, **kwargs)],
            "branch": [{"branch": r[0], "value": r[1]} for r in QuotationRepository.branch_performance(db, schema, **kwargs)],
            "source": [{"source": r[0], "count": r[1]} for r in QuotationRepository.source_mix(db, schema, **kwargs)]
        }

    # Tables
    @staticmethod
    def get_tables(db, schema, **kwargs):
        return {
            "high_value_quotes": [
                {
                    "quotation_number": r[0],
                    "source": r[1],
                    "amount": r[2],
                    "status": r[3],
                    "created_at": str(r[4])
                }
                for r in QuotationRepository.high_value_quotes(db, schema, **kwargs)
            ],
            "expiring_quotes": [
                {
                    "quotation_number": r[0],
                    "valid_till": str(r[1]),
                    "amount": r[2],
                    "created_by": r[3]
                }
                for r in QuotationRepository.expiring_quotes(db, schema, **kwargs)
            ]
        }

    # Alerts
    @staticmethod
    def get_alerts(db, schema, **kwargs):
        rate = QuotationRepository.low_acceptance(db, schema, **kwargs)

        return {
            "low_acceptance_alert": rate < 0.20 if rate else False,
            "high_value_pending": [
                {"quotation_number": r[0], "amount": r[1]}
                for r in QuotationRepository.high_value_pending(db, schema, **kwargs)
            ],
            "critical_expiry": [
                {"quotation_number": r[0], "valid_till": str(r[1])}
                for r in QuotationRepository.critical_expiry(db, schema, **kwargs)
            ]
        }
from app.repositories.gma_repo import GMARepository

class GMAService:

    # KPI
    @staticmethod
    def get_kpi(db, schema, **kwargs):
        return {
            "total_gma_sheets": GMARepository.total_gma(db, schema, **kwargs),
            "approved_gma_sheets": GMARepository.approved_gma(db, schema, **kwargs),
            "pending_gma_sheets": GMARepository.pending_gma(db, schema, **kwargs),
            "avg_gross_margin": round(GMARepository.avg_margin(db, schema, **kwargs), 2)
        }

    # Charts
    @staticmethod
    def get_charts(db, schema, **kwargs):
        return {
            "status_distribution": [
                {"status": r[0], "count": r[1]}
                for r in GMARepository.status_distribution(db, schema, **kwargs)
            ],
            "branch_gma": [
                {"branch": r[0], "count": r[1]}
                for r in GMARepository.branch_gma(db, schema, **kwargs)
            ],
            "monthly_gma": [
                {"month": str(r[0]), "count": r[1]}
                for r in GMARepository.monthly_gma(db, schema, **kwargs)
            ]
        }

    # Tables
    @staticmethod
    def get_tables(db, schema, **kwargs):
        return {
            "recent_gma": [
                {
                    "gma_id": r[0],
                    "source_type": r[1],
                    "contract_duration": r[2],
                    "start_date": str(r[3]),
                    "branch": r[4],
                    "price": r[5],
                    "status": r[6],
                    "created_at": str(r[7])
                }
                for r in GMARepository.recent_gma(db, schema, **kwargs)
            ],
            "approved_summary": [
                {
                    "gma_id": r[0],
                    "cost": r[1],
                    "price": r[2],
                    "gross_margin": r[3],
                    "gm_with_doc": r[4],
                    "visits": r[5],
                    "approved_on": str(r[6])
                }
                for r in GMARepository.approved_summary(db, schema, **kwargs)
            ]
        }

    # Alerts
    @staticmethod
    def get_alerts(db, schema, **kwargs):
        return {
            "pending_approval": [
                {
                    "id": r[0],
                    "source": r[1],
                    "status": r[2],
                    "deadline": str(r[3]),
                    "created_at": str(r[4])
                }
                for r in GMARepository.pending_alert(db, schema, **kwargs)
            ],
            "low_margin": [
                {
                    "id": r[0],
                    "price": r[1],
                    "cost": r[2],
                    "margin": r[3],
                    "status": r[4]
                }
                for r in GMARepository.low_margin(db, schema, **kwargs)
            ]
        }
from app.repositories.gma_repo import GMARepository

class GMAService:

    @staticmethod
    def get_kpi(db, schema, **kwargs):
        return {
            "total_gma": GMARepository.total_gma(db, schema, **kwargs),
            "approved_gma": GMARepository.approved_gma(db, schema, **kwargs),
            "pending_gma": GMARepository.pending_gma(db, schema, **kwargs),
            "avg_margin": float(GMARepository.avg_margin(db, schema, **kwargs) or 0),
        }

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
            ],
            "monthly_gma_value": [
                {
                    "month": r[0],
                    "total_cost": float(r[1]) if r[1] else 0,
                    "total_price": float(r[2]) if r[2] else 0,
                    "avg_margin": float(r[3]) if r[3] else 0
                }
                for r in GMARepository.monthly_gma_value(db, schema, **kwargs)
            ]
        }

    @staticmethod
    def get_tables(db, schema, **kwargs):
        return {
            "recent_gma": [
                {
                    "id": r[0],
                    "source_type": r[1],
                    "contract_duration": r[2],
                    "proposed_start_date": str(r[3]) if r[3] else None,
                    "branch_name": r[4],
                    "total_annual_price": float(r[5]) if r[5] else 0,
                    "status": r[6],
                    "created_at": str(r[7])
                }
                for r in GMARepository.recent_gma(db, schema, **kwargs)
            ],
            "approved_summary": [
                {
                    "id": r[0],
                    "total_annual_cost": float(r[1]) if r[1] else 0,
                    "total_annual_price": float(r[2]) if r[2] else 0,
                    "overall_gross_margin": float(r[3]) if r[3] else 0,
                    "gm_with_doc": float(r[4]) if r[4] else 0,
                    "total_visits_per_month": r[5],
                    "approved_on": str(r[6]) if r[6] else None
                }
                for r in GMARepository.approved_summary(db, schema, **kwargs)
            ]
        }

    @staticmethod
    def get_alerts(db, schema, **kwargs):
        return {
            "pending_alert": [
                {
                    "id": r[0],
                    "source_type": r[1],
                    "status": r[2],
                    "deadline": str(r[3]) if r[3] else None,
                    "created_at": str(r[4])
                }
                for r in GMARepository.pending_alert(db, schema, **kwargs)
            ]
        }
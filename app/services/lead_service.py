from app.repositories.lead_repo import LeadRepository

class LeadService:

    # ================= KPI =================
    @staticmethod
    def get_kpi(db, schema):
        return {
            "total_active_leads": LeadRepository.total_active_leads(db, schema),
            "qualified_leads": LeadRepository.qualified_leads(db, schema),
            "pending_followups": LeadRepository.pending_followups(db, schema),
            "conversion_rate": round(LeadRepository.conversion_rate(db, schema) or 0, 2)
        }

    # ================= CHARTS =================
    @staticmethod
    def get_charts(db, schema):
        return {
            "status_wise": [{"status": r[0], "count": r[1]} for r in LeadRepository.status_wise_leads(db, schema)],
            "source": [{"source": r[0], "count": r[1]} for r in LeadRepository.leads_by_source(db, schema)],
            "followup_activity": [{"date": str(r[0]), "count": r[1]} for r in LeadRepository.daily_followups(db, schema)],
            "priority": [{"priority": r[0], "count": r[1]} for r in LeadRepository.leads_by_priority(db, schema)]
        }

    # ================= TABLES =================
    @staticmethod
    def get_tables(db, schema):
        return {
            "recent_leads": [
                {
                    "lead_id": r[0], "lead_name": r[1], "mobile": r[2],
                    "type": r[3], "source": r[4], "priority": r[5],
                    "status": r[6], "created_date": str(r[7])
                } for r in LeadRepository.recent_leads(db, schema)
            ],
            "upcoming_followups": [
                {
                    "lead_name": r[0],
                    "followup_date": str(r[1]),
                    "priority": r[2]
                }
                    for r in LeadRepository.upcoming_followups(db, schema)
            ]
        }

    # ================= ALERTS =================
    @staticmethod
    def get_alerts(db, schema):
        return {
            "overdue_followups": [
                {"lead_id": r[0], "lead_name": r[1], "date": str(r[2]), "status": r[3]}
                for r in LeadRepository.overdue_followups(db, schema)
            ],
            "urgent_leads": [
                {"lead_id": r[0], "lead_name": r[1], "mobile": r[2], "priority": r[3], "status": r[4], "created": str(r[5])}
                for r in LeadRepository.urgent_leads(db, schema)
            ],
            "pending_leads": [
                {"lead_id": r[0], "lead_name": r[1], "priority": r[2], "status": r[3], "updated": str(r[4])}
                for r in LeadRepository.pending_leads(db, schema)
            ]
        }
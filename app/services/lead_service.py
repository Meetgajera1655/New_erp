from app.repositories.lead_repo import LeadRepository
from app.filters.lead_filter import apply_lead_filters

class LeadService:

    # ================= KPI =================
    @staticmethod
    def get_kpi(db, schema, allowed_modules: list = None, **kwargs):
        where_clause_l, params_l = apply_lead_filters(alias="l", date_column="created_at", **kwargs)
        where_clause_f, params_f = apply_lead_filters(alias="f", branch_alias="l", date_column="created_at", **kwargs)
        
        print("LEADS WHERE:", where_clause_l)
        print("FOLLOW WHERE:", where_clause_f)
        print("PARAMS:", params_l)

        out = {}
        if not allowed_modules or "LEADS_MANAGEMENT" in allowed_modules:
            out["total_active_leads"] = LeadRepository.total_active_leads(db, schema, where_clause_l, params_l)
            out["qualified_leads"] = LeadRepository.qualified_leads(db, schema, where_clause_l, params_l)
            out["conversion_rate"] = round(LeadRepository.conversion_rate(db, schema, where_clause_l, params_l) or 0, 2)
            
        if not allowed_modules or "FOLLOW_UP_MANAGEMENT" in allowed_modules:
            # For count of pending followups, we might still want to apply the general filters (branch/date)
            out["pending_followups"] = LeadRepository.pending_followups(db, schema, where_clause_f, params_f)
            
        return out

    # ================= CHARTS =================
    @staticmethod
    def get_charts(db, schema, allowed_modules: list = None, **kwargs):
        where_clause_l, params_l = apply_lead_filters(alias="l", date_column="created_at", **kwargs)
        where_clause_f, params_f = apply_lead_filters(alias="f", branch_alias="l", date_column="created_at", **kwargs)
        
        print("LEADS WHERE:", where_clause_l)
        print("FOLLOW WHERE:", where_clause_f)
        
        out = {}
        if not allowed_modules or "LEADS_MANAGEMENT" in allowed_modules:
            out["status_wise"] = [{"status": r[0], "count": r[1]} for r in LeadRepository.status_wise_leads(db, schema, where_clause_l, params_l)]
            out["source"] = [{"source": r[0], "count": r[1]} for r in LeadRepository.leads_by_source(db, schema, where_clause_l, params_l)]
            out["priority"] = [{"priority": r[0], "count": r[1]} for r in LeadRepository.leads_by_priority(db, schema, where_clause_l, params_l)]
            
        if not allowed_modules or "FOLLOW_UP_MANAGEMENT" in allowed_modules:
            out["followup_activity"] = [{"date": str(r[0]), "count": r[1]} for r in LeadRepository.daily_followups(db, schema, where_clause_f, params_f)]
            
        return out

    # ================= TABLES =================
    @staticmethod
    def get_tables(db, schema, allowed_modules: list = None, **kwargs):
        where_clause_l, params_l = apply_lead_filters(alias="l", date_column="created_at", **kwargs)
        
        # followups table logic in REPO uses 'leads' table for upcoming/overdue in some cases
        # but let's generate the clause for 'l' anyway
        
        out = {}
        if not allowed_modules or "LEADS_MANAGEMENT" in allowed_modules:
            out["recent_leads"] = [
                {
                    "lead_id": r[0], "lead_name": r[1], "mobile": r[2],
                    "type": r[3], "source": r[4], "priority": r[5],
                    "status": r[6], "created_date": str(r[7])
                } for r in LeadRepository.recent_leads(db, schema, where_clause_l, params_l)
            ]
            
        if not allowed_modules or "FOLLOW_UP_MANAGEMENT" in allowed_modules:
            out["upcoming_followups"] = [
                {
                    "lead_name": r[0],
                    "followup_date": str(r[1]),
                    "priority": r[2]
                }
                for r in LeadRepository.upcoming_followups(db, schema, where_clause_l, params_l) # Uses 'leads' table alias 'l'
            ]
            
        return out

    # ================= ALERTS =================
    @staticmethod
    def get_alerts(db, schema, allowed_modules: list = None, **kwargs):
        where_clause_l, params_l = apply_lead_filters(alias="l", date_column="created_at", **kwargs)
        
        out = {}
        if not allowed_modules or "LEADS_MANAGEMENT" in allowed_modules:
            out["urgent_leads"] = [
                {"lead_id": r[0], "lead_name": r[1], "mobile": r[2], "priority": r[3], "status": r[4], "created": str(r[5])}
                for r in LeadRepository.urgent_leads(db, schema, where_clause_l, params_l)
            ]
            out["pending_leads"] = [
                {"lead_id": r[0], "lead_name": r[1], "priority": r[2], "status": r[3], "updated": str(r[4])}
                for r in LeadRepository.pending_leads(db, schema, where_clause_l, params_l)
            ]
            
        if not allowed_modules or "FOLLOW_UP_MANAGEMENT" in allowed_modules:
            out["overdue_followups"] = [
                {"lead_id": r[0], "lead_name": r[1], "date": str(r[2]), "status": r[3]}
                for r in LeadRepository.overdue_followups(db, schema, where_clause_l, params_l) # Still leads table
            ]
            
        return out
from app.repositories.support_repository import SupportRepository

class SupportService:

    @staticmethod
    def get_kpi(db, schema, **kwargs):
        return {
            "total_tickets": SupportRepository.total_tickets(db, schema, **kwargs),
            "open_tickets": SupportRepository.open_tickets(db, schema, **kwargs),
            "closed_tickets": SupportRepository.closed_tickets(db, schema, **kwargs),
            "high_priority": SupportRepository.high_priority(db, schema, **kwargs),
        }

    @staticmethod
    def get_charts(db, schema, **kwargs):
        return {
            "status_chart": [
                {"status": r[0], "count": r[1]}
                for r in SupportRepository.status_chart(db, schema, **kwargs)
            ],
            "daily_tickets": [
                {"date": str(r[0]), "count": r[1]}
                for r in SupportRepository.daily_tickets(db, schema, **kwargs)
            ],
            "priority_chart": [
                {"priority": r[0], "count": r[1]}
                for r in SupportRepository.priority_chart(db, schema, **kwargs)
            ],
        }

    @staticmethod
    def get_tables(db, schema, **kwargs):
        return {
            "recent_tickets": [
                {
                    "ticket_number": r[0],
                    "customer": r[1],
                    "issue_type": r[2],
                    "priority": r[3],
                    "status": r[4],
                    "created_at": str(r[5]),
                    "branch": r[6],
                }
                for r in SupportRepository.recent_tickets(db, schema, **kwargs)
            ],
            "open_high_priority": [
                {
                    "ticket_number": r[0],
                    "customer": r[1],
                    "issue_type": r[2],
                    "priority": r[3],
                    "status": r[4],
                    "created_at": str(r[5]),
                }
                for r in SupportRepository.open_high_priority(db, schema, **kwargs)
            ],
        }

    @staticmethod
    def get_alerts(db, schema, **kwargs):
        return {
            "high_priority_alert": [
                {
                    "ticket_number": r[0],
                    "issue_type": r[1],
                    "priority": r[2],
                    "status": r[3],
                    "created_at": str(r[4]),
                }
                for r in SupportRepository.high_priority_alert(db, schema, **kwargs)
            ],
            "old_open_tickets": [
                {
                    "ticket_number": r[0],
                    "priority": r[1],
                    "status": r[2],
                    "created_at": str(r[3]),
                }
                for r in SupportRepository.old_open_tickets(db, schema, **kwargs)
            ],
        }
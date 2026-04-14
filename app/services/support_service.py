from app.repositories.support_repository import SupportRepository

class SupportService:

    @staticmethod
    def get_kpi(db, schema):
        return {
            "total_tickets": SupportRepository.total_tickets(db, schema),
            "open_tickets": SupportRepository.open_tickets(db, schema),
            "closed_tickets": SupportRepository.closed_tickets(db, schema),
            "high_priority": SupportRepository.high_priority(db, schema),
        }

    @staticmethod
    def get_charts(db, schema):
        return {
            "status_chart": [
                {"status": r[0], "count": r[1]}
                for r in SupportRepository.status_chart(db, schema)
            ],
            "daily_tickets": [
                {"date": str(r[0]), "count": r[1]}
                for r in SupportRepository.daily_tickets(db, schema)
            ],
            "priority_chart": [
                {"priority": r[0], "count": r[1]}
                for r in SupportRepository.priority_chart(db, schema)
            ],
        }

    @staticmethod
    def get_tables(db, schema):
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
                for r in SupportRepository.recent_tickets(db, schema)
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
                for r in SupportRepository.open_high_priority(db, schema)
            ],
        }

    @staticmethod
    def get_alerts(db, schema):
        return {
            "high_priority_alert": [
                {
                    "ticket_number": r[0],
                    "issue_type": r[1],
                    "priority": r[2],
                    "status": r[3],
                    "created_at": str(r[4]),
                }
                for r in SupportRepository.high_priority_alert(db, schema)
            ],
            "old_open_tickets": [
                {
                    "ticket_number": r[0],
                    "priority": r[1],
                    "status": r[2],
                    "created_at": str(r[3]),
                }
                for r in SupportRepository.old_open_tickets(db, schema)
            ],
        }
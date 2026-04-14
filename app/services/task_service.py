from app.repositories.task_repository import TaskRepository

class TaskService:

    @staticmethod
    def get_kpi(db, schema):
        return {
            "total_tasks": TaskRepository.total_tasks(db, schema),
            "completed_tasks": TaskRepository.completed_tasks(db, schema),
            "pending_tasks": TaskRepository.pending_tasks(db, schema),
            "overdue_tasks": TaskRepository.overdue_tasks(db, schema),
        }

    @staticmethod
    def get_charts(db, schema):
        return {
            "status_chart": [
                {"status": r[0], "count": r[1]}
                for r in TaskRepository.status_chart(db, schema)
            ],
            "monthly_trend": [
                {"month": str(r[0]), "count": r[1]}
                for r in TaskRepository.monthly_trend(db, schema)
            ],
            "technician_workload": [
                {"technician": r[0], "count": r[1]}
                for r in TaskRepository.technician_workload(db, schema)
            ],
        }

    @staticmethod
    def get_tables(db, schema):
        return {
            "recent_tasks": [
                {
                    "task_number": r[0],
                    "customer": r[1],
                    "so_number": r[2],
                    "service": r[3],
                    "scheduled_date": str(r[4]),
                    "start_time": str(r[5]),
                    "end_time": str(r[6]),
                    "status": r[7],
                }
                for r in TaskRepository.recent_tasks(db, schema)
            ],
            "material_usage": [
                {
                    "task_number": r[0],
                    "product": r[1],
                    "uom": r[2],
                    "required_qty": r[3],
                    "used_qty": r[4],
                    "scheduled_date": str(r[5]),
                }
                for r in TaskRepository.material_usage(db, schema)
            ],
        }

    @staticmethod
    def get_alerts(db, schema):
        return {
            "overdue_tasks": [
                {
                    "task_number": r[0],
                    "customer_id": r[1],
                    "service": r[2],
                    "scheduled_date": str(r[3]),
                    "status": r[4],
                }
                for r in TaskRepository.overdue_alert(db, schema)
            ],
            "technician_overload": [
                {
                    "technician": r[0],
                    "date": str(r[1]),
                    "task_count": r[2],
                }
                for r in TaskRepository.technician_overload(db, schema)
            ],
        }
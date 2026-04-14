from app.repositories.employee_repo import EmployeeRepository

class EmployeeService:

    # KPI
    @staticmethod
    def get_kpi(db, schema):
        return {
            "total_active_workforce": EmployeeRepository.total_active(db, schema),
            "open_hiring_requisitions": EmployeeRepository.open_hiring(db, schema),
            "department_distribution": [
                {"department": r[0], "count": r[1]}
                for r in EmployeeRepository.dept_distribution(db, schema)
            ],
            "monthly_payroll": EmployeeRepository.payroll(db, schema)
        }

    # Charts
    @staticmethod
    def get_charts(db, schema):
        return {
            "hiring_pipeline": [
                {"status": r[0], "count": r[1]}
                for r in EmployeeRepository.hiring_pipeline(db, schema)
            ],
            "employment_type": [
                {"type": r[0], "count": r[1]}
                for r in EmployeeRepository.employment_type(db, schema)
            ],
            "onboarding_trend": [
                {"date": str(r[0]), "count": r[1]}
                for r in EmployeeRepository.onboarding_trend(db, schema)
            ]
        }

    # Tables
    @staticmethod
    def get_tables(db, schema):
        return {
            "critical_hiring": [
                {
                    "id": r[0],
                    "department": r[1],
                    "designation": r[2],
                    "positions": r[3],
                    "expected_joining": str(r[4])
                }
                for r in EmployeeRepository.critical_hiring(db, schema)
            ],
            "compensation_audit": [
                {
                    "emp_id": r[0],
                    "name": r[1],
                    "department": r[2],
                    "salary_type": r[3],
                    "basic": r[4],
                    "hra": r[5],
                    "deductions": r[6]
                }
                for r in EmployeeRepository.compensation_audit(db, schema)
            ]
        }

    # Alerts
    @staticmethod
    def get_alerts(db, schema):
        return {
            "delayed_joining": [
                {
                    "id": r[0],
                    "department": r[1],
                    "designation": r[2],
                    "expected_joining": str(r[3]),
                    "status": r[4]
                }
                for r in EmployeeRepository.delayed_joining(db, schema)
            ],
            "low_leave_balance": [
                {
                    "user_id": r[0],
                    "casual_leave": r[1],
                    "sick_leave": r[2]
                }
                for r in EmployeeRepository.low_leave(db, schema)
            ]
        }
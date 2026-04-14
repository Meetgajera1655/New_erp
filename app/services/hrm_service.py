from app.repositories.hrm_repository import HRMRepository

class HRMService:

    @staticmethod
    def get_kpi(db, schema):
        return {
            "total_employees": HRMRepository.total_employees(db, schema),
            "active_employees": HRMRepository.active_employees(db, schema),
            "inactive_employees": HRMRepository.inactive_employees(db, schema),
            "total_salary_paid": HRMRepository.total_salary_paid(db, schema),
            "total_leaves": HRMRepository.total_leaves(db, schema),
            "pending_salary": HRMRepository.pending_salary(db, schema)
        }

    @staticmethod
    def get_charts(db, schema):
        return {
        "employees_by_role": [
            {"role": r[0], "count": r[1]}
            for r in HRMRepository.employees_by_role(db, schema)
        ],
        "salary_trend": [
            {"year": r[0], "month": r[1], "salary": float(r[2])}
            for r in HRMRepository.salary_trend(db, schema)
        ],
        "employees_by_department_type": [
            {
                "department": r[0],
                "employment_type": r[1],
                "count": r[2]
            }
            for r in HRMRepository.employees_by_department_and_type(db, schema)
        ]

    }

    @staticmethod
    def get_tables(db, schema):
        return {
            "employee_list": [
                {
                    "employee_name": r[0],
                    "email": r[1],
                    "phone": r[2],
                    "role": r[3],
                    "status": r[4],
                    "created_at": r[5]
                }
                for r in HRMRepository.employee_list(db, schema)
            ],
            "salary_slips": [
                {
                    "employee_name": r[0],
                    "year": r[1],
                    "month": r[2],
                    "basic_salary": float(r[3]),
                    "net_salary": float(r[4]),
                    "payment_date": r[5]
                }
                for r in HRMRepository.salary_slip_list(db, schema)
            ]
        }

    @staticmethod
    def get_alerts(db, schema):
        return {
        "unpaid_salary": [
            {
                "employee": r[0],
                "year": r[1],
                "month": r[2],
                "salary": float(r[3])
            }
            for r in HRMRepository.unpaid_salary(db, schema)
        ],
        "high_leave": [
            {
                "employee": r[0],
                "leave_count": r[1]
            }
            for r in HRMRepository.high_leave(db, schema)
        ]
    }
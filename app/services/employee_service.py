import time
from sqlalchemy import text
from app.filters.employee_filter import apply_employee_filters

class EmployeeService:

    @staticmethod
    def get_kpi(db, schema, **kwargs):
        start_time = time.time()
        branch = kwargs.get("branch")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        period = kwargs.get("period")

        # 1. User Filter (date_of_joining)
        where_u, params_u = apply_employee_filters("u", "date_of_joining", branch, from_date, to_date, period, branch_alias="ub")
        
        # 2. Hiring Filter (expected_date_of_joining)
        where_hr, params_hr = apply_employee_filters("hr", "expected_date_of_joining", branch, from_date, to_date, period, branch_alias="ub")

        # OPTIMIZATION: Combine multiple KPI aggregations into single queries
        # a. Metrics from users table
        user_metrics = db.execute(text(f"""
            SELECT 
                COUNT(*) AS total_active,
                COALESCE(SUM(us.basic_salary), 0) AS payroll
            FROM "{schema}".users u
            LEFT JOIN "{schema}".user_branches ub ON u.id = ub.user_id
            LEFT JOIN "{schema}".user_salary_details us ON u.id = us.user_id
            WHERE u.is_active = TRUE AND {where_u}
        """), params_u).fetchone()

        # b. Metrics from hiring_requests table
        hiring_metrics = db.execute(text(f"""
            SELECT COUNT(*) 
            FROM "{schema}".hiring_requests hr
            LEFT JOIN "{schema}".user_branches ub ON hr.created_by::BIGINT = ub.user_id
            WHERE hr.status = 'PENDING' AND {where_hr}
        """), params_hr).scalar()

        # c. Department Distribution
        dept_dist = [
            {"department": r[0], "count": r[1]}
            for r in db.execute(text(f"""
                SELECT u.department, COUNT(*)
                FROM "{schema}".users u
                LEFT JOIN "{schema}".user_branches ub ON u.id = ub.user_id
                WHERE {where_u}
                GROUP BY u.department
            """), params_u).fetchall()
        ]

        print(f"Employee KPI Query Time: {time.time() - start_time:.4f}s")

        return {
            "total_active_workforce": user_metrics[0],
            "open_hiring_requisitions": hiring_metrics,
            "department_distribution": dept_dist,
            "monthly_payroll": float(user_metrics[1])
        }

    @staticmethod
    def get_charts(db, schema, **kwargs):
        start_time = time.time()
        branch = kwargs.get("branch")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        period = kwargs.get("period")

        where_u, params_u = apply_employee_filters("u", "date_of_joining", branch, from_date, to_date, period, branch_alias="ub")
        where_hr, params_hr = apply_employee_filters("hr", "expected_date_of_joining", branch, from_date, to_date, period, branch_alias="ub")

        results = {
            "hiring_pipeline": [
                {"status": r[0], "count": r[1]}
                for r in db.execute(text(f"""
                    SELECT hr.status, COUNT(*)
                    FROM "{schema}".hiring_requests hr
                    LEFT JOIN "{schema}".user_branches ub ON hr.created_by::BIGINT = ub.user_id
                    WHERE {where_hr}
                    GROUP BY hr.status
                """), params_hr).fetchall()
            ],
            "employment_type": [
                {"type": r[0], "count": r[1]}
                for r in db.execute(text(f"""
                    SELECT u.employment_type, COUNT(*)
                    FROM "{schema}".users u
                    LEFT JOIN "{schema}".user_branches ub ON u.id = ub.user_id
                    WHERE {where_u}
                    GROUP BY u.employment_type
                """), params_u).fetchall()
            ],
            "onboarding_trend": [
                {"date": str(r[0]), "count": r[1]}
                for r in db.execute(text(f"""
                    SELECT DATE(u.date_of_joining), COUNT(*)
                    FROM "{schema}".users u
                    LEFT JOIN "{schema}".user_branches ub ON u.id = ub.user_id
                    WHERE {where_u}
                    GROUP BY DATE(u.date_of_joining)
                    ORDER BY DATE(u.date_of_joining)
                """), params_u).fetchall()
            ]
        }

        print(f"Employee Charts Query Time: {time.time() - start_time:.4f}s")
        return results

    @staticmethod
    def get_tables(db, schema, **kwargs):
        start_time = time.time()
        branch = kwargs.get("branch")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        period = kwargs.get("period")

        where_u, params_u = apply_employee_filters("u", "date_of_joining", branch, from_date, to_date, period, branch_alias="ub")
        where_hr, params_hr = apply_employee_filters("hr", "expected_date_of_joining", branch, from_date, to_date, period, branch_alias="ub")

        results = {
            "critical_hiring": [
                {
                    "id": r[0], "department": r[1], "designation": r[2],
                    "positions": r[3], "expected_joining": str(r[4])
                }
                for r in db.execute(text(f"""
                    SELECT hr.id, hr.department, hr.designation, hr.number_of_positions, hr.expected_date_of_joining
                    FROM "{schema}".hiring_requests hr
                    LEFT JOIN "{schema}".user_branches ub ON hr.created_by::BIGINT = ub.user_id
                    WHERE hr.number_of_positions > 5 AND {where_hr}
                    LIMIT 20
                """), params_hr).fetchall()
            ],
            "compensation_audit": [
                {
                    "emp_id": r[0], "name": r[1], "department": r[2],
                    "salary_type": r[3], "basic": float(r[4] or 0), "hra": float(r[5] or 0), "deductions": float(r[6] or 0)
                }
                for r in db.execute(text(f"""
                    SELECT u.emp_id, u.first_name, u.department, s.salary_type, s.basic_salary, s.hra, s.deductions
                    FROM "{schema}".users u
                    LEFT JOIN "{schema}".user_branches ub ON u.id = ub.user_id
                    JOIN "{schema}".user_salary_details s ON u.id = s.user_id
                    WHERE {where_u}
                    LIMIT 20
                """), params_u).fetchall()
            ]
        }

        print(f"Employee Tables Query Time: {time.time() - start_time:.4f}s")
        return results

    @staticmethod
    def get_alerts(db, schema, **kwargs):
        start_time = time.time()
        branch = kwargs.get("branch")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        period = kwargs.get("period")

        where_u, params_u = apply_employee_filters("u", "date_of_joining", branch, from_date, to_date, period, branch_alias="ub")
        where_hr, params_hr = apply_employee_filters("hr", "expected_date_of_joining", branch, from_date, to_date, period, branch_alias="ub")

        results = {
            "delayed_joining": [
                {
                    "id": r[0], "department": r[1], "designation": r[2],
                    "expected_joining": str(r[3]), "status": r[4]
                }
                for r in db.execute(text(f"""
                    SELECT hr.id, hr.department, hr.designation, hr.expected_date_of_joining, hr.status
                    FROM "{schema}".hiring_requests hr
                    LEFT JOIN "{schema}".user_branches ub ON hr.created_by::BIGINT = ub.user_id
                    WHERE hr.expected_date_of_joining < CURRENT_DATE AND hr.status != 'CONVERTED' AND {where_hr}
                """), params_hr).fetchall()
            ],
            "low_leave_balance": [
                {"user_id": r[0], "casual_leave": r[1], "sick_leave": r[2]}
                for r in db.execute(text(f"""
                    SELECT l.user_id, l.casual_leave, l.sick_leave
                    FROM "{schema}".user_leave_details l
                    JOIN "{schema}".users u ON l.user_id = u.id
                    LEFT JOIN "{schema}".user_branches ub ON u.id = ub.user_id
                    WHERE (l.casual_leave <= 1 OR l.sick_leave <= 1) AND {where_u}
                """), params_u).fetchall()
            ]
        }

        print(f"Employee Alerts Query Time: {time.time() - start_time:.4f}s")
        return results
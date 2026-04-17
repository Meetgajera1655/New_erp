import time
from sqlalchemy import text
from app.filters.hrm_filter import apply_hrm_filters

class HRMService:

    @staticmethod
    def get_kpi(db, schema, **kwargs):
        start_time = time.time()
        branch = kwargs.get("branch")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        period = kwargs.get("period")

        # 1. Filters (Using junction joins as standard)
        where_u, params_u = apply_hrm_filters("u", "date_of_joining", branch, from_date, to_date, period, branch_alias="ub")
        where_ss, params_ss = apply_hrm_filters("ss", "created_at", branch, from_date, to_date, period, branch_alias="ub")
        where_h, params_h = apply_hrm_filters("h", "holiday_date", branch, from_date, to_date, period, branch_alias="ub")

        # OPTIMIZATION: Combine multiple KPI aggregations into single queries
        # a. Metrics from users table (Total, Active, Inactive)
        user_metrics = db.execute(text(f"""
            SELECT 
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE u.status = 'ACTIVE') AS active,
                COUNT(*) FILTER (WHERE u.status = 'INACTIVE') AS inactive
            FROM "{schema}".users u
            LEFT JOIN "{schema}".user_branches ub ON u.id = ub.user_id 
            WHERE 1=1 AND {where_u}
        """), params_u).fetchone()

        # b. Metrics from salary table (Paid, Pending)
        salary_metrics = db.execute(text(f"""
            SELECT 
                COALESCE(SUM(ss.net_salary), 0) AS total_paid,
                COUNT(*) FILTER (WHERE ss.payment_status = 'UNPAID') AS pending_count
            FROM "{schema}".hrm_salary_month ss 
            LEFT JOIN "{schema}".user_branches ub ON ss.user_id = ub.user_id 
            WHERE 1=1 AND {where_ss}
        """), params_ss).fetchone()

        # c. Metrics from holidays table (Leaves)
        leaves_count = db.execute(text(f"""
            SELECT COUNT(h.id) FROM "{schema}".hrm_holidays h 
            LEFT JOIN "{schema}".user_branches ub ON h.created_by::BIGINT = ub.user_id 
            WHERE 1=1 AND {where_h}
        """), params_h).scalar()

        print(f"HRM KPI Query Time: {time.time() - start_time:.4f}s")

        return {
            "total_employees": user_metrics[0],
            "active_employees": user_metrics[1],
            "inactive_employees": user_metrics[2],
            "total_salary_paid": float(salary_metrics[0]),
            "total_leaves": leaves_count,
            "pending_salary": salary_metrics[1]
        }

    @staticmethod
    def get_charts(db, schema, **kwargs):
        start_time = time.time()
        branch = kwargs.get("branch")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        period = kwargs.get("period")

        where_u, params_u = apply_hrm_filters("u", "date_of_joining", branch, from_date, to_date, period, branch_alias="ub")
        where_ss, params_ss = apply_hrm_filters("ss", "created_at", branch, from_date, to_date, period, branch_alias="ub")

        results = {
            "employees_by_role": [
                {"role": r[0], "count": r[1]}
                for r in db.execute(text(f"""
                    SELECT r.name, COUNT(u.id)
                    FROM "{schema}".users u
                    LEFT JOIN "{schema}".user_branches ub ON u.id = ub.user_id
                    JOIN "{schema}".roles r ON u.role_id = r.id
                    WHERE {where_u}
                    GROUP BY r.name
                """), params_u).fetchall()
            ],
            "salary_trend": [
                {"year": r[0], "month": r[1], "salary": float(r[2])}
                for r in db.execute(text(f"""
                    SELECT ss.salary_year, ss.salary_month, SUM(ss.net_salary)
                    FROM "{schema}".hrm_salary_month ss
                    LEFT JOIN "{schema}".user_branches ub ON ss.user_id = ub.user_id
                    WHERE {where_ss}
                    GROUP BY ss.salary_year, ss.salary_month
                    ORDER BY ss.salary_year, ss.salary_month
                """), params_ss).fetchall()
            ],
            "employees_by_department_type": [
                {"department": r[0], "employment_type": r[1], "count": r[2]}
                for r in db.execute(text(f"""
                    SELECT u.department, u.employment_type, COUNT(u.id)
                    FROM "{schema}".users u
                    LEFT JOIN "{schema}".user_branches ub ON u.id = ub.user_id
                    WHERE {where_u}
                    GROUP BY u.department, u.employment_type
                    ORDER BY u.department
                """), params_u).fetchall()
            ]
        }

        print(f"HRM Charts Query Time: {time.time() - start_time:.4f}s")
        return results

    @staticmethod
    def get_tables(db, schema, **kwargs):
        start_time = time.time()
        branch = kwargs.get("branch")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        period = kwargs.get("period")

        where_u, params_u = apply_hrm_filters("u", "date_of_joining", branch, from_date, to_date, period, branch_alias="ub")
        where_ss, params_ss = apply_hrm_filters("ss", "created_at", branch, from_date, to_date, period, branch_alias="ub")

        results = {
            "employee_list": [
                {
                    "employee_name": f"{r[0]} {r[1]}", "email": r[2], "phone": r[3],
                    "role": r[4], "status": r[5], "created_at": r[6]
                }
                for r in db.execute(text(f"""
                    SELECT u.first_name, u.last_name, u.email, u.contact_number, r.name, u.status, u.created_at
                    FROM "{schema}".users u
                    LEFT JOIN "{schema}".user_branches ub ON u.id = ub.user_id
                    JOIN "{schema}".roles r ON u.role_id = r.id
                    WHERE {where_u}
                    ORDER BY u.created_at DESC
                    LIMIT 20
                """), params_u).fetchall()
            ],
            "salary_slips": [
                {
                    "employee_name": f"{r[0]} {r[1]}", "year": r[2], "month": r[3],
                    "basic_salary": float(r[4]), "net_salary": float(r[5]), "payment_date": r[6]
                }
                for r in db.execute(text(f"""
                    SELECT u.first_name, u.last_name, ss.salary_year, ss.salary_month, ss.basic_salary, ss.net_salary, ss.payment_date
                    FROM "{schema}".hrm_salary_month ss
                    LEFT JOIN "{schema}".user_branches ub ON ss.user_id = ub.user_id
                    JOIN "{schema}".users u ON ss.user_id = u.id
                    JOIN "{schema}".hrm_salary_slip s ON s.salary_month_id = ss.id
                    WHERE {where_ss}
                    ORDER BY ss.salary_year DESC, ss.salary_month DESC
                    LIMIT 20
                """), params_ss).fetchall()
            ]
        }

        print(f"HRM Tables Query Time: {time.time() - start_time:.4f}s")
        return results

    @staticmethod
    def get_alerts(db, schema, **kwargs):
        start_time = time.time()
        branch = kwargs.get("branch")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        period = kwargs.get("period")

        where_ss, params_ss = apply_hrm_filters("ss", "created_at", branch, from_date, to_date, period, branch_alias="ub")
        where_h, params_h = apply_hrm_filters("h", "holiday_date", branch, from_date, to_date, period, branch_alias="ub")

        results = {
            "unpaid_salary": [
                {"employee": f"{r[0]} {r[1]}", "year": r[2], "month": r[3], "salary": float(r[4])}
                for r in db.execute(text(f"""
                    SELECT u.first_name, u.last_name, ss.salary_year, ss.salary_month, ss.net_salary
                    FROM "{schema}".hrm_salary_month ss
                    LEFT JOIN "{schema}".user_branches ub ON ss.user_id = ub.user_id
                    JOIN "{schema}".users u ON ss.user_id = u.id
                    WHERE ss.payment_status = 'UNPAID' AND {where_ss}
                """), params_ss).fetchall()
            ],
            "high_leave": [
                {"employee": f"{r[0]} {r[1]}", "leave_count": r[2]}
                for r in db.execute(text(f"""
                    SELECT u.first_name, u.last_name, COUNT(h.id)
                    FROM "{schema}".hrm_holidays h
                    LEFT JOIN "{schema}".users _u_filter ON h.created_by = _u_filter.email
                    LEFT JOIN "{schema}".user_branches ub ON _u_filter.id = ub.user_id
                    JOIN "{schema}".users u ON u.id = _u_filter.id
                    WHERE {where_h}
                    GROUP BY u.first_name, u.last_name
                    HAVING COUNT(h.id) > 5
                """), params_h).fetchall()
            ]
        }

        print(f"HRM Alerts Query Time: {time.time() - start_time:.4f}s")
        return results
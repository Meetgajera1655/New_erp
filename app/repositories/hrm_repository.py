from sqlalchemy import text

class HRMRepository:

    # ================= KPI =================

    @staticmethod
    def total_employees(db, schema):
        return db.execute(text(f"SELECT COUNT(*) FROM {schema}.users")).scalar()

    @staticmethod
    def active_employees(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(*) FROM {schema}.users
            WHERE status = 'ACTIVE'
        """)).scalar()

    @staticmethod
    def inactive_employees(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(*) FROM {schema}.users
            WHERE status = 'INACTIVE'
        """)).scalar()

    @staticmethod
    def total_salary_paid(db, schema):
        return db.execute(text(f"""
            SELECT COALESCE(SUM(net_salary),0)
            FROM {schema}.hrm_salary_month
            WHERE salary_year = EXTRACT(YEAR FROM CURRENT_DATE)
            AND salary_month = EXTRACT(MONTH FROM CURRENT_DATE)
        """)).scalar()

    @staticmethod
    def total_leaves(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(*) FROM {schema}.hrm_holidays
        """)).scalar()

    @staticmethod
    def pending_salary(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(*) FROM {schema}.hrm_salary_month
            WHERE payment_status = 'UNPAID'
        """)).scalar()

    # ================= CHARTS =================

    @staticmethod
    def employees_by_role(db, schema):
        return db.execute(text(f"""
            SELECT r.name, COUNT(u.id)
            FROM {schema}.users u
            JOIN {schema}.roles r ON u.role_id = r.id
            GROUP BY r.name
        """)).fetchall()

    @staticmethod
    def salary_trend(db, schema):
        return db.execute(text(f"""
            SELECT salary_year, salary_month, SUM(net_salary)
            FROM {schema}.hrm_salary_month
            GROUP BY salary_year, salary_month
            ORDER BY salary_year, salary_month
        """)).fetchall()

    @staticmethod
    def employees_by_department_and_type(db, schema):
        return db.execute(text(f"""
            SELECT 
                department,
                employment_type,
                COUNT(id) AS total_employees
            FROM {schema}.users
            WHERE is_active = true
            GROUP BY department, employment_type
            ORDER BY department, total_employees DESC
        """)).fetchall()
    # ================= TABLES =================

    @staticmethod
    def employee_list(db, schema):
        return db.execute(text(f"""
            SELECT 
                u.first_name || ' ' || u.last_name,
                u.email,
                u.contact_number,
                r.name,
                u.status,
                u.created_at
            FROM {schema}.users u
            JOIN {schema}.roles r ON u.role_id = r.id
            ORDER BY u.created_at DESC
        """)).fetchall()

    @staticmethod
    def salary_slip_list(db, schema):
        return db.execute(text(f"""
            SELECT 
                u.first_name || ' ' || u.last_name,
                sm.salary_year,
                sm.salary_month,
                sm.basic_salary,
                sm.net_salary,
                sm.payment_date
            FROM {schema}.hrm_salary_slip s
            JOIN {schema}.hrm_salary_month sm ON s.salary_month_id = sm.id
            JOIN {schema}.users u ON sm.user_id = u.id
            ORDER BY sm.salary_year DESC, sm.salary_month DESC
        """)).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def unpaid_salary(db, schema):
        return db.execute(text(f"""
            SELECT 
                u.first_name || ' ' || u.last_name,
                sm.salary_year,
                sm.salary_month,
                sm.net_salary
            FROM {schema}.hrm_salary_month sm
            JOIN {schema}.users u ON sm.user_id = u.id
            WHERE sm.payment_status = 'UNPAID'
        """)).fetchall()
    
    @staticmethod
    def high_leave(db, schema):
        return db.execute(text(f"""
        SELECT 
            u.first_name || ' ' || u.last_name,
            COUNT(h.id)
        FROM {schema}.hrm_holidays h
        JOIN {schema}.users u ON u.id = h.created_by::BIGINT
        GROUP BY u.first_name, u.last_name
        HAVING COUNT(h.id) > 5
    """)).fetchall()
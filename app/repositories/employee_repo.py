from sqlalchemy import text

class EmployeeRepository:

    # ================= KPI =================

    @staticmethod
    def total_active(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM "{schema}".users
            WHERE is_active = TRUE
        """)).scalar()

    @staticmethod
    def open_hiring(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM "{schema}".hiring_requests
            WHERE status = 'PENDING'
        """)).scalar()

    @staticmethod
    def dept_distribution(db, schema):
        return db.execute(text(f"""
            SELECT department, COUNT(id)
            FROM "{schema}".users
            GROUP BY department
        """)).fetchall()

    @staticmethod
    def payroll(db, schema):
        return db.execute(text(f"""
            SELECT COALESCE(SUM(basic_salary),0)
            FROM "{schema}".user_salary_details
            WHERE user_id IN (
                SELECT id FROM "{schema}".users WHERE is_active = TRUE
            )
        """)).scalar()

    # ================= CHARTS =================

    @staticmethod
    def hiring_pipeline(db, schema):
        return db.execute(text(f"""
            SELECT status, COUNT(id)
            FROM "{schema}".hiring_requests
            GROUP BY status
        """)).fetchall()

    @staticmethod
    def employment_type(db, schema):
        return db.execute(text(f"""
            SELECT employment_type, COUNT(id)
            FROM "{schema}".users
            GROUP BY employment_type
        """)).fetchall()

    @staticmethod
    def onboarding_trend(db, schema):
        return db.execute(text(f"""
            SELECT DATE(date_of_joining), COUNT(id)
            FROM "{schema}".users
            GROUP BY DATE(date_of_joining)
            ORDER BY DATE(date_of_joining)
        """)).fetchall()

    # ================= TABLES =================

    @staticmethod
    def critical_hiring(db, schema):
        return db.execute(text(f"""
            SELECT id, department, designation,
                   number_of_positions, expected_date_of_joining
            FROM "{schema}".hiring_requests
            WHERE number_of_positions > 5
        """)).fetchall()

    @staticmethod
    def compensation_audit(db, schema):
        return db.execute(text(f"""
            SELECT u.emp_id, u.first_name, u.department,
                   s.salary_type, s.basic_salary,
                   s.hra, s.deductions
            FROM "{schema}".users u
            JOIN "{schema}".user_salary_details s
            ON u.id = s.user_id
        """)).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def delayed_joining(db, schema):
        return db.execute(text(f"""
            SELECT id, department, designation,
                   expected_date_of_joining, status
            FROM "{schema}".hiring_requests
            WHERE expected_date_of_joining < CURRENT_DATE
            AND status != 'CONVERTED'
        """)).fetchall()

    @staticmethod
    def low_leave(db, schema):
        return db.execute(text(f"""
            SELECT user_id, casual_leave, sick_leave
            FROM "{schema}".user_leave_details
            WHERE casual_leave <= 1
            OR sick_leave <= 1
        """)).fetchall()
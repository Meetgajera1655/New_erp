from sqlalchemy import text
from app.filters.employee_filter import apply_employee_filters

class EmployeeRepository:

    # ================= KPI =================

    @staticmethod
    def total_active(db, schema, **kwargs):
        # 1. Generate filter using business date column (date_of_joining)
        where_u, params = apply_employee_filters(alias="u", date_column="date_of_joining", **kwargs)
        
        query_sql = f"""
            SELECT COUNT(u.id)
            FROM "{schema}".users u
            WHERE u.is_active = TRUE
            AND {where_u}
        """
        print("USER WHERE:", where_u)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def open_hiring(db, schema, **kwargs):
        # 1. Generate filter using business date column (expected_date_of_joining)
        where_hr, params = apply_employee_filters(alias="hr", date_column="expected_date_of_joining", **kwargs)
        
        query_sql = f"""
            SELECT COUNT(hr.id)
            FROM "{schema}".hiring_requests hr
            WHERE hr.status = 'PENDING'
            AND {where_hr}
        """
        print("HIRING WHERE:", where_hr)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def dept_distribution(db, schema, **kwargs):
        where_u, params = apply_employee_filters(alias="u", date_column="date_of_joining", **kwargs)
        query_sql = f"""
            SELECT u.department, COUNT(u.id)
            FROM "{schema}".users u
            WHERE {where_u}
            GROUP BY u.department
        """
        print("USER WHERE:", where_u)
        print("PARAMS:", params)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def payroll(db, schema, **kwargs):
        where_u, params = apply_employee_filters(alias="u", date_column="date_of_joining", **kwargs)
        query_sql = f"""
            SELECT COALESCE(SUM(us.basic_salary), 0)
            FROM "{schema}".user_salary_details us
            JOIN "{schema}".users u ON us.user_id = u.id
            WHERE u.is_active = TRUE
            AND {where_u}
        """
        print("USER WHERE:", where_u)
        print("PARAMS:", params)
        return db.execute(text(query_sql), params).scalar()

    # ================= CHARTS =================

    @staticmethod
    def hiring_pipeline(db, schema, **kwargs):
        where_hr, params = apply_employee_filters(alias="hr", date_column="expected_date_of_joining", **kwargs)
        query_sql = f"""
            SELECT hr.status, COUNT(hr.id)
            FROM "{schema}".hiring_requests hr
            WHERE {where_hr}
            GROUP BY hr.status
        """
        print("HIRING WHERE:", where_hr)
        print("PARAMS:", params)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def employment_type(db, schema, **kwargs):
        where_u, params = apply_employee_filters(alias="u", date_column="date_of_joining", **kwargs)
        query_sql = f"""
            SELECT u.employment_type, COUNT(u.id)
            FROM "{schema}".users u
            WHERE {where_u}
            GROUP BY u.employment_type
        """
        print("USER WHERE:", where_u)
        print("PARAMS:", params)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def onboarding_trend(db, schema, **kwargs):
        where_u, params = apply_employee_filters(alias="u", date_column="date_of_joining", **kwargs)
        query_sql = f"""
            SELECT DATE(u.date_of_joining), COUNT(u.id)
            FROM "{schema}".users u
            WHERE {where_u}
            GROUP BY DATE(u.date_of_joining)
            ORDER BY DATE(u.date_of_joining)
        """
        print("USER WHERE:", where_u)
        print("PARAMS:", params)
        return db.execute(text(query_sql), params).fetchall()

    # ================= TABLES =================

    @staticmethod
    def critical_hiring(db, schema, **kwargs):
        where_hr, params = apply_employee_filters(alias="hr", date_column="expected_date_of_joining", **kwargs)
        query_sql = f"""
            SELECT hr.id, hr.department, hr.designation,
                   hr.number_of_positions, hr.expected_date_of_joining
            FROM "{schema}".hiring_requests hr
            WHERE hr.number_of_positions > 5
            AND {where_hr}
        """
        print("HIRING WHERE:", where_hr)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def compensation_audit(db, schema, **kwargs):
        where_u, params = apply_employee_filters(alias="u", date_column="date_of_joining", **kwargs)
        query_sql = f"""
            SELECT u.emp_id, u.first_name, u.department,
                   s.salary_type, s.basic_salary,
                   s.hra, s.deductions
            FROM "{schema}".users u
            JOIN "{schema}".user_salary_details s
            ON u.id = s.user_id
            WHERE {where_u}
        """
        print("USER WHERE:", where_u)
        return db.execute(text(query_sql), params).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def delayed_joining(db, schema, **kwargs):
        where_hr, params = apply_employee_filters(alias="hr", date_column="expected_date_of_joining", **kwargs)
        query_sql = f"""
            SELECT hr.id, hr.department, hr.designation,
                   hr.expected_date_of_joining, hr.status
            FROM "{schema}".hiring_requests hr
            WHERE hr.expected_date_of_joining < CURRENT_DATE
            AND hr.status != 'CONVERTED'
            AND {where_hr}
        """
        print("HIRING WHERE:", where_hr)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def low_leave(db, schema, **kwargs):
        where_u, params = apply_employee_filters(alias="u", date_column="date_of_joining", **kwargs)
        query_sql = f"""
            SELECT l.user_id, l.casual_leave, l.sick_leave
            FROM "{schema}".user_leave_details l
            JOIN "{schema}".users u ON l.user_id = u.id
            WHERE (l.casual_leave <= 1 OR l.sick_leave <= 1)
            AND {where_u}
        """
        print("USER WHERE:", where_u)
        return db.execute(text(query_sql), params).fetchall()
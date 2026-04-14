from sqlalchemy import text

class BranchRepository:

    @staticmethod
    def count_active_branches(db, schema):
        query = text(f"""
            SELECT COUNT(id)
            FROM "{schema}".branches
            WHERE LOWER(status) = 'active'
        """)
        return db.execute(query).scalar()

    @staticmethod
    def branch_type_distribution(db, schema):
        query = text(f"""
            SELECT branch_type, COUNT(id)
            FROM "{schema}".branches
            GROUP BY branch_type
        """)
        return db.execute(query).fetchall()

    @staticmethod
    def branch_density(db, schema):
        query = text(f"""
            SELECT state, COUNT(id)
            FROM "{schema}".branches
            GROUP BY state
        """)
        return db.execute(query).fetchall()

    @staticmethod
    def employee_branch_ratio(db, schema):
        query = text(f"""
            SELECT 
                (SELECT COUNT(id) FROM "{schema}".users)::float /
                NULLIF((SELECT COUNT(id) FROM "{schema}".branches), 0)
        """)
        return db.execute(query).scalar()

   
    @staticmethod
    def branch_growth_trend(db, schema):
        query = text(f"""
            SELECT DATE_TRUNC('month', created_at), COUNT(id)
            FROM "{schema}".branches
            GROUP BY DATE_TRUNC('month', created_at)
            ORDER BY DATE_TRUNC('month', created_at)
        """)
        return db.execute(query).fetchall()

 
    @staticmethod
    def status_breakdown(db, schema):
        query = text(f"""
            SELECT status, COUNT(id)
            FROM "{schema}".branches
            GROUP BY status
        """)
        return db.execute(query).fetchall()
    
    @staticmethod
    def branch_directory(db, schema):
        query = text(f"""
        SELECT branch_name, branch_code, city, email, phone_number
        FROM "{schema}".branches
    """)
        return db.execute(query).fetchall()


    @staticmethod
    def recent_branch_activations(db, schema):
        query = text(f"""
        SELECT branch_name, branch_code, branch_type, created_at, created_by
        FROM "{schema}".branches
        ORDER BY created_at DESC
        LIMIT 10
    """)
        return db.execute(query).fetchall()
    

    @staticmethod
    def inactive_branches(db, schema):
        query = text(f"""
        SELECT branch_name, branch_code, status
        FROM "{schema}".branches
        WHERE LOWER(status) = 'inactive'
    """)
        return db.execute(query).fetchall()


    @staticmethod
    def invalid_branch_codes(db, schema):
        query = text(f"""
        SELECT branch_name, branch_code
        FROM "{schema}".branches
        WHERE LENGTH(branch_code) != 3
           OR branch_code !~ '^[A-Z0-9]{3}$'
    """)
        return db.execute(query).fetchall()
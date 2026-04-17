from sqlalchemy import text
from app.filters.branch_management_filter import apply_branch_management_filters

class BranchRepository:

    @staticmethod
    def count_active_branches(db, schema, **kwargs):
        where_clause, params = apply_branch_management_filters(schema=schema, alias="b", **kwargs)
        query_sql = f"""
            SELECT COUNT(b.id)
            FROM "{schema}".branches b
            WHERE LOWER(b.status) = 'active'
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def branch_type_distribution(db, schema, **kwargs):
        where_clause, params = apply_branch_management_filters(schema=schema, alias="b", **kwargs)
        query_sql = f"""
            SELECT b.branch_type, COUNT(b.id)
            FROM "{schema}".branches b
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY b.branch_type"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def branch_density(db, schema, **kwargs):
        where_clause, params = apply_branch_management_filters(schema=schema, alias="b", **kwargs)
        query_sql = f"""
            SELECT b.state, COUNT(b.id)
            FROM "{schema}".branches b
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY b.state"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def employee_branch_ratio(db, schema, **kwargs):
        where_clause, params = apply_branch_management_filters(schema=schema, alias="b", **kwargs)
        branches_query = f'SELECT COUNT(b.id) FROM "{schema}".branches b'
        if where_clause and where_clause != "1=1":
            branches_query += f" WHERE {where_clause}"
        
        query_sql = f"""
            SELECT 
                (SELECT COUNT(id) FROM "{schema}".users)::float /
                NULLIF(({branches_query}), 0)
        """
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).scalar()

   
    @staticmethod
    def branch_growth_trend(db, schema, **kwargs):
        where_clause, params = apply_branch_management_filters(schema=schema, alias="b", **kwargs)
        query_sql = f"""
            SELECT DATE_TRUNC('month', b.created_at), COUNT(b.id)
            FROM "{schema}".branches b
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY DATE_TRUNC('month', b.created_at) ORDER BY DATE_TRUNC('month', b.created_at)"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()

 
    @staticmethod
    def status_breakdown(db, schema, **kwargs):
        where_clause, params = apply_branch_management_filters(schema=schema, alias="b", **kwargs)
        query_sql = f"""
            SELECT b.status, COUNT(b.id)
            FROM "{schema}".branches b
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY b.status"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()
    
    @staticmethod
    def branch_directory(db, schema, **kwargs):
        where_clause, params = apply_branch_management_filters(schema=schema, alias="b", **kwargs)
        query_sql = f"""
            SELECT b.branch_name, b.branch_code, b.city, b.email, b.phone_number
            FROM "{schema}".branches b
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()


    @staticmethod
    def recent_branch_activations(db, schema, **kwargs):
        where_clause, params = apply_branch_management_filters(schema=schema, alias="b", **kwargs)
        query_sql = f"""
            SELECT b.branch_name, b.branch_code, b.branch_type, b.created_at, b.created_by
            FROM "{schema}".branches b
            WHERE 1=1
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        query_sql += " ORDER BY b.created_at DESC LIMIT 10"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()
    

    @staticmethod
    def inactive_branches(db, schema, **kwargs):
        where_clause, params = apply_branch_management_filters(schema=schema, alias="b", **kwargs)
        query_sql = f"""
            SELECT b.branch_name, b.branch_code, b.status
            FROM "{schema}".branches b
            WHERE LOWER(b.status) = 'inactive'
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()


    @staticmethod
    def invalid_branch_codes(db, schema, **kwargs):
        where_clause, params = apply_branch_management_filters(schema=schema, alias="b", **kwargs)
        query_sql = f"""
            SELECT b.branch_name, b.branch_code
            FROM "{schema}".branches b
            WHERE (LENGTH(b.branch_code) != 3 OR b.branch_code !~ '^[A-Z0-9]{{3}}$')
        """
        if where_clause and where_clause != "1=1":
            query_sql += f" AND {where_clause}"
        print(f"WHERE: {where_clause}\\nPARAMS: {params}\\nFINAL QUERY: {query_sql}")
        return db.execute(text(query_sql), params).fetchall()
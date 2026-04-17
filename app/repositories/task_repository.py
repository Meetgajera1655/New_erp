from sqlalchemy import text
from app.filters.task_filter import apply_task_filters

class TaskRepository:

    # ================= KPI =================

    @staticmethod
    def total_tasks(db, schema, **kwargs):
        where_clause, params = apply_task_filters(alias="t", **kwargs)
        query_sql = f"""
            SELECT COUNT(t.id) FROM {schema}.tasks t
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def completed_tasks(db, schema, **kwargs):
        where_clause, params = apply_task_filters(alias="t", **kwargs)
        query_sql = f"""
            SELECT COUNT(t.id)
            FROM {schema}.tasks t
            WHERE t.status = 'COMPLETED'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def pending_tasks(db, schema, **kwargs):
        where_clause, params = apply_task_filters(alias="t", **kwargs)
        query_sql = f"""
            SELECT COUNT(t.id)
            FROM {schema}.tasks t
            WHERE t.status = 'PENDING'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    @staticmethod
    def overdue_tasks(db, schema, **kwargs):
        where_clause, params = apply_task_filters(alias="t", **kwargs)
        query_sql = f"""
            SELECT COUNT(t.id)
            FROM {schema}.tasks t
            WHERE t.scheduled_date < CURRENT_DATE
            AND t.status != 'COMPLETED'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).scalar()

    # ================= CHARTS =================

    @staticmethod
    def status_chart(db, schema, **kwargs):
        where_clause, params = apply_task_filters(alias="t", **kwargs)
        query_sql = f"""
            SELECT t.status, COUNT(t.id)
            FROM {schema}.tasks t
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY t.status"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def monthly_trend(db, schema, **kwargs):
        where_clause, params = apply_task_filters(alias="t", **kwargs)
        query_sql = f"""
            SELECT DATE_TRUNC('month', t.created_at), COUNT(t.id)
            FROM {schema}.tasks t
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY 1 ORDER BY 1"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def technician_workload(db, schema, **kwargs):
        where_clause, params = apply_task_filters(alias="t", **kwargs)
        query_sql = f"""
            SELECT u.first_name, COUNT(t.id)
            FROM {schema}.tasks t
            JOIN {schema}.users u
            ON t.created_by = u.id::varchar
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY u.first_name ORDER BY COUNT(t.id) DESC"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    # ================= TABLES =================

    @staticmethod
    def recent_tasks(db, schema, **kwargs):
        where_clause, params = apply_task_filters(alias="t", **kwargs)
        query_sql = f"""
            SELECT 
                t.task_number,
                c.full_name,
                s.so_number,
                t.service_type_name,
                t.scheduled_date,
                t.start_time,
                t.end_time,
                t.status
            FROM {schema}.tasks t
            JOIN {schema}.customers c ON t.customer_id = c.id
            LEFT JOIN {schema}.sales_orders s ON t.sales_order_id = s.id
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " ORDER BY t.created_at DESC LIMIT 20"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def material_usage(db, schema, **kwargs):
        where_clause, params = apply_task_filters(alias="t", **kwargs)
        query_sql = f"""
            SELECT 
                t.task_number,
                p.product_name,
                m.uom,
                m.required_qty,
                m.used_qty,
                t.scheduled_date
            FROM {schema}.task_materials m
            JOIN {schema}.tasks t ON m.task_id = t.id
            JOIN {schema}.inventory_products p ON m.product_id = p.id
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def overdue_alert(db, schema, **kwargs):
        where_clause, params = apply_task_filters(alias="t", **kwargs)
        query_sql = f"""
            SELECT 
                t.task_number,
                t.customer_id,
                t.service_type_name,
                t.scheduled_date,
                t.status
            FROM {schema}.tasks t
            WHERE t.scheduled_date < CURRENT_DATE
            AND t.status != 'COMPLETED'
        """
        if where_clause:
            query_sql += f" AND {where_clause}"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()

    @staticmethod
    def technician_overload(db, schema, **kwargs):
        where_clause, params = apply_task_filters(alias="t", **kwargs)
        query_sql = f"""
            SELECT 
                u.first_name,
                t.scheduled_date,
                COUNT(t.id)
            FROM {schema}.tasks t
            JOIN {schema}.users u
            ON t.created_by = u.id::varchar
            WHERE 1=1
        """
        if where_clause:
            query_sql += f" AND {where_clause}"
        query_sql += " GROUP BY u.first_name, t.scheduled_date HAVING COUNT(t.id) > 5"

        print("WHERE:", where_clause)
        print("PARAMS:", params)
        print("QUERY:", query_sql)
        return db.execute(text(query_sql), params).fetchall()
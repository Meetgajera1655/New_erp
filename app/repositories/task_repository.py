from sqlalchemy import text

class TaskRepository:

    # ================= KPI =================

    @staticmethod
    def total_tasks(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id) FROM {schema}.tasks
        """)).scalar()

    @staticmethod
    def completed_tasks(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM {schema}.tasks
            WHERE status = 'COMPLETED'
        """)).scalar()

    @staticmethod
    def pending_tasks(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM {schema}.tasks
            WHERE status = 'PENDING'
        """)).scalar()

    @staticmethod
    def overdue_tasks(db, schema):
        return db.execute(text(f"""
            SELECT COUNT(id)
            FROM {schema}.tasks
            WHERE scheduled_date < CURRENT_DATE
            AND status != 'COMPLETED'
        """)).scalar()

    # ================= CHARTS =================

    @staticmethod
    def status_chart(db, schema):
        return db.execute(text(f"""
            SELECT status, COUNT(id)
            FROM {schema}.tasks
            GROUP BY status
        """)).fetchall()

    @staticmethod
    def monthly_trend(db, schema):
        return db.execute(text(f"""
            SELECT DATE_TRUNC('month', created_at), COUNT(id)
            FROM {schema}.tasks
            GROUP BY 1
            ORDER BY 1
        """)).fetchall()

    @staticmethod
    def technician_workload(db, schema):
        return db.execute(text(f"""
            SELECT u.first_name, COUNT(t.id)
            FROM {schema}.tasks t
            JOIN {schema}.users u
            ON t.created_by = u.id::varchar
            GROUP BY u.first_name
            ORDER BY COUNT(t.id) DESC
        """)).fetchall()

    # ================= TABLES =================

    @staticmethod
    def recent_tasks(db, schema):
        return db.execute(text(f"""
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
            ORDER BY t.created_at DESC
            LIMIT 20
        """)).fetchall()

    @staticmethod
    def material_usage(db, schema):
        return db.execute(text(f"""
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
        """)).fetchall()

    # ================= ALERTS =================

    @staticmethod
    def overdue_alert(db, schema):
        return db.execute(text(f"""
            SELECT 
                task_number,
                customer_id,
                service_type_name,
                scheduled_date,
                status
            FROM {schema}.tasks
            WHERE scheduled_date < CURRENT_DATE
            AND status != 'COMPLETED'
        """)).fetchall()

    @staticmethod
    def technician_overload(db, schema):
        return db.execute(text(f"""
            SELECT 
                u.first_name,
                t.scheduled_date,
                COUNT(t.id)
            FROM {schema}.tasks t
            JOIN {schema}.users u
            ON t.created_by = u.id::varchar
            GROUP BY u.first_name, t.scheduled_date
            HAVING COUNT(t.id) > 5
        """)).fetchall()
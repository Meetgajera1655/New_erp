from sqlalchemy import text

class FinancialRepository:

    # 1. Country Revenue Trend
    @staticmethod
    def country_revenue(db, schema):
        return db.execute(text(f"""
            SELECT 
                invoice_date,
                SUM(grand_total)
            FROM {schema}.sales_invoices
            GROUP BY invoice_date
            ORDER BY invoice_date
        """)).fetchall()

    # 2. Branch Revenue Trend
    @staticmethod
    def branch_revenue_trend(db, schema):
        return db.execute(text(f"""
            SELECT 
                branch_id,
                invoice_date,
                SUM(grand_total)
            FROM {schema}.sales_invoices
            GROUP BY branch_id, invoice_date
            ORDER BY invoice_date
        """)).fetchall()

    # 3. Revenue Breakup
    @staticmethod
    def revenue_breakup(db, schema):
        return db.execute(text(f"""
            SELECT 
                item_type,
                SUM(line_total)
            FROM {schema}.sales_invoice_lines
            GROUP BY item_type
        """)).fetchall()

    # 4. Technician Productivity (FIXED)
    @staticmethod
    def technician_productivity(db, schema):
        return db.execute(text(f"""
            SELECT 
                tt.user_id,
                tt.employee_name,
                COUNT(t.id),
                COALESCE(SUM(si.grand_total), 0),
                CASE 
                    WHEN COUNT(t.id)=0 THEN 0
                    ELSE COALESCE(SUM(si.grand_total),0)/COUNT(t.id)
                END
            FROM {schema}.task_technicians tt
            JOIN {schema}.tasks t ON tt.task_id = t.id
            LEFT JOIN {schema}.sales_invoices si 
                ON si.sales_order_id = t.sales_order_id
            WHERE tt.is_primary = TRUE
              AND t.status = 'COMPLETED'
            GROUP BY tt.user_id, tt.employee_name
        """)).fetchall()

    # 5. Chemical Consumption (FIXED)
    @staticmethod
    def chemical_consumption(db, schema):
        return db.execute(text(f"""
            SELECT 
                product_id,
                SUM(used_qty)
            FROM {schema}.task_materials
            GROUP BY product_id
        """)).fetchall()

    # 6. Collection vs Outstanding
    @staticmethod
    def collection_vs_outstanding(db, schema):
        return db.execute(text(f"""
            SELECT 
                COALESCE(SUM(received_amount),0),
                COALESCE(SUM(pending_amount),0)
            FROM {schema}.sales_invoices
        """)).fetchone()

    # 7. Employee Growth
    @staticmethod
    def employee_growth(db, schema):
        return db.execute(text(f"""
            SELECT 
                date_of_joining,
                COUNT(id)
            FROM {schema}.users
            GROUP BY date_of_joining
            ORDER BY date_of_joining
        """)).fetchall()

    # 8. Invoice Status Breakdown
    @staticmethod
    def invoice_status(db, schema):
        return db.execute(text(f"""
            SELECT 
                status,
                COUNT(*)
            FROM {schema}.sales_invoices
            GROUP BY status
        """)).fetchall()

    # ---------- TABLES ----------
    @staticmethod
    def revenue_summary(db, schema):
        return db.execute(text(f"""
            SELECT branch_id, SUM(grand_total) AS total_revenue
            FROM {schema}.sales_invoices
            GROUP BY branch_id
        """)).fetchall()

    @staticmethod
    def task_summary(db, schema):
        return db.execute(text(f"""
            SELECT status, COUNT(*) AS total_tasks
            FROM {schema}.tasks
            GROUP BY status
        """)).fetchall()

    @staticmethod
    def customer_revenue(db, schema):
        return db.execute(text(f"""
            SELECT customer_id, SUM(grand_total) AS total_revenue
            FROM {schema}.sales_invoices
            GROUP BY customer_id
        """)).fetchall()

    @staticmethod
    def invoice_details(db, schema):
        return db.execute(text(f"""
            SELECT invoice_number, customer_id, invoice_date, grand_total, status
            FROM {schema}.sales_invoices
        """)).fetchall()
    
    @staticmethod
    def product_consumption(db, schema):
        return db.execute(text(f"""
        SELECT 
            product_id AS product_code,
            SUM(used_qty) AS consumable_qty
        FROM {schema}.task_materials
        GROUP BY product_id
    """)).fetchall()

    @staticmethod
    def vendor_outstanding(db, schema):
        return db.execute(text(f"""
            SELECT 
                v.vendor_name,
                SUM(pb.paid_amount) AS paid_amount,
                SUM(pb.pending_amount) AS pending_amount,
                SUM(pb.net_payable) AS total
            FROM {schema}.purchase_bills pb
            JOIN {schema}.vendors v ON pb.vendor_id = v.id
            GROUP BY v.vendor_name
        """)).fetchall()        
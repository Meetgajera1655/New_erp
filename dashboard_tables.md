# Dashboard Tables Documentation

This document lists all available dashboard tables grouped by their corresponding dashboard module.

## Branch_Management Dashboard

### branch_density
| Column Name |
|---|
| state |
| count |

### branch_directory
| Column Name |
|---|
| branch_name |
| branch_code |
| city |
| email |
| phone_number |

### branch_growth_trend
| Column Name |
|---|
| month |
| count |

### branch_type_distribution
| Column Name |
|---|
| branch_type |
| count |

### inactive_branch_alerts
| Column Name |
|---|
| branch_name |
| branch_code |
| status |

### invalid_branch_code_alerts
| Column Name |
|---|
| branch_name |
| branch_code |

### recent_branch_activations
| Column Name |
|---|
| branch_name |
| branch_code |
| branch_type |
| created_at |
| created_by |

### state_distribution
| Column Name |
|---|
| state |
| count |

### status_breakdown
| Column Name |
|---|
| status |
| count |

## Contract Dashboard

### branch_contracts
| Column Name |
|---|
| branch |
| count |

### expiring_contracts
| Column Name |
|---|
| contract_id |
| customer_name |
| value |
| end |
| status |
| branch |

### expiry_alert
| Column Name |
|---|
| contract_id |
| customer_name |
| end_date |
| value |
| status |

### monthly_contract_revenue
| Column Name |
|---|
| month |
| total_revenue |

### monthly_value
| Column Name |
|---|
| month |
| value |

### no_sales_order
| Column Name |
|---|
| contract_id |
| value |
| start_date |
| status |

### recent_contracts
| Column Name |
|---|
| contract_id |
| customer_name |
| gma_id |
| value |
| start |
| end |
| status |
| branch |

### status_distribution
| Column Name |
|---|
| status |
| count |

## Customer Dashboard

### active_contracts
| Column Name |
|---|
| customer_id |
| name |
| contract_id |
| start |
| end |
| value |
| status |

### branch_customers
| Column Name |
|---|
| branch |
| count |

### customer_type
| Column Name |
|---|
| type |
| count |

### inactive_customers
| Column Name |
|---|
| customer_id |
| name |
| type |
| branch_id |
| status |

### monthly_trend
| Column Name |
|---|
| month |
| count |

### no_contract_customers
| Column Name |
|---|
| customer_id |
| name |
| type |
| created_at |

### recent_customers
| Column Name |
|---|
| customer_id |
| name |
| type |
| phone |
| branch |
| status |
| created_at |

## Employee Dashboard

### compensation_audit
| Column Name |
|---|
| emp_id |
| name |
| department |
| salary_type |
| basic |
| hra |
| deductions |

### critical_hiring
| Column Name |
|---|
| id |
| department |
| designation |
| positions |
| expected_joining |

### delayed_joining
| Column Name |
|---|
| id |
| department |
| designation |
| expected_joining |
| status |

### employment_type
| Column Name |
|---|
| type |
| count |

### hiring_pipeline
| Column Name |
|---|
| status |
| count |

### low_leave_balance
| Column Name |
|---|
| user_id |
| casual_leave |
| sick_leave |

### onboarding_trend
| Column Name |
|---|
| date |
| count |

## Financial Dashboard

### branch_revenue
| Column Name |
|---|
| branch_name |
| date |
| revenue |

### chemical_consumption
| Column Name |
|---|
| product_name |
| total_consumption |

### country_revenue
| Column Name |
|---|
| date |
| revenue |

### customer_revenue
| Column Name |
|---|
| customer_id |
| customer_name |
| total_revenue |

### employee_growth
| Column Name |
|---|
| date |
| count |

### invoice_details
| Column Name |
|---|
| invoice_number |
| customer_id |
| customer_name |
| invoice_date |
| grand_total |
| status |

### invoice_status
| Column Name |
|---|
| status |
| count |

### product_consumption
| Column Name |
|---|
| product_code |
| product_name |
| consumable_qty |

### revenue_breakup
| Column Name |
|---|
| type |
| value |

### revenue_summary
| Column Name |
|---|
| branch_id |
| branch_name |
| total_revenue |

### task_summary
| Column Name |
|---|
| status |
| total_tasks |

### technician_productivity
| Column Name |
|---|
| technician_id |
| technician_name |
| total_tasks |
| total_revenue |
| productivity |

### vendor_outstanding
| Column Name |
|---|
| vendor_name |
| paid_amount |
| pending_amount |
| grand_total |

## Gma Dashboard

### approved_summary
| Column Name |
|---|
| id |
| total_annual_cost |
| total_annual_price |
| overall_gross_margin |
| gm_with_doc |
| total_visits_per_month |
| approved_on |

### branch_gma
| Column Name |
|---|
| branch |
| count |

### monthly_gma
| Column Name |
|---|
| month |
| count |

### monthly_gma_value
| Column Name |
|---|
| month |
| total_cost |
| total_price |
| avg_margin |

### pending_alert
| Column Name |
|---|
| id |
| source_type |
| status |
| deadline |
| created_at |

### recent_gma
| Column Name |
|---|
| id |
| source_type |
| contract_duration |
| proposed_start_date |
| branch_name |
| total_annual_price |
| status |
| created_at |

### status_distribution
| Column Name |
|---|
| status |
| count |

## Hrm Dashboard

### employee_list
| Column Name |
|---|
| employee_name |
| email |
| phone |
| role |
| status |
| created_at |
| branch |

### employees_by_department_type
| Column Name |
|---|
| department |
| employment_type |
| count |

### employees_by_role
| Column Name |
|---|
| role |
| count |

### high_leave
| Column Name |
|---|
| employee |
| leave_count |

### salary_slips
| Column Name |
|---|
| employee_name |
| year |
| month |
| basic_salary |
| net_salary |
| payment_date |

### salary_trend
| Column Name |
|---|
| year |
| month |
| salary |

### unpaid_salary
| Column Name |
|---|
| employee |
| year |
| month |
| salary |

## Inventory Dashboard

### branch_stock
| Column Name |
|---|
| branch_id |
| total_stock |

### branch_stock_table
| Column Name |
|---|
| branch_id |
| branch_name |
| product_name |
| category |
| assets_qty |
| consumable_qty |
| resell_qty |
| in_transit_qty |
| reserved_qty |
| status |

### central_stock_entries
| Column Name |
|---|
| entry_id |
| product_name |
| supplier_name |
| invoice_number |
| invoice_date |
| total_qty |
| assets_qty |
| consumable_qty |
| resell_qty |
| total_with_tax |
| created_at |

### expired_consumables
| Column Name |
|---|
| product_name |
| expiry_date |

### high_in_transit_stock
| Column Name |
|---|
| product_name |
| in_transit_qty |

### high_reserved_stock
| Column Name |
|---|
| product_name |
| reserved_qty |

### inventory_value_by_category
| Column Name |
|---|
| category |
| value |

### low_stock_alerts
| Column Name |
|---|
| product_name |
| branch_id |

### low_stock_table
| Column Name |
|---|
| product_name |
| product_code |
| branch_id |
| branch_name |
| category |
| assets_qty |
| consumable_qty |
| resell_qty |
| status |

### monthly_stock_comparison
| Column Name |
|---|
| month |
| assets |
| consumables |
| resell |

### out_of_stock_alerts
| Column Name |
|---|
| product_name |

### out_of_stock_table
| Column Name |
|---|
| product_name |
| product_code |
| branch_id |
| branch_name |
| category |

### stock_by_category
| Column Name |
|---|
| category |
| total_stock |

### stock_movement_trend
| Column Name |
|---|
| date |
| movement |

### stock_movements
| Column Name |
|---|
| reference_type |
| reference_id |
| product_id |
| product_name |
| branch_id |
| branch_name |
| stock_type |
| quantity_delta |
| action |
| created_by |
| created_at |

### stock_transfers_table
| Column Name |
|---|
| product_name |
| assets_qty |
| consumable_qty |
| resell_qty |
| source_branch_id |
| branch_name |

## Petty_Cash Dashboard

### approved_payments
| Column Name |
|---|
| request_id |
| paid_by |
| category |
| approved_amount |
| payment_mode |
| transaction_ref |
| payment_date |
| status |

### branch_expense
| Column Name |
|---|
| branch |
| amount |

### high_amount
| Column Name |
|---|
| id |
| category |
| amount |
| status |
| submitted_at |

### monthly_requests
| Column Name |
|---|
| date |
| count |

### pending_old
| Column Name |
|---|
| id |
| category |
| amount |
| status |
| submitted_at |

### recent_requests
| Column Name |
|---|
| request_id |
| employee |
| branch |
| category |
| requested_amount |
| approved_amount |
| status |
| submitted_at |

### status_chart
| Column Name |
|---|
| status |
| count |

## Purchase Dashboard

### daily_po
| Column Name |
|---|
| date |
| count |

### high_value
| Column Name |
|---|
| po_number |
| amount |

### late_delivery
| Column Name |
|---|
| po_number |
| delivery_date |

### monthly_purchase_value
| Column Name |
|---|
| month |
| total |

### po_status
| Column Name |
|---|
| status |
| count |

### recent_po
| Column Name |
|---|
| po_number |
| vendor |
| po_date |
| delivery_date |
| amount |
| status |
| branch |

### vendor_spending
| Column Name |
|---|
| vendor |
| amount |

### vendor_summary
| Column Name |
|---|
| vendor |
| branch |
| total_orders |
| total_amount |
| last_purchase |

## Quotation Dashboard

### branch
| Column Name |
|---|
| branch |
| value |

### critical_expiry
| Column Name |
|---|
| quotation_number |
| valid_till |

### expiring_quotes
| Column Name |
|---|
| quotation_number |
| valid_till |
| amount |
| created_by |

### high_value_pending
| Column Name |
|---|
| quotation_number |
| amount |

### high_value_quotes
| Column Name |
|---|
| quotation_number |
| source |
| amount |
| status |
| created_at |

### monthly
| Column Name |
|---|
| month |
| value |

### source
| Column Name |
|---|
| source |
| count |

### status
| Column Name |
|---|
| status |
| count |

## Sales_Order Dashboard

### branch_sales
| Column Name |
|---|
| branch |
| amount |

### high_value_orders
| Column Name |
|---|
| so_number |
| customer_id |
| amount |
| status |
| created_at |

### monthly_revenue
| Column Name |
|---|
| month |
| revenue |

### pending_orders
| Column Name |
|---|
| so_number |
| customer_id |
| status |
| created_at |

### recent_orders
| Column Name |
|---|
| so_number |
| customer_name |
| order_type |
| amount |
| status |
| branch |
| created_at |

### sales_order_items
| Column Name |
|---|
| so_number |
| customer_name |
| product_name |
| quantity |
| uom |
| unit_price |
| tax_amount |
| line_total |

### status_chart
| Column Name |
|---|
| status |
| count |

## Support Dashboard

### daily_tickets
| Column Name |
|---|
| date |
| count |

### high_priority_alert
| Column Name |
|---|
| ticket_number |
| issue_type |
| priority |
| status |
| created_at |

### old_open_tickets
| Column Name |
|---|
| ticket_number |
| priority |
| status |
| created_at |

### open_high_priority
| Column Name |
|---|
| ticket_number |
| customer |
| issue_type |
| priority |
| status |
| created_at |

### priority_chart
| Column Name |
|---|
| priority |
| count |

### recent_tickets
| Column Name |
|---|
| ticket_number |
| customer |
| issue_type |
| priority |
| status |
| created_at |
| branch |

### status_chart
| Column Name |
|---|
| status |
| count |

## Task Dashboard

### material_usage
| Column Name |
|---|
| task_number |
| product |
| uom |
| required_qty |
| used_qty |
| scheduled_date |

### monthly_trend
| Column Name |
|---|
| month |
| count |

### overdue_tasks
| Column Name |
|---|
| task_number |
| customer_id |
| service |
| scheduled_date |
| status |

### recent_tasks
| Column Name |
|---|
| task_number |
| customer |
| so_number |
| service |
| scheduled_date |
| start_time |
| end_time |
| status |

### status_chart
| Column Name |
|---|
| status |
| count |

### technician_overload
| Column Name |
|---|
| technician |
| date |
| task_count |

### technician_workload
| Column Name |
|---|
| technician |
| count |

## Vendor Dashboard

### active_contract_list
| Column Name |
|---|
| id |
| vendor_name |
| contract_type |
| contract_end_date |
| payment_terms |

### contract_status_split
| Column Name |
|---|
| has_contract |
| count |

### expiring_contract_alerts
| Column Name |
|---|
| id |
| vendor_name |
| contract_end_date |

### low_vendor_rating_alerts
| Column Name |
|---|
| id |
| vendor_name |
| vendor_rating |

### rating_distribution
| Column Name |
|---|
| vendor_rating |
| count |

### recent_vendor_additions
| Column Name |
|---|
| id |
| vendor_name |
| vendor_type |
| created_at |
| vendor_status |

### vendors_by_category
| Column Name |
|---|
| vendor_category |
| count |


# Dashboard Tables Documentation

## Branch_Management Dashboard
### branch_directory
| Column Name |
|---|
| branch_name |
| branch_code |
| city |
| email |
| phone_number |

### recent_branch_activations
| Column Name |
|---|
| branch_name |
| branch_code |
| branch_type |
| created_at |
| created_by |

## Contract Dashboard
### expiring_contracts
| Column Name |
|---|
| contract_id |
| customer_name |
| value |
| end |
| status |
| branch |

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

## Financial Dashboard
### customer_revenue
| Column Name |
|---|
| customer_id |
| customer_name |
| total_revenue |

### invoice_details
| Column Name |
|---|
| invoice_number |
| customer_id |
| customer_name |
| invoice_date |
| grand_total |
| status |

### product_consumption
| Column Name |
|---|
| product_code |
| product_name |
| consumable_qty |

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

### salary_slips
| Column Name |
|---|
| employee_name |
| year |
| month |
| basic_salary |
| net_salary |
| payment_date |

## Inventory Dashboard
### branch_stock
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

### low_stock_products
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

### out_of_stock_products
| Column Name |
|---|
| product_name |
| product_code |
| branch_id |
| branch_name |
| category |

### recent_stock_movements
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

### stock_transfers
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

## Purchase Dashboard
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

### vendor_summary
| Column Name |
|---|
| vendor |
| branch |
| total_orders |
| total_amount |
| last_purchase |

## Quotation Dashboard
### expiring_quotes
| Column Name |
|---|
| quotation_number |
| valid_till |
| amount |
| created_by |

### high_value_quotes
| Column Name |
|---|
| quotation_number |
| source |
| amount |
| status |
| created_at |

## Sales_Order Dashboard
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

## Support Dashboard
### open_high_priority
| Column Name |
|---|
| ticket_number |
| customer |
| issue_type |
| priority |
| status |
| created_at |

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

## Vendor Dashboard
### active_contract_list
| Column Name |
|---|
| id |
| vendor_name |
| contract_type |
| contract_end_date |
| payment_terms |

### recent_vendor_additions
| Column Name |
|---|
| id |
| vendor_name |
| vendor_type |
| created_at |
| vendor_status |


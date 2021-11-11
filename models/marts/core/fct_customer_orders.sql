with orders as (

    select
        id as order_id,
        user_id as customer_id,
        order_date as order_placed_at,
        status as order_status

    from {{ source('jaffle_shop', 'orders') }}

),

customers as (

    select
        first_name as customer_first_name,
        last_name as customer_last_name

    from {{ source('jaffle_shop', 'customers') }}
),

payments as (

    select
        ORDERID as order_id

    from {{ source('stripe', 'payment') }}
    
)

WITH paid_orders as (

    select 
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name

    FROM orders

    left join (
        
        select 
            order_id, 
            max(CREATED) as payment_finalized_date, 
            sum(AMOUNT) / 100.0 as total_amount_paid

        from payments
        where STATUS <> 'fail'
        group by 1

    ) p 
        ON orders.ID = p.order_id

    left join customers
        on orders.USER_ID = customers.ID ),

customer_orders as (

    select 
        customers.customer_id
        , min(order_placed_at) as first_order_date
        , max(order_placed_at) as most_recent_order_date
        , count(orders.id) AS number_of_orders

    from customers

    left join orders
        on orders.customer_id = customers.id 

    group by 1

)

select
    p.*,

    ROW_NUMBER() OVER (ORDER BY p.order_id) as transaction_seq,

    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY p.order_id) as customer_sales_seq,

    CASE 
        WHEN c.first_order_date = p.order_placed_at
            THEN 'new'
        ELSE 'return' 
    END as nvsr,

    x.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos

FROM paid_orders p

left join customer_orders as c USING (customer_id)

LEFT OUTER JOIN 
(
    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from paid_orders p

    left join paid_orders t2 
        on p.customer_id = t2.customer_id and p.order_id >= t2.order_id

    group by 1
    order by p.order_id

) x 
    on x.order_id = p.order_id

ORDER BY order_id
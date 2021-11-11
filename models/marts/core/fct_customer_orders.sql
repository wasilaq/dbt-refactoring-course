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
        id as customer_id,
        first_name as customer_first_name,
        last_name as customer_last_name

    from {{ source('jaffle_shop', 'customers') }}
),

payments as (

    select
        ORDERID as order_id,
        created,
        amount,
        status

    from {{ source('stripe', 'payment') }}

),

p as (

    select 
        order_id, 
        max(created) as payment_finalized_date, 
        sum(amount) / 100.0 as total_amount_paid

    from payments
    where status <> 'fail'
    group by 1

),

paid_orders as (

    select 
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name

    from orders

    left join p 
        on orders.order_id = p.order_id

    left join customers
        on orders.customer_id = customers.customer_id ),

x as (

    select
        paid_orders.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from paid_orders

    left join paid_orders t2 
        on paid_orders.customer_id = t2.customer_id and paid_orders.order_id >= t2.order_id

    group by 1
    order by paid_orders.order_id

),

customer_orders as (

    select 
        customers.customer_id
        , min(order_placed_at) as first_order_date
        , max(order_placed_at) as most_recent_order_date
        , count(orders.order_id) AS number_of_orders

    from customers

    left join orders
        on orders.customer_id = customers.customer_id 

    group by 1

)

select
    paid_orders.*,

    row_number() over (order by paid_orders.order_id) as transaction_seq,

    row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,

    case 
        when c.first_order_date = paid_orders.order_placed_at
            then 'new'
        else 'return' 
    end as nvsr,

    x.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos

from paid_orders

left join customer_orders as c using (customer_id)

left outer join x
    on x.order_id = paid_orders.order_id

order by order_id
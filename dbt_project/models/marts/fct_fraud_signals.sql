-- models/marts/fct_fraud_signals.sql
-- Applies fraud detection rules to every transaction.
-- Each rule produces a boolean flag + a risk score contribution.

with txns as (
    select
        id                  as transaction_id,
        reference,
        merchant_id,
        customer_id,
        amount_ngn,
        currency,
        channel,
        status,
        ip_country,
        is_fraud_sim,
        created_at,
        extract(hour from created_at)   as txn_hour,
        date_trunc('day', created_at)   as txn_date
    from {{ source('raw', 'transactions') }}
    where status = 'success'
      and amount_ngn is not null
),

-- Rule 1: Velocity — customer with many transactions in a 60-min window
velocity as (
    select
        customer_id,
        date_trunc('hour', created_at)  as window_hour,
        count(*)                         as txn_count_1h,
        sum(amount_ngn)                  as amount_sum_1h
    from txns
    group by 1, 2
),

-- Rule 2: Merchant risk score — merchants with high fraud sim rate
merchant_risk as (
    select
        merchant_id,
        count(*)                                        as total_txns,
        sum(case when is_fraud_sim then 1 else 0 end)   as fraud_sim_count,
        round(
            100.0 * sum(case when is_fraud_sim then 1 else 0 end) / nullif(count(*), 0)
        , 2)                                            as fraud_sim_rate
    from txns
    group by 1
),

-- Combine all signals
final as (
    select
        t.transaction_id,
        t.reference,
        t.merchant_id,
        t.customer_id,
        t.amount_ngn,
        t.currency,
        t.channel,
        t.status,
        t.ip_country,
        t.is_fraud_sim,
        t.created_at,
        t.txn_hour,
        t.txn_date,

        -- Velocity flag
        coalesce(v.txn_count_1h, 0)             as customer_txns_last_1h,
        coalesce(v.amount_sum_1h, 0)            as customer_amount_last_1h,
        case when coalesce(v.txn_count_1h, 0) >= 10  then true else false end as flag_velocity,

        -- Large round amount flag
        case when t.amount_ngn >= 2000000
              and t.amount_ngn % 500000 = 0     then true else false end as flag_large_round,

        -- Night transaction flag (midnight to 5am)
        case when t.txn_hour between 0 and 4    then true else false end as flag_night_txn,

        -- Foreign IP flag
        case when t.ip_country not in ('NG')    then true else false end as flag_foreign_ip,

        -- Merchant risk
        coalesce(mr.fraud_sim_rate, 0)          as merchant_fraud_sim_rate,
        case when coalesce(mr.fraud_sim_rate, 0) > 20
                                                then true else false end as flag_risky_merchant,

        -- Composite risk score (0-100)
        least(100,
            case when coalesce(v.txn_count_1h, 0) >= 10 then 35 else 0 end +
            case when t.amount_ngn >= 2000000 and t.amount_ngn % 500000 = 0 then 30 else 0 end +
            case when t.txn_hour between 0 and 4 then 15 else 0 end +
            case when t.ip_country not in ('NG') then 10 else 0 end +
            case when coalesce(mr.fraud_sim_rate, 0) > 20 then 10 else 0 end
        )                                       as risk_score,

        -- Risk tier
        case
            when least(100,
                    case when coalesce(v.txn_count_1h, 0) >= 10 then 35 else 0 end +
                    case when t.amount_ngn >= 2000000 and t.amount_ngn % 500000 = 0 then 30 else 0 end +
                    case when t.txn_hour between 0 and 4 then 15 else 0 end +
                    case when t.ip_country not in ('NG') then 10 else 0 end +
                    case when coalesce(mr.fraud_sim_rate, 0) > 20 then 10 else 0 end
                 ) >= 60 then 'high'
            when least(100,
                    case when coalesce(v.txn_count_1h, 0) >= 10 then 35 else 0 end +
                    case when t.amount_ngn >= 2000000 and t.amount_ngn % 500000 = 0 then 30 else 0 end +
                    case when t.txn_hour between 0 and 4 then 15 else 0 end +
                    case when t.ip_country not in ('NG') then 10 else 0 end +
                    case when coalesce(mr.fraud_sim_rate, 0) > 20 then 10 else 0 end
                 ) >= 30 then 'medium'
            else 'low'
        end                                     as risk_tier

    from txns t
    left join velocity v
        on  t.customer_id = v.customer_id
        and date_trunc('hour', t.created_at) = v.window_hour
    left join merchant_risk mr
        on t.merchant_id = mr.merchant_id
)

select * from final

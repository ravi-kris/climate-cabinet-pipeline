{{ config(tags=["silver"]) }}

with source_data as (
    select *
    from {{ source('raw', 'campaign_finance') }}
),
normalized as (
    select
        trim(cast(filing_id as varchar)) as filing_id,
        trim(cast(candidate_id as varchar)) as candidate_id,
        trim(cast(candidate_name as varchar)) as candidate_name,
        trim(cast(donor_id as varchar)) as donor_id,
        trim(cast(donor_name as varchar)) as donor_name,
        case
            when upper(trim(donor_type)) in ('PAC', 'POLITICAL ACTION COMMITTEE') then 'PAC'
            when upper(trim(donor_type)) in ('INDIVIDUAL', 'PERSON', 'CITIZEN') then 'INDIVIDUAL'
            else 'OTHER'
        end as donor_type,
        cast(amount as numeric(18,2)) as amount_usd,
        cast(contribution_date as date) as contribution_date,
        upper(trim(state)) as state,
        upper(trim(district)) as district
    from source_data
)
select *
from normalized
where amount_usd > 0

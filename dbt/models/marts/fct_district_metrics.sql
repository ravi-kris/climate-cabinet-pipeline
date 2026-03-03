{{ config(tags=["gold"]) }}

select
    state,
    district,
    candidate_id,
    candidate_name,
    sum(amount_usd) as total_contributions,
    sum(case when donor_type = 'PAC' then amount_usd else 0 end) as pac_contributions,
    sum(case when donor_type = 'INDIVIDUAL' then amount_usd else 0 end) as individual_contributions,
    count(*) as contribution_count,
    count(distinct donor_id) as unique_donors
from {{ ref('stg_contributions') }}
group by 1,2,3,4

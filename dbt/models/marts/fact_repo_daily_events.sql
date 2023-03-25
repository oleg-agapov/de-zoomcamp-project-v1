{% set partitions_to_replace = [
    'date(current_date)',
    'date(date_sub(current_date, interval 1 day))'
] %}

{{
    config(
        materialized = "incremental",
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "event_date",
            "data_type": "date"
        },
        partitions = partitions_to_replace,
    )
}}

select
	date(e.created_at) AS event_date,
	e.repo_id,
	e.repo_name,
	e.event_type,
	e.payload_action AS event_subtype,
    t.category as event_category,
    t.description,
	count(1) AS events
from {{ ref('stg_github__events') }} e
left join {{ ref('event_types') }} t
    on t.type = e.event_type

-- recalculate yesterday + today
{% if is_incremental() %}
where date(e.created_at) in ({{ partitions_to_replace | join(',') }})
{% endif %}

group by 1, 2, 3, 4, 5, 6, 7

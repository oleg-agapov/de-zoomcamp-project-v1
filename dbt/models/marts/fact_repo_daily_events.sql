{% set partitions_to_replace = [
    'timestamp(current_date)',
    'timestamp(date_sub(current_date, interval 1 day))'
] %}

{{
    config(
        materialized = "incremental",
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

{% if var('from', None) and var('to', None) %}
where timestamp(e.created_at) between '{{ var("from") }}' AND '{{ var("to") }}'
{% elif is_incremental() %}
-- recalculate yesterday + today
where timestamp(e.report_date) in ({{ partitions_to_replace | join(',') }})
{% endif %}

group by 1, 2, 3, 4, 5, 6, 7

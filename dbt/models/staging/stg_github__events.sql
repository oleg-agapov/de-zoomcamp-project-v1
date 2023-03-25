{% set partitions_to_replace = [
    'timestamp(current_date)',
    'timestamp(date_sub(current_date, interval 1 day))'
] %}

{{
    config(
        materialized = "incremental",
        partition_by = {
            "field": "created_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = ["event_type"],
        partitions = partitions_to_replace
    )
}}

select
    id as event_id,
    created_at,
    public,
    `type` as event_type,
    --
    repo.id as repo_id,
    repo.name as repo_name,
    repo.url as repo_url,
    --
    actor.avatar_url as actor_avatar_url,
    actor.display_login as actor_display_login,
    actor.gravatar_id as actor_gravatar_id,
    actor.id as actor_id,
    actor.login as actor_login,
    actor.url as actor_url,
    --
    payload.action as payload_action,
    payload.description as payload_description,
    payload.distinct_size as payload_distinct_size,
    payload.number as payload_number,
    payload.push_id as payload_push_id,
    payload.pusher_type as payload_pusher_type,
    payload.ref as payload_ref,
    payload.ref_type as payload_ref_type,
    payload.repository_id as payload_repository_id,
    payload.size as payload_size,
    --
    row_created_at,
    `year` as report_year,
    `month` as report_month,
    `date` as report_date
from
{{ source('raw_data', 'github') }}

{% if var('from', None) and var('to', None) %}
where timestamp(`date`) between '{{ var("from") }}' AND '{{ var("to") }}'
{% elif is_incremental() %}
-- recalculate yesterday + today
where timestamp(`date`) in ({{ partitions_to_replace | join(',') }})
{% endif %}

-- deduplication
qualify row_number() over (partition by event_id order by created_at) = 1

{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "creation_datetime_utc",
      "data_type": "timestamp",
    },
    cluster_by = ["new_pk"]
) }}

with questions as (

	SELECT *
	from {{ ref('int_stackoverflow_companies_questions') }}
	
),

answers as (
	
	SELECT *
	from {{ ref('stg_stackoverflow_posts_answers') }}
	
),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['questions_pk','companies_pk']) }} as new_pk,
        q.questions_pk,
        q.companies_pk,
        q.tags,
        a._pk as answer_pk,
        a.comment_count,
        a.community_owned_datetime_utc,
        a.creation_datetime_utc,
        a.favorite_count,
        a.last_activity_datetime_utc,
        a.last_edit_datetime_utc,
        a.last_editor_display_name,
        a.last_editor_user_id,
        a.owner_display_name,
        a.owner_user_id,
        a.parent_id,
        a.score,
        a.view_count
    from questions q
    inner join answers a on q.questions_pk = a.parent_id
	
)

select * from final

{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "creation_datetime_utc",
      "data_type": "timestamp",
    },
    cluster_by = ["_pk"]
) }}


with stackoverflow_questions as (

    select *
    from {{ ref('stg_stackoverflow_posts_questions') }}
	
),

companies as (

    select *
    from {{ ref('list_of_companies_snapshot') }},
    unnest (split(tags)) as tag
    where dbt_valid_to is null
	
),

questions_unnested as (

    select
        _pk,
        tag
    from stackoverflow_questions,
    unnest (split(tags)) as tag

),

questions_filtered as (
    select
        questions_unnested._pk as questions_pk,
        companies.dbt_scd_id as companies_pk,
        companies.organization as organization_name
    from questions_unnested
    inner join companies on questions_unnested.tag = companies.tag
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['questions_pk', 'companies_pk']) }} as _pk,
        questions_filtered.questions_pk,
        questions_filtered.companies_pk,
        questions_filtered.organization_name,
        stackoverflow_questions.title,
        stackoverflow_questions.accepted_answer_id,
        stackoverflow_questions.answer_count,
        stackoverflow_questions.comment_count,
        stackoverflow_questions.community_owned_datetime_utc,
        stackoverflow_questions.creation_datetime_utc,
        stackoverflow_questions.favorite_count,
        stackoverflow_questions.last_activity_datetime_utc,
        stackoverflow_questions.last_edit_datetime_utc,
        stackoverflow_questions.last_editor_display_name,
        stackoverflow_questions.last_editor_user_id,
        stackoverflow_questions.owner_display_name,
        stackoverflow_questions.owner_user_id,
        stackoverflow_questions.score,
        stackoverflow_questions.tags,
        stackoverflow_questions.view_count
    from questions_filtered
    inner join stackoverflow_questions on questions_filtered.questions_pk = stackoverflow_questions._pk
)


select * from final
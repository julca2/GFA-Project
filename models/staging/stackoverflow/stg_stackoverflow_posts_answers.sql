with source as (

    select * from {{ source('Jul_Dataset', 'so_posts_answers') }}

),

renamed as (

    select
		{{ dbt_utils.generate_surrogate_key(['id', 'tags']) }} as _pk,
        id,
        title,
        body,
        accepted_answer_id,
        answer_count,
        comment_count,
        community_owned_date as community_owned_datetime_utc,
        creation_date as creation_datetime_utc,
        favorite_count,
        last_activity_date as last_activity_datetime_utc,
        last_edit_date as last_edit_datetime_utc,
        last_editor_display_name,
        last_editor_user_id,
        owner_display_name,
        owner_user_id,
        parent_id,
        post_type_id,
        score,
        tags,
        view_count

    from source
	WHERE EXTRACT(YEAR FROM creation_date) = 2022 

)

select * from renamed
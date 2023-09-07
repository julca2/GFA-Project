with so_questions as (
    SELECT *
    FROM {{ ref('int_stackoverflow_companies_questions') }}
),
final as (
    SELECT
        ROW_NUMBER() OVER () AS _pk,
        EXTRACT(quarter from creation_datetime_utc) as quarter,
        EXTRACT(year from creation_datetime_utc) as year,
        organization_name,
        COUNT(_pk) AS post_count,
        SUM(answer_count) AS answer_count,
        AVG(answer_count) as avg_answer_count,
        SUM(comment_count) as comment_count,
        AVG(comment_count) as avg_comment_count,
        SUM(favorite_count) as favorite_count,
        AVG(favorite_count) as avg_favorite_count,
        SUM(view_count) as view_count,
        AVG(view_count) as avg_view_count,
        COUNT(CASE WHEN accepted_answer_id is not null THEN 1 END) AS accepted_answer_count,
        COUNT(CASE WHEN answer_count = 0 THEN 1 END) AS no_answer_count,
        (COUNT(CASE WHEN answer_count = 0 THEN 1 END)) / (COUNT(_pk)) AS avg_no_answer_count,
        COUNT (DISTINCT CASE WHEN tags is not null THEN 1 END) as tags_count,
        MAX(last_activity_datetime_utc) as last_activity_datetime_utc,
        MAX(last_edit_datetime_utc) as last_edit_datetime_utc
    FROM so_questions
    GROUP BY 
        quarter,
        organization_name,
        year
)
  
select 
    _pk,
    quarter,
    year,
	CASE
		WHEN quarter = 1 THEN '2022-01-01'
		WHEN quarter = 2 THEN '2022-04-01'
		WHEN quarter = 3 THEN '2022-07-01'
		WHEN quarter = 4 THEN '2022-10-01'
    END as first_day_of_period,
    organization_name,
    post_count,
    answer_count,
    avg_answer_count,
    comment_count,
    avg_comment_count,
    favorite_count,
    avg_favorite_count,
    view_count,
    avg_view_count,
    accepted_answer_count,
    no_answer_count,
    avg_no_answer_count,
    tags_count,
    last_activity_datetime_utc,
    last_edit_datetime_utc
from final
order by year, quarter
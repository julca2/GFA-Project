with so_questions as (

	SELECT *
	FROM {{ ref ('int_stackoverflow_companies_questions') }}
	
),

final as (

	SELECT
		ROW_NUMBER() OVER () AS _pk,
		date(
		EXTRACT(year from creation_datetime_utc),
		EXTRACT(month from creation_datetime_utc),
		EXTRACT(day from creation_datetime_utc)
		) as first_day_of_period,
		EXTRACT(month from creation_datetime_utc) as month,
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
		COUNT(CASE WHEN answer_count is null THEN 1 END) AS no_answer_count,
		(COUNT(CASE WHEN answer_count = 0 THEN 1 END)) / (COUNT(_pk) ) AS avg_no_answer_count,
		--score,
		COUNT (DISTINCT CASE WHEN tags is not null THEN 1 END) as tags_count,
		MAX(last_activity_datetime_utc) as last_activity_datetime_utc,
		MAX(last_edit_datetime_utc) as last_edit_datetime_utc
	FROM so_questions
	GROUP BY 
		first_day_of_period,
		month,
		quarter,
		year,
		organization_name
	ORDER BY first_day_of_period
)
	
select * from final
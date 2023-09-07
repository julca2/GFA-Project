with github_table as (

	SELECT *
	FROM {{ ref ('int_github') }}
	
),



final as (

	SELECT
		ROW_NUMBER() OVER () AS _pk,
		EXTRACT(quarter from created_at_datetime_utc) as quarter,
		EXTRACT(year from created_at_datetime_utc) as year,
		organization_name,
		repository_account,
		repository_name,
		count(event_id) as event_count,
		count(user_id) as user_count,
		COUNT(CASE WHEN type LIKE 'Issue%' THEN 1 END) AS issues_count,
		COUNT(CASE WHEN type = 'WatchEvent' THEN 1 END) AS watch_count,
		COUNT(CASE WHEN type = 'ForkEvent' THEN 1 END) AS fork_count,
		COUNT(CASE WHEN type = 'PushEvent' THEN 1 END) AS push_count,
		COUNT(CASE WHEN type LIKE 'PullRequest%' THEN 1 END) AS pr_count,
		COUNT(CASE WHEN type = 'DeleteEvent' THEN 1 END) AS delete_count,
		COUNT(CASE WHEN type = 'PublicEvent' THEN 1 END) AS public_count,
		COUNT(CASE WHEN type = 'CreateEvent' THEN 1 END) AS create_count,
		COUNT(CASE WHEN type = 'GollumEvent' THEN 1 END) AS gollum_count,
		COUNT(CASE WHEN type = 'MemberEvent' THEN 1 END) AS member_count,
		COUNT(CASE WHEN type = 'CommitCommentEvent' THEN 1 END) AS commit_comment_count,
		COUNT(*) AS total_event_count
	FROM github_table
	GROUP BY 
		quarter,
		year,
		organization_name,
		repository_account,
		repository_name
	ORDER BY quarter
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
    repository_account,
    repository_name,
    event_count,
    user_count,
    issues_count,
    watch_count,
    fork_count,
    push_count,
    pr_count,
    delete_count,
    public_count,
    create_count,
    gollum_count,
    member_count,
    commit_comment_count,
    total_event_count
from final
order by year, quarter
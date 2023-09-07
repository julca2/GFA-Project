with github_table as (
    SELECT *
    FROM {{ ref('int_github') }}
),
final as (
    SELECT
        ROW_NUMBER() OVER () AS _pk,
        EXTRACT(month from created_at_datetime_utc) as month,
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
        month,
        quarter,
        year,
        organization_name,
        repository_account,
        repository_name
)
  
select 
    _pk,
    month,
    quarter,
    year,
    CASE
        WHEN month = 1 THEN '2022-01-01'
        WHEN month = 2 THEN '2022-02-01'
        WHEN month = 3 THEN '2022-03-01'
        WHEN month = 4 THEN '2022-04-01'
        WHEN month = 5 THEN '2022-05-01'
        WHEN month = 6 THEN '2022-06-01'
        WHEN month = 7 THEN '2022-07-01'
        WHEN month = 8 THEN '2022-08-01'
        WHEN month = 9 THEN '2022-09-01'
        WHEN month = 10 THEN '2022-10-01'
        WHEN month = 11 THEN '2022-11-01'
        WHEN month = 12 THEN '2022-12-01'
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
order by year, quarter, month
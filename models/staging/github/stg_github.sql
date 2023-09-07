{{ config(materialized='view') }}
with source as (

    select * from {{ source('Jul_Dataset', 'github_organizations') }}

),

renamed as (

    select
		id AS _pk,
        type,
        public,
        payload,
		split(repo.name, '/')[safe_offset(0)] as organisation,
        split(repo.name, '/')[safe_offset(1)] as repository_name,
		repo.id as repo_id,
        repo.url as repo_url,
        actor.id as actor_id,
        actor.login as actor_login,
        actor.url as actor_url,
        org.id as org_id,
        org.login as org_login,
        org.url as org_url,
        created_at as created_at_datetime_utc,
    from source

)

select * from renamed

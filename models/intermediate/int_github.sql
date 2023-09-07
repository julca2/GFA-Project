{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "created_at_datetime_utc",
      "data_type": "timestamp",
    },
    cluster_by = "_pk"
) }}


with github_table as (

	SELECT
		_pk,
		actor_id,
		organisation,
		repository_name,
		created_at_datetime_utc,
		type
	FROM {{ ref ('stg_github') }}
	
),

companies as (

	SELECT
		dbt_scd_id as _pk,
		repository_account,
		repository_name,
		organization
	FROM {{ ref ('list_of_companies_snapshot')}}
	
),

final as (

	SELECT
		{{ dbt_utils.generate_surrogate_key(['companies._pk', 'github_table._pk']) }} as _pk,
		companies.repository_account,
		companies.repository_name,
		github_table.actor_id as user_id,
		github_table._pk as event_id,
		github_table.type,
		companies.organization as organization_name,
		github_table.created_at_datetime_utc as created_at_datetime_utc
	FROM companies
	INNER JOIN github_table
		ON companies.repository_name = github_table.repository_name
		AND companies.repository_account = github_table.organisation
		
	UNION ALL

	SELECT
		{{ dbt_utils.generate_surrogate_key(['github_table.created_at_datetime_utc', 'github_table._pk']) }} as _pk,
		companies.repository_account,
		companies.repository_name,
		github_table.actor_id as user_id,
		github_table._pk as event_id,
		github_table.type,
		companies.organization as organization_name,
		github_table.created_at_datetime_utc as created_at_datetime_utc
	FROM companies
	INNER JOIN github_table
		ON companies.repository_account = github_table.organisation
	WHERE companies.repository_name is null
	ORDER BY created_at_datetime_utc
)

SELECT * FROM final
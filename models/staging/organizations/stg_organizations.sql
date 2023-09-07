
with source as (

    select * from {{ source('Jul_Dataset', 'v3') }}

),

organizations as (

    select
		{{ dbt_utils.generate_surrogate_key(['_airbyte_emitted_at', 'organization']) }} as _pk,
		organization,
		l1_type,
        l2_type,
        l3_type,
        repository_account,
        repository_name,
        CASE WHEN open_source_available = 'Yes' THEN 'true' ELSE 'false' END as is_open_source_available,
        tags,
        _airbyte_emitted_at as updated_at_datetime_utc
    from source

)

select * from organizations

{% snapshot list_of_companies_snapshot %}


{{
    config(
      target_database='reliable-airway-391712',
      target_schema = 'jul_snapshots',
      unique_key='_pk',
      invalidate_hard_deletes=True,
      strategy='check',
      check_cols=['_pk', 'tags', 'l1_type','l2_type', 'l3_type', 'repository_account','repository_name', ' is_open_source_available'],
    )
}}


    select *
    from {{ ref('stg_organizations') }}

{% endsnapshot %}
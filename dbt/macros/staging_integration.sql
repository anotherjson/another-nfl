{% macro get_latest_extraction_path(dataset_name) %}
  {% set query %}
    select max(etl_date) as latest_etl_date
    from (
        select regexp_extract(file_path, 'etl_date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1) as etl_date
        from glob('data/{{ dataset_name }}/*/etl_date=*/data.parquet')
    )
  {% endset %}
  
  {% set result = run_query(query) %}
  {% if result %}
    {% set latest_date = result.columns[0].values()[0] %}
    {{ return('data/' ~ dataset_name ~ '/*/etl_date=' ~ latest_date ~ '/data.parquet') }}
  {% else %}
    {{ return('data/' ~ dataset_name ~ '/*/etl_date=*/data.parquet') }}
  {% endif %}
{% endmacro %}

{% macro add_staging_metadata() %}
  -- Add standard staging metadata columns
  current_timestamp as dbt_loaded_at,
  '{{ this.schema }}' as dbt_schema,
  '{{ this.name }}' as dbt_model,
  '{{ var("dbt_run_id", "unknown") }}' as dbt_run_id
{% endmacro %}

{% macro create_staging_table_query(dataset_name, select_clause, additional_columns='') %}
  select
    -- Core data columns
    {{ select_clause }},
    
    -- Staging metadata
    {{ add_staging_metadata() }}
    
    {% if additional_columns %}
    , {{ additional_columns }}
    {% endif %}
    
  from '{{ get_dataset_path(dataset_name) }}'
{% endmacro %}

{% macro get_dataset_path(dataset_name) %}
  {% set non_year_datasets = ['team_desc', 'players'] %}
  {% if dataset_name in non_year_datasets %}
    {{ return('../data/' ~ dataset_name ~ '/etl_date=*/data.parquet') }}
  {% else %}
    {{ return('../data/' ~ dataset_name ~ '/*/etl_date=*/data.parquet') }}
  {% endif %}
{% endmacro %}

{% macro get_staging_table_name(dataset_name) %}
  {{ return('stg_' ~ dataset_name) }}
{% endmacro %}

{% macro materialize_as_table_with_indexes(dataset_name) %}
  {% set post_hooks = ["analyze {{ this }}"] %}
  
  {% if dataset_name in ['pbp', 'schedules'] %}
    {% set post_hooks = post_hooks + ["create index if not exists idx_" ~ dataset_name ~ "_game_id on {{ this }} (game_id)"] %}
  {% endif %}
  
  {% if dataset_name in ['weekly', 'pbp'] %}
    {% set post_hooks = post_hooks + ["create index if not exists idx_" ~ dataset_name ~ "_player_id on {{ this }} (player_id)"] %}
  {% endif %}
  
  {% if dataset_name == 'team_desc' %}
    {% set post_hooks = post_hooks + ["create index if not exists idx_" ~ dataset_name ~ "_team_abbr on {{ this }} (team_abbr)"] %}
  {% endif %}
  
  {{
    config(
      materialized='table',
      post_hook=post_hooks
    )
  }}
{% endmacro %}

{% macro get_all_staging_models() %}
  {% set staging_models = [
    'pbp',
    'weekly', 
    'team_desc',
    'schedules'
  ] %}
  {{ return(staging_models) }}
{% endmacro %}
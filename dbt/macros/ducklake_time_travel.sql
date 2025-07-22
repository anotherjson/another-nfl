{% macro ducklake_time_travel(schema_name, table_name, as_of_date=none) %}
  {#
    Macro to generate DuckLake time travel queries
    
    Args:
      schema_name: The schema name in DuckLake catalog (e.g., 'nfl_raw')
      table_name: The table name in DuckLake catalog (e.g., 'team_desc')
      as_of_date: Optional date for time travel (defaults to latest)
    
    Returns:
      SQL query string to select from DuckLake table with time travel
  #}
  
  {% if as_of_date %}
    {# Time travel query - get data as of specific date #}
    {% set query %}
      WITH ducklake_version AS (
        SELECT file_path
        FROM postgres_query('{{ env_var("POSTGRES_CONNECTION") }}',
          'SELECT tv.file_path
           FROM ducklake_catalog.tables t
           JOIN ducklake_catalog.table_versions tv ON t.table_id = tv.table_id
           WHERE t.schema_name = ''{{ schema_name }}''
           AND t.table_name = ''{{ table_name }}''
           AND tv.etl_date <= ''{{ as_of_date }}''
           ORDER BY tv.etl_date DESC
           LIMIT 1'
        )
      )
      SELECT * FROM read_parquet((SELECT file_path FROM ducklake_version))
    {% endset %}
  {% else %}
    {# Latest version query #}
    {% set query %}
      WITH ducklake_latest AS (
        SELECT file_path
        FROM postgres_query('{{ env_var("POSTGRES_CONNECTION") }}',
          'SELECT tv.file_path
           FROM ducklake_catalog.tables t
           JOIN ducklake_catalog.table_versions tv ON t.table_id = tv.table_id
           WHERE t.schema_name = ''{{ schema_name }}''
           AND t.table_name = ''{{ table_name }}''
           ORDER BY tv.version_number DESC
           LIMIT 1'
        )
      )
      SELECT * FROM read_parquet((SELECT file_path FROM ducklake_latest))
    {% endset %}
  {% endif %}
  
  {{ return(query) }}
  
{% endmacro %}
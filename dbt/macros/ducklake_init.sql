{% macro register_ducklake_sources() %}
  {% if execute %}
    {% set attach_query %}
      ATTACH '{{ env_var("DUCKLAKE_ATTACHMENT") }}' AS nfl_ducklake (DATA_PATH '{{ env_var("NFL_DATA_PATH") }}');
    {% endset %}
    {% do run_query(attach_query) %}
    {{ log("DuckLake attached successfully", info=true) }}
  {% endif %}
{% endmacro %}
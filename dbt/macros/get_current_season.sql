{% macro get_current_season() %}
  {% if target.name == 'prod' %}
    {% set current_year = modules.datetime.date.today().year %}
    {% if modules.datetime.date.today().month >= 9 %}
      {{ current_year }}
    {% else %}
      {{ current_year - 1 }}
    {% endif %}
  {% else %}
    {{ var('current_season') }}
  {% endif %}
{% endmacro %}
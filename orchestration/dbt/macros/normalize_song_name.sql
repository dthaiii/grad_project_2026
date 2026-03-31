{% macro normalize_song_name(column_name) %}
    REGEXP_REPLACE({{ column_name }}, r'([^\p{ASCII}]+)|(")', '')
{% endmacro %}
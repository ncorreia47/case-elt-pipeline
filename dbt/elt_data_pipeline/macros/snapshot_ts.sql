{% macro snapshot_ts() %}
    to_char('{{ run_started_at }}'::timestamp, 'YYYYMMDDHH24MISS')
{% endmacro %}
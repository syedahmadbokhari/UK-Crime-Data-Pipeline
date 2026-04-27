-- Macro: assert a model has at least min_rows rows.
-- Used as a data quality gate before marts run.
{% macro assert_min_rows(model, min_rows) %}
    {% set row_count_query %}
        select count(*) as cnt from {{ model }}
    {% endset %}
    {% set results = run_query(row_count_query) %}
    {% if execute %}
        {% set cnt = results.columns[0].values()[0] %}
        {% if cnt < min_rows %}
            {{ exceptions.raise_compiler_error(
                "Row count gate failed: " ~ model ~ " has " ~ cnt ~ " rows (minimum: " ~ min_rows ~ ")"
            ) }}
        {% endif %}
        {{ log("Row count OK: " ~ model ~ " = " ~ cnt ~ " rows", info=True) }}
    {% endif %}
{% endmacro %}

{% macro effective_view_count(watch_time, duration) %}
    -- Full views (each full play is always > 3s if duration > 3s, so all count)
    -- Plus 1 if the remainder of the last partial view exceeds 3 seconds
    floor(div0({{ watch_time }}, {{ duration }}))
    + case
        when ({{ watch_time }} - floor(div0({{ watch_time }}, {{ duration }})) * {{ duration }}) > 3
        then 1
        else 0
    end
{% endmacro %}

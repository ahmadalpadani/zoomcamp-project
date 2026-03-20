{% macro get_time_segment(column_name) %}
    case 
        when extract(hour from {{ column_name }}) between 0 and 5 then 'Dawn'
        when extract(hour from {{ column_name }}) between 6 and 11 then 'Morning'
        when extract(hour from {{ column_name }}) between 12 and 17 then 'Afternoon'
        else 'Night'
    end
{% endmacro %}
{% macro get_customer_loyalty(order_count_column) %}
    case 
        when {{ order_count_column }} > 5 then 'VVIP / Enthusiast'
        when {{ order_count_column }} > 1 then 'Repeat Customer'
        else 'One-time Buyer'
    end
{% endmacro %}
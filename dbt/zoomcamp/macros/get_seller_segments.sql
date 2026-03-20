{% macro get_seller_quality_segment(rating_column) %}
    case 
        when {{ rating_column }} >= 4.5 then 'Excellent'
        when {{ rating_column }} >= 3.0 then 'Good'
        else 'Needs Improvement'
    end
{% endmacro %}

{% macro get_seller_revenue_tier(revenue_column) %}
    case 
        when {{ revenue_column }} > 10000 then 'Top Tier'
        when {{ revenue_column }} > 1000 then 'Mid Tier'
        else 'New/Low Tier'
    end
{% endmacro %}
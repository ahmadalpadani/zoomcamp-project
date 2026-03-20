{% macro pivot_payments(column_name, value_column='payment_value') %}

    {% set payment_types = ['credit_card', 'debit_card', 'boleto', 'voucher'] %}

    {% for payment_type in payment_types %}
        SUM(
            CASE 
                WHEN {{ column_name }} = '{{ payment_type }}' THEN {{ value_column }} 
                ELSE 0 
            END
        ) AS {{ payment_type }}_amount
        {%- if not loop.last -%}, {% endif -%}
    {% endfor %}

{% endmacro %}
{#
    This macro returns the postal code as a string in the standard format of the specified country.
#}

{% macro standardise_postal_code(postal_code, country_code) -%}

    CASE {{ country_code }}  
        WHEN 'US' THEN LPAD(CAST(postal_code AS STRING), 5, '0')
        ELSE UPPER(CAST(postal_code AS STRING))
    END

{%- endmacro %}
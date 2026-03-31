{#
    This macro returns the category of the webpage based on its name.
#}

{% macro get_page_category(page_name) -%}

    CASE {{ page_name }}  
        WHEN 'About' THEN 'Platform Information'
        WHEN 'Help' THEN 'Platform Information'
        WHEN 'Home' THEN 'Landing'
        WHEN 'Register' THEN 'Registration'
        WHEN 'Submit Registration' THEN 'Registration'
        WHEN 'Settings' THEN 'Customisation'
        WHEN 'Save Settings' THEN 'Customisation'
        WHEN 'Cancel' THEN 'Customisation'
        WHEN 'Cancellation Confirmation' THEN 'Customisation'
        WHEN 'NextSong' THEN 'Streaming'
        WHEN 'PlayAd' THEN 'Advertising'
        WHEN 'Error' THEN 'Error'
        WHEN 'Login' THEN 'Authentication'
        WHEN 'Logout' THEN 'Authentication'
        WHEN 'Upgrade' THEN 'Subscription'
        WHEN 'Submit Upgrade' THEN 'Subscription'
        WHEN 'Downgrade' THEN 'Subscription'
        WHEN 'Submit Downgrade' THEN 'Subscription'
        ELSE 'UNKNOWN'
    END

{%- endmacro %}
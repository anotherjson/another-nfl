{% macro calculate_fantasy_points(
    passing_yards='passing_yards',
    passing_tds='passing_tds', 
    interceptions='interceptions',
    rushing_yards='rushing_yards',
    rushing_tds='rushing_tds',
    receiving_yards='receiving_yards',
    receiving_tds='receiving_tds',
    receptions='receptions',
    fumbles_lost='fumbles_lost'
) %}

    (
        -- Passing points (1 point per 25 yards, 4 points per TD, -2 per INT)
        (coalesce({{ passing_yards }}, 0) / 25.0) + 
        (coalesce({{ passing_tds }}, 0) * 4) + 
        (coalesce({{ interceptions }}, 0) * -2) +
        
        -- Rushing points (1 point per 10 yards, 6 points per TD)
        (coalesce({{ rushing_yards }}, 0) / 10.0) + 
        (coalesce({{ rushing_tds }}, 0) * 6) +
        
        -- Receiving points (1 point per 10 yards, 6 points per TD, 1 point per reception in PPR)
        (coalesce({{ receiving_yards }}, 0) / 10.0) + 
        (coalesce({{ receiving_tds }}, 0) * 6) +
        coalesce({{ receptions }}, 0) +
        
        -- Fumbles lost (-2 points)
        (coalesce({{ fumbles_lost }}, 0) * -2)
    )

{% endmacro %}
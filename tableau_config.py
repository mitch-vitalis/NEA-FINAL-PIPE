# tableau_config.py
"""
Configuration settings for Tableau-ready KPI calculations
Easily adjustable business rules without code changes
"""

# Days on Hand Thresholds
INVENTORY_THRESHOLDS = {
    'CRITICAL_LOW': 3,      # Days
    'LOW_STOCK': 7,         # Days  
    'HIGH_STOCK': 30,       # Days
    'EXCESS_STOCK': 60      # Days
}

# Reorder Point Settings (days of inventory to maintain)
REORDER_DAYS_BY_CATEGORY = {
    'flower': 14,           # 2 weeks
    'concentrate': 21,      # 3 weeks
    'edible': 30,           # 1 month
    'vape': 21,             # 3 weeks
    'preroll': 10,          # 1.5 weeks (faster turnover)
    'default': 21           # 3 weeks default
}

# Stock Performance Alert Settings
PERFORMANCE_ALERT_DAYS = {
    'NO_RECENT_SALES': 90,      # No sales in X days
    'SLOW_MOVER': 60,           # Last sale > X days ago
    'DECLINING_SALES': 30,      # Last sale > X days ago
    'MIN_VELOCITY_90D': 5       # Minimum units sold in 90 days
}

# Velocity Trend Calculation
VELOCITY_TREND_THRESHOLDS = {
    'ACCELERATING': 1.2,        # 7D avg > 30D avg * 1.2
    'DECLINING': 0.8,           # 7D avg < 30D avg * 0.8
    'STABLE': 'between'         # Between the above thresholds
}

# Location-Specific Settings (customize by store if needed)
LOCATION_SETTINGS = {
    'Fall River': {
        'high_volume_location': True,
        'reorder_multiplier': 1.0
    },
    'Seekonk': {
        'high_volume_location': True, 
        'reorder_multiplier': 1.0
    },
    'New Bedford': {
        'high_volume_location': False,
        'reorder_multiplier': 0.8
    },
    'Wholesale': {
        'high_volume_location': True,
        'reorder_multiplier': 1.5  # Wholesale needs higher safety stock
    }
}

# Brand Priority Settings (for inventory allocation decisions)
BRAND_PRIORITY = {
    'NEA Fire': 1,          # Highest priority
    'NEA Premium': 1,
    'NEA Awarded': 1,
    'Valorem': 2,
    'Cannatini': 2,
    'Dab FX': 3,
    'Double Baked': 3,
    'Farm To Fam': 3,
    'SWEETSPOT': 3,
    'default': 4
}

# Seasonal Adjustment Factors (future enhancement)
SEASONAL_FACTORS = {
    'january': 0.9,
    'february': 0.9,
    'march': 1.0,
    'april': 1.1,    # 4/20 month
    'may': 1.0,
    'june': 1.0,
    'july': 1.1,     # Summer peak
    'august': 1.1,   # Summer peak
    'september': 1.0,
    'october': 1.2,  # Pre-holiday stocking
    'november': 1.3, # Holiday season
    'december': 1.2  # Holiday season
}

# Tableau Display Settings
TABLEAU_FORMATTING = {
    'currency_format': '${:,.2f}',
    'percentage_format': '{:.1f}%',
    'quantity_format': '{:,.0f}',
    'days_format': '{:.0f} days'
}

# Color Coding for Tableau (hex codes)
STATUS_COLORS = {
    'STOCK_OUT': '#DC3545',        # Red
    'CRITICAL_LOW': '#FD7E14',     # Orange
    'LOW_STOCK': '#FFC107',        # Yellow
    'OPTIMAL': '#28A745',          # Green
    'HIGH_STOCK': '#17A2B8',       # Blue
    'EXCESS_STOCK': '#6F42C1',     # Purple
    'NO_RECENT_SALES': '#6C757D'   # Gray
}

# Export these for easy import
__all__ = [
    'INVENTORY_THRESHOLDS',
    'REORDER_DAYS_BY_CATEGORY', 
    'PERFORMANCE_ALERT_DAYS',
    'VELOCITY_TREND_THRESHOLDS',
    'LOCATION_SETTINGS',
    'BRAND_PRIORITY',
    'SEASONAL_FACTORS',
    'TABLEAU_FORMATTING',
    'STATUS_COLORS'
]
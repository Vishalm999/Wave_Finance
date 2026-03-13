# vendor_ageing/semantic/intent.py

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any

@dataclass
class AgeingIntent:
    """
    Semantic representation of user query intent
    for vendor ageing analysis.
    """

    # Core metrics
    metric: str = "total_overdue"  # total_overdue, average_delay_days, count_vendors
    ageing_bucket: Optional[str] = None  # specific_bucket, all_buckets, custom_range

    # Grouping dimensions
    dimensions: List[str] = field(default_factory=list)  # vendor, company, segment, profit_center

    # Date filters
    date_field: str = "posting_date"   # Primary date for ageing
    date_range: str = "as_of_today"    # as_of_today, as_of_date, custom_range
    custom: Optional[Dict[str, str]] = None
    custom_dates: Optional[List[Dict]] = None

    # Ageing specific
    delay_days_filter: Optional[Dict] = None  # {min: X, max: Y, condition: ">"}
    overdue_amount_filter: Optional[Dict] = None  # {min: X, max: Y, condition: ">"}

    # Result shaping
    is_trend: bool = False
    time_grain: Optional[str] = None
    compare_to: Optional[str] = None
    order_by: Optional[str] = None
    order_direction: Optional[str] = None
    limit: Optional[int] = None

    # Filters
    filters: Dict = field(default_factory=dict)

    # Query type
    query_type: str = "aggregate"  # aggregate, list, count
    select_columns: Optional[List[str]] = None

    # Debug
    original_question: Optional[str] = None
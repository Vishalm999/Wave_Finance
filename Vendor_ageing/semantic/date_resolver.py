# vendor_ageing/semantic/date_resolver.py

from datetime import date, datetime, timedelta
from typing import List, Dict, Optional
import re

def get_fiscal_year_from_posting_date(posting_date_column: str = "Posting_Date") -> str:
    """
    Returns SQL expression to calculate fiscal year from Posting_Date.
    FY starts April 1st. Example: Apr 2023 - Mar 2024 = FY 2023
    """
    return f"""
    CASE 
        WHEN CAST(SUBSTR(CAST({posting_date_column} AS VARCHAR), 5, 2) AS INTEGER) >= 4 
        THEN CAST(SUBSTR(CAST({posting_date_column} AS VARCHAR), 1, 4) AS INTEGER)
        ELSE CAST(SUBSTR(CAST({posting_date_column} AS VARCHAR), 1, 4) AS INTEGER) - 1
    END
    """.strip()

def get_fiscal_year_date_range(fiscal_year: int) -> tuple:
    """
    Returns (start_date, end_date) in YYYYMMDD format for a given fiscal year.
    Example: FY 2023 -> ('20230401', '20240331')
    """
    start_date = f"{fiscal_year}0401"
    end_date = f"{fiscal_year + 1}0331"
    return (start_date, end_date)

def get_last_day_of_month(year: int, month: int) -> int:
    """Returns the last day of the given month."""
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    return (next_month - timedelta(days=1)).day

def parse_natural_date(date_str: str) -> Optional[str]:
    """
    Parse natural language dates like "11 may 2024" into YYYYMMDD format.
    """
    date_str = date_str.strip().lower()
    
    month_map = {
        'jan': 1, 'january': 1,
        'feb': 2, 'february': 2,
        'mar': 3, 'march': 3,
        'apr': 4, 'april': 4,
        'may': 5,
        'jun': 6, 'june': 6,
        'jul': 7, 'july': 7,
        'aug': 8, 'august': 8,
        'sep': 9, 'sept': 9, 'september': 9,
        'oct': 10, 'october': 10,
        'nov': 11, 'november': 11,
        'dec': 12, 'december': 12
    }
    
    # Pattern: "11 may 2024" or "11-may-2024" or "11/may/2024"
    pattern = r'(\d{1,2})[/\s-]+([a-z]+)[/\s-]+(\d{4})'
    match = re.search(pattern, date_str)
    
    if match:
        day = int(match.group(1))
        month_name = match.group(2)
        year = int(match.group(3))
        
        if month_name in month_map:
            month = month_map[month_name]
            if 1 <= day <= get_last_day_of_month(year, month):
                return f"{year}{month:02d}{day:02d}"
    
    # Pattern: "july 2022" (month + year only)
    pattern_month_year = r'([a-z]+)[/\s-]+(\d{4})'
    match_month_year = re.search(pattern_month_year, date_str)
    
    if match_month_year:
        month_name = match_month_year.group(1)
        year = int(match_month_year.group(2))
        
        if month_name in month_map:
            month = month_map[month_name]
            return f"{year}{month:02d}01"
    
    # Pattern: "2024-05-11" or "2024/05/11"
    pattern2 = r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})'
    match2 = re.search(pattern2, date_str)
    
    if match2:
        year = int(match2.group(1))
        month = int(match2.group(2))
        day = int(match2.group(3))
        return f"{year}{month:02d}{day:02d}"
    
    return None

def parse_month_to_date(month_str: str) -> Optional[tuple]:
    """
    Convert month name (without year) to month number and infer year.
    """
    today = date.today()
    current_year = today.year
    current_month = today.month
    
    # Determine current FY start year (Apr-Mar)
    if current_month >= 4:
        current_fy_start_year = current_year
    else:
        current_fy_start_year = current_year - 1
    
    month_map = {
        'jan': 1, 'january': 1,
        'feb': 2, 'february': 2,
        'mar': 3, 'march': 3,
        'apr': 4, 'april': 4,
        'may': 5,
        'jun': 6, 'june': 6,
        'jul': 7, 'july': 7,
        'aug': 8, 'august': 8,
        'sep': 9, 'sept': 9, 'september': 9,
        'oct': 10, 'october': 10,
        'nov': 11, 'november': 11,
        'dec': 12, 'december': 12
    }
    
    month_name = month_str.strip().lower()
    if month_name not in month_map:
        return None
    
    month_num = month_map[month_name]
    
    # Determine year based on FY (Apr-Mar)
    if month_num >= 4:
        year = current_fy_start_year
    else:
        year = current_fy_start_year + 1
    
    last_day = get_last_day_of_month(year, month_num)
    start_date = f"{year}{month_num:02d}01"
    end_date = f"{year}{month_num:02d}{last_day:02d}"
    
    return (year, month_num, start_date, end_date)

def resolve_custom_dates(
    dates_list: List[Dict],
    column_sql: str = "Posting_Date"
) -> Optional[str]:
    """
    Converts a list of custom dates to a SQL condition.
    """
    if not dates_list:
        return None

    conditions = []
    expr = f'TRY(DATE_PARSE(CAST("{column_sql}" AS VARCHAR), \'%Y%m%d\'))'

    # FY quarter → start/end month mapping
    # Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar (next calendar year)
    QUARTER_MONTHS = {
        1: (4,  6),
        2: (7,  9),
        3: (10, 12),
        4: (1,  3),   # Q4 is in the NEXT calendar year relative to FY start
    }

    # Pre-compute week/named-period boundaries (needed for "this_week", "last_week" etc.)
    _today = date.today()
    _this_week_start = _today - timedelta(days=_today.weekday())
    _this_week_end   = _this_week_start + timedelta(days=6)
    _last_week_start = _this_week_start - timedelta(days=7)
    _last_week_end   = _this_week_start - timedelta(days=1)
    _today_m = _today.month
    _today_y = _today.year
    _cur_fy  = _today_y if _today_m >= 4 else _today_y - 1
    _last_fy = _cur_fy - 1

    NAMED_PERIOD_MAP = {
        "this_week":  (_this_week_start.strftime("%Y%m%d"), _this_week_end.strftime("%Y%m%d"),   "This Week"),
        "last_week":  (_last_week_start.strftime("%Y%m%d"), _last_week_end.strftime("%Y%m%d"),   "Last Week"),
        "this_year":  (f"{_cur_fy}0401",  f"{_cur_fy+1}0331",  f"FY {_cur_fy}-{str(_cur_fy+1)[-2:]}"),
        "last_year":  (f"{_last_fy}0401", f"{_last_fy+1}0331", f"FY {_last_fy}-{str(_last_fy+1)[-2:]}"),
        "this_month": (
            f"{_today_y}{_today_m:02d}01",
            f"{_today_y}{_today_m:02d}{get_last_day_of_month(_today_y, _today_m):02d}",
            _today.strftime("%B %Y")
        ),
        "last_month": (
            f"{(_today_y if _today_m > 1 else _today_y-1)}{((_today_m-1) if _today_m > 1 else 12):02d}01",
            f"{(_today_y if _today_m > 1 else _today_y-1)}{((_today_m-1) if _today_m > 1 else 12):02d}{get_last_day_of_month((_today_y if _today_m > 1 else _today_y-1), ((_today_m-1) if _today_m > 1 else 12)):02d}",
            (_today.replace(day=1) - timedelta(days=1)).strftime("%B %Y")
        ),
    }

    for d in dates_list:
        year    = d.get("year")
        quarter = d.get("quarter")
        month   = d.get("month_num") or d.get("month")
        day     = d.get("day")
        period  = d.get("period")   # named period: "this_week", "last_week", "this_year", "last_year" etc.

        # ── Named period (this_week, last_week, this_year, last_year, etc.) ──
        if period and period in NAMED_PERIOD_MAP:
            start, end, label = NAMED_PERIOD_MAP[period]
            conditions.append(
                f"({expr} BETWEEN DATE_PARSE('{start}', '%Y%m%d') AND DATE_PARSE('{end}', '%Y%m%d'))"
            )
            continue

        # ── Quarter entry: expand to full 3-month range ───────────────
        if quarter and year:
            start_month, end_month = QUARTER_MONTHS[quarter]
            # Q4 spans into the next calendar year
            if quarter == 4:
                start_year = year + 1
                end_year   = year + 1
            else:
                start_year = year
                end_year   = year
            last_day = get_last_day_of_month(end_year, end_month)
            start = f"{start_year}{start_month:02d}01"
            end   = f"{end_year}{end_month:02d}{last_day:02d}"
            conditions.append(
                f"({expr} BETWEEN DATE_PARSE('{start}', '%Y%m%d') AND DATE_PARSE('{end}', '%Y%m%d'))"
            )

        # ── Specific day ──────────────────────────────────────────────
        elif day and month and year:
            date_str = f"{year}{month:02d}{day:02d}"
            conditions.append(
                f"{expr} = DATE_PARSE('{date_str}', '%Y%m%d')"
            )

        # ── Whole month ───────────────────────────────────────────────
        elif month and year:
            last_day = get_last_day_of_month(year, month)
            start = f"{year}{month:02d}01"
            end   = f"{year}{month:02d}{last_day:02d}"
            conditions.append(
                f"({expr} BETWEEN DATE_PARSE('{start}', '%Y%m%d') AND DATE_PARSE('{end}', '%Y%m%d'))"
            )

        # ── Whole fiscal year ─────────────────────────────────────────
        elif year:
            start_date, end_date = get_fiscal_year_date_range(year)
            conditions.append(
                f"({expr} BETWEEN DATE_PARSE('{start_date}', '%Y%m%d') AND DATE_PARSE('{end_date}', '%Y%m%d'))"
            )

    return f"({' OR '.join(conditions)})" if len(conditions) > 1 else (conditions[0] if conditions else None)

def resolve_date_filter(
    date_range: str,
    column_sql: str = "Posting_Date",
    custom: Optional[Dict] = None,
    custom_dates: Optional[List[Dict]] = None
) -> str:
    """
    Converts a semantic date range into a Presto SQL condition for ageing reports.
    """
    today = date.today()
    year = today.year
    month = today.month
    day = today.day
    
    expr = f'TRY(DATE_PARSE(CAST("{column_sql}" AS VARCHAR), \'%Y%m%d\'))'

    # -----------------------------
    # Custom dates (highest priority)
    # -----------------------------
    if custom_dates:
        resolved = resolve_custom_dates(custom_dates, column_sql)
        if resolved:
            return resolved

    # -----------------------------
    # Custom explicit range
    # -----------------------------
    if date_range == "custom_range" and custom:
        start = custom.get("start")
        end = custom.get("end")
        
        if start and end:
            # Handle natural language dates
            start_parsed = parse_natural_date(start) if isinstance(start, str) and not start.isdigit() else start
            end_parsed = parse_natural_date(end) if isinstance(end, str) and not end.isdigit() else end
            
            if start_parsed and end_parsed:
                return f"{expr} BETWEEN DATE_PARSE('{start_parsed}', '%Y%m%d') AND DATE_PARSE('{end_parsed}', '%Y%m%d')"

    # -----------------------------
    # Handle "last N days" for ageing
    # -----------------------------
    last_days_match = re.match(r'last[_\s]+(\d+)[_\s]+days?', date_range.lower().replace(' ', '_'))
    if last_days_match:
        days = int(last_days_match.group(1))
        return f"{expr} >= CURRENT_DATE - INTERVAL '{days}' DAY"


    # -----------------------------
    # Handle quarter queries (Q1, Q2, Q3, Q4)
    # -----------------------------
    quarter_match = re.match(r'q([1-4])[_\s]+(\d{4})', date_range.lower().replace(' ', '_'))
    if quarter_match:
        quarter_num = int(quarter_match.group(1))
        query_year = int(quarter_match.group(2))
        
        # Financial year quarters (Apr-Mar)
        if quarter_num == 1:
            start_date = f"{query_year}0401"
            end_date = f"{query_year}0630"
        elif quarter_num == 2:
            start_date = f"{query_year}0701"
            end_date = f"{query_year}0930"
        elif quarter_num == 3:
            start_date = f"{query_year}1001"
            end_date = f"{query_year}1231"
        else:  # Q4
            start_date = f"{query_year+1}0101"
            end_date = f"{query_year+1}0331"
        
        return f"{expr} BETWEEN DATE_PARSE('{start_date}', '%Y%m%d') AND DATE_PARSE('{end_date}', '%Y%m%d')"

    # -----------------------------
    # Handle "last N years/quarters/months"
    # -----------------------------
    multi_period_match = re.match(r'last[_\s]+(\d+)[_\s]+(years?|quarters?|months?)', date_range.lower().replace(' ', '_'))
    if multi_period_match:
        count = int(multi_period_match.group(1))
        period_type = multi_period_match.group(2).rstrip('s')
        
        if month >= 4:
            current_fy_start_year = year
        else:
            current_fy_start_year = year - 1
        
        if period_type == 'year':
            end_fy = current_fy_start_year - 1
            start_fy = end_fy - count + 1
            start_date = f"{start_fy}0401"
            end_date = f"{end_fy+1}0331"
            return f"{expr} BETWEEN DATE_PARSE('{start_date}', '%Y%m%d') AND DATE_PARSE('{end_date}', '%Y%m%d')"
        
        elif period_type == 'quarter':
            # FY Quarters: Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar
            # Determine current FY quarter
            if month >= 4 and month <= 6:
                current_quarter = 1
                fy_year = year
            elif month >= 7 and month <= 9:
                current_quarter = 2
                fy_year = year
            elif month >= 10 and month <= 12:
                current_quarter = 3
                fy_year = year
            else:  # Jan-Mar
                current_quarter = 4
                fy_year = year - 1
            
            # Calculate target quarter (going back 'count' quarters)
            target_quarter = current_quarter - count
            target_fy_year = fy_year
            
            while target_quarter <= 0:
                target_quarter += 4
                target_fy_year -= 1
            
            # Get start and end dates for the target quarter range
            # Start from target_quarter of target_fy_year
            # End at current_quarter of current fy_year (previous quarter end)
            
            # Calculate previous quarter end
            prev_quarter = current_quarter - 1
            prev_fy_year = fy_year
            if prev_quarter == 0:
                prev_quarter = 4
                prev_fy_year -= 1
            
            # Map quarter to months
            quarter_to_months = {
                1: (4, 6),   # Q1: Apr-Jun
                2: (7, 9),   # Q2: Jul-Sep
                3: (10, 12), # Q3: Oct-Dec
                4: (1, 3)    # Q4: Jan-Mar
            }
            
            start_month, _ = quarter_to_months[target_quarter]
            _, end_month = quarter_to_months[prev_quarter]
            
            # Adjust year for Q4 (Jan-Mar)
            start_year = target_fy_year if start_month >= 4 else target_fy_year + 1
            end_year = prev_fy_year if end_month >= 4 else prev_fy_year + 1
            
            # Get last day of end month
            if end_month in [1, 3, 5, 7, 8, 10, 12]:
                end_day = 31
            elif end_month in [4, 6, 9, 11]:
                end_day = 30
            else:
                end_day = 28  # Feb (simplified)
            
            start_date = f"{start_year}{start_month:02d}01"
            end_date = f"{end_year}{end_month:02d}{end_day}"
            
            return f"{expr} BETWEEN DATE_PARSE('{start_date}', '%Y%m%d') AND DATE_PARSE('{end_date}', '%Y%m%d')"
        
        elif period_type == 'month':
            start_month = month - count
            start_year = year
            
            while start_month <= 0:
                start_month += 12
                start_year -= 1
            
            start_date = f"{start_year}{start_month:02d}01"
            
            # End is last month end
            if month == 1:
                end_month = 12
                end_year = year - 1
            else:
                end_month = month - 1
                end_year = year
            
            last_day = get_last_day_of_month(end_year, end_month)
            end_date = f"{end_year}{end_month:02d}{last_day:02d}"
            return f"{expr} BETWEEN DATE_PARSE('{start_date}', '%Y%m%d') AND DATE_PARSE('{end_date}', '%Y%m%d')"

    # -----------------------------
    # Explicit 4-digit year ONLY
    # -----------------------------
    if date_range.isdigit() and len(date_range) == 4:
        year_int = int(date_range)
        if 1900 <= year_int <= 2200:
            start_date, end_date = get_fiscal_year_date_range(year_int)
            return f"{expr} BETWEEN DATE_PARSE('{start_date}', '%Y%m%d') AND DATE_PARSE('{end_date}', '%Y%m%d')"

    # -----------------------------
    # Financial year ranges
    # -----------------------------
    if month >= 4:
        current_fy = year
        last_fy = year - 1
    else:
        current_fy = year - 1
        last_fy = year - 2

    fy_start = f"{current_fy}0401"
    fy_end = f"{current_fy+1}0331"
    last_fy_start = f"{last_fy}0401"
    last_fy_end = f"{last_fy+1}0331"

    # Calculate this_month
    this_month_start = str(year) + str(month).zfill(2) + "01"
    last_day_this_month = get_last_day_of_month(year, month)
    this_month_end = str(year) + str(month).zfill(2) + str(last_day_this_month).zfill(2)

    # Last month
    if month == 1:
        last_month_num = 12
        last_month_year = year - 1
    else:
        last_month_num = month - 1
        last_month_year = year
    
    last_month_start = str(last_month_year) + str(last_month_num).zfill(2) + "01"
    last_day_last_month = get_last_day_of_month(last_month_year, last_month_num)
    last_month_end = str(last_month_year) + str(last_month_num).zfill(2) + str(last_day_last_month).zfill(2)

    # This week (Mon–Sun of current calendar week)
    today_obj = date(year, month, day)
    this_week_start_obj = today_obj - timedelta(days=today_obj.weekday())          # Monday
    this_week_end_obj   = this_week_start_obj + timedelta(days=6)                  # Sunday
    this_week_start = this_week_start_obj.strftime("%Y%m%d")
    this_week_end   = this_week_end_obj.strftime("%Y%m%d")

    # Last week (Mon–Sun of previous calendar week)
    last_week_start_obj = this_week_start_obj - timedelta(days=7)
    last_week_end_obj   = this_week_start_obj - timedelta(days=1)
    last_week_start = last_week_start_obj.strftime("%Y%m%d")
    last_week_end   = last_week_end_obj.strftime("%Y%m%d")

    # Calculate this_quarter
    current_quarter = ((month - 1) // 3) + 1
    this_q_start_month = ((current_quarter - 1) * 3) + 1
    this_q_end_month = this_q_start_month + 2
    
    this_quarter_start = str(year) + str(this_q_start_month).zfill(2) + "01"
    last_day_q = get_last_day_of_month(year, this_q_end_month)
    this_quarter_end = str(year) + str(this_q_end_month).zfill(2) + str(last_day_q).zfill(2)

    # Last quarter
    if current_quarter == 1:
        last_q_start_month = 10
        last_q_end_month = 12
        last_q_year = year - 1
    else:
        last_q_start_month = ((current_quarter - 2) * 3) + 1
        last_q_end_month = last_q_start_month + 2
        last_q_year = year
    
    last_quarter_start = str(last_q_year) + str(last_q_start_month).zfill(2) + "01"
    last_day_lq = get_last_day_of_month(last_q_year, last_q_end_month)
    last_quarter_end = str(last_q_year) + str(last_q_end_month).zfill(2) + str(last_day_lq).zfill(2)

    semantic_map = {
        "current_financial_year": f"{expr} BETWEEN DATE_PARSE('{fy_start}', '%Y%m%d') AND DATE_PARSE('{fy_end}', '%Y%m%d')",
        "last_financial_year": f"{expr} BETWEEN DATE_PARSE('{last_fy_start}', '%Y%m%d') AND DATE_PARSE('{last_fy_end}', '%Y%m%d')",
        "today": f"DATE({expr}) = CURRENT_DATE",
        "yesterday": f"DATE({expr}) = CURRENT_DATE - INTERVAL '1' DAY",
        "this_month": f"{expr} BETWEEN DATE_PARSE('{this_month_start}', '%Y%m%d') AND DATE_PARSE('{this_month_end}', '%Y%m%d')",
        "last_month": f"{expr} BETWEEN DATE_PARSE('{last_month_start}', '%Y%m%d') AND DATE_PARSE('{last_month_end}', '%Y%m%d')",
        "this_quarter": f"{expr} BETWEEN DATE_PARSE('{this_quarter_start}', '%Y%m%d') AND DATE_PARSE('{this_quarter_end}', '%Y%m%d')",
        "last_quarter": f"{expr} BETWEEN DATE_PARSE('{last_quarter_start}', '%Y%m%d') AND DATE_PARSE('{last_quarter_end}', '%Y%m%d')",
        "this_year": f"DATE_TRUNC('year', {expr}) = DATE_TRUNC('year', CURRENT_DATE)",
        "last_year": f"{expr} BETWEEN DATE_PARSE('{last_fy_start}', '%Y%m%d') AND DATE_PARSE('{last_fy_end}', '%Y%m%d')",
        "this_week": f"{expr} BETWEEN DATE_PARSE('{this_week_start}', '%Y%m%d') AND DATE_PARSE('{this_week_end}', '%Y%m%d')",
        "last_week": f"{expr} BETWEEN DATE_PARSE('{last_week_start}', '%Y%m%d') AND DATE_PARSE('{last_week_end}', '%Y%m%d')",
    }

    if date_range in semantic_map:
        return semantic_map[date_range]

    # -----------------------------
    # Rolling windows
    # -----------------------------
    if date_range.startswith("rolling_"):
        # Dynamic: rolling_N_days where N is any number
        rolling_match = re.match(r'rolling_(\d+)_days?', date_range)
        days = int(rolling_match.group(1)) if rolling_match else 30
        return f"{expr} >= CURRENT_DATE - INTERVAL '{days}' DAY"

    # -----------------------------
    # For ageing reports, default to as_of_today logic
    # -----------------------------
    return f"{expr} <= CURRENT_DATE"

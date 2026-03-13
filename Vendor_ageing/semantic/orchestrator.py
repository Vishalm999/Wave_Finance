# vendor_ageing/semantic/orchestrator.py

from semantic.intent import AgeingIntent
from semantic.registry import SemanticRegistry
from semantic.sql_builder import SQLBuilder, SQLRenderer
from semantic.date_resolver import resolve_date_filter, get_fiscal_year_date_range

class AgeingOrchestrator:
    """
    Orchestrates semantic SQL generation for Vendor Ageing.
    """

    def __init__(self, model_path: str):
        self.registry = SemanticRegistry(model_path)
        self.sql_builder = SQLBuilder(self.registry)

    def build_sql_from_intent(self, intent: AgeingIntent):
        warnings = []
        
        try:
            # Build date filter
            date_sql = None
            
            # PRIORITY 1: Check for custom_dates first (for "and" queries like "Q1 and Q3")
            if intent.custom_dates:
                date_sql = resolve_date_filter(None, "Posting_Date", intent.custom, intent.custom_dates)
                
                # If custom_dates use named period keys (this_week, last_year etc.),
                # inject a period_label dimension so the query groups by period and shows separate rows
                if any(d.get("period") for d in intent.custom_dates):
                    if "period_label" not in intent.dimensions:
                        intent.dimensions.append("period_label")
                    # Build the CASE expression for period_label and store on intent for sql_builder
                    from semantic.date_resolver import get_last_day_of_month
                    from datetime import date as _date, timedelta as _timedelta
                    _today = _date.today()
                    _twm = _today.month; _twy = _today.year
                    _this_w_start = _today - _timedelta(days=_today.weekday())
                    _this_w_end   = _this_w_start + _timedelta(days=6)
                    _last_w_start = _this_w_start - _timedelta(days=7)
                    _last_w_end   = _this_w_start - _timedelta(days=1)
                    _cur_fy  = _twy if _twm >= 4 else _twy - 1
                    _last_fy = _cur_fy - 1
                    _lm_num  = (_twm - 1) if _twm > 1 else 12
                    _lm_yr   = _twy if _twm > 1 else _twy - 1

                    _period_cases = []
                    for d in intent.custom_dates:
                        p = d.get("period")
                        if p == "this_week":
                            _period_cases.append(f"WHEN TRY(DATE_PARSE(CAST(\"Posting_Date\" AS VARCHAR), '%Y%m%d')) BETWEEN DATE_PARSE('{_this_w_start.strftime('%Y%m%d')}', '%Y%m%d') AND DATE_PARSE('{_this_w_end.strftime('%Y%m%d')}', '%Y%m%d') THEN 'This Week'")
                        elif p == "last_week":
                            _period_cases.append(f"WHEN TRY(DATE_PARSE(CAST(\"Posting_Date\" AS VARCHAR), '%Y%m%d')) BETWEEN DATE_PARSE('{_last_w_start.strftime('%Y%m%d')}', '%Y%m%d') AND DATE_PARSE('{_last_w_end.strftime('%Y%m%d')}', '%Y%m%d') THEN 'Last Week'")
                        elif p == "this_year":
                            _period_cases.append(f"WHEN TRY(DATE_PARSE(CAST(\"Posting_Date\" AS VARCHAR), '%Y%m%d')) BETWEEN DATE_PARSE('{_cur_fy}0401', '%Y%m%d') AND DATE_PARSE('{_cur_fy+1}0331', '%Y%m%d') THEN 'FY {_cur_fy}-{str(_cur_fy+1)[-2:]}'")
                        elif p == "last_year":
                            _period_cases.append(f"WHEN TRY(DATE_PARSE(CAST(\"Posting_Date\" AS VARCHAR), '%Y%m%d')) BETWEEN DATE_PARSE('{_last_fy}0401', '%Y%m%d') AND DATE_PARSE('{_last_fy+1}0331', '%Y%m%d') THEN 'FY {_last_fy}-{str(_last_fy+1)[-2:]}'")
                        elif p == "this_month":
                            _ldom = get_last_day_of_month(_twy, _twm)
                            _period_cases.append(f"WHEN TRY(DATE_PARSE(CAST(\"Posting_Date\" AS VARCHAR), '%Y%m%d')) BETWEEN DATE_PARSE('{_twy}{_twm:02d}01', '%Y%m%d') AND DATE_PARSE('{_twy}{_twm:02d}{_ldom:02d}', '%Y%m%d') THEN 'This Month'")
                        elif p == "last_month":
                            _ldom2 = get_last_day_of_month(_lm_yr, _lm_num)
                            _period_cases.append(f"WHEN TRY(DATE_PARSE(CAST(\"Posting_Date\" AS VARCHAR), '%Y%m%d')) BETWEEN DATE_PARSE('{_lm_yr}{_lm_num:02d}01', '%Y%m%d') AND DATE_PARSE('{_lm_yr}{_lm_num:02d}{_ldom2:02d}', '%Y%m%d') THEN 'Last Month'")
                    if _period_cases:
                        intent.filters["__period_label_expr__"] = "CASE " + " ".join(_period_cases) + " ELSE 'Other' END"
            
            # PRIORITY 2: Check if year is specified in custom field
            elif intent.custom and "year" in intent.custom:
                year = intent.custom["year"]
                start_date, end_date = get_fiscal_year_date_range(year)
                date_sql = f"TRY(DATE_PARSE(CAST(Posting_Date AS VARCHAR), '%Y%m%d')) BETWEEN DATE_PARSE('{start_date}', '%Y%m%d') AND DATE_PARSE('{end_date}', '%Y%m%d')"
            
            # PRIORITY 3: Check if it's a 4-digit year in date_range
            elif intent.date_range and intent.date_range.isdigit() and len(intent.date_range) == 4:
                year = int(intent.date_range)
                start_date, end_date = get_fiscal_year_date_range(year)
                date_sql = f"TRY(DATE_PARSE(CAST(Posting_Date AS VARCHAR), '%Y%m%d')) BETWEEN DATE_PARSE('{start_date}', '%Y%m%d') AND DATE_PARSE('{end_date}', '%Y%m%d')"
            
            # Handle specific date range keywords
            elif intent.date_range == "as_of_today":
                date_sql = "TRY(DATE_PARSE(CAST(Posting_Date AS VARCHAR), '%Y%m%d')) <= CURRENT_DATE"
            
            elif intent.date_range == "as_of_date" and intent.custom and intent.custom.get("date"):
                date = intent.custom.get("date")
                date_sql = f"TRY(DATE_PARSE(CAST(Posting_Date AS VARCHAR), '%Y%m%d')) <= DATE '{date}'"
            
            # Use date_resolver for complex date ranges
            elif intent.date_range and intent.date_range not in ["as_of_today", "as_of_date"]:
                date_sql = resolve_date_filter(intent.date_range, "Posting_Date", intent.custom, intent.custom_dates)
            
            # "all_years": remove date filter entirely so all data is returned
            if intent.date_range == "all_years":
                date_sql = None
            
            # Default to current financial year ONLY if no custom_dates and no date_sql yet
            elif not date_sql and not intent.custom_dates:
                date_sql = resolve_date_filter("current_financial_year", "Posting_Date")
            
            # Build SQL
            sql_query_obj = self.sql_builder.build_base_query(
                metric=intent.metric,
                ageing_bucket=intent.ageing_bucket,
                dimensions=intent.dimensions,
                date_filter=date_sql,
                delay_days_filter=intent.delay_days_filter,
                overdue_amount_filter=intent.overdue_amount_filter,
                time_grain=intent.time_grain,
                is_trend=intent.is_trend,
                order_by=intent.order_by,
                order_direction=intent.order_direction or "DESC",
                limit=intent.limit,
                compare_to=intent.compare_to,
                filters=intent.filters,
                query_type=intent.query_type,
                select_columns=intent.select_columns
            )
            
        except Exception as e:
            warnings.append(f"SQLBuilder error: {e}")
            sql_query_obj = None

        return sql_query_obj, warnings
# vendor_ageing/semantic/sql_builder.py

from dataclasses import dataclass
from typing import List, Dict, Any, Optional

@dataclass
class SelectItem:
    expression: str
    alias: Optional[str] = None
    display: bool = True        # False = used internally (sorting/grouping) but hidden from API response

@dataclass
class WhereCondition:
    expression: str

@dataclass
class GroupByItem:
    expression: str

@dataclass
class OrderByItem:
    expression: str
    direction: str = "DESC"

@dataclass
class SQLQuery:
    select: List[SelectItem]
    from_table: str
    where: List[WhereCondition]
    group_by: List[GroupByItem]
    order_by: List[OrderByItem]
    limit: Optional[int] = None

    @property
    def display_columns(self) -> List[str]:
        """Returns only the column aliases that should be shown to the user."""
        return [item.alias for item in self.select if item.display and item.alias]

class SQLBuilder:
    def __init__(self, registry):
        self.registry = registry
        self.fact_table = registry.fact.table

    def build_base_query(
        self,
        metric: str,
        ageing_bucket: Optional[str],
        dimensions: List[str],
        date_filter: Optional[str],
        delay_days_filter: Optional[Dict],
        overdue_amount_filter: Optional[Dict],
        time_grain: Optional[str],
        is_trend: bool,
        order_by: Optional[str],
        order_direction: str,
        limit: Optional[int],
        compare_to: Optional[str],
        filters: Optional[Dict],
        query_type: str,
        select_columns: Optional[List[str]]
    ) -> SQLQuery:
        
        if query_type == "list":
            return self._build_list_query(
                select_columns or dimensions,
                date_filter,
                delay_days_filter,
                overdue_amount_filter,
                order_by,
                order_direction,
                limit,
                filters
            )
        
        return self._build_aggregate_query(
            metric,
            ageing_bucket,
            dimensions,
            date_filter,
            delay_days_filter,
            overdue_amount_filter,
            time_grain,
            is_trend,
            order_by,
            order_direction,
            limit,
            compare_to,
            filters
        )

    def _build_list_query(
        self,
        columns: List[str],
        date_filter: Optional[str],
        delay_days_filter: Optional[Dict],
        overdue_amount_filter: Optional[Dict],
        order_by: Optional[str],
        order_direction: str,
        limit: Optional[int],
        filters: Optional[Dict]
    ) -> SQLQuery:
        
        select_items: List[SelectItem] = []
        where_conditions: List[WhereCondition] = []
        
        # Build SELECT
        column_map = {
            "vendor_no": "Vendor_no",
            "vendor_name": "Vendor_name",
            "company_code": "Company_code",
            "segment": "Segment",
            "profit_center": "Profit_Centre_text",
            "delay_days": "Delay_days",
            "overdue": "Overdue",
            "posting_date": "Posting_Date",
            "city":"City",
            "document_type":"Document_Type",
            "gl_account":"GL_Account_Desc",
            "description":"Description",
            "supplier_ref_no":"Supplier_Ref_No"
        }
        
        if not columns:
            columns = ["vendor_no", "vendor_name", "overdue", "delay_days", "company_code"]
        
        for col in columns:
            if col in column_map:
                db_col = column_map[col]
                if col == "overdue":
                    cleaned = f"""CASE 
                                 WHEN "{db_col}" LIKE '%-' THEN TRY_CAST('-' || REPLACE("{db_col}", '-', '') AS DOUBLE)
                                 ELSE TRY_CAST("{db_col}" AS DOUBLE)
                                 END"""
                    select_items.append(SelectItem(f"ROUND({cleaned}, 2)", col))
                elif col == "posting_date":
                    select_items.append(SelectItem(f'TRY(DATE_PARSE(CAST("{db_col}" AS VARCHAR), \'%Y%m%d\'))', col))
                elif col == "company_code":
                    company_name_case = f"""CASE "{db_col}"
                        WHEN 1000 THEN 'Wave City'
                        WHEN 1100 THEN 'WMCC'
                        WHEN 1300 THEN 'Wave Estate'
                        ELSE CAST("{db_col}" AS VARCHAR)
                    END"""
                    select_items.append(SelectItem(company_name_case, "company_name"))
                else:
                    select_items.append(SelectItem(f'"{db_col}"', col))
        
        if date_filter:
            where_conditions.append(WhereCondition(date_filter))
        
        if delay_days_filter:
            min_val = delay_days_filter.get("min")
            max_val = delay_days_filter.get("max")
            condition = delay_days_filter.get("condition", "BETWEEN")
            if condition == ">":
                where_conditions.append(WhereCondition(f"TRY_CAST(Delay_days AS DOUBLE) > {min_val}"))
            elif condition == "<":
                where_conditions.append(WhereCondition(f"TRY_CAST(Delay_days AS DOUBLE) < {max_val}"))
            elif condition == ">=":
                where_conditions.append(WhereCondition(f"TRY_CAST(Delay_days AS DOUBLE) >= {min_val}"))
            elif condition == "<=":
                where_conditions.append(WhereCondition(f"TRY_CAST(Delay_days AS DOUBLE) <= {max_val}"))
            elif condition == "=":
                where_conditions.append(WhereCondition(f"TRY_CAST(Delay_days AS DOUBLE) = {min_val}"))
            elif condition == "BETWEEN" and min_val is not None and max_val is not None:
                where_conditions.append(WhereCondition(f"TRY_CAST(Delay_days AS DOUBLE) BETWEEN {min_val} AND {max_val}"))
        
        if overdue_amount_filter:
            min_val = overdue_amount_filter.get("min")
            max_val = overdue_amount_filter.get("max")
            condition = overdue_amount_filter.get("condition", ">")
            cleaned_overdue = """CASE WHEN "Overdue" LIKE '%-' 
                                THEN TRY_CAST('-' || REPLACE("Overdue", '-', '') AS DOUBLE)
                                ELSE TRY_CAST("Overdue" AS DOUBLE) END"""
            if condition == ">":
                where_conditions.append(WhereCondition(f"{cleaned_overdue} > {min_val}"))
            elif condition == "<":
                where_conditions.append(WhereCondition(f"{cleaned_overdue} < {max_val}"))
            elif condition == "BETWEEN" and min_val and max_val:
                where_conditions.append(WhereCondition(f"{cleaned_overdue} BETWEEN {min_val} AND {max_val}"))
        
        CASE_INSENSITIVE_COLS = {
            "city", "vendor_name", "document_type", "description",
            "profit_center_desc", "gl_account_desc",
            "recon_account_desc", "supplier_ref_no"
        }
        INT_CODE_TO_DESC_COL = {
            "profit_center": ("Profit_Center",   "Profit_center_desc"),
            "gl_account":    ("GL_Account",       "GL_Account_Desc"),
        }

        def _is_numeric_value(val):
            try:
                float(str(val))
                return True
            except (ValueError, TypeError):
                return False

        if filters:
            for key, value in filters.items():
                if key in INT_CODE_TO_DESC_COL:
                    code_col, desc_col = INT_CODE_TO_DESC_COL[key]
                    _strip_desc = desc_col in {"Profit_center_desc", "GL_Account_Desc"}
                    def _desc_like(col, val, strip):
                        if strip:
                            clean_val = val.replace('-', '').replace(' ', '')
                            return (f"REPLACE(REPLACE(UPPER(\"{col}\"), '-', ''), ' ', '') "
                                    f"LIKE UPPER('%{clean_val}%')")
                        return f"UPPER(\"{col}\") LIKE UPPER('%{val}%')"
                    if isinstance(value, list):
                        if all(_is_numeric_value(v) for v in value):
                            values_str = ", ".join(str(v) for v in value)
                            where_conditions.append(WhereCondition(f'"{code_col}" IN ({values_str})'))
                        else:
                            like_conds = [_desc_like(desc_col, v, _strip_desc) for v in value]
                            where_conditions.append(WhereCondition(f"({' OR '.join(like_conds)})"))
                    else:
                        if _is_numeric_value(value):
                            where_conditions.append(WhereCondition(f'"{code_col}" = {value}'))
                        else:
                            where_conditions.append(WhereCondition(_desc_like(desc_col, value, _strip_desc)))
                    continue

                col_name = column_map.get(key, key)
                is_string = key in CASE_INSENSITIVE_COLS
                if isinstance(value, list):
                    if is_string:
                        values_str = ", ".join(f"UPPER('{v}')" for v in value)
                        where_conditions.append(WhereCondition(f'UPPER("{col_name}") IN ({values_str})'))
                    else:
                        values_str = ", ".join(str(v) for v in value)
                        where_conditions.append(WhereCondition(f'"{col_name}" IN ({values_str})'))
                else:
                    if is_string:
                        where_conditions.append(WhereCondition(f'UPPER("{col_name}") LIKE UPPER(\'%{value}%\')'))
                    else:
                        where_conditions.append(WhereCondition(f'"{col_name}" = {value}'))
        
        order_by_items = []
        if order_by:
            if order_by in column_map:
                if order_by in ["overdue", "delay_days"]:
                    order_by_items.append(OrderByItem(order_by, order_direction))
                else:
                    order_by_col = column_map[order_by]
                    order_by_items.append(OrderByItem(f'"{order_by_col}"', order_direction))
        else:
            order_by_items.append(OrderByItem("overdue", order_direction or "DESC"))
        
        return SQLQuery(
            select=select_items,
            from_table=self.fact_table,
            where=where_conditions,
            group_by=[],
            order_by=order_by_items,
            limit=limit or 100
        )

    def _build_aggregate_query(
        self,
        metric: str,
        ageing_bucket: Optional[str],
        dimensions: List[str],
        date_filter: Optional[str],
        delay_days_filter: Optional[Dict],
        overdue_amount_filter: Optional[Dict],
        time_grain: Optional[str],
        is_trend: bool,
        order_by: Optional[str],
        order_direction: str,
        limit: Optional[int],
        compare_to: Optional[str],
        filters: Optional[Dict]
    ) -> SQLQuery:
        
        select_items: List[SelectItem] = []
        where_conditions: List[WhereCondition] = []
        group_by_items: List[GroupByItem] = []

        # ── period_label injection (for "X vs Y" named-period queries) ────────
        # Guard: only inject when NOT doing a time-grain breakdown.
        # When time_grain is set (day/week/month/quarter/year), the time-grain
        # column IS the period column — injecting the label on top would produce
        # a redundant "Last Week" column alongside the per-day / per-week rows.
        period_label_expr = (filters or {}).pop("__period_label_expr__", None)
        if period_label_expr and not (is_trend and time_grain):
            select_items.append(SelectItem(period_label_expr, "period"))
            group_by_items.append(GroupByItem(period_label_expr))

        # Metric selection
        # For time-grain trend queries, metrics are appended AFTER the grain columns
        # (so total_overdue is always the last column). For all other queries, append now.
        metrics_to_add = []
        if " and " in metric:
            metrics_to_add = [m.strip() for m in metric.split(" and ")]
        else:
            metrics_to_add = [metric]

        def _append_metrics():
            for single_metric in metrics_to_add:
                if single_metric in self.registry.metrics:
                    metric_def = self.registry.metrics[single_metric]
                    metric_sql = self._build_metric_expression(metric_def)
                    select_items.append(SelectItem(metric_sql, single_metric))
        
        # Ageing bucket dimension
        if ageing_bucket == "all_buckets":
            bucket_case = """
            CASE 
                WHEN TRY_CAST(Delay_days AS DOUBLE) = 0 THEN 'Not Due'
                WHEN TRY_CAST(Delay_days AS DOUBLE) BETWEEN 1 AND 15 THEN '1-15 days'
                WHEN TRY_CAST(Delay_days AS DOUBLE) BETWEEN 16 AND 30 THEN '16-30 days'
                WHEN TRY_CAST(Delay_days AS DOUBLE) BETWEEN 31 AND 45 THEN '31-45 days'
                WHEN TRY_CAST(Delay_days AS DOUBLE) BETWEEN 46 AND 60 THEN '46-60 days'
                WHEN TRY_CAST(Delay_days AS DOUBLE) BETWEEN 61 AND 90 THEN '61-90 days'
                WHEN TRY_CAST(Delay_days AS DOUBLE) BETWEEN 91 AND 180 THEN '91-180 days'
                WHEN TRY_CAST(Delay_days AS DOUBLE) BETWEEN 181 AND 360 THEN '181-360 days'
                WHEN TRY_CAST(Delay_days AS DOUBLE) > 360 THEN '360+ days'
                ELSE 'Unknown'
            END"""
            select_items.append(SelectItem(bucket_case, "ageing_bucket"))
            group_by_items.append(GroupByItem(bucket_case))
        
        # Dimensions
        for dim in dimensions:
            if dim in self.registry.dimensions:
                col = f'"{self.registry.dimensions[dim].column}"'

                if dim == "company_code":
                    company_name_case = f"""CASE {col}
                        WHEN 1000 THEN 'Wave City'
                        WHEN 1100 THEN 'WMCC'
                        WHEN 1300 THEN 'Wave Estate'
                        ELSE CAST({col} AS VARCHAR)
                    END"""
                    select_items.append(SelectItem(company_name_case, "company_name"))
                    group_by_items.append(GroupByItem(col))

                elif dim == "segment":
                    segment_case = f"""CASE {col}
                        WHEN 1  THEN 'Mall/Multiplex'
                        WHEN 2  THEN 'Cinema'
                        WHEN 10 THEN 'Common'
                        WHEN 11 THEN 'Sugar'
                        WHEN 20 THEN 'Residential'
                        WHEN 21 THEN 'Commercial'
                        WHEN 22 THEN 'Steel'
                        WHEN 33 THEN 'Distilleary'
                        WHEN 44 THEN 'Power'
                        WHEN 50 THEN 'Infrastructure'
                        WHEN 55 THEN 'Agro'
                        WHEN 60 THEN 'Trading'
                        WHEN 66 THEN 'Bio Gas'
                        WHEN 70 THEN 'Sports Management'
                        WHEN 77 THEN 'Bottles'
                        WHEN 80 THEN 'Metro'
                        WHEN 99 THEN 'Head Office HO'
                        ELSE CAST({col} AS VARCHAR)
                    END"""
                    select_items.append(SelectItem(segment_case, "segment"))
                    group_by_items.append(GroupByItem(col))

                elif dim == "recon_account":
                    select_items.append(SelectItem('"Recon_Account"',      "recon_account"))
                    select_items.append(SelectItem('"Recon_Account_Desc"', "recon_account_desc"))
                    group_by_items.append(GroupByItem('"Recon_Account"'))
                    group_by_items.append(GroupByItem('"Recon_Account_Desc"'))

                elif dim == "profit_center":
                    select_items.append(SelectItem('"Profit_Center"',      "profit_center"))
                    select_items.append(SelectItem('"Profit_center_desc"', "profit_center_desc"))
                    group_by_items.append(GroupByItem('"Profit_Center"'))
                    group_by_items.append(GroupByItem('"Profit_center_desc"'))

                elif dim == "gl_account":
                    select_items.append(SelectItem('"GL_Account"',      "gl_account"))
                    select_items.append(SelectItem('"GL_Account_Desc"', "gl_account_desc"))
                    group_by_items.append(GroupByItem('"GL_Account"'))
                    group_by_items.append(GroupByItem('"GL_Account_Desc"'))

                elif dim == "document_type":
                    select_items.append(SelectItem('"Document_Type"', "document_type"))
                    select_items.append(SelectItem('"Description"',   "description"))
                    group_by_items.append(GroupByItem('"Document_Type"'))
                    group_by_items.append(GroupByItem('"Description"'))

                elif dim == "period_label":
                    pass  # injected via __period_label_expr__ above

                else:
                    select_items.append(SelectItem(col, dim))
                    group_by_items.append(GroupByItem(col))
        
        # Vendor-wise: group by delay_days so per-vendor rows are not collapsed.
        # We only add it to GROUP BY here — the SELECT column comes from _append_metrics()
        # (the delay_days metric expression) to avoid a duplicate/ambiguous alias.
        if "vendor_no" in dimensions and "vendor_name" in dimensions:
            delay_days_expr = 'CAST(FLOOR(TRY_CAST("Delay_days" AS DOUBLE)) AS INTEGER)'
            # Strip any delay_days already in select (avoid duplicates)
            select_items = [item for item in select_items if item.alias != "delay_days"]
            group_by_items.append(GroupByItem(delay_days_expr))

        # ── Append metrics last for non-trend queries (overdue always last col) ─
        if not (is_trend and time_grain):
            _append_metrics()

        # ── Time grain for trend analysis ─────────────────────────────────────
        if is_trend and time_grain:
            date_expr = 'TRY(DATE_PARSE(CAST("Posting_Date" AS VARCHAR), \'%Y%m%d\'))'
            
            if time_grain == "year":
                fy_expr = f"""CASE 
                    WHEN MONTH({date_expr}) <= 3 THEN CONCAT(CAST(YEAR({date_expr}) - 1 AS VARCHAR), '-', SUBSTR(CAST(YEAR({date_expr}) AS VARCHAR), -2))
                    ELSE CONCAT(CAST(YEAR({date_expr}) AS VARCHAR), '-', SUBSTR(CAST(YEAR({date_expr}) + 1 AS VARCHAR), -2))
                END"""
                select_items.append(SelectItem(fy_expr, "financial_year"))
                group_by_items.append(GroupByItem(fy_expr))
                _append_metrics()
                
            elif time_grain == "quarter":
                year_expr  = f"YEAR({date_expr})"
                month_expr = f"MONTH({date_expr})"
                fy_quarter_expr = f"""CASE 
                    WHEN {month_expr} BETWEEN 4 AND 6 THEN 1
                    WHEN {month_expr} BETWEEN 7 AND 9 THEN 2
                    WHEN {month_expr} BETWEEN 10 AND 12 THEN 3
                    ELSE 4
                END"""
                fy_year_expr = f"""CASE 
                    WHEN {month_expr} <= 3 THEN {year_expr} - 1
                    ELSE {year_expr}
                END"""
                fy_display = f"""CONCAT(
                    'FY ', 
                    CAST({fy_year_expr} AS VARCHAR), 
                    '-', 
                    SUBSTR(CAST({fy_year_expr} + 1 AS VARCHAR), -2),
                    ' Q',
                    CAST({fy_quarter_expr} AS VARCHAR)
                )"""
                select_items.append(SelectItem(fy_display,       "period"))
                select_items.append(SelectItem(fy_year_expr,     "fy_year",  display=False))
                select_items.append(SelectItem(fy_quarter_expr,  "quarter",  display=False))
                group_by_items.append(GroupByItem(fy_year_expr))
                group_by_items.append(GroupByItem(fy_quarter_expr))
                _append_metrics()
                
            elif time_grain == "month":
                year_expr  = f"YEAR({date_expr})"
                month_expr = f"MONTH({date_expr})"
                month_name_expr = f"""CASE {month_expr}
                    WHEN 1 THEN 'January'
                    WHEN 2 THEN 'February'
                    WHEN 3 THEN 'March'
                    WHEN 4 THEN 'April'
                    WHEN 5 THEN 'May'
                    WHEN 6 THEN 'June'
                    WHEN 7 THEN 'July'
                    WHEN 8 THEN 'August'
                    WHEN 9 THEN 'September'
                    WHEN 10 THEN 'October'
                    WHEN 11 THEN 'November'
                    WHEN 12 THEN 'December'
                END"""
                period_display = f"CONCAT({month_name_expr}, ' ', CAST({year_expr} AS VARCHAR))"
                select_items.append(SelectItem(period_display,    "period"))
                select_items.append(SelectItem(month_name_expr,   "month_name", display=False))
                select_items.append(SelectItem(year_expr,         "year",       display=False))
                select_items.append(SelectItem(month_expr,        "month",      display=False))
                group_by_items.append(GroupByItem(year_expr))
                group_by_items.append(GroupByItem(month_expr))
                _append_metrics()

            elif time_grain == "week":
                # ── NEW: weekly grouping ──────────────────────────────────────
                # Groups by ISO week-of-year.  Label: "Week N" (e.g. "Week 10").
                # Two hidden columns (year, week_num) are used for chronological ORDER BY.
                week_num_expr   = f"WEEK({date_expr})"
                year_expr_w     = f"YEAR({date_expr})"
                week_label_expr = f"CONCAT('Week ', CAST({week_num_expr} AS VARCHAR))"
                select_items.append(SelectItem(week_label_expr, "period"))
                select_items.append(SelectItem(year_expr_w,     "year",     display=False))
                select_items.append(SelectItem(week_num_expr,   "week_num", display=False))
                group_by_items.append(GroupByItem(year_expr_w))
                group_by_items.append(GroupByItem(week_num_expr))
                _append_metrics()

            elif time_grain == "day":
                select_items.append(SelectItem(date_expr, "date"))
                group_by_items.append(GroupByItem(date_expr))
                _append_metrics()
        
        # ── WHERE conditions ──────────────────────────────────────────────────
        if date_filter:
            where_conditions.append(WhereCondition(date_filter))
        
        if delay_days_filter:
            min_val   = delay_days_filter.get("min")
            max_val   = delay_days_filter.get("max")
            condition = delay_days_filter.get("condition", "BETWEEN")
            if condition == ">":
                where_conditions.append(WhereCondition(f"TRY_CAST(Delay_days AS DOUBLE) > {min_val}"))
            elif condition == "<":
                where_conditions.append(WhereCondition(f"TRY_CAST(Delay_days AS DOUBLE) < {max_val}"))
            elif condition == ">=":
                where_conditions.append(WhereCondition(f"TRY_CAST(Delay_days AS DOUBLE) >= {min_val}"))
            elif condition == "<=":
                where_conditions.append(WhereCondition(f"TRY_CAST(Delay_days AS DOUBLE) <= {max_val}"))
            elif condition == "=":
                where_conditions.append(WhereCondition(f"TRY_CAST(Delay_days AS DOUBLE) = {min_val}"))
            elif condition == "BETWEEN" and min_val is not None and max_val is not None:
                where_conditions.append(WhereCondition(f"TRY_CAST(Delay_days AS DOUBLE) BETWEEN {min_val} AND {max_val}"))
        
        if overdue_amount_filter:
            min_val   = overdue_amount_filter.get("min")
            max_val   = overdue_amount_filter.get("max")
            condition = overdue_amount_filter.get("condition", ">")
            cleaned_overdue = """CASE WHEN "Overdue" LIKE '%-' 
                                THEN TRY_CAST('-' || REPLACE("Overdue", '-', '') AS DOUBLE)
                                ELSE TRY_CAST("Overdue" AS DOUBLE) END"""
            if condition == ">":
                where_conditions.append(WhereCondition(f"{cleaned_overdue} > {min_val}"))
            elif condition == "<":
                where_conditions.append(WhereCondition(f"{cleaned_overdue} < {max_val}"))
            elif condition == "BETWEEN" and min_val and max_val:
                where_conditions.append(WhereCondition(f"{cleaned_overdue} BETWEEN {min_val} AND {max_val}"))
        
        CASE_INSENSITIVE_COLS = {
            "city", "vendor_name", "document_type", "description",
            "profit_center_desc", "gl_account_desc",
            "recon_account_desc", "supplier_ref_no"
        }
        INT_CODE_TO_DESC_COL = {
            "profit_center": ("Profit_Center",   "Profit_center_desc"),
            "gl_account":    ("GL_Account",       "GL_Account_Desc"),
        }

        def _is_numeric_value(val):
            try:
                float(str(val))
                return True
            except (ValueError, TypeError):
                return False

        if filters:
            for key, value in filters.items():
                if key in INT_CODE_TO_DESC_COL:
                    code_col, desc_col = INT_CODE_TO_DESC_COL[key]
                    _strip_desc = desc_col in {"Profit_center_desc", "GL_Account_Desc"}
                    def _desc_like(col, val, strip):
                        if strip:
                            clean_val = val.replace('-', '').replace(' ', '')
                            return (f"REPLACE(REPLACE(UPPER(\"{col}\"), '-', ''), ' ', '') "
                                    f"LIKE UPPER('%{clean_val}%')")
                        return f"UPPER(\"{col}\") LIKE UPPER('%{val}%')"
                    if isinstance(value, list):
                        if all(_is_numeric_value(v) for v in value):
                            values_str = ", ".join(str(v) for v in value)
                            where_conditions.append(WhereCondition(f'"{code_col}" IN ({values_str})'))
                        else:
                            like_conds = [_desc_like(desc_col, v, _strip_desc) for v in value]
                            where_conditions.append(WhereCondition(f"({' OR '.join(like_conds)})"))
                    else:
                        if _is_numeric_value(value):
                            where_conditions.append(WhereCondition(f'"{code_col}" = {value}'))
                        else:
                            where_conditions.append(WhereCondition(_desc_like(desc_col, value, _strip_desc)))
                    continue

                if key in self.registry.dimensions:
                    col       = f'"{self.registry.dimensions[key].column}"'
                    dim_type  = self.registry.dimensions[key].type
                    is_ci_str = key in CASE_INSENSITIVE_COLS
                    if isinstance(value, list):
                        if is_ci_str:
                            like_conditions = [f"UPPER({col}) LIKE UPPER('%{v}%')" for v in value]
                            where_conditions.append(WhereCondition(f"({' OR '.join(like_conditions)})"))
                        else:
                            if dim_type == "string":
                                values_str = ", ".join(f"'{v}'" for v in value)
                            else:
                                values_str = ", ".join(str(v) for v in value)
                            where_conditions.append(WhereCondition(f"{col} IN ({values_str})"))
                    else:
                        if is_ci_str:
                            where_conditions.append(WhereCondition(f"UPPER({col}) LIKE UPPER('%{value}%')"))
                        elif dim_type == "string":
                            where_conditions.append(WhereCondition(f"{col} = '{value}'"))
                        else:
                            where_conditions.append(WhereCondition(f"{col} = {value}"))
        
        # ── ORDER BY ──────────────────────────────────────────────────────────
        order_by_items = []
        if order_by:
            order_by_items.append(OrderByItem(order_by, order_direction))
        elif is_trend and time_grain:
            if time_grain == "year":
                order_by_items.append(OrderByItem("financial_year", "ASC"))
            elif time_grain == "quarter":
                order_by_items.append(OrderByItem("fy_year", "ASC"))
                order_by_items.append(OrderByItem("quarter", "ASC"))
            elif time_grain == "month":
                order_by_items.append(OrderByItem("year",  "ASC"))
                order_by_items.append(OrderByItem("month", "ASC"))
            elif time_grain == "week":
                order_by_items.append(OrderByItem("year",     "ASC"))
                order_by_items.append(OrderByItem("week_num", "ASC"))
            elif time_grain == "day":
                order_by_items.append(OrderByItem("date", "ASC"))
        else:
            order_by_items.append(OrderByItem("total_overdue", order_direction or "DESC"))
        
        return SQLQuery(
            select=select_items,
            from_table=self.fact_table,
            where=where_conditions,
            group_by=group_by_items,
            order_by=order_by_items,
            limit=limit
        )
    
    def _build_metric_expression(self, metric_def):
        expr = metric_def.expression
        for m, mdef in self.registry.measures.items():
            if m in expr:
                if mdef.sql:
                    col_expr = f"{mdef.aggregation}({mdef.sql})"
                else:
                    col_expr = f'{mdef.aggregation}("{mdef.column}")'
                expr = expr.replace(m, col_expr)
        if "overdue" in metric_def.name.lower():
            expr = f"ROUND({expr}, 2)"
        return expr

class SQLRenderer:
    @staticmethod
    def render(query: SQLQuery) -> str:
        parts = []
        parts.append("SELECT " + ", ".join(f"{s.expression} AS {s.alias}" for s in query.select))
        parts.append(f"FROM {query.from_table}")
        if query.where:
            parts.append("WHERE " + " AND ".join(w.expression for w in query.where))
        if query.group_by:
            parts.append("GROUP BY " + ", ".join(g.expression for g in query.group_by))
        if query.order_by:
            parts.append("ORDER BY " + ", ".join(f"{o.expression} {o.direction}" for o in query.order_by))
        if query.limit:
            parts.append(f"LIMIT {query.limit}")
        return "\n".join(parts)
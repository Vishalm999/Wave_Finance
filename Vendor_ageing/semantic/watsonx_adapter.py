# vendor_ageing/semantic/watsonx_adapter.py

import json
import re
from typing import Dict, List, Optional, Union
from semantic.intent import AgeingIntent

# =========================
# COMPANY / PROJECT MAP
# =========================
COMPANY_MAP = {
    "wave city": 1000,
    "wmcc": 1100,
    "wave estate": 1300,
    "1000": 1000,
    "1100": 1100,
    "1300": 1300
}

# =========================
# SEGMENT MAPPING
# =========================
SEGMENT_MAP = {
    "1":["Mall","Multiplex"],
    "2":"Cinema",
    "10": "Common",
    "11":"Sugar",
    "20": "Residential",
    "21":"Commercial",
    "22":"Steel",
    "33":"Distilleary",
    "44":"Power",
    "50":"Infrastructure",
    "55":"Agro",
    "60":"Trading",
    "66":"Bio Gas",
    "70":"Sports Management",
    "77":"Bottles",
    "80":"Metro",
    "99":"Head Office HO",
}

# Reverse mapping for name to code
SEGMENT_NAME_TO_CODE = {
    "residential": 20,
    "commercial": 21,
    "steel": 22,
    "common": 10,
    "sugar": 11,
    "distilleary": 33,
    "power": 44,
    "infrastructure": 50,
    "agro": 55,
    "trading": 60,
    "bio gas": 66,
    "sports management": 70,
    "bottles": 77,
    "metro": 80,
    "head office ho": 99,
    "cinema": 2,
    "mall": 1,
    "multiplex": 1,
}

# Dimension Aliases (for grouping dimensions only, NOT for time periods)
DIMENSION_ALIASES = {
    "project": "company_code",      # "project wise" = "company code wise"
    "projects": "company_code",
    "vendor": "vendor_no",          # "vendor wise" = group by vendor
    "location": "city",             # "location wise" = "city wise"
    "locations": "city"
}

# DO NOT map these - they are time grain patterns:
# - "quarter wise", "quarterly" → is_trend: true, time_grain: "quarter"
# - "month wise", "monthly" → is_trend: true, time_grain: "month"
# - "week wise", "weekly" → is_trend: true, time_grain: "week"
# - "day wise", "daily" → is_trend: true, time_grain: "day"
# - "year wise", "yearly" → is_trend: true, time_grain: "year"

class WatsonxSemanticAdapter:
    """
    Adapter to convert Natural Language -> AgeingIntent
    """

    def __init__(
        self,
        model,
        company_map: Optional[Dict] = None,
        segment_map: Optional[Dict] = None,
    ):
        self.model = model
        self.company_map = company_map or COMPANY_MAP
        self.segment_map = segment_map or SEGMENT_MAP

    def extract_intent(self, user_query: str) -> AgeingIntent:
        prompt = self._build_enhanced_prompt(user_query)
        
        raw_text = self.model.generate_text(
            prompt=prompt,
            params={
                "max_new_tokens": 1200,
                "temperature": 0.1,
                "decoding_method": "greedy",
            }
        )

        intent_dict = self._parse_json(raw_text)
        self._validate_and_fix_intent(intent_dict, user_query)
        
        return AgeingIntent(**intent_dict, original_question=user_query)

    def _build_enhanced_prompt(self, user_query: str) -> str:
        company_info = "\n".join([f"  - '{name}' → company_code {code}" for name, code in self.company_map.items()])
        segment_info = "\n".join([f"  - code {code} → '{desc}'" for code, desc in self.segment_map.items() if isinstance(code, str) and code.isdigit()])

        # ── Dynamic date values (computed fresh on every call, never hardcoded) ──
        from datetime import date as _date
        _today            = _date.today()
        _today_yyyymmdd   = _today.strftime("%Y%m%d")
        _today_str        = _today.strftime("%B %d, %Y")
        _cur_fy           = (_today.year - 1) if _today.month <= 3 else _today.year
        _cur_fy_end       = _cur_fy + 1
        _cur_fy_start_yyyymmdd = f"{_cur_fy}0401"

        return f"""You are an expert semantic intent extractor for vendor ageing analysis.

## YOUR TASK
Convert the user's natural language query into a structured JSON intent object for ageing reports.

## !!!CRITICAL!!! - "AND" QUERIES - READ THIS FIRST!!!
When user says "Q1 and Q3" or "April and August" or "2021 and 2023":
- They want ONLY those specific periods in the output (Q1 + Q3, NOT Q1 through Q3)
- They want separate rows for each period
- MUST use custom_dates field with array of objects
- NEVER use date_range with BETWEEN - that shows ALL periods in the range!

EXAMPLES OF CORRECT PATTERNS:
- "Q1 and Q3" → custom_dates: [{{"year": 2025, "quarter": 1}}, {{"year": 2025, "quarter": 3}}]
- "August and Nov" → custom_dates: [{{"year": 2025, "month": 8}}, {{"year": 2025, "month": 11}}]
- "2021 and 2023" → custom_dates: [{{"year": 2021}}, {{"year": 2023}}]
- ALWAYS include: is_trend: true, time_grain: "quarter"/"month"/"year"

## IMPORTANT: FINANCIAL YEAR LOGIC
- Financial Year (FY) runs from April 1 to March 31
- FY 2025 means April 1, 2025 to March 31, 2026
- FY 2023 means April 1, 2023 to March 31, 2024
- Current date: {_today_str}
- Current FY: {_cur_fy} (April 1, {_cur_fy} - March 31, {_cur_fy_end})
- When user mentions just a year like "2023", they mean FY 2023 (April 1, 2023 - March 31, 2024)
- When NO year is mentioned, default to current FY (2025)

## AGEING DATA SCHEMA

### CORE COLUMNS:
  - Vendor_no, Vendor_name, Company_Code, Segment, Document_Type,
  - Profit_Centre_text, GL_Account_Desc, Delay_days, Overdue
  - Posting_Date (PRIMARY date for ageing calculation, stored as YYYYMMDD integer)
  - Note: Fiscal year is calculated from Posting_Date (FY logic: Apr-Mar)

### AGEING BUCKETS (from Delay_days):
  - Not Due (Delay_days = 0)
  - 1-15 days (1 <= Delay_days <= 15)
  - 16-30 days (16 <= Delay_days <= 30)
  - 31-45 days (31 <= Delay_days <= 45)
  - 46-60 days (46 <= Delay_days <= 60)
  - 61-90 days (61 <= Delay_days <= 90)
  - 91-180 days (91 <= Delay_days <= 180)
  - 181-360 days (181 <= Delay_days <= 360)
  - 360+ days (Delay_days > 360)

## COMPANY MAPPINGS (Projects)
{company_info}

IMPORTANT DISTINCTIONS:
- "Wave City" → Company (company_code: 1000) - use for project-wise queries


## METRICS:
  - total_overdue: SUM of Overdue amounts (rounded to 2 decimals)
  - delay_days: Delay days value (integer, handles trailing negative)
  - max_delay_days: MAX of Delay_days
  - count_vendors: Used for "count of vendors" - automatically adds customer_no and customer_name as dimensions to show per-customer breakdown with overdue amounts and delay days
  - count_rows: COUNT of all records (use for "count" of recon_account or general count queries - NOT distinct)
  - overdue_by_bucket: Overdue grouped by ageing bucket

## DIMENSIONS:
  - vendor_no, vendor_name (for per-vendor breakdown - always shows total_overdue and delay_days)
  - company_code, segment, profit_center
  - city (location), supplier_ref_no
  - recon_account, recon_account_desc, gl_account, profit_center_desc
  - gl_account_desc, document_type, description
  - ageing_bucket (derived from Delay_days)
  - ALIASES: "project" = company_code, "location" = city

## JSON OUTPUT SCHEMA
{{
  "metric": string,
  "ageing_bucket": string|null,
  "dimensions": [string, ...],
  "date_range": string,
  "custom": {{"start": string, "end": string, "date": string, "year": int}}|null,
  "custom_dates": [{{"year": int, "month": int, "day": int}}]|null,
  "delay_days_filter": {{"min": int, "max": int, "condition": string}}|null,
  "overdue_amount_filter": {{"min": number, "max": number, "condition": string}}|null,
  "is_trend": boolean,
  "time_grain": "day"|"week"|"month"|"quarter"|"year"|null,
  "compare_to": "mom"|"yoy"|null,
  "order_by": string|null,
  "order_direction": "asc"|"desc"|null,
  "limit": number|null,
  "filters": {{
    "company_code": int | [int, ...],
    "segment": int | [int, ...],
    "vendor_no": int | [int, ...]
  }},
  "query_type": "aggregate"|"list"|"count",
  "select_columns": [string, ...]|null
}}

## EXAMPLES

### Example 1: Total overdue for specific FY
User: "Show total overdue amount for 2023"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "2023",
  "custom": {{"year": 2023}},
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": "total_overdue",
  "order_direction": "desc",
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 2: Overdue with delay filter for multiple years
User: "Show overdue amount where delay is of 50 days for last two years"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "last_2_years",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": {{"min": 50, "max": 50, "condition": "BETWEEN"}},
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 3: Last quarter overdue
User: "Show total overdue amount in last quarter"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "last_quarter",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 4: Count vendors with delay > 60 (SHOWS PER-VENDOR BREAKDOWN)
User: "Show count of vendors where delay days > 60"
{{
  "metric": "count_vendors",
  "ageing_bucket": null,
  "dimensions": ["vendor_no", "vendor_name"],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": {{"min": 60, "max": null, "condition": ">"}},
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 4b: General count (use count_rows)
User: "Show count where delay is greater than one year"
{{
  "metric": "count_rows",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": {{"min": 365, "max": null, "condition": ">"}},
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 5: No year mentioned (defaults to current FY)
User: "Show total overdue by company"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": ["company_code"],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": "total_overdue",
  "order_direction": "desc",
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 6: Specific quarter and year
User: "Show overdue for Q1 of 2023"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "q1_2023",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 7: Quarter 4 with year
User: "Show overdue for quarter 4 of 2025"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "q4_2025",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 8: Last N quarters
User: "Show overdue for last 2 quarters"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "last_2_quarters",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 9: Last N months
User: "Show overdue for last 2 months"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "last_2_months",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 10: Delay with AND condition (treat as BETWEEN)
User: "Show total overdue amount in last quarter where delay is > 30 days and delay < 45 days"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "last_quarter",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": {{"min": 30, "max": 45, "condition": "BETWEEN"}},
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 11: Time period conversion (1 year = 365 days)
User: "Show overdue where delay is greater than 1 year"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": {{"min": 365, "max": null, "condition": ">"}},
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 12: Top N vendors
User: "Show top 10 vendors by overdue amount"
{{
  "metric": null,
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": "overdue",
  "order_direction": "desc",
  "limit": 10,
  "filters": {{}},
  "query_type": "list",
  "select_columns": ["vendor_no", "vendor_name", "overdue", "delay_days"]
}}

### Example 13: YoY comparison
User: "Show overdue YoY"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": true,
  "time_grain": "year",
  "compare_to": "yoy",
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 14: MoM analysis for last year
User: "Show mom analysis of last year"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "last_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": true,
  "time_grain": "month",
  "compare_to": "mom",
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 16: Multiple consecutive years → end = March 31 of (last_year + 1)
User: "Show overdue where delay is less than 50 days for 2022, 2023 and 2024"
NOTE: 3 years → start=20220401 (Apr 1, 2022), end=20250331 (Mar 31, 2025 = last_year 2024 + 1)
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "custom_range",
  "custom": {{"start": "20220401", "end": "20250331"}},
  "custom_dates": null,
  "delay_days_filter": {{"min": null, "max": 50, "condition": "<"}},
  "overdue_amount_filter": null,
  "is_trend": true,
  "time_grain": "year",
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 17: Segment name mapping
User: "Show total overdue for last 2 years where segment is Residential"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "last_2_years",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{"segment": 20}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 19: Company (Project) - Wave City
User: "Show overdue for Wave City for 2023"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "2023",
  "custom": {{"year": 2023}},
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{"company_code": 1000}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 20: Monthly breakdown (time_grain, NOT product)
User: "Show monthly overdue for wave city for 2024 where delay is between 100 and 150 days"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "2024",
  "custom": {{"year": 2024}},
  "custom_dates": null,
  "delay_days_filter": {{"min": 100, "max": 150, "condition": "BETWEEN"}},
  "overdue_amount_filter": null,
  "is_trend": true,
  "time_grain": "month",
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{"company_code": 1000}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 21: Top 10 vendor wise
User: "Show top 10 vendor wise overdue where delay is more than 50 days for last 5 years"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": ["vendor_no", "vendor_name"],
  "date_range": "last_5_years",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": {{"min": 50, "max": null, "condition": ">"}},
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": "total_overdue",
  "order_direction": "desc",
  "limit": 10,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 22: Vendor with highest overdue (aggregate, not list)
User: "Show vendor with highest overdue from Q1 of 2024"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": ["vendor_no", "vendor_name"],
  "date_range": "q1_2024",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": "total_overdue",
  "order_direction": "desc",
  "limit": 1,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 23: Top 10 vendors with highest overdue (aggregate)
User: "Show top 10 vendors with highest overdue"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": ["vendor_no", "vendor_name"],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": "total_overdue",
  "order_direction": "desc",
  "limit": 10,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 24: Quarterly comparison (Q1 and Q3) - ULTRA CRITICAL
User: "Show overdue of Q1 and Q3"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": null,
  "custom": null,
  "custom_dates": [{{"year": 2025, "quarter": 1}}, {{"year": 2025, "quarter": 3}}],
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": true,
  "time_grain": "quarter",
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 24b: Multiple quarters same year - ULTRA CRITICAL
User: "Show overdue for Q1 and Q2 and Q3"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": null,
  "custom": null,
  "custom_dates": [{{"year": 2025, "quarter": 1}}, {{"year": 2025, "quarter": 2}}, {{"year": 2025, "quarter": 3}}],
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": true,
  "time_grain": "quarter",
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 24c: Quarters across DIFFERENT fiscal years - ULTRA CRITICAL
User: "Show overdue for Q1 of 2023 and Q3 of 2024"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": null,
  "custom": null,
  "custom_dates": [{{"year": 2023, "quarter": 1}}, {{"year": 2024, "quarter": 3}}],
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": true,
  "time_grain": "quarter",
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 24d: All 4 quarters - ULTRA CRITICAL
User: "Show overdue for Q1 and Q2 and Q3 and Q4"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": null,
  "custom": null,
  "custom_dates": [{{"year": 2025, "quarter": 1}}, {{"year": 2025, "quarter": 2}}, {{"year": 2025, "quarter": 3}}, {{"year": 2025, "quarter": 4}}],
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": true,
  "time_grain": "quarter",
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 25: Multiple companies comparison
User: "Show overdue for Wave City and WMCC"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{"company_code": [1000, 1100]}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 26: City wise / Location wise
User: "Show overdue city wise"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": ["city"],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 27: Document type wise
User: "Show overdue by document type"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": ["document_type"],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 28: GL Account wise
User: "Show overdue GL account wise"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": ["gl_account"],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 29: Specific vendor by name (uses LIKE operator)
User: "Show me overdue of vendor Ajay"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": ["vendor_no", "vendor_name"],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{"vendor_name": "Ajay"}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 29b: Multiple vendors by name - CRITICAL
User: "Show me overdue for Babita and Nitesh"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": ["vendor_no", "vendor_name"],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{"vendor_name": ["Babita", "Nitesh"]}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 30: Multiple months (August and Nov)
User: "Show me overdue in August and Nov"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": null,
  "custom": null,
  "custom_dates": [{{"year": 2025, "month": 8}}, {{"year": 2025, "month": 11}}],
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": true,
  "time_grain": "month",
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 31: Multiple years (2021 and 2023)
User: "Show me overdue in 2021 and 2023"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": null,
  "custom": null,
  "custom_dates": [{{"year": 2021}}, {{"year": 2023}}],
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": true,
  "time_grain": "year",
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 32: TILL pattern without year
User: "Show vendor wise overdue till December"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": ["vendor_no", "vendor_name"],
  "date_range": "custom_range",
  "custom": {{"start": "20250401", "end": "20251231"}},
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 33: TILL pattern with year
User: "Show vendor wise overdue till August 2024"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": ["vendor_no", "vendor_name"],
  "date_range": "custom_range",
  "custom": {{"start": "20240401", "end": "20240831"}},
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example 34: FROM pattern
User: "Show overdue from August"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "custom_range",
  "custom": {{"start": "20250801", "end": "{_today_yyyymmdd}"}},
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example NEW-WEEK-VS: This week vs last week → two rows via custom_dates period keys
User: "Show overdue this week vs last week"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": null,
  "custom": null,
  "custom_dates": [{{"period": "this_week"}}, {{"period": "last_week"}}],
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}
RULE: "X vs Y" / "X and Y" for two time periods → custom_dates with period keys, NEVER a single date_range.

### Example NEW-YEAR-VS: This year vs last year → two rows via custom_dates period keys
User: "Show overdue this year vs last year"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": null,
  "custom": null,
  "custom_dates": [{{"period": "this_year"}}, {{"period": "last_year"}}],
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}
RULE: "this year vs last year" uses period keys, NOT date_range: "current_financial_year" (that gives only one row).

### Example NEW-MONTH-VS: This month vs last month → two rows via custom_dates period keys
User: "Show overdue this month vs last month"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": null,
  "custom": null,
  "custom_dates": [{{"period": "this_month"}}, {{"period": "last_month"}}],
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}

### Example NEW-MONTHWISE: "month wise" / "monthwise" = time grain month, NO other dimensions
User: "Show month wise overdue for wave city and wmcc"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": true,
  "time_grain": "month",
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{"company_code": [1000, 1100]}},
  "query_type": "aggregate",
  "select_columns": null
}}
RULE: "month wise" / "monthwise" / "monthly" ALL mean is_trend: true, time_grain: "month". dimensions MUST be [] (empty). NEVER add document_type or description as a dimension for month-wise queries.

### Example NEW-WEEKWISE: "week wise" / "weekly" = time grain week, one row per week
User: "Show weekly overdue for last month"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "last_month",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": true,
  "time_grain": "week",
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}
RULE: "week wise" / "weekwise" / "weekly" ALL mean is_trend: true, time_grain: "week". dimensions MUST be [] (empty). Result shows one row per calendar week labelled "Week N" (e.g. "Week 10", "Week 11"). NEVER use time_grain: "day" for weekly queries. NEVER use custom_dates for weekly queries — use date_range directly.

### Example NEW-DAILY: "daily overdue" = time grain day, one row per date (NO period label)
User: "Show daily overdue for last week"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "last_week",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": true,
  "time_grain": "day",
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}
RULE: "daily" / "day wise" / "date wise" means is_trend: true, time_grain: "day". NEVER use custom_dates with period keys (like "last_week") for daily queries — that generates a redundant "Last Week" label column. Always use date_range directly (e.g. "last_week", "this_week", "last_month"). Result will have columns: [date, total_overdue] — one row per posting date.

### Example NEW-THREE-COMPANIES: All three companies mentioned → all three codes in filter
User: "Show overdue for wave city, wmcc and wave estate"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": ["company_code"],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": "total_overdue",
  "order_direction": "desc",
  "limit": null,
  "filters": {{"company_code": [1000, 1100, 1300]}},
  "query_type": "aggregate",
  "select_columns": null
}}
RULE: Wave City=1000, WMCC=1100, Wave Estate=1300. When all three are mentioned, ALL THREE codes must appear in the filter list. NEVER omit any mentioned company.

### Example NEW-THREE-COMPANIES-B: All three companies with another dimension
User: "Show description wise overdue for wave city, wmcc and wave estate"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": ["document_type", "company_code"],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": "total_overdue",
  "order_direction": "desc",
  "limit": null,
  "filters": {{"company_code": [1000, 1100, 1300]}},
  "query_type": "aggregate",
  "select_columns": null
}}
RULE: "description wise" maps to dimension "document_type" (which includes the Description column automatically). All three company codes [1000, 1100, 1300] must be in the filter.

### Example NEW-BETWEEN-MONTHS: "between [month] and [month]" = continuous range, NOT discrete months
User: "show overdue between april and november"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "custom_range",
  "custom": {{"start": "20250401", "end": "20251130"}},
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": true,
  "time_grain": "month",
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}
RULE: "between X and Y" for months = continuous date range from start of X to end of Y. Use custom_range with start/end dates. NEVER use custom_dates here — that would give only two isolated months instead of the full range.

### Example NEW-PC-MULTI: Multiple profit center string names → list filter, NOT separate fields
User: "Show overdue for profit center UCHTD-CORPORATE and veridia enclave"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": ["profit_center"],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": "total_overdue",
  "order_direction": "desc",
  "limit": null,
  "filters": {{"profit_center": ["UCHTD-CORPORATE", "veridia enclave"]}},
  "query_type": "aggregate",
  "select_columns": null
}}
RULE: Both profit center names go into filters.profit_center as a LIST. Never put the second value into company_code or any other field.

### Example NEW-ALL-YEARS: Year-wise with NO year mentioned → all years in data
User: "Show yearwise overdue"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": [],
  "date_range": "all_years",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": true,
  "time_grain": "year",
  "compare_to": null,
  "order_by": null,
  "order_direction": null,
  "limit": null,
  "filters": {{}},
  "query_type": "aggregate",
  "select_columns": null
}}
RULE: "yearwise"/"year wise"/"yearly"/"year over year" with NO year specified → date_range: "all_years" (removes date filter, shows ALL financial years in the data)

### Example NEW-A: Document type filter (string code, NOT numeric)
User: "show overdue for doc type kr"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": ["document_type"],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": "total_overdue",
  "order_direction": "desc",
  "limit": null,
  "filters": {{"document_type": "KR"}},
  "query_type": "aggregate",
  "select_columns": null
}}
RULE: Document_Type values (KR, KZ, KA, AB, RE, etc.) are ALWAYS string codes, NEVER integers.

### Example NEW-B: Description filter — "vendor" in value does NOT mean vendor dimension
User: "show overdue for description vendor document"
{{
  "metric": "total_overdue",
  "ageing_bucket": null,
  "dimensions": ["document_type"],
  "date_range": "current_financial_year",
  "custom": null,
  "custom_dates": null,
  "delay_days_filter": null,
  "overdue_amount_filter": null,
  "is_trend": false,
  "time_grain": null,
  "compare_to": null,
  "order_by": "total_overdue",
  "order_direction": "desc",
  "limit": null,
  "filters": {{"description": "vendor document"}},
  "query_type": "aggregate",
  "select_columns": null
}}
RULE: "vendor" inside a description text value is NOT a vendor dimension. Never add vendor_no/vendor_name dimensions from description filter values.


## KEY RULES

0. **Count Metric Selection (CRITICAL)**:
   - "Show count of vendors..." → metric: "count_vendors", dimensions: ["vendor_no", "vendor_name"]
   - "Show count where..." → metric: "count_rows" (total row count)
   - DEFAULT: Use "count_rows" for any count EXCEPT "count of vendors"

1. **Year Extraction**:
   - "for 2023" → date_range: "2023", custom: {{"year": 2023}}
   - "in 2024" → date_range: "2024", custom: {{"year": 2024}}
   - NO year mentioned → date_range: "current_financial_year"
   - "last year" → date_range: "last_financial_year"
   - "last 2 years" → date_range: "last_2_years"
   - "last 3 years" → date_range: "last_3_years"
   - **"2021 and 2023"** (non-consecutive / specific years only):
     → is_trend: true, time_grain: "year"
     → custom_dates: [{{"year": 2021}}, {{"year": 2023}}]

   - **!!!CRITICAL!!! Consecutive multi-year ranges: "2022, 2023 and 2024"**:
     → start = April 1 of the FIRST year → "20220401"
     → end   = March 31 of (LAST year + 1) → "20250331"
     → is_trend: true, time_grain: "year"

2. **Quarter Patterns**:
   - "last quarter" → date_range: "last_quarter"
   - "last 2 quarters" → date_range: "last_2_quarters"
   - "this quarter" → date_range: "this_quarter"
   - "Q1 2023" → date_range: "q1_2023"
   - **!!!ULTRA CRITICAL!!! - "Q1 and Q3"**:
     → custom_dates: [{{"year": 2025, "quarter": 1}}, {{"year": 2025, "quarter": 3}}]
     → is_trend: true, time_grain: "quarter"

3. **Month Patterns**:
   - "last month" → date_range: "last_month"
   - "last 2 months" → date_range: "last_2_months"
   - "this month" → date_range: "this_month"
   - "last week" / "previous week" → date_range: "last_week"
   - "this week" / "current week" → date_range: "this_week"
   - **!!!CRITICAL!!! "this week vs last week"**:
     → custom_dates: [{{"period": "this_week"}}, {{"period": "last_week"}}], is_trend: false
   - **!!!ULTRA CRITICAL!!! "between [month] and [month]" = CONTINUOUS range**:
     → date_range: "custom_range", custom: {{"start": "...", "end": "..."}}, is_trend: true, time_grain: "month"
     → NEVER use custom_dates for "between X and Y"
   - **"April and August" (no "between") = two discrete months**:
     → custom_dates: [{{"year": 2025, "month": 4}}, {{"year": 2025, "month": 8}}]
     → is_trend: true, time_grain: "month"

3a. **!!!CRITICAL!!! "TILL" Patterns**:
   - "till December" (no year) → date_range: "custom_range", custom: {{"start": "20250401", "end": "20251231"}}
   - "till August 2024" → date_range: "custom_range", custom: {{"start": "20240401", "end": "20240831"}}

3a2. **!!!CRITICAL!!! "FROM" Patterns**:
   - "from August" → date_range: "custom_range", custom: {{"start": "20250801", "end": "{_today_yyyymmdd}"}}
   - "from August 2024" → date_range: "custom_range", custom: {{"start": "20240801", "end": "{_today_yyyymmdd}"}}
   - End date is ALWAYS {_today_yyyymmdd} (today)

3b. **Time Grain Patterns (CRITICAL - Not dimension aliases!)**:
   - "weekly" / "week wise" / "weekwise" → is_trend: true, time_grain: "week"
   - "daily" / "day wise" / "date wise" → is_trend: true, time_grain: "day"
   - "monthly" / "month wise" / "monthwise" → is_trend: true, time_grain: "month"
   - "quarterly" / "quarter wise" / "quarterwise" → is_trend: true, time_grain: "quarter"
   - "yearly" / "year wise" / "yearwise" → is_trend: true, time_grain: "year"
   - These show breakdown BY time period ONLY — dimensions array must stay [] (empty)
   - !!!NEVER add document_type, description, or any other dimension when time grain is set!!!
   - !!!CRITICAL!!! For "weekly" / "daily": ALWAYS use date_range directly (e.g. "last_week", "last_month").
     NEVER use custom_dates with period keys for these — that generates a "Last Week" label column
     instead of per-week / per-day rows.

3b2. **!!!CRITICAL!!! Year-wise with NO year = ALL years in data**:
   - "yearwise" / "year wise" / "yearly" / "year on year" / "YoY" with NO year → date_range: "all_years"
   - NEVER default to current_financial_year for any of these when no year is specified

3c. **Vendor Wise Patterns (CRITICAL)**:
   - "vendor wise" → query_type: "aggregate", dimensions: ["vendor_no", "vendor_name"], metric: "total_overdue and delay_days"
   - "top 10 vendor wise" → limit: 10, order_by: "total_overdue", order_direction: "desc"
   - IMPORTANT: Vendor queries MUST be aggregate (not list) to sum multiple entries per vendor

3d. **Other Dimension Patterns**:
   - "city wise" / "location wise" → dimensions: ["city"]
   - "document type wise" → dimensions: ["document_type"]
   - "GL account wise" → dimensions: ["gl_account"]
   - "recon account wise" → dimensions: ["recon_account"]
   - "profit center desc wise" → dimensions: ["profit_center_desc"]
   - "supplier ref wise" → dimensions: ["supplier_ref_no"]
   - "segment wise" → dimensions: ["segment"]

3e. **!!!CRITICAL!!! Multiple Vendor Names**:
   - "overdue for Babita and Nitesh" → filters: {{"vendor_name": ["Babita", "Nitesh"]}}
   - Always add both vendor_no and vendor_name to dimensions

3f. **!!!CRITICAL!!! Multiple Profit Center / GL Account String Values**:
   - "profit center UCHTD-CORPORATE and veridia enclave" →
     filters: {{"profit_center": ["UCHTD-CORPORATE", "veridia enclave"]}}, dimensions: ["profit_center"]
   - NEVER split them into separate filters for different fields

4. **Date Range Patterns**:
   - "between 1 Jan 2023 and 31 Mar 2023" → date_range: "custom_range", custom: {{"start": "20230101", "end": "20230331"}}
   - "between april and november" → custom_range, start="20250401", end="20251130", time_grain: "month"
   - !!!DECISION RULE!!!: "between [month] and [month]" → ALWAYS custom_range. "April and August" (no between) → ALWAYS custom_dates.

4b. **!!!ULTRA CRITICAL!!! "AND"/"VS" and Combined Filters**:
   - Companies: "Wave City and WMCC" → filters: {{"company_code": [1000, 1100]}}, dimensions: ["company_code"]
   - Quarters: "Q1 and Q3" → custom_dates with quarter objects
   - Three companies: "wave city, wmcc and wave estate" → filters: {{"company_code": [1000, 1100, 1300]}}
   - Time period "vs": custom_dates with period keys (this_week, last_week, this_year, last_year, this_month, last_month)

5. **Delay Days Filter**:
   - "delay of 50 days" → {{"min": 50, "max": 50, "condition": "="}}
   - "delay > 60" → {{"min": 60, "max": null, "condition": ">"}}
   - "delay between 30 and 45" → {{"min": 30, "max": 45, "condition": "BETWEEN"}}
   - 1 year = 365 days, 1 month = 30 days

6. **List Queries (Top N)**:
   - "Show top 10 vendors by overdue" → query_type: "list", order_by: "overdue", limit: 10

7. **Comparison Queries (QoQ/MoM/YoY)**:
   - "QoQ" → is_trend: true, time_grain: "quarter", compare_to: "qoq"
   - "MoM" → is_trend: true, time_grain: "month", compare_to: "mom"
   - "YoY" / "year on year" (NO year) → is_trend: true, time_grain: "year", date_range: "all_years"

8. **Ageing Buckets**: Use ageing_bucket: "all_buckets" when user asks for buckets/ageing analysis

9. **Default FY**: When NO time period is mentioned, use "current_financial_year"

9b. **!!!CRITICAL!!! order_by for time-based queries**:
   - When is_trend: true, ALWAYS set order_by: null
   - NEVER set order_by: "total_overdue" when is_trend is true

10. **Multiple Values with "and" (CRITICAL)**:
    - "for document KR and KZ" → dimensions: ["document_type"], filters: {{"document_type": ["KR", "KZ"]}}
    - "for company 1000 and 1100" → dimensions: ["company_code"], filters: {{"company_code": [1000, 1100]}}

10a. **!!!CRITICAL!!! Document Type Filter Rules**:
    - Document_Type values are ALWAYS short alphabetic codes like "KR", "KZ", "KA", "AB", "RE"
    - They are NEVER numeric

10b. **!!!CRITICAL!!! Description Filter Rules**:
    - "vendor" inside a description value does NOT mean add vendor dimensions
    - "for description vendor document" → filters: {{"description": "vendor document"}}

11. **Segment Name Mapping**: "Residential"→20, "Commercial"→21, "Steel"→22, "Common"→10

12. **Specific Date Queries**:
    - "on 11 aug 2023" → Use = DATE '2023-08-11'
    - "as of 11 aug 2023" → Use <= DATE '2023-08-11'

13. **Dimension Aliases**: "project wise" → dimensions: ["company_code"]

15. **Count of Vendors**:
    - "count of vendors" → metric: "count_vendors", dimensions: ["vendor_no", "vendor_name"]

USER QUERY: "{user_query}"

JSON OUTPUT:"""

    def _parse_json(self, text: str) -> Dict:
        if not text:
            raise ValueError("Empty response from model")

        cleaned = re.sub(r"```(?:json)?", "", text, flags=re.IGNORECASE).strip()
        match = re.search(r"\{[\s\S]*\}", cleaned)
        if not match:
            raise ValueError(f"No JSON object found in model output: {text}")

        json_str = match.group(0)
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON from model: {json_str}") from e

    def _validate_and_fix_intent(self, intent_dict: Dict, user_query: str = ""):
        # Ensure required fields
        if "metric" not in intent_dict:
            intent_dict["metric"] = "total_overdue"
        
        if "dimensions" not in intent_dict:
            intent_dict["dimensions"] = []
        
        if "filters" not in intent_dict:
            intent_dict["filters"] = {}
        
        if "query_type" not in intent_dict:
            intent_dict["query_type"] = "aggregate"
        
        if "date_range" not in intent_dict:
            intent_dict["date_range"] = "current_financial_year"
        
        if user_query:
            question_lower = user_query.lower()
            
            # 1. Check for company_code "and"/"vs" patterns — supports 2 OR 3 companies
            company_names_found = []
            for name in ["wave city", "wmcc", "wave estate"]:
                if name in question_lower and self.company_map.get(name) not in company_names_found:
                    company_names_found.append(self.company_map[name])
            
            if len(company_names_found) >= 2:
                intent_dict["filters"]["company_code"] = company_names_found
                if "company_code" not in intent_dict.get("dimensions", []):
                    intent_dict["dimensions"].append("company_code")
            
            # 2. Check for quarter patterns: "Q1 and Q3", "q1 vs q2"
            quarter_pattern = r'\bq([1-4])\s+(?:and|vs)\s+q([1-4])\b'
            quarter_match = re.search(quarter_pattern, question_lower)
            if quarter_match:
                if not intent_dict.get("is_trend"):
                    intent_dict["is_trend"] = True
                    intent_dict["time_grain"] = "quarter"
            
            # 3. If filters contain a list, add the key as dimension for breakdown
            if "filters" in intent_dict:
                for filter_key, filter_value in intent_dict["filters"].items():
                    if isinstance(filter_value, list) and len(filter_value) >= 2:
                        dim_key = filter_key
                        if filter_key not in intent_dict.get("dimensions", []):
                            intent_dict["dimensions"].append(dim_key)

        # Handle dimension aliases (project -> company_code)
        if "dimensions" in intent_dict:
            mapped_dims = []
            for dim in intent_dict["dimensions"]:
                if dim in DIMENSION_ALIASES:
                    mapped_dims.append(DIMENSION_ALIASES[dim])
                else:
                    mapped_dims.append(dim)
            intent_dict["dimensions"] = mapped_dims

        # Convert numeric filters and map segment names
        if "filters" in intent_dict:
            if "segment" in intent_dict["filters"]:
                value = intent_dict["filters"]["segment"]
                if isinstance(value, str) and not value.isdigit():
                    segment_lower = value.lower()
                    if segment_lower in SEGMENT_NAME_TO_CODE:
                        intent_dict["filters"]["segment"] = SEGMENT_NAME_TO_CODE[segment_lower]
                elif isinstance(value, list):
                    mapped_values = []
                    for v in value:
                        if isinstance(v, str) and not v.isdigit():
                            v_lower = v.lower()
                            if v_lower in SEGMENT_NAME_TO_CODE:
                                mapped_values.append(SEGMENT_NAME_TO_CODE[v_lower])
                        elif str(v).isdigit():
                            mapped_values.append(int(v))
                    if mapped_values:
                        intent_dict["filters"]["segment"] = mapped_values
            
            for key in ["company_code", "segment", "vendor_no"]:
                if key in intent_dict["filters"]:
                    value = intent_dict["filters"][key]
                    if isinstance(value, list):
                        intent_dict["filters"][key] = [int(v) if str(v).isdigit() else v for v in value]
                    elif isinstance(value, (str, int, float)) and str(value).isdigit():
                        intent_dict["filters"][key] = int(value)
        
        # Special handling for count_vendors metric
        if intent_dict.get("metric") == "count_vendors":
            if "vendor_no" not in intent_dict["dimensions"]:
                intent_dict["dimensions"].insert(0, "vendor_no")
            if "vendor_name" not in intent_dict["dimensions"]:
                intent_dict["dimensions"].insert(1, "vendor_name")
            intent_dict["metric"] = "total_overdue"
            intent_dict["query_type"] = "aggregate"
        
        # For vendor-wise aggregate queries, automatically add delay_days
        if (intent_dict.get("query_type") == "aggregate" and 
            "vendor_no" in intent_dict.get("dimensions", []) and 
            "vendor_name" in intent_dict.get("dimensions", [])):
            current_metric = intent_dict.get("metric", "total_overdue")
            if "delay_days" not in current_metric:
                intent_dict["metric"] = f"{current_metric} and delay_days"
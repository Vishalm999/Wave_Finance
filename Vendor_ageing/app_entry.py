# vendor_ageing/app_entry.py
import os
from dotenv import load_dotenv
from typing import Optional, Dict

from ibm_watsonx_ai import Credentials, APIClient
from ibm_watsonx_ai.foundation_models.inference import ModelInference

from semantic.orchestrator import AgeingOrchestrator
from semantic.watsonx_adapter import WatsonxSemanticAdapter

# Ageing-specific mappings
from semantic.watsonx_adapter import COMPANY_MAP #SEGMENT_MAP

# -------------------------------------------------
# Load environment variables ONCE at startup
# -------------------------------------------------
load_dotenv()

# -------------------------------------------------
# Watsonx Model Initialization (singleton)
# -------------------------------------------------
credentials = Credentials(
    url=os.getenv("WATSONX_URL"),
    api_key=os.getenv("WATSONX_API_KEY"),
)

api_client = APIClient(credentials)
api_client.set.default_project(os.getenv("WATSONX_PROJECT_ID"))

model = ModelInference(
    model_id="meta-llama/llama-3-3-70b-instruct",
    api_client=api_client,
)

# -------------------------------------------------
# Initialize Adapter + Orchestrator once
# -------------------------------------------------
adapter = WatsonxSemanticAdapter(
    model=model,
    company_map=COMPANY_MAP,
    #segment_map=SEGMENT_MAP
)

orchestrator = AgeingOrchestrator(
    model_path="semantic/model"
)

# -------------------------------------------------
# SQL Execution Function
# -------------------------------------------------
def execute_sql(sql: str) -> Dict[str, Optional[list]]:
    """
    Executes SQL in Presto and returns columns + rows.
    """
    import prestodb
    from prestodb.auth import BasicAuthentication

    conn = prestodb.dbapi.connect(
        host=os.getenv("PRESTO_HOST"),
        port=int(os.getenv("PRESTO_PORT", 443)),
        user=os.getenv("PRESTO_USERNAME"),
        catalog=os.getenv("PRESTO_CATALOG"),
        schema=os.getenv("PRESTO_SCHEMA"),
        http_scheme="https",
        auth=BasicAuthentication(
            os.getenv("PRESTO_USERNAME"),
            os.getenv("PRESTO_PASSWORD")
        ),
    )

    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [c[0] for c in cursor.description]
    rows = cursor.fetchall()

    return {"columns": columns, "rows": rows}

# -------------------------------------------------
# Main Query Runner
# -------------------------------------------------
def run_query(user_question: str, execute: bool = True) -> Dict:
    """
    1. Convert Natural Language -> SemanticIntent
    2. Build SQL using semantic layer
    3. Optionally execute SQL
    """

    # 1️⃣ Extract Semantic Intent from user query
    intent = adapter.extract_intent(user_question)

    # 2️⃣ Build SQL using orchestrator
    warnings = []
    display_columns = None   # Will hold the list of columns to show the user
    try:
        # Orchestrator returns AST + warnings
        sql_query_obj, builder_warnings = orchestrator.build_sql_from_intent(intent)
        warnings = builder_warnings or []

        if sql_query_obj:
            from semantic.sql_builder import SQLRenderer
            sql = SQLRenderer.render(sql_query_obj)
            # Capture which columns should be visible in the final response
            display_columns = sql_query_obj.display_columns or None
        else:
            sql = ""

    except Exception as e:
        sql = ""
        warnings.append(f"SQLBuilder error: {str(e)}")

    # 3️⃣ Optional: Execute SQL
    result_data = {"columns": None, "rows": None}
    if execute and sql:
        try:
            result_data = execute_sql(sql)
        except Exception as e:
            warnings.append(f"SQL execution error: {str(e)}")

    # 4️⃣ Filter out internal columns (used for ORDER BY only, not for display)
    final_columns, final_rows = filter_display_columns(
        result_data["columns"],
        result_data["rows"],
        display_columns
    )

    # 5️⃣ Compute total overdue from the DISPLAY rows (after filtering)
    total_overdue = compute_overdue_total(final_columns, final_rows)

    # 6️⃣ Prepare API-ready response
    return {
        #"question": user_question,
        "sql": sql,
        "warnings": warnings,
        "columns": final_columns,
        "rows": final_rows,
        "total_overdue": total_overdue
    }


def filter_display_columns(
    columns: Optional[list],
    rows: Optional[list],
    display_columns: Optional[list]
) -> tuple:
    """
    Strips internal/sorting-only columns from query results before
    returning to the user. The SQL itself is never modified — ORDER BY
    keeps working correctly — only the response payload is trimmed.

    Args:
        columns        : Full column list returned by the database
                         e.g. ["total_overdue", "period", "fy_year", "quarter"]
        rows           : Full row data returned by the database
        display_columns: Columns marked display=True in SQLQuery
                         e.g. ["total_overdue", "period"]
                         None means "show everything" (no filtering)

    Returns:
        (filtered_columns, filtered_rows)

    Examples:
        QoQ query   → hides fy_year, quarter      → shows total_overdue, period
        MoM query   → hides year, month, month_name → shows total_overdue, period
        YoY query   → nothing hidden               → shows total_overdue, financial_year
        Customer    → nothing hidden               → shows all customer columns
    """
    # Nothing to filter — return as-is
    if not columns or not rows or not display_columns:
        return columns, rows

    # Find which indices to keep
    keep_indices = [
        i for i, col in enumerate(columns)
        if col in display_columns
    ]

    # If every column is already display-visible, skip processing
    if len(keep_indices) == len(columns):
        return columns, rows

    # Build filtered columns
    filtered_columns = [columns[i] for i in keep_indices]

    # Build filtered rows (strip hidden values from every row)
    filtered_rows = [
        [row[i] for i in keep_indices]
        for row in rows
    ]

    return filtered_columns, filtered_rows


def compute_overdue_total(columns: list, rows: list) -> Optional[float]:
    """
    Computes the grand total of overdue from already-fetched query results.
    Works for ANY query that returns an overdue column — monthly, quarterly,
    yearly, customer-wise, sales-group-wise, etc.

    Strategy:
      - Find which column index holds overdue values by name
        (looks for 'total_overdue', 'overdue', or any column containing 'overdue')
      - Sum all non-null numeric values in that column
      - Returns None if no overdue column exists (e.g. pure list queries)

    Args:
        columns : list of column name strings  e.g. ["total_overdue", "period", ...]
        rows    : list of row tuples/lists      e.g. [[1234.5, "April 2023", ...], ...]

    Returns:
        Rounded float total, or None if not applicable.

    Examples:
        # MoM query  → sums all monthly overdue values
        # QoQ query  → sums all quarterly overdue values
        # Customer-wise → sums all customer overdue values
        # Sales-group-wise → sums all group overdue values
        # Single-value query → returns that single value as-is
    """

    # Nothing to work with
    if not columns or not rows:
        return None

    # ── Find the overdue column index ──────────────────────────────────────────
    # Priority 1: exact match "total_overdue"
    # Priority 2: exact match "overdue"
    # Priority 3: any column whose name contains "overdue"
    overdue_col_index = None

    cols_lower = [c.lower() for c in columns]

    if "total_overdue" in cols_lower:
        overdue_col_index = cols_lower.index("total_overdue")
    elif "overdue" in cols_lower:
        overdue_col_index = cols_lower.index("overdue")
    else:
        # Fallback: first column that contains the word "overdue"
        for i, col in enumerate(cols_lower):
            if "overdue" in col:
                overdue_col_index = i
                break

    # No overdue column found in this query (e.g. count-only queries)
    if overdue_col_index is None:
        return None

    # ── Sum all values in that column ─────────────────────────────────────────
    total = 0.0
    for row in rows:
        value = row[overdue_col_index]
        if value is not None:
            try:
                total += float(value)
            except (TypeError, ValueError):
                # Skip non-numeric cells gracefully
                pass

    return round(total, 2)
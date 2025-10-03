import azure.functions as func
import logging
import pyodbc
import json
import re
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

clientId = "5ff915a7-f48f-42aa-aee0-ebd9763dee2e"
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
server_name = "ygwzzxhcfjfezazar2k3aa2lsy-dcjdhmjmsclu7h2ae2fzwpacuq.datawarehouse.fabric.microsoft.com"
database_name = "fut_scraper"

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server_name};"
    f"DATABASE={database_name};"
    f"UID={clientId};"
    f"PWD={CLIENT_SECRET};"
    f"Authentication=ActiveDirectoryServicePrincipal"
)


def build_paginated_sql(sql_statement: str, page: int, page_size: int) -> str:
    # Detect ORDER BY in the original SQL
    order_match = re.search(r"order\s+by[\s\S]*$", sql_statement, flags=re.IGNORECASE)
    user_order = order_match.group(0) if order_match else "ORDER BY (SELECT NULL)"

    # Remove ORDER BY from the inner query
    sql_without_order = re.sub(r"order\s+by[\s\S]*$", "", sql_statement, flags=re.IGNORECASE).strip()

    # Final paginated SQL
    paginated_sql = f"""
    WITH base_query AS (
        {sql_without_order}
    )
    SELECT *
    FROM base_query
    {user_order}
    OFFSET {(page - 1) * page_size} ROWS
    FETCH NEXT {page_size} ROWS ONLY;
    """
    return paginated_sql


@app.function_name(name="queryLakehouse")
@app.route(route="queryLakehouse", methods=["POST"])
def query_lakehouse(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing Lakehouse POST request")

    try:
        body = req.get_json()
        sql_statement = body.get("sql")
        page = int(body.get("page", 1))
        page_size = int(body.get("pageSize", 50))

        if not sql_statement:
            return func.HttpResponse("Missing SQL statement", status_code=400)

        # Enforce only SELECTs for safety
        if not sql_statement.strip().lower().startswith("select"):
            return func.HttpResponse("Only SELECT queries are allowed", status_code=400)

        # Cap page size to prevent abuse
        page_size = min(page_size, 200)

        # Build safe paginated SQL
        paginated_sql = build_paginated_sql(sql_statement, page, page_size)

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(paginated_sql)
        rows = cursor.fetchall()

        columns = [col[0] for col in cursor.description]
        result = [dict(zip(columns, row)) for row in rows]

        return func.HttpResponse(
            json.dumps({
                "page": page,
                "pageSize": page_size,
                "rows": result
            }, default=str),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error querying Fabric Lakehouse: {e}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

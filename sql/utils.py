from pathlib import Path

def get_sql(query_name: str) -> str:
    """ Loads a SQL file from /sql directory. """
    sql_dir = Path(__file__).resolve().parents[0]  # points to /sql/
    file_path = sql_dir / query_name
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()
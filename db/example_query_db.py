import sys

sys.path.append("../")
from rafael.utils import SqlLiteConnection
from rafael.config import DB_FILE

if __name__ == "__main__":
    query = "select * from ads"
    conn = SqlLiteConnection(DB_FILE)
    df = conn.query_db(query)
    conn.close_connection()
    print("shape:", df.shape)
    print("columns:", df.columns)

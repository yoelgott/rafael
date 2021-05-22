from flask import Flask, request
from flask_restful import Resource, Api

import sys

sys.path.append("../")
from rafael.utils import SqlLiteConnection
from rafael.config import DB_FILE, RESULTS_TABLE_NAME

app = Flask(__name__)
api = Api(app)


def get_ads(top=None):
    col_name = "sorting_step1"
    query = f"select {col_name} from {RESULTS_TABLE_NAME}"
    if top is not None:
        query += f" limit {top}"
    conn = SqlLiteConnection(DB_FILE)
    df = conn.query_db(query)
    conn.close_connection()
    return df[col_name].to_list()


class Rafael(Resource):
    def get(self):
        top = request.headers.get("TOP")
        try:
            top = int(str(top).strip())
        except:
            top = None
        ads = get_ads(top)
        return {"ads": ads}


api.add_resource(Rafael, "/get-ads")

if __name__ == "__main__":
    app.run(debug=True)

from flask import Flask, Response, stream_with_context
import time
import uuid
import random

app = Flask(__name__)

# creating a mock end point
@app.route("/large_datastore/<int:rowcount>", methods=['GET'])
def get_large_date(rowcount):
    """ returns N rows of data"""
    @stream_with_context
    def n_large():
        """ This function is the generator of the mock data """
        for i in range(rowcount):
            time.sleep(.01)
            txid = uuid.uuid4()
            uid = uuid.uuid4()
            amount = round(random.uniform(-3000, 1000), 2)
            yield f"({txid}, {uid}, {amount})\n"
            print(amount)

    return Response(n_large())

if __name__ == "__main__":
    app.run(debug=True)
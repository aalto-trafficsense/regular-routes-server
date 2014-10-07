from flask import Flask
from views import *

app = Flask(__name__)

DataCollectorView.register(app)

@app.route('/')
def start_page():
    return "Server is up and running!"


if __name__ == '__main__':
    app.run(debug=True)



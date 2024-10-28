import random
from cassandra.cluster import Cluster
from flask import (Flask, request, jsonify)

def create_app():
    app = Flask(__name__)
    app.json.sort_keys = False
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()

    # Register a new channel
    @app.route('/channels', methods=['PUT'])
    def put_channel():
        pass

    # Get channel by ID.
    @app.route('/channels/<channelId>', methods=['GET'])
    def get_channel(channelId):
        pass

    # Delete channel by ID.
    @app.route('/channels/<channelId>', methods=['DELETE'])
    def delete_channel(channelId):
        pass

    # Add message to channel.
    @app.route('/channels/<channelId>/messages', methods=['PUT'])
    def put_message():
        pass

    return app
import random
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from flask import (Flask, request, jsonify)

def create_app():
    app = Flask(__name__)
    app.json.sort_keys = False
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS chat WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}")
    session.set_keyspace('chat')
    session.row_factory = dict_factory
    session.execute("CREATE TABLE IF NOT EXISTS channels (id text PRIMARY KEY, owner text, topic text)")
    session.execute("CREATE TABLE IF NOT EXISTS messages (message_id text, channel_id text, text text, author text, PRIMARY KEY ((message_id), channel_id, author))")
    session.execute("CREATE TABLE IF NOT EXISTS members (member_id text, channel_id text, member text, PRIMARY KEY ((member_id), channel_id, member))")
    
    session.execute("CREATE TABLE IF NOT EXISTS messages_by_channel (message_id text, channel_id text, text text, author text, PRIMARY KEY ((message_id, channel_id), author))")
    session.execute("CREATE TABLE IF NOT EXISTS members_by_channel (member_id text, channel_id text, member text, PRIMARY KEY ((member_id), channel_id), member)")

    # Register a new channel [NOTE] in progress
    @app.route('/channels', methods=['PUT'])
    def put_channel():
        reqBody = request.json
        id = reqBody.get("id")
        owner = reqBody.get("owner")
        topic = reqBody.get("topic")
        print(id)
        if id != None and owner != None:
            session.execute(f"INSERT INTO channels (id, owner, topic) VALUES ('{id}', '{owner}', '{topic}') IF NOT EXISTS")
            return {"id": id}, 201
        return {"message": "Invalid input, missing name or owner. Or the channel with such id already exists."}, 400

    # Get channel by ID.
    @app.route('/channels/<channelId>', methods=['GET'])
    def get_channel(channelId):
        rows = session.execute(f"SELECT * FROM channels WHERE id='{channelId}'")
        if rows:
            channel = {
                "id": rows[0]['id'],
                "owner": rows[0]['owner'],
                "topic": rows[0]['topic']
            }

            return channel, 200 
        return {"message": "Channel not found"}, 404
        
    # Delete channel by ID. [NOTE] in progress
    @app.route('/channels/<channelId>', methods=['DELETE'])
    def delete_channel(channelId):
        rows = session.execute(f"SELECT * FROM channels WHERE id='{channelId}'")
        if rows:
            session.execute(f"DELETE FROM channels WHERE id='{channelId}'")
            return {"message": "channel deleted"}, 204
        return {"message": "Channel not found"}, 404

    # Add message to channel. [NOTE] in progress
    @app.route('/channels/<channelId>/messages', methods=['PUT'])
    def put_message(channelId):
        reqBody = request.json
        text = reqBody.get("text")
        author = reqBody.get("author")
        rows = session.execute(f"SELECT * FROM channels WHERE id='{channelId}'")
        if text != None and author != None and rows:
                session.execute(f"INSERT INTO messages (id, text, author) VALUES ('{channelId}', '{text}', '{author}') IF NOT EXISTS")
                return {"message": "Message added"}, 201
        return {"message": "Invalid input, missing text or author"}, 400
    
    return app
from flask import Flask, request, jsonify, make_response
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime
from cassandra.query import dict_factory

app = Flask(__name__)
app.json.sort_keys = False

cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect()

session.execute('''
    CREATE KEYSPACE IF NOT EXISTS chat
    WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
    }
''')
session.set_keyspace('chat')

session.row_factory = dict_factory

session.execute ('''
    CREATE TABLE IF NOT EXISTS channels (
        id TEXT PRIMARY KEY,
        owner TEXT,
        topic TEXT 
    )
''')

session.execute ('''
    CREATE TABLE IF NOT EXISTS members (
        channel_id TEXT,
        member_id TEXT,
        PRIMARY KEY (channel_id, member_id)
    )
''')

session.execute ('''
    CREATE TABLE IF NOT EXISTS messages (
        channel_id TEXT,
        author TEXT,
        text TEXT,
        timestamp BIGINT,
        PRIMARY KEY (channel_id, timestamp)
    )
''')

session.execute('''
    CREATE TABLE IF NOT EXISTS messages_by_author (
        channel_id text,
        author text,
        text text,
        timestamp BIGINT,
        PRIMARY KEY ((channel_id, author), timestamp)
    ) 
''')

# Register a new channel [NOTE] done
@app.route('/channels', methods=['PUT'])
def put_channel():
    reqBody = request.json
    id = reqBody.get("id")
    owner = reqBody.get("owner")
    topic = reqBody.get("topic")
    if id != None and owner != None:
        session.execute(f"INSERT INTO channels (id, owner, topic) VALUES ('{id}', '{owner}', '{topic}') IF NOT EXISTS")
        session.execute(f"INSERT INTO members (channel_id, member_id) VALUES ('{id}', '{owner}') IF NOT EXISTS")
        return {"id": id}, 201
    return {"message": "Invalid input, missing name or owner. Or the channel with such id already exists."}, 400

# Get channel by ID. [NOTE] done
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
    
# Delete channel by ID. [NOTE] done?
@app.route('/channels/<channelId>', methods=['DELETE'])
def delete_channel(channelId):
    rows = session.execute(f"SELECT * FROM channels WHERE id='{channelId}'")
    if rows:
        session.execute(f"DELETE FROM channels WHERE id ='{channelId}' IF EXISTS")
        # session.execute(f"DELETE FROM members WHERE channel_id ='{channelId}' IF EXISTS")
        
        rows = session.execute(f"SELECT * FROM messages WHERE channel_id='{channelId}'")
        for row in rows:
            session.execute(f"DELETE FROM messages_by_author WHERE channel_id ='{channelId}' AND author ='{row['author']}' AND timestamp = {row['timestamp']} IF EXISTS")
            session.execute(f"DELETE FROM messages WHERE channel_id ='{channelId}' AND timestamp = {row['timestamp']} IF EXISTS")
        
        rows = session.execute(f"SELECT * FROM members WHERE channel_id ='{channelId}'")
        for row in rows:
            session.execute(f"DELETE FROM members WHERE channel_id ='{channelId}' AND member_id ='{row['member_id']}' IF EXISTS")


        return {"message": "Channel deleted"}, 204
    return {"message": "Channel not found"}, 404

# Add message to channel. [NOTE] done
@app.route('/channels/<channelId>/messages', methods=['PUT'])
def put_message(channelId):
    reqBody = request.json
    text = reqBody.get("text")
    author = reqBody.get("author")
    rows = session.execute(f"SELECT * FROM channels WHERE id='{channelId}'")
    if text != None and author != None and rows:
            timestamp = int(datetime.timestamp(datetime.now())*1000)
            session.execute(f"INSERT INTO messages (channel_id, author, text, timestamp) VALUES ('{channelId}', '{author}', '{text}', {timestamp}) IF NOT EXISTS")
            session.execute(f"INSERT INTO messages_by_author (channel_id, author, text, timestamp) VALUES ('{channelId}', '{author}', '{text}', {timestamp}) IF NOT EXISTS")
            return {"message": "Message added"}, 201
    return {"message": "Invalid input, missing text or author"}, 400

@app.route('/channels/<channelId>/messages', methods=['GET'])
def get_msg(channelId):
    start_at = request.args.get('startAt', type=int)
    author = request.args.get('author', type=str)

    query = "SELECT text, author, timestamp FROM"
    params = [channelId]

    if (start_at and author):
        query += " messages_by_author WHERE channel_id = %s AND author = %s AND timestamp >= %s"
        params.append(author)
        params.append(start_at)
    elif start_at:
        query += " messages WHERE channel_id = %s AND timestamp >= %s"
        params.append(start_at)
    elif author:
        query += " messages_by_author WHERE channel_id = %s AND author = %s"  
        params.append(author)
    else:
        query += " messages WHERE channel_id = %s"

    messages = session.execute(SimpleStatement(query), params)
    result = [{"text": msg['text'], "author": msg['author'], "timestamp": msg['timestamp']} for msg in messages]

    return jsonify(result), 200

@app.route('/channels/<channelId>/members', methods=['PUT'])
def add_mem(channelId):
    data = request.json
    member = data.get("member")

    if not member:
        return make_response("Invalid input, missing member.", 400)
    
    existing = session.execute("INSERT INTO members (channel_id, member_id) VALUES (%s, %s) IF NOT EXISTS",
        (channelId, member))
    
    if existing.one()['[applied]']:
        return "", 201
    else:
        return make_response("Member already in the channel.", 400)

@app.route('/channels/<channelId>/members', methods=['GET'])
def get_mems(channelId):
    members = session.execute("SELECT member_id FROM members WHERE channel_id = %s",
        (channelId,))
    if not members:
        return make_response("Channel not found", 404)

    result = [member['member_id'] for member in members]
    return jsonify(result), 200

@app.route('/channels/<channelId>/members/<memberId>', methods=['DELETE'])
def del_mem(channelId, memberId):
    result = session.execute("DELETE FROM members WHERE channel_id = %s AND member_id = %s IF EXISTS",
        (channelId, memberId))
    if not result.one()['[applied]']:
        return make_response("Member not found", 404)

    return "", 204

if __name__ == '__main__':
    app.run(debug=True)

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import mysql.connector
import datetime

from logger import Logger

app = Flask(__name__)
CORS(app)

# For testing purposes, you can set the server ID as an environment variable while running the container instance of the server.
# os.environ['SERVER_ID'] = '1231'
# Log Entry will follow the format: 
# timestamp: 


'''
(/config,method=POST): This endpoint initializes the shard tables in the server database after the container
is loaded. The shards are configured according to the request payload.
Sample Payload Json:
{
    "schema": {"columns":["Stud_id","Stud_name","Stud_marks"],
                "dtypes": ["Number","String","String"]},
    "shards": ["sh1","sh2"]
}
Response Json:
{
    "message" : "Server0:sh1, Server0:sh2 configured",
    "status" : "success"
},
Response Code = 200
''' 
@app.route('/config', methods=['POST'])
def config():
    mydb = mysql.connector.connect(
        host="localhost", 
        user="root",
        password="abc",
        database="shards")
    mycursor = mydb.cursor(dictionary=True)
    data = request.json
    shards = data['shards']
    schema = data['schema']
    columns = schema['columns']
    dtypes = schema['dtypes']
    server_id = os.environ['SERVER_ID']
    for i in range(len(dtypes)):
        if dtypes[i] == 'Number':
            dtypes[i] = 'INT'
        elif dtypes[i] == 'String':
            dtypes[i] = 'VARCHAR(255)'
    for shard in shards:
        logger[shard] = Logger(server_id, shard)
        mycursor.execute(f'CREATE TABLE {shard} (Stud_id INT PRIMARY KEY, Stud_name VARCHAR(255), Stud_marks INT);')
        
    mydb.commit()
    mycursor.close()
    mydb.close()
    message = ''
    for i in range(len(shards)):
        message += 'Server{}:{}'.format(server_id, shards[i])
        if i != len(shards) - 1:
            message += ', '
    message += ' configured'
    return jsonify({'message': message, 'status': 'success'}), 200


'''
(/heartbeat,method=GET): This endpoint is used for health checks of the server instance.
No request payload is required and it returns an empty response with a 200 status code.
'''
@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    return '', 200

'''
(/copy,method=POST): This endpoint returns all the data from the shard table in the server database.
Sample Payload Json:
{
    "shards":["sh1","sh2"]
}
Response Json:
{
    "sh1" : [{"Stud_id":1232,"Stud_name":ABC,"Stud_marks":25},
            {"Stud_id":1234,"Stud_name":DEF,"Stud_marks":28},
            ....],
    "sh2" : [{"Stud_id":2255,"Stud_name":GHI,"Stud_marks":27},
            {"Stud_id":2535,"Stud_name":JKL,"Stud_marks":23},
            ....],
    "status" : "success"
},
Response Code = 200
'''
@app.route('/copy', methods=['GET'])
def copy():
    mydb = mysql.connector.connect(
        host="localhost", 
        user="root",
        password="abc",
        database="shards")
    mycursor = mydb.cursor(dictionary=True)
    data = request.json
    shards = data['shards']
    response = {}
    for shard in shards:
        mycursor.execute("SELECT * FROM {};".format(shard))
        response[shard] = mycursor.fetchall()
    response['status'] = 'success'
    mycursor.close()
    mydb.close()
    return jsonify(response), 200

'''
(/read,method=POST): This endpoint returns the data from a shard. It expects a range of Stud_ids and the shard name.
Sample Payload Json:
{
    "shard":"sh2",
    "Stud_id": {"low":2235, "high":2555}
}
Response Json:
{
    "data" : [{"Stud_id":2535,"Stud_name":JKL,"Stud_marks":23},
            {"Stud_id":2536,"Stud_name":MNO,"Stud_marks":22},
            .....,
            {"Stud_id":2554,"Stud_name":XYZ,"Stud_marks":25},
            {"Stud_id":2255,"Stud_name":GHI,"Stud_marks":27}],
    "status" : "success"
},
Response Code = 200
'''
@app.route('/read', methods=['POST'])
def read():
    mydb = mysql.connector.connect(
        host="localhost", 
        user="root",
        password="abc",
        database="shards")
    mycursor = mydb.cursor(dictionary=True)
    data = request.json
    shard = data['shard']
    low = data['Stud_id']['low']
    high = data['Stud_id']['high']
    mycursor.execute("SELECT * FROM {} WHERE Stud_id BETWEEN {} AND {};".format(shard, low, high))
    response = mycursor.fetchall()
    mycursor.close()
    mydb.close()
    response = response if (len(response) > 0) else []
    return jsonify({'data': response, 'status': 'success'}), 200


def logQuery(shard, query, mode):
    try:
        if mode == 'log':
            logger[shard].addLogEntry(query)
        elif mode == 'execute':
            executeSQLQuery(query)
            logger[shard].addCommitEntry(query)
        elif mode == 'log_execute':
            logger[shard].addLogEntry(query)
            executeSQLQuery(query)
            logger[shard].addCommitEntry(query)
    except Exception as e:
        print(f'SERVER: {e}')
        return False
    return True


'''
(/write,method=POST): This endpoint writes the data to the shard table in the server database. It expects multiple entries and updates a curr_idx with the number of entries.
Sample Payload Json:
{
    "shard":"sh2",
    "curr_idx": 507
    "data": [{"Stud_id":2255,"Stud_name":GHI,"Stud_marks":27}, ...] /* 5 entries */
}
Response Json:
{
    "message": "Data entries added",
    "current_idx": 512, /* 5 entries added */
    "status" : "success"
},
Response Code = 200
'''
@app.route('/write', methods=['POST'])
def write():
    mydb = mysql.connector.connect(
        host="localhost", 
        user="root",
        password="abc",
        database="shards")
    mycursor = mydb.cursor(dictionary=True)
    data = request.json
    shard = data['shard']
    curr_idx = data['curr_idx']
    entries = data['data']
    mode = data['mode']
    for entry in entries:
        columns = ', '.join(entry.keys())
        values = ', '.join(['"{}"'.format(str(x)) for x in entry.values()])
        query = 'INSERT INTO {} ({}) VALUES ({});'.format(shard, columns, values)
        if not logQuery(shard, query, mode):
            return jsonify({'message': 'Error in logging query', 'status': 'failure'}), 500
        
    mydb.commit()
    mycursor.close()
    mydb.close()
    return jsonify({'message': 'Data entries added', 'current_idx': curr_idx + len(entries), 'status': 'success'}), 200


'''
(/update,method=POST): This endpoint updates the data in the shard table in the server database. It expects a Stud_id, shard name and the updated data.
Sample Payload Json:
{
    "shard":"sh2",
    "Stud_id":2255,
    "data": {"Stud_id":2255,"Stud_name":GHI,"Stud_marks":28} /* see marks got updated */
}
Response Json:
{
    "message": "Data entry for Stud_id:2255 updated",
    "status" : "success"
},
Response Code = 200
'''
@app.route('/update', methods=['PUT'])
def update():
    mydb = mysql.connector.connect(
        host="localhost", 
        user="root",
        password="abc",
        database="shards")
    mycursor = mydb.cursor(dictionary=True)
    data = request.json
    shard = data['shard']
    Stud_id = data['Stud_id']
    entry = data['data']
    update = ', '.join(['{} = "{}"'.format(k, v) for k, v in entry.items()])
    mycursor.execute("UPDATE {} SET {} WHERE Stud_id = {}".format(shard, update, Stud_id))
    mydb.commit()
    mycursor.close()
    mydb.close()
    return jsonify({'message': 'Data entry for Stud_id:{} updated'.format(Stud_id), 'status': 'success'}), 200


'''
(/delete,method=POST): This endpoint deletes the data from the shard table in the server database. It expects a Stud_id and the shard name.
Sample Payload Json:
{
    "shard":"sh2",
    "Stud_id":2255
}
Response Json:
{
    "message": "Data entry with Stud_id:2255 removed",
    "status" : "success"
},
Response Code = 200
'''
@app.route('/del', methods=['DELETE'])
def delete():
    mydb = mysql.connector.connect(
        host="localhost", 
        user="root",
        password="abc",
        database="shards")
    mycursor = mydb.cursor(dictionary=True)
    data = request.json
    shard = data['shard']
    Stud_id = data['Stud_id']
    mycursor.execute("DELETE FROM {} WHERE Stud_id = {}".format(shard, Stud_id))
    mydb.commit()
    mycursor.close()
    mydb.close()
    return jsonify({'message': 'Data entry with Stud_id:{} removed'.format(Stud_id), 'status': 'success'}), 200

@app.route('/vote/<shard_id>', methods=['GET'])
def vote(shard_id):
    # get the election index from logger object of the shard id
    electionIndex = logger[shard_id].getElectionIndex()
    return jsonify({'vote': electionIndex}), 200

@app.route('/logs/<shard_id>', methods=['GET'])
def logs(shard_id):
    # get the logs from logger object of the shard id
    logs = logger[shard_id].getLogs()
    return jsonify({'logs': logs}), 200

def executeSQLQuery(query):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="abc",
        database="shards")
    mycursor = mydb.cursor(dictionary=True)
    mycursor.execute(query)
    response = mycursor.fetchall()
    mydb.commit()
    mycursor.close()
    mydb.close()
    return response

@app.route('/replicate/<shard_id>', methods=['POST'])
def replicate(shard_id):
    payload = request.json
    logs = payload['logs']

    # delete the existing logger object
    del logger[shard_id]

    # create a new log file
    logger[shard_id] = Logger(os.environ['SERVER_ID'], shard_id)
    for log in logs:
        # parse the log into components: "timestamp: log_id: message: checksum"
        # timestamp is of the form "YYYY-MM-DD HH:MM:SS"
        print(f'SERVER: {log}')
        components = log.split(': ')
        checksum = components[-1]
        message = components[-2]
        log_id = components[-3]
        print(f'SERVER: {log_id} {message} {checksum}')
        
        # check if the log is valid
        if hash(f'{log_id}: {message}') != int(checksum):
            print('SERVER: Invalid log entry')
            continue
            
        # if message == "COMMIT" then execute the query
        # else add the log entry to uncommited logs
        if message == 'COMMIT':
            logger[shard_id].addCommitEntry(message)
            executeSQLQuery(message)
        else:
            logger[shard_id].addLogEntry(message)
    return jsonify({'message': 'Logs replicated successfully'}), 200


if __name__ == '__main__':
    
    # Connect to the MySQL database
    mydb = mysql.connector.connect(
        host="localhost", 
        user="root",
        password="abc")
    mycursor = mydb.cursor(dictionary=True)
    # Create a database to store the shards
    # if database exists clear the database
    mycursor.execute("DROP DATABASE IF EXISTS shards;")
    mycursor.execute("CREATE DATABASE shards;")
    mycursor.execute("USE shards;")
    print("Database created")
    mycursor.close()
    mydb.close()

    logger = {}

    # create a logs directory
    os.makedirs('logs', exist_ok=True)

    app.run(host='0.0.0.0', port=5000, debug=True)

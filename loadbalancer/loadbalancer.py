from flask import Flask, request, jsonify
import json
import requests
from flask_cors import CORS
import os
import string
import random
import sys
import threading
import time
import mysql.connector


sys.path.append('../utils')
from hashring import HashRing
write_results = {}
write_results_lock = threading.Lock()

app = Flask(__name__)
CORS(app)
replica_lock = threading.Lock() # Lock for the replicas list
shard_to_hr = {} # This dictionary will map the shard_id to hashring. key: shard_id, value: (hashring)
shard_to_hrlock = {} # Lock for the shard_to_hr dictionary
shard_datalock = {} # Lock for the shard data
server_locks = {} # Locks for the servers
# shardid_to_idx = {} # This dictionary will map the shard_id to the index in the shards list
# shardid_to_idxlock = {} # Locks for the shardid_to_idx dictionary

'''
This function is called when a new server is added to the load balancer. It creates a new container with the server image and adds it to the hashring.
'''
def add_servers(n, shard_mapping, mycursor):
    global replicas
    hostnames = []
    for server in shard_mapping:
        hostnames.append(server)

    replica_names = []
    replica_lock.acquire()
    for replica in replica_names:
        replica_names.append(replica[0])

    # We go through the list of preferred hostnames and check if the hostname already exists, or if no hostname is provided, we generate a random hostname   
    for i in range(n):
        if (i >= len(hostnames)) or (hostnames[i] in replica_names):
            for j in range(len(replica_names)+1):
                new_name = 'S'+ str(j)
                if new_name not in replica_names:
                    hostnames.append(new_name)
                    replica_names.append(new_name)
                    break
        elif hostnames[i] not in replica_names:
            replica_names.append(hostnames[i])

    # Spawn the containers from the load balancer
    for i in range(n):
        container_name = "Server_"
        serverid = -1
        # Allocate the first free server ID between 1 and num_servers
        if len(server_ids) == 0:
            global next_server_id
            serverid = next_server_id
            next_server_id += 1
        else:
            serverid = min(server_ids)
            server_ids.remove(min(server_ids))
        # Generate the container name: Server_<serverid>
        container_name += str(serverid)
        container = os.popen(f'docker run --name {container_name} --network mynet --network-alias {container_name} -e SERVER_ID={serverid} -d serverim:latest').read()
        # wait for the container to start and initialise
        time.sleep(5)
        server_locks[container_name] = threading.Lock()

        if len(container) == 0:
            replica_lock.release()
            return jsonify({'message': 'Server creation failed', 'status': 'failure'}), 400 
        
        replicas.append([hostnames[i], container_name])
        shards_list = shard_mapping[hostnames[i]]
        shards_list = sorted(shards_list)
        try:
            data_payload = {}
            data_payload['schema'] = studT_schema
            data_payload['shards'] = shards_list
            reply = requests.post(f'http://{container_name}:{serverport}/config', 
                                    json = data_payload)
            time.sleep(3)
            if reply.status_code != 200:
                replica_lock.release()
                return jsonify({'message': 'Server configuration failed', 'status': 'failure'}), 400
        except requests.exceptions.ConnectionError:
            replica_lock.release()
            return jsonify({'message': 'Server configuration failed. Connection Error', 'status': 'failure'}), 400
        
        for shard in shards_list:
            mycursor.execute(f"INSERT INTO MapT VALUES ('{shard}', {serverid});")
            # Add the server to the hashring
            shard_to_hrlock[shard].acquire()
            shard_to_hr[shard].add_server(container_name)
            shard_to_hrlock[shard].release()

    replica_lock.release()
    return jsonify({'message': 'Servers spawned and configured', 'status': 'success'}), 200
    
'''
(/init, method=POST): This endpoint initializes the distributed database across different shards and replicas in  the  server  containers
Sample Payload:
{
    "N":3
    "schema":{"columns":["Stud_id","Stud_name","Stud_marks"],"dtypes":["Number","String","String"]}
    "shards":[{"Stud_id_low":0, "Shard_id": "sh1", "Shard_size":4096},
              {"Stud_id_low":4096, "Shard_id": "sh2", "Shard_size":4096},
              {"Stud_id_low":8192, "Shard_id": "sh3", "Shard_size":4096},]
    "servers":{"Server0":["sh1","sh2"],
               "Server1":["sh2","sh3"],
               "Server2":["sh1","sh3"]}
}

Sample response:
{
    "message": "Configured Database",
    "status": "success"
}    
'''
@app.route('/init', methods=['POST'])
def init():
    content = request.json
    
    if 'N' not in content or 'schema' not in content or 'shards' not in content or 'servers' not in content:
        message = '<ERROR> N, schema, shards or servers not present in request'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    n = content['N'] # Number of servers
    global studT_schema
    studT_schema = content['schema'] # Schema of the database
    shards = content['shards'] # Shards with Stud_id_low, Shard_id, Shard_size
    shard_mapping = content['servers'] # Servers with Shard_id

    # Sanity check
    if len(shard_mapping) != n:
        message = '<ERROR> Number of servers does not match the number of servers provided'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    for server, shards_list in shard_mapping.items():
        if len(shards_list) == 0:
            message = '<ERROR> No shards assigned to server'
            return jsonify({'message': message, 'status': 'failure'}), 400
        
    for shard in shards:
        if shard['Stud_id_low'] < 0:
            message = '<ERROR> Stud_id_low cannot be negative'
            return jsonify({'message': message, 'status': 'failure'}), 400
        if shard['Shard_size'] <= 0:
            message = '<ERROR> Shard_size cannot be non-positive'
            return jsonify({'message': message, 'status': 'failure'}), 400
    
    # Initialising the database
    mydb = mysql.connector.connect(
    host="localhost", 
    user="root",
    password="abc")
    mycursor = mydb.cursor()
    mycursor.execute("SHOW DATABASES;")
    databases = mycursor.fetchall()
    for db in databases:
        if db[0] == 'loadbalancer':
            message = '<ERROR> Database already initialized'
            return jsonify({'message': message, 'status': 'failure'}), 400
    
    mycursor.execute("CREATE DATABASE loadbalancer;")
    mycursor.execute("USE loadbalancer;")

    # Create the table for the load balancer with shard_id being string
    mycursor.execute("CREATE TABLE ShardT (Stud_id_low INT PRIMARY KEY, Shard_id VARCHAR(255), Shard_size INT);")
    mycursor.execute("CREATE TABLE MapT (Shard_id VARCHAR(255), Server_id INT);")
    
    # Insert the shards into the ShardT table
    for shard in shards:
        mycursor.execute(f"INSERT INTO ShardT VALUES ({shard['Stud_id_low']}, '{shard['Shard_id']}', {shard['Shard_size']});")
    
    # Create locks and HashRing objects for each shard
    for shard in shards:
        id = shard['Shard_id']
        shard_to_hrlock[id] = threading.Lock()
        shard_datalock[id] = {
                'noReaderLock': threading.Lock(),
                'noWaitingReaderLock': threading.Lock(),
                'noWriterLock': threading.Lock(),
                'dataLock': threading.Lock(),
                'numWriters': 0,
                'numReaders': 0,
                'waitingReaders': 0,
        }
        shard_to_hr[shard['Shard_id']] = HashRing(hashtype = "sha256")

    # Add the servers to the load balancer
    response = add_servers(n, shard_mapping, mycursor)
    if response[1] != 200:
        return response
        
    mydb.commit()
    mycursor.close()
    mydb.close()
    message = 'Configured Database'
    return jsonify({'message': message, 'status': 'success'}), 200

'''
(/status, method=GET):This endpoint sends the database configurations upon request
Sample Response = 
{
    "N":3
    "schema":{"columns":["Stud_id","Stud_name","Stud_marks"],"dtypes":["Number","String","String"]}
    "shards":[{"Stud_id_low":0, "Shard_id": "sh1", "Shard_size":4096},
              {"Stud_id_low":4096, "Shard_id": "sh2", "Shard_size":4096},
              {"Stud_id_low":8192, "Shard_id": "sh3", "Shard_size":4096},]
    "servers":{"Server0":["sh1","sh2"],
               "Server1":["sh2","sh3"],
               "Server2":["sh1","sh3"]}
}
'''
@app.route('/status', methods=['GET'])
def status():
    
    mydb = mysql.connector.connect(
    host="localhost", 
    user="root",
    password="abc",
    database="loadbalancer")
    mycursor = mydb.cursor()

    mycursor.execute("SHOW DATABASES;")
    databases = mycursor.fetchall()
    db_exists = False
    for db in databases:
        if db[0] == 'loadbalancer':
            db_exists = True
            break
    if not db_exists:
        message = '<ERROR> Database not initialized'
        return jsonify({'message': message, 'status': 'failure'}), 400

    mycursor.execute("SELECT * FROM ShardT;")
    shards = mycursor.fetchall()
    
    shards_list = []
    for shard in shards:
        shards_list.append({
            "Stud_id_low": shard[0],
            "Shard_id": shard[1],
            "Shard_size": shard[2]
        })
    
    mycursor.execute("SELECT * FROM MapT;")
    shard_mapping = mycursor.fetchall()
    servers_dict = {}
    for shard in shard_mapping:
        if shard[1] in servers_dict:
            servers_dict[shard[1]].append(shard[0])
        else:
            servers_dict[shard[1]] = [shard[0]]
    
    for replica in replicas:
        print(replica[0], replica[1])
    servers = {}
    for server in servers_dict:
        # Here convert the internal server names to external server names
        for replica in replicas:
            if replica[1] == f'Server_{server}':
                servers[replica[0]] = servers_dict[server]
                break

    mycursor.close()
    mydb.close()
    return jsonify({'N': len(servers), 'schema': studT_schema, 'shards': shards_list, 'servers': servers}), 200

'''
(/add,method=POST): This  endpoint  adds  new  server  instances  in  the  load  balancer  to  scale  up  with increasing  client  numbers  in  the  system.  The  endpoint  expects  a  JSON  payload  that  mentions  the  number  of  newinstances, their server names, and the shard placements. An example request and response is below.
Payload Json= 
{
    "n" : 2,
    new_shards:[{"Stud_id_low":12288, "Shard_id": "sh5", "Shard_size":4096}]
    "servers" : {"Server4":["sh3","sh5"], /*new shards must be defined*/
                 "Server[5]":["sh2","sh5"]}
}
Response Json =
{
    "N":5,
    "message" : "Add Server:4 and Server:58127", /*server id is randomly set in case ofServer[5]*/
    "status" : "successful"
},
Response Code = 200
'''
@app.route('/add', methods=['POST'])
def add():
    content = request.json

    if 'n' not in content or 'new_shards' not in content or 'servers' not in content:
        message = '<ERROR> n, new_shards or servers not present in request'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    n = content['n']
    new_shards = content['new_shards']
    shard_mapping = content['servers']

    # Sanity check
    if len(shard_mapping) != n:
        message = '<ERROR> Number of servers does not match the number of servers provided'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    for server in shard_mapping:
        if len(shard_mapping[server]) == 0:
            message = '<ERROR> No shards assigned to server'
            return jsonify({'message': message, 'status': 'failure'}), 400
    
    for shard in new_shards:
        if shard['Stud_id_low'] < 0:
            message = '<ERROR> Stud_id_low cannot be negative'
            return jsonify({'message': message, 'status': 'failure'}), 400
        if shard['Shard_size'] <= 0:
            message = '<ERROR> Shard_size cannot be non-positive'
            return jsonify({'message': message, 'status': 'failure'}), 400
    
    mydb = mysql.connector.connect(
        host="localhost", 
        user="root",
        password="abc",
        database="loadbalancer"
    )
    mycursor = mydb.cursor()

    # Insert the shards into the ShardT table
    for shard in new_shards:
        mycursor.execute(f"INSERT INTO ShardT VALUES ({shard['Stud_id_low']}, '{shard['Shard_id']}', {shard['Shard_size']})")

    for shard in new_shards:
        shard_to_hrlock[shard['Shard_id']] = threading.Lock()
        shard_to_hr[shard['Shard_id']] = HashRing(hashtype = "sha256")
        # shardid_to_idx[shard['Shard_id']] = 0
        # shardid_to_idxlock[shard['Shard_id']] = threading.Lock()
        shard_datalock[shard['Shard_id']] = {
                'noReaderLock': threading.Lock(),
                'noWaitingReaderLock': threading.Lock(),
                'noWriterLock': threading.Lock(),
                'dataLock': threading.Lock(),
                'numWriters': 0,
                'numReaders': 0,
                'waitingReaders': 0,
        }


    response = add_servers(n, shard_mapping, mycursor)
    if response[1] != 200:
        return response
    
    mydb.commit()
    mycursor.close()
    mydb.close()

    message = f'Added Servers: {", ".join(shard_mapping.keys())}'
    return jsonify({'N': len(replicas), 'message': message, 'status': 'successful'}), 200

'''
(/rm,method=DELETE): 
'''
@app.route('/rm', methods=['DELETE'])
def remove():
    content = request.json

    if 'n' not in content or 'servers' not in content:
        message = '<ERROR> n or servers not present in request'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    n = content['n']
    hostnames = content['servers']
    if len(hostnames) > n:
        message = '<ERROR> Length of server list is more than removable instances'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    global replicas
    replica_names = []
    replica_lock.acquire()
    for replica in replicas:
        replica_names.append(replica[0])

    # Sanity check
    for hostname in hostnames:
        if hostname not in replica_names:
            message = f'<ERROR> Hostname {hostname} does not exist'
            return jsonify({'message': message, 'status': 'failure'}), 400
        
    if n > len(replica_names):
        message = '<ERROR> n is more than number of servers available'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    new_replicas = []
    mydb = mysql.connector.connect(
        host="localhost", 
        user="root",
        password="abc",
        database="loadbalancer"
    )
    mycursor = mydb.cursor()
    for replica in replicas:
        if replica[0] in hostnames:
            hostname = replica[0]
            os.system(f'docker stop {replica[1]} && docker rm {replica[1]}')
            # Find the shard IDs that the server is responsible for
            shard_ids = []
            mycursor.execute(f"SELECT Shard_id FROM MapT WHERE Server_id = {replica[1][7:]};")
            shard_ids = mycursor.fetchall()
            # Remove the server from the MapT table
            mycursor.execute(f"DELETE FROM MapT WHERE Server_id = {replica[1][7:]};")
            mydb.commit()

            # Remove the server from hashrings of the shards
            server_ids.add(int(replica[1][7:]))
            for shard in shard_ids:
                id = shard[0]
                shard_to_hrlock[id].acquire()
                shard_to_hr[id].remove_server(replica[1])
                shard_to_hrlock[id].release()
            n -= 1
        else:
            new_replicas.append(replica)
    replicas = new_replicas

    # Then delete the unnamed replicas
    replicas_tobedeleted = replicas.copy()
    
    random.shuffle(replicas_tobedeleted)
    while len(replicas_tobedeleted) > n:
        replicas_tobedeleted.pop()

    for i in range(n):
        replica = replicas_tobedeleted[i]
        os.system(f'docker stop {replica[1]} && docker rm {replica[1]}')
        server_locks.pop(replica[1])
        
        # Find the shard IDs that the server is responsible for
        shard_ids = []
        mycursor.execute(f"SELECT Shard_id FROM MapT WHERE Server_id = {replica[1][7:]};")
        shard_ids = mycursor.fetchall()
        # Remove the server from the MapT table
        mycursor.execute(f"DELETE FROM MapT WHERE Server_id = {replica[1][7:]};")
        mydb.commit()
        # Remove the server from hashrings of the shards
        server_ids.add(int(replica[1][7:]))
        for shard in shard_ids:
            id = shard[0]
            shard_to_hrlock[id].acquire()
            shard_to_hr[id].remove_server(replica[1])
            shard_to_hrlock[id].release()

    mycursor.close()
    mydb.close()

    new_replicas = []
    replica_names = []
    for replica in replicas:
        if replica not in replicas_tobedeleted:
            new_replicas.append(replica)
    replicas = new_replicas

    deleted_replica_names = [replica[0] for replica in replicas_tobedeleted]
    deleted_replica_names += hostnames

    message = {
        'N': len(replicas),
        'servers': deleted_replica_names
    }
    replica_lock.release()
    return jsonify({'message': message, 'status': 'successful'}), 200    

# Function to read data from a shard
def read_shard_data(tid, shard, stud_id_low, stud_id_high, data):
    shard_to_hrlock[shard].acquire()
    server = shard_to_hr[shard].get_server(random.randint(0, 999999))
    shard_to_hrlock[shard].release()

    print(f"Read Thread {tid}: Shard {shard}, Server {server}",flush=True)
    if server == None:
        # return empty data
        message = []
        data[shard] = {'message': message, 'status': 'success'}
        return
    
    try:
        server_locks[server].acquire()
        reply = requests.post(f'http://{server}:{serverport}/read', json = {
            "shard": shard,
            "Stud_id": {"low": stud_id_low, "high": stud_id_high}
        })
        server_locks[server].release()
        data[shard] = reply.json()
        print(f"Read Thread {tid}: Data {data[shard]}",flush=True)
        if reply.status_code != 200:
            message = '<ERROR> Read failed'
            data[shard] = {'message': message, 'status': 'failure'}
    except requests.exceptions.ConnectionError:
        message = '<ERROR> Server unavailable'
        data[shard] = {'message': message, 'status': 'failure'}
    
    print(f"Read Thread {tid}: Done",flush=True)

'''
(/read, method=POST):
'''
@app.route('/read', methods=['POST'])
def read():
    content = request.json

    # check if 'Stud_id' exists in content
    if 'Stud_id' not in content:
        message = '<ERROR> Stud_id not present in request'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    # check if 'low' and 'high' exist in content['Stud_id']
    if 'low' not in content['Stud_id'] or 'high' not in content['Stud_id']:
        message = '<ERROR> low or high not present in Stud_id'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    stud_id_low = content['Stud_id']['low']
    stud_id_high = content['Stud_id']['high']

    mydb = mysql.connector.connect(
    host="localhost", 
    user="root",
    password="abc",
    database="loadbalancer"
    )
    mycursor = mydb.cursor()
    mycursor.execute(f"SELECT Shard_id FROM ShardT WHERE Stud_id_low <= {stud_id_high} AND Stud_id_low + Shard_size > {stud_id_low};")
    shards_list = mycursor.fetchall()
    mycursor.close()
    mydb.close()

    shards_queried = []
    # For each shard, find a server using the hashring and forward the request to the server
    data = {}

    threads = []
    for i in range(len(shards_list)):
        id = shards_list[i][0]
        shard = shards_list[i]
        shards_queried.append(id)
        data[id] = {}
        # start the thread if no writer is waiting to write

        shard_datalock[id]['noWriterLock'].acquire()
        while(shard_datalock[id]['numWriters'] > 0):
            shard_datalock[id]['noWriterLock'].release()
            time.sleep(1)
            shard_datalock[id]['noWriterLock'].acquire()
        shard_datalock[id]['noWriterLock'].release()

        shard_datalock[id]['noReaderLock'].acquire()
        shard_datalock[id]['numReaders'] += 1
        if shard_datalock[id]['numReaders'] == 1:
            shard_datalock[id]['dataLock'].acquire()
        shard_datalock[id]['noReaderLock'].release()
        
        t = threading.Thread(target = read_shard_data, args = (i, id, stud_id_low, stud_id_high, data))
        t.start()
        threads.append((t, id))


    for t, id in threads:
        t.join()
        shard_datalock[id]['noReaderLock'].acquire()
        shard_datalock[id]['numReaders'] -= 1
        if shard_datalock[id]['numReaders'] == 0:
            shard_datalock[id]['dataLock'].release()
        shard_datalock[id]['noReaderLock'].release()
    
    merged_data = []
    for shard in shards_list:
        print('data: ', data, flush=True)
        if data[shard[0]]['status'] == 'failure':
            message = '<ERROR> Server unavailable'
            return jsonify({'message': message, 'status': 'failure'}), 400
        
        merged_data += data[shard[0]]['data']

    response = {
        "shards_queried": shards_queried,
        "data": merged_data,
        "status": "success"
    }
    return jsonify(response), 200

def write_shard_data(tid, shard, data_to_insert):
    global write_results
    print(f"Write Thread {tid}: Shard {shard}",flush=True)
    shard_to_hrlock[shard].acquire()
    
    # find all the servers having the shard
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="abc",
        database="loadbalancer"
    )
    mycursor = mydb.cursor()
    mycursor.execute(f"SELECT Server_id FROM MapT WHERE Shard_id = '{shard}';")
    servers = mycursor.fetchall()
    mycursor.close()
    mydb.close()

    if len(servers) == 0:
        message = '<ERROR> No servers available for the shard'
        write_results[tid] = {'message': message, 'status': 'failure'}
        return
    
    for server in servers:
        server_name = f'Server_{server[0]}'
        
        print(f"Write Thread {tid}: Shard {shard} Server {server_name}",flush=True)
        try:
            server_locks[server_name].acquire()
            reply = requests.post(f'http://{server_name}:{serverport}/write', json = {
                "shard": shard,
                "curr_idx": 0,
                "data": data_to_insert
            })
            server_locks[server_name].release()
            if reply.status_code != 200:
                shard_to_hrlock[shard].release()
                message = '<ERROR> Write failed'
                write_results[tid] = {'message': message, 'status': 'failure'}
                return
        except requests.exceptions.ConnectionError:
            shard_to_hrlock[shard].release()
            message = '<ERROR> Server unavailable'
            write_results[tid] = {'message': message, 'status': 'failure'}
            return
    
    shard_to_hrlock[shard].release()
    write_results[tid] = {'message': f'{len(data_to_insert)} Data entries added', 'status': 'success'}
    print(f"Write Thread {tid}: Done",flush=True)
    return

@app.route('/write', methods=['POST'])
def write():
    content = request.json

    if 'data' not in content:
        message = '<ERROR> data not present in request'
        return jsonify({'message': message, 'status': 'failure'}), 400
    data = content['data']

    data = sorted(data, key=lambda x: x['Stud_id'])
    shards = []
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="abc",
            database="loadbalancer"
        )

        mycursor = mydb.cursor()
        mycursor.execute("SELECT * FROM ShardT;")
        shards = mycursor.fetchall()

        mycursor.close()
        mydb.close()
    except mysql.connector.Error as err:
        return jsonify({'message': f'<ERROR> MySQL Error: {err}', 'status': 'failure'}), 500

    data_idx = 0
    threads = []
    global write_results
    global write_results_lock
    write_results_lock.acquire()
    write_results = {}

    for i in range(len(shards)):
        shard = shards[i]
        id = shard[1]
        print(f"Processing Shard {id}", flush=True)
        data_to_insert = []
        while data_idx < len(data) and data[data_idx]['Stud_id'] < shard[0] + shard[2]:
            data_to_insert.append(data[data_idx])
            data_idx += 1
        if len(data_to_insert) == 0:
            write_results[i] = {'status': 'success', 'message': 'no data to insert'}
            continue

        shard_datalock[id]['noWriterLock'].acquire()
        shard_datalock[id]['numWriters'] += 1
        shard_datalock[id]['noWriterLock'].release()

        shard_datalock[id]['noReaderLock'].acquire()

        while shard_datalock[id]['numReaders'] > 0:
            shard_datalock[id]['noReaderLock'].release()
            time.sleep(1)
            shard_datalock[id]['noReaderLock'].acquire()

        shard_datalock[id]['noReaderLock'].release()

        shard_datalock[id]['dataLock'].acquire()
        t = threading.Thread(target=write_shard_data, args=(i, id, data_to_insert))
        threads.append((t, id))
        t.start()

    for t, id in threads:
        t.join()
        shard_datalock[id]['noWriterLock'].acquire()
        shard_datalock[id]['numWriters'] -= 1
        shard_datalock[id]['dataLock'].release()
        shard_datalock[id]['noWriterLock'].release()

    for i in range(len(shards)):
        if i not in write_results:
            message = '<ERROR> No response received from the server, i not present'
            write_results_lock.release()
            return jsonify({'message': message, 'status': 'failure'}), 400
        if 'status' not in write_results[i]:
            message = '<ERROR> \'status\' not present in write_results[i]'
            write_results_lock.release()
            return jsonify({'message': message, 'status': 'failure'}), 400
        if write_results[i]['status'] == 'failure':
            write_results_lock.release()
            return jsonify(write_results[i]), 400

    write_results_lock.release()
    response = {
        "message": f"{len(data)} Data entries added",
        "status": "success"
    }

    return jsonify(response), 200

def update_shard_data(tid, shard, stud_id, new_data):
    # find all the servers having the shard
    mydb = mysql.connector.connect(
        host="localhost",
        user = "root",
        password = "abc",
        database = "loadbalancer"
    )
    mycursor = mydb.cursor()
    mycursor.execute(f"SELECT Server_id FROM MapT WHERE Shard_id = '{shard}';")
    servers = mycursor.fetchall()
    mycursor.close()

    if len(servers) == 0:
        message = '<ERROR> No servers available for the shard'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    for server in servers:
        server_name = f'Server_{server[0]}'
        try:
            server_locks[server_name].acquire()
            reply = requests.put(f'http://{server_name}:{serverport}/update', json = {
                "shard": shard,
                "Stud_id": stud_id,
                "data": new_data
            })
            server_locks[server_name].release()
            if reply.status_code != 200:
                message = '<ERROR> Update failed'
                return jsonify({'message': message, 'status': 'failure'}), 400
        except requests.exceptions.ConnectionError:
            message = '<ERROR> Server unavailable'
            return jsonify({'message': message, 'status': 'failure'}), 400
        
    return jsonify({'message': f"Data entry for Stud_id:{stud_id} updated", 'status':'success'}), 200

'''
(/update, method=PUT):
'''
@app.route('/update', methods=['PUT'])
def update():
    content = request.json
    # check if 'Stud_id' and 'data' exist in content

    if 'data' not in content or 'Stud_id' not in content:
        message = '<ERROR> Stud_id or data not present in request'
        return jsonify({'message': message, 'status': 'failure'}), 400

    stud_id = content['Stud_id']
    new_data = content['data']

    # Find the shard that contains the data
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="abc",
        database="loadbalancer"
    )
    mycursor = mydb.cursor()
    mycursor.execute(f"SELECT Shard_id FROM ShardT WHERE Stud_id_low <= {stud_id} AND Stud_id_low + Shard_size > {stud_id};")
    shard = mycursor.fetchone()
    mycursor.close()
    mydb.close()
    id = shard[0]

    
    shard_datalock[id]['noWriterLock'].acquire()
    shard_datalock[id]['numWriters'] += 1
    shard_datalock[id]['noWriterLock'].release()

    shard_datalock[id]['noReaderLock'].acquire()
    while shard_datalock[id]['numReaders'] > 0:
        shard_datalock[id]['noReaderLock'].release()
        time.sleep(1)
        shard_datalock[id]['noReaderLock'].acquire()

    shard_datalock[id]['noReaderLock'].release()

    shard_datalock[id]['dataLock'].acquire()
    response = update_shard_data(0, id, stud_id, new_data)
    shard_datalock[id]['noWriterLock'].acquire()
    shard_datalock[id]['numWriters'] -= 1
    shard_datalock[id]['dataLock'].release()
    shard_datalock[id]['noWriterLock'].release()


    return response

def delete_shard_data(tid, shard, stud_id):
    # find all the servers having the shard
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="abc",
        database="loadbalancer"
    )
    mycursor = mydb.cursor()
    mycursor.execute(f"SELECT Server_id FROM MapT WHERE Shard_id = '{shard}';")
    servers = mycursor.fetchall()
    mycursor.close()

    if len(servers) == 0:
        message = '<ERROR> No servers available for the shard'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    for server in servers:
        server_name = f'Server_{server[0]}'
        try:
            server_locks[server_name].acquire()
            reply = requests.delete(f'http://{server_name}:{serverport}/del', json = {
                "shard": shard,
                "Stud_id": stud_id
            })
            server_locks[server_name].release()
            if reply.status_code != 200:
                message = '<ERROR> Delete failed'
                return jsonify({'message': message, 'status': 'failure'}), 400
        except requests.exceptions.ConnectionError:
            message = '<ERROR> Server unavailable'
            return jsonify({'message': message, 'status': 'failure'}), 400
        
    return jsonify({'message': f"Data entry for Stud_id:{stud_id} deleted", 'status':'success'}), 200


'''
(/del, method=DELETE):
'''
@app.route('/del', methods=['DELETE'])
def delete():
    content = request.json

    # check if 'Stud_id' exists in content
    if 'Stud_id' not in content:
        message = '<ERROR> Stud_id not present in request'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    Stud_id = content['Stud_id']

    # Find the shard that contains the data
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="abc",
        database="loadbalancer"
    )

    mycursor = mydb.cursor()
    mycursor.execute(f"SELECT Shard_id FROM ShardT WHERE Stud_id_low <= {Stud_id} AND Stud_id_low + Shard_size > {Stud_id};")    
    shard = mycursor.fetchone()
    shard = shard[0]
    
    shard_datalock[shard]['noWriterLock'].acquire()
    shard_datalock[shard]['numWriters'] += 1
    shard_datalock[shard]['noWriterLock'].release()

    shard_datalock[shard]['noReaderLock'].acquire()
    while shard_datalock[shard]['numReaders'] > 0:
        shard_datalock[shard]['noReaderLock'].release()
        time.sleep(1)
        shard_datalock[shard]['noReaderLock'].acquire()

    shard_datalock[shard]['noReaderLock'].release()

    shard_datalock[shard]['dataLock'].acquire()

    response = delete_shard_data(0, shard, Stud_id)

    shard_datalock[shard]['noWriterLock'].acquire()
    shard_datalock[shard]['numWriters'] -= 1
    shard_datalock[shard]['dataLock'].release()
    shard_datalock[shard]['noWriterLock'].release()

    return response
    
    # # find all the servers having the shard
    # mycursor.execute(f"SELECT Server_id FROM MapT WHERE Shard_id = '{shard}';")
    # servers = mycursor.fetchall()

    # if len(servers) == 0:
    #     message = '<ERROR> No servers available for the shard'
    #     return jsonify({'message': message, 'status': 'failure'}), 400
    
    # # delete the entry in all servers
    # for server in servers:
    #     server_name = f'Server_{server[0]}'
    #     try:
    #         server_locks[server_name].acquire()
    #         reply = requests.delete(f'http://{server_name}:{serverport}/del', json = {
    #             "shard": shard,
    #             "Stud_id": Stud_id
    #         })
    #         server_locks[server_name].release()
    #         if reply.status_code != 200:
    #             message = '<ERROR> Delete failed'
    #             return jsonify({'message': message, 'status': 'failure'}), 400
    #     except requests.exceptions.ConnectionError:
    #         message = '<ERROR> Server unavailable'
    #         return jsonify({'message': message, 'status': 'failure'}), 400
        
    # entry deleted in all servers
    mycursor.close()
    mydb.close()
    message = f"Data entry for Stud_id:{Stud_id} deleted"
    return jsonify({'message': message, 'status':'success'}), 200

@app.route('/read/<server>', methods=['GET'])
def read_server(server):
    
    global replicas, serverport

    server_name = None
    for replica in replicas:
        if replica[0] == server:
            server_name = replica[1]
            break
    
    if server_name == None:
        message = '<ERROR> Server not found'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="abc",
        database="loadbalancer"
    )

    mycursor = mydb.cursor()
    server_id = int(server_name[7:])
    mycursor.execute(f"SELECT Shard_id FROM MapT WHERE Server_id = {server_id};")
    shards = mycursor.fetchall()
    data = {}
    for shard in shards:
        id = shard[0]
        try:
            reply = requests.get(f'http://{server_name}:{serverport}/copy', json={
                "shards": [id]
            })
            if reply.status_code == 200:
                data[id] = reply.json()[id]
                continue

            return jsonify({'message': 'Data retrieval failed', 'status': 'failure'}), 400
        except requests.exceptions.ConnectionError:
            message = '<ERROR> Server unavailable'
            data[id] = {'message': message, 'status': 'failure'}
    
    mycursor.close()
    mydb.close()
    return jsonify({'Data': data, 'status': 'success'}), 200

'''
'''
def respawn_server(replica):
    print(f'Replica {replica[1]} is down',flush=True)
    os.system(f'docker stop {replica[1]} && docker rm {replica[1]}')

    serverid = int(replica[1][7:])
    os.system(f'docker run --name {replica[1]} --network mynet --network-alias {replica[1]} -e SERVER_ID={serverid} -d serverim:latest')
    time.sleep(5)

    print(f'DEBUG RESPAWN: Replica {replica[1]} respawned', flush=True)
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="abc",
        database="loadbalancer"
    )
    mycursor = mydb.cursor()
    mycursor.execute(f"SELECT Shard_id FROM MapT WHERE Server_id = {serverid};")
    shards = mycursor.fetchall()
    # Remove the server from the hashrings of the shards
    for shard in shards:
        id = shard[0]
        shard_to_hrlock[id].acquire()
        shard_to_hr[id].remove_server(replica[1])
        shard_to_hrlock[id].release()

    # Remove the server from the MapT table
    mycursor.execute(f"DELETE FROM MapT WHERE Server_id = {serverid};")
    mydb.commit()

    shards_list = []
    for shard in shards:
        shards_list.append(shard[0])
    
    try:
        data_payload = {}
        data_payload['schema'] = studT_schema
        data_payload['shards'] = shards_list
        reply = requests.post(f'http://{replica[1]}:{serverport}/config', 
                                json = data_payload)
        time.sleep(3)
        if reply.status_code != 200:
            replica_lock.release()
            return jsonify({'message': 'Server configuration failed', 'status': 'failure'}), 400
    except requests.exceptions.ConnectionError:
        replica_lock.release()
        return jsonify({'message': 'Server configuration failed. Connection Error', 'status': 'failure'}), 400
    print("DEBUG RESPAWN: Server configuration successful", flush=True)
    
    # first we will complete the data replication to the new server
    for shard in shards:
        id = shard[0]
        shard_to_hrlock[id].acquire()
        server = shard_to_hr[id].get_server(random.randint(0, 999999))
        shard_data = []
        try:
            reply = requests.get(f'http://{server}:{serverport}/copy', json = {
                "shards": [id],
            })
            data = reply.json()
            shard_data = data[id] 
            if reply.status_code != 200:
                return jsonify({'message': 'Data replication failed', 'status': 'failure'}), 400
        except requests.exceptions.ConnectionError:
            message = '<ERROR> Server unavailable'
            return jsonify({'message': message, 'status': 'failure'}), 400
        
        if len(shard_data) == 0:
            continue

        # write the data to the new server
        try:
            reply = requests.post(f'http://{replica[1]}:{serverport}/write', json = {
                "shard": id,
                "curr_idx": 0,
                "data": shard_data
            })
            shard_to_hrlock[id].release()
            if reply.status_code != 200:
                return jsonify({'message': 'Data replication failed', 'status': 'failure'}), 400
        except requests.exceptions.ConnectionError:
            shard_to_hrlock[id].release()
            message = '<ERROR> Server unavailable'
            return jsonify({'message': message, 'status': 'failure'}), 400

        # add the server in the HashRing of the shard
        shard_to_hrlock[id].acquire()
        shard_to_hr[id].add_server(replica[1])

        # Add the server to the MapT table
        mycursor.execute(f"INSERT INTO MapT VALUES ('{id}', {serverid});")
        mydb.commit()
        shard_to_hrlock[id].release()

    print("DEBUG RESPAWN: Data replication successful", flush=True)
    mycursor.close()
    mydb.close()
    print(f'DEBUG RESPAWN: Replica {replica[1]} completely functional', flush=True)

def manage_replicas():
    '''
    Entrypoint for thread that checks the replicas for heartbeats every 10 seconds.
    '''
    while True:
        replica_lock.acquire()
        for replica in replicas:
            try:
                reply = requests.get(f'http://{replica[1]}:{serverport}/heartbeat')
            except requests.exceptions.ConnectionError:
                respawn_server(replica)
            else:
                if reply.status_code != 200:
                    respawn_server(replica)              
        replica_lock.release()
        time.sleep(10)
    
if __name__ == '__main__':
    serverport = 5000

    # Replicas is a list of lists. Each list has two entries: the External Name (user-specified or randomly generated) and the Container Name
    # The Container Name is the name of the container, and is the same as the hostname of the container. It is always of the form Server_<serverid>
    replicas = []

    # Bookkeeping for server IDs
    server_ids = set()
    next_server_id = 1

    num_servers = 0
    studT_schema = ""

    # Setting up and spawning the thread that manages the replicas
    thread = threading.Thread(target=manage_replicas)
    thread.start()
    
    # Start the server
    app.run(host='0.0.0.0', port=5000, debug=True)
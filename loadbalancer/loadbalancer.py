from flask import Flask, request, jsonify
import requests
from flask_cors import CORS
import random
import sys
import threading
import time
import mysql.connector

sys.path.append('../utils')
from hashring import HashRing

app = Flask(__name__)
CORS(app)
replica_lock = threading.Lock() # Lock for the replicas list
shard_to_hr = {} # This dictionary will map the shard_id to hashring. key: shard_id, value: (hashring)
shard_to_hrlock = {} # Lock for the shard_to_hr dictionary
shard_datalock = {} # Lock for the shard data

def connect_to_db():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="abc",
        database="loadbalancer"
    )
    mycursor = mydb.cursor()
    return mydb, mycursor

def close_db(mydb, mycursor):
    mycursor.close()
    mydb.close()

def acquire_reader_lock(shard):
    shard_datalock[shard]['noWaitingReaderLock'].acquire()
    shard_datalock[shard]['waitingReaders'] += 1
    shard_datalock[shard]['noWaitingReaderLock'].release()

    shard_datalock[shard]['noWriterLock'].acquire()
    while(shard_datalock[shard]['numWriters'] > 0):
        shard_datalock[shard]['noWriterLock'].release()
        time.sleep(0.2)
        shard_datalock[shard]['noWriterLock'].acquire()
    
    shard_datalock[shard]['noReaderLock'].acquire()
    shard_datalock[shard]['numReaders'] += 1
    shard_datalock[shard]['noReaderLock'].release()
    shard_datalock[shard]['noWaitingReaderLock'].acquire()
    shard_datalock[shard]['waitingReaders'] -= 1
    shard_datalock[shard]['noWaitingReaderLock'].release()

    shard_datalock[shard]['noWriterLock'].release()          

def release_reader_lock(shard):
    shard_datalock[shard]['noReaderLock'].acquire()
    shard_datalock[shard]['numReaders'] -= 1
    shard_datalock[shard]['noReaderLock'].release()

def acquire_writer_lock(shard):
    shard_datalock[shard]['noReaderLock'].acquire()
    while shard_datalock[shard]['numReaders'] > 0:
        shard_datalock[shard]['noReaderLock'].release()
        time.sleep(0.2)
        shard_datalock[shard]['noReaderLock'].acquire()

    shard_datalock[shard]['noReaderLock'].release()

    shard_datalock[shard]['noWriterLock'].acquire()
    shard_datalock[shard]['numWriters'] += 1
    shard_datalock[shard]['noWriterLock'].release()

    shard_datalock[shard]['dataLock'].acquire()

def release_writer_lock(shard):
    shard_datalock[shard]['dataLock'].release()

    shard_datalock[shard]['noWriterLock'].acquire()
    shard_datalock[shard]['numWriters'] -= 1
    shard_datalock[shard]['noWriterLock'].release()

def conduct_election(shard):
    try:
        reply = requests.get(f'http://shardmanager:5001/election/{shard}')
    except requests.exceptions.ConnectionError:
        message = '<ERROR> Shard Manager not responding. Election Failed '
        return jsonify({'message': message, 'status': 'failure'}), 400
    if reply.status_code != 200:
        message = f'<ERROR> Election failed for shard {shard}'
        return jsonify({'message': message, 'status': 'failure'}), 400
    primary_server = reply.json()['primary']
    mydb, mycursor = connect_to_db()
    primary_server_name = f'Server_{primary_server}' 
    if primary_server == None:
        primary_server_name = 'None'

    mycursor.execute(f"UPDATE ShardT SET Primary_server = '{primary_server_name}' WHERE Shard_id = '{shard}';")
    mydb.commit()
    
    close_db(mydb, mycursor)
    return reply.json()

def add_servers(n, shard_mapping):
    
    mydb, mycursor = connect_to_db()
    global replicas
    hostnames = []
    for server in shard_mapping:
        hostnames.append(server)

    replica_names = []
    replica_lock.acquire()
    for replica in replica_names:
        replica_names.append(replica[0])

    server_spawned = []

    # We go through the list of preferred hostnames and check if the hostname already exists, or if no hostname is provided, we generate a random hostname   
    for i in range(n):
        if (i >= len(hostnames)) or (hostnames[i] in replica_names):
            for j in range(len(replica_names)+1):
                new_name = 'S'+ str(j)
                if new_name not in replica_names:
                    hostnames[i] = new_name
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
        shards_list = shard_mapping[hostnames[i]]
        shards_list = sorted(shards_list)

        try:
            reply = requests.post(f'http://shardmanager:5001/add_server', json = {
                "server_id": serverid,
                "container_name": container_name,
                "hostname": hostnames[i],
                "shards": shards_list,
                "schema": studT_schema
            })
        except requests.exceptions.ConnectionError:
            replica_lock.release()
            message = '<ERROR> Shard Manager not responding. Server addition failed '
            return jsonify({'message': message, 'status': 'failure'}), 400

        if reply.status_code != 200:
            server_ids.add(serverid)
            continue

        replicas.append([hostnames[i], container_name])
        server_spawned.append(container_name)
        
        for shard in shards_list:
            mycursor.execute(f"INSERT INTO MapT VALUES ('{shard}', {serverid});")
            mydb.commit()
            shard_to_hrlock[shard].acquire()
            shard_to_hr[shard].add_server(container_name)
            shard_to_hrlock[shard].release()
    
    close_db(mydb, mycursor)
    replica_lock.release()
    response = {
        'message': 'Servers spawned and configured',
        'status': 'success',
        'Servers': server_spawned
    }
    return response, 200

@app.route('/set_new_primary', methods=['POST'])
def set_new_primary():
    content = request.json

    if 'shard' not in content or 'server' not in content:
        message = '<ERROR> shard or server not present in request'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    shard = content['shard']
    server = content['server']

    mydb, mycursor = connect_to_db()
    
    mycursor.execute(f"UPDATE ShardT SET Primary_server = '{server}' WHERE Shard_id = '{shard}';")
    mydb.commit()
    close_db(mydb, mycursor)

    return jsonify({'message': 'Primary server set', 'status': 'success'}), 200

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
    mycursor.execute("CREATE TABLE ShardT (Stud_id_low INT PRIMARY KEY, Shard_id VARCHAR(255), Shard_size INT, Primary_server VARCHAR(255));")
    mycursor.execute("CREATE TABLE MapT (Shard_id VARCHAR(255), Server_id INT);")
    mydb.commit()

    for shard in shards:
        mycursor.execute(f"INSERT INTO ShardT VALUES ({shard['Stud_id_low']}, '{shard['Shard_id']}', {shard['Shard_size']}, 'None');")
        mydb.commit()

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

    close_db(mydb, mycursor)

    # Add the servers to the load balancer
    response = add_servers(n, shard_mapping)
    if response[1] != 200:
        return response
    for shard in shards:
        reply = conduct_election(shard['Shard_id'])
        if reply['status'] != 'success':
            return reply
    message = 'Configured Database'
    return jsonify({'message': message, 'status': 'success', 'Added Servers': response[0]['Servers']}), 200

@app.route('/status', methods=['GET'])
def status():
    
    mydb, mycursor = connect_to_db()

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
        server_name = shard[3]
        replica_lock.acquire()
        for replica in replicas:
            if replica[1] == server_name:
                server_name = replica[0]
                break
        replica_lock.release()
        shards_list.append({
            "Stud_id_low": shard[0],
            "Shard_id": shard[1],
            "Shard_size": shard[2],
            "Primary_server": server_name
        })
    
    mycursor.execute("SELECT * FROM MapT;")
    shard_mapping = mycursor.fetchall()
    servers_dict = {}
    for shard in shard_mapping:
        if shard[1] in servers_dict:
            servers_dict[shard[1]].append(shard[0])
        else:
            servers_dict[shard[1]] = [shard[0]]
    
    servers = {}
    for server in servers_dict:
        for replica in replicas:
            if replica[1] == f'Server_{server}':
                servers[replica[0]] = servers_dict[server]
                break

    close_db(mydb, mycursor)
    return jsonify({'N': len(servers), 'schema': studT_schema, 'shards': shards_list, 'servers': servers, }), 200

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
    
    mydb, mycursor = connect_to_db()

    # Get the list of all the shards
    mycursor.execute("SELECT * FROM ShardT;")
    shards = mycursor.fetchall()
    shard_ids = {}
    for shard in shards:
        shard_ids[shard[1]] = True
    
    new_shards_copy = new_shards
    
    # Check if the new shards are already present
    for shard in new_shards:
        if shard['Shard_id'] in shard_ids:
            new_shards_copy.remove(shard)
        
    new_shards = new_shards_copy

    # Insert the shards into the ShardT table
    for shard in new_shards:
        mycursor.execute(f"INSERT INTO ShardT VALUES ({shard['Stud_id_low']}, '{shard['Shard_id']}', {shard['Shard_size']}, 'None');")
        mydb.commit()

    for shard in new_shards:
        shard_to_hrlock[shard['Shard_id']] = threading.Lock()
        shard_to_hr[shard['Shard_id']] = HashRing(hashtype = "sha256")
        shard_datalock[shard['Shard_id']] = {
                'noReaderLock': threading.Lock(),
                'noWaitingReaderLock': threading.Lock(),
                'noWriterLock': threading.Lock(),
                'dataLock': threading.Lock(),
                'numWriters': 0,
                'numReaders': 0,
                'waitingReaders': 0
        }

    # go through all the shards in the shard_mapping
    # if the shard is not new and has no primary server, elect a primary server
    total_shards_in_query = []
    
    for server, shards_list in shard_mapping.items():
        for shard in shards_list:
            total_shards_in_query.append(shard)
    
    for shard in total_shards_in_query:
        mycursor.execute(f"SELECT * FROM ShardT WHERE Shard_id = '{shard}';")
        shard_info = mycursor.fetchall()
        if shard not in new_shards and shard_info[0][3] == 'None':
            # append in new shards
            new_shards.append({
                'Stud_id_low': shard_info[0][0],
                'Shard_id': shard_info[0][1],
                'Shard_size': shard_info[0][2]
            })

    close_db(mydb, mycursor)

    response= add_servers(n, shard_mapping)
    if response[1] != 200:
        return response
    
    # conduct elections for the new shards
    for shard in new_shards:
        reply = conduct_election(shard['Shard_id'])
        if reply['status'] != 'success':
            print(reply)
            return reply
    message = 'Added Servers:'

    if (len(response[0]['Servers']) == 0):
        return jsonify({'message': 'No servers added', 'status': 'failure'}), 400

    replica_lock.acquire()
    for server in response[0]['Servers']:
        # convert into hostname
        for replica in replicas:
            if replica[1] == server:
                message += f' {replica[0]}'
                break
    replica_lock.release()
    return jsonify({'N': len(replicas), 'message': message, 'status': 'success'}), 200

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
    mydb, mycursor = connect_to_db()

    shards_up_for_reelection = {}
    for replica in replicas:
        if replica[0] in hostnames:
            hostname = replica[0]
            # Find the shard IDs that the server is responsible for
            shard_ids = []
            mycursor.execute(f"SELECT Shard_id FROM MapT WHERE Server_id = {replica[1][7:]};")
            shard_ids = mycursor.fetchall()
            # Remove the server from the MapT table
            mycursor.execute(f"DELETE FROM MapT WHERE Server_id = {replica[1][7:]};")
            mydb.commit()
            mycursor.execute(f"SELECT Shard_id FROM ShardT WHERE Primary_server = '{replica[1]}';")
            shards = mycursor.fetchall()
            for shard in shards:
                shards_up_for_reelection[shard[0]] = True
            # Remove the server from hashrings of the shards
            server_ids.add(int(replica[1][7:]))
            for shard in shard_ids:
                id = shard[0]
                shard_to_hrlock[id].acquire()
                shard_to_hr[id].remove_server(replica[1])
                shard_to_hrlock[id].release()
            n -= 1
            try:
                reply = requests.delete(f'http://shardmanager:5001/remove_server', json = {
                    "server_id": int(replica[1][7:])
                })
            except requests.exceptions.ConnectionError:
                message = '<ERROR> Shard Manager not responding. Server removal failed '
                return jsonify({'message': message, 'status': 'failure'}), 400
            
            if reply.status_code != 200:
                message = f'<ERROR> Server removal failed for server {replica[1]}'
                return jsonify({'message': message, 'status': 'failure'}), 400
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
        # server_locks.pop(replica[1])
        
        # Find the shard IDs that the server is responsible for
        shard_ids = []
        mycursor.execute(f"SELECT Shard_id FROM MapT WHERE Server_id = {replica[1][7:]};")
        shard_ids = mycursor.fetchall()
        # Remove the server from the MapT table
        mycursor.execute(f"DELETE FROM MapT WHERE Server_id = {replica[1][7:]};")
        mydb.commit()
        mycursor.execute(f"SELECT Shard_id FROM ShardT WHERE Primary_server = '{replica[1]}';")
        shards = mycursor.fetchall()
        for shard in shards:
            shards_up_for_reelection[shard[0]] = True
        # Remove the server from hashrings of the shards
        server_ids.add(int(replica[1][7:]))
        for shard in shard_ids:
            id = shard[0]
            shard_to_hrlock[id].acquire()
            shard_to_hr[id].remove_server(replica[1])
            shard_to_hrlock[id].release()

        try:
            reply = requests.delete(f'http://shardmanager:5001/remove_server', json = {
                "server_id": int(replica[1][7:])
            })
        except requests.exceptions.ConnectionError:
            message = '<ERROR> Shard Manager not responding. Server removal failed '
            return jsonify({'message': message, 'status': 'failure'}), 400
        
        if reply.status_code != 200:
            message = f'<ERROR> Server removal failed for server {replica[1]}'
            return jsonify({'message': message, 'status': 'failure'}), 400

    close_db(mydb, mycursor)

    for shard in shards_up_for_reelection:
        reply = conduct_election(shard)
        if reply['status'] != 'success':
            return reply

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
    return jsonify({'message': message, 'status': 'success'}), 200    

def read_shard_data(tid, shard, stud_id_low, stud_id_high, data):
    
    shard_to_hrlock[shard].acquire()
    server = shard_to_hr[shard].get_server(random.randint(0, 999999))
    shard_to_hrlock[shard].release()

    print(f"Read Thread {tid}: Shard {shard}, Server {server}",flush=True)
    if server == None:
        # return empty data
        message = {
            "data": [],
            "status": "success"
        }
        data[shard] = message
        return
    
    acquire_reader_lock(shard)
    try:
        reply = requests.post(f'http://{server}:{serverport}/read', json = {
            "shard": shard,
            "Stud_id": {"low": stud_id_low, "high": stud_id_high}
        })
        data[shard] = reply.json()
        if reply.status_code != 200:
            message = '<ERROR> Read failed'
            data[shard] = {'message': message, 'status': 'failure'}
    except requests.exceptions.ConnectionError:
        message = '<ERROR> Server unavailable'
        data[shard] = {'message': message, 'status': 'failure'}

    release_reader_lock(shard)
    print(f"Read Thread {tid}: Done",flush=True)

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

    mydb, mycursor = connect_to_db()
    mycursor.execute(f"SELECT Shard_id FROM ShardT WHERE Stud_id_low <= {stud_id_high} AND Stud_id_low + Shard_size > {stud_id_low};")
    shards_list = mycursor.fetchall()
    close_db(mydb, mycursor)
    shards_queried = []
    # For each shard, find a server using the hashring and forward the request to the server
    data = {}

    threads = []
    for i in range(len(shards_list)):
        id = shards_list[i][0]
        shard = shards_list[i]
        shards_queried.append(id)
        data[id] = {}
        
        t = threading.Thread(target = read_shard_data, args = (i, id, stud_id_low, stud_id_high, data))
        threads.append((t, id))
        t.start()


    for t, id in threads:
        t.join()
        
    merged_data = []
    for shard in shards_list:
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

def write_shard_data(tid, shard, data_to_insert, primary_server, write_results):
    print(f"Write Thread {tid}: Shard {shard}",flush=True)
    
    # find all the servers having the shard
    mydb, mycursor = connect_to_db()
    mycursor.execute(f"SELECT Server_id FROM MapT WHERE Shard_id = '{shard}';")
    servers = mycursor.fetchall()
    close_db(mydb, mycursor)

    if len(servers) == 0:
        message = 'No servers available for the shard'
        write_results[tid] = {'message': message, 'status': 'success', 'numEntries': len(data_to_insert)}
        return
    
    acquire_writer_lock(shard)

    write_acks = {}
    count_success = 0.0
    data_payload = {
        "shard": shard,
        "curr_idx": 0,
        "data": data_to_insert,
        "mode": "log"
    }
    
    reply = requests.post(f'http://{primary_server}:{serverport}/write', json = data_payload)
    for server in servers:
        if server[0] == int(primary_server[7:]):
            continue
        server_name = f'Server_{server[0]}'
        try:
            reply = requests.post(f'http://{server_name}:{serverport}/write', json = {
                "shard": shard,
                "curr_idx": 0,
                "data": data_to_insert,
                "mode": 'log_execute'
            })
        except requests.exceptions.ConnectionError:
            write_acks[server_name] = False
            continue

        if reply.status_code != 200:
            write_acks[server_name] = False
            continue
        write_acks[server_name] = True
        print(f"Write Thread {tid}: Shard {shard} Server {server_name} write done",flush=True)
        count_success += 1
    
    data_payload['mode'] = 'execute'
    reply = requests.post(f'http://{primary_server}:{serverport}/write', json = data_payload)
    
    if reply.status_code != 200:
        write_acks[primary_server] = False
        print(f"Write Thread {tid}: Shard {shard} Primary Server {primary_server} execute failed",flush=True)
    
    else: 
        write_acks[primary_server] = True
        count_success += 1
        print(f"Write Thread {tid}: Shard {shard} Primary Server {primary_server} executed",flush=True)

    if (2 * count_success < len(servers)):
        # Write failed
        for server in write_acks:
            if write_acks[server] == True:
                print(f"Write Thread {tid}: Shard {shard} Server {server} rollback",flush=True)
                for data in data_to_insert:
                    try:
                        reply = requests.delete(f'http://{server}:{serverport}/del', json = {
                            "shard": shard,
                            "Stud_id": data['Stud_id']
                        })
                        if reply.status_code != 200:
                            print(f"Write Thread {tid}: Shard {shard} Server {server} delete failed",flush=True)
                            print(reply.json())
                    except requests.exceptions.ConnectionError:
                        continue
        write_results[tid] = {'message': 'Write failed', 'status': 'failure', 'numEntries': len(data_to_insert)}
        return

    if write_acks[primary_server] == False:
        conduct_election(shard)

    for server in write_acks:
        server_id = int(server[7:])
        if write_acks[server] == False:
            reply = requests.post(f'http://shardmanager:5001/respawn/{server_id}')
            if reply.status_code != 200:
                print(f"Write Thread {tid}: Shard {shard} Server {server} respawn failed",flush=True)
                print(reply.json())
                continue
            
            print(f"Write Thread {tid}: Shard {shard} Server {server} respawned",flush=True)

    release_writer_lock(shard)
    write_results[tid] = {'message': 'Data entries added', 'status': 'success', 'numEntries': len(data_to_insert)}
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
    
    mydb, mycursor = connect_to_db()

    mycursor.execute("SELECT * FROM ShardT;")
    shards = mycursor.fetchall()

    close_db(mydb, mycursor)

    data_idx = 0
    threads = []
    write_results = {}

    for i in range(len(shards)):
        shard = shards[i]
        id = shard[1]
        data_to_insert = []
        while data_idx < len(data) and data[data_idx]['Stud_id'] < shard[0] + shard[2]:
            data_to_insert.append(data[data_idx])
            data_idx += 1
        if len(data_to_insert) == 0:
            write_results[i] = {'status': 'success', 'message': 'no data to insert'}
            continue
        
        primary_server = shard[3]
        if primary_server == 'None':
            write_results[i] = {'status': 'failure', 'message': 'No primary server', 'numEntries': len(data_to_insert)}
            continue

        t = threading.Thread(target=write_shard_data, args=(i, id, data_to_insert,primary_server,write_results))
        threads.append((t, id))
        t.start()

    for t, id in threads:
        t.join()

    numEntries = len(data)

    for i in range(len(shards)):
        if i not in write_results:
            message = '<ERROR> No response received from the server, i not present'
            return jsonify({'message': message, 'status': 'failure'}), 400
        if 'status' not in write_results[i]:
            message = '<ERROR> \'status\' not present in write_results[i]'
            return jsonify({'message': message, 'status': 'failure'}), 400
        if write_results[i]['status'] == 'failure':
            numEntries -= write_results[i]['numEntries']

    if (numEntries == 0):
        message = '<ERROR> No data entries added'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    response = {
        "message": f"{numEntries} Data entries added",
        "status": "success"
    }

    return jsonify(response), 200

def send_modify_request(method, server, shard, stud_id, new_data, mode):
    try:
        if (method == 'Update'):
            reply = requests.put(f'http://{server}:{serverport}/update', json = {
                "shard": shard,
                "Stud_id": stud_id,
                "data": new_data, 
                "mode": mode
            })
        elif (method == 'Delete'):
            reply = requests.delete(f'http://{server}:{serverport}/del', json = {
                "shard": shard,
                "Stud_id": stud_id,
                "mode": mode
            })
        
        if reply.status_code != 200:
            message = f'<ERROR> {method} failed'
            return -1
    except requests.exceptions.ConnectionError:
        message = '<ERROR> Server unavailable'
        print(message)
        return -1    
    return 0

def modify_shard_data(method, shard, stud_id, new_data):
    # find all the servers having the shard
    mydb, mycursor = connect_to_db()
    mycursor.execute(f"SELECT Server_id FROM MapT WHERE Shard_id = '{shard}';")
    servers = mycursor.fetchall()
    
    mycursor.execute(f"SELECT Primary_server FROM ShardT WHERE Shard_id = '{shard}';")
    primary_server = mycursor.fetchone()
    primary_server = primary_server[0]

    close_db(mydb, mycursor)

    if len(servers) == 0:
        message = '<ERROR> No servers available for the shard'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    write_acks = {}
    count_success = 0.0

    # store the old value in case of rollback
    # read the old value from the primary server

    reply = requests.post(f'http://{primary_server}:{serverport}/read', json = {
        "shard": shard,
        "Stud_id": {"low": stud_id, "high": stud_id}
    })
    reply = reply.json()
    old_data = reply['data']
    old_data = old_data[0]

    reply = send_modify_request(method, primary_server, shard, stud_id, new_data, 'log')
    print(f'{method} logged for {primary_server}')
    for server in servers:
        if server[0] == int(primary_server[7:]):
            continue
        server_name = f'Server_{server[0]}'
        reply = send_modify_request(method, server_name, shard, stud_id, new_data, 'log_execute')
        if reply == -1:
            write_acks[server_name] = False
            continue

        count_success += 1
        write_acks[server_name] = True

    reply = send_modify_request(method, primary_server, shard, stud_id, new_data, 'execute')
    if reply == -1:
        write_acks[primary_server] = False
        print(f'{method} execute failed for {primary_server}')

    else:
        write_acks[primary_server] = True
        count_success += 1
        print(f'{method} executed for {primary_server}')

    if (2 * count_success < len(servers)):
        for server in write_acks:
            if write_acks[server] == True:
                if method == 'Delete':
                    try:
                        reply = requests.post(f'http://{server}:{serverport}/write', json = {
                            "shard": shard,
                            "curr_idx": 0,
                            "data": [old_data],
                            "mode": 'log_execute'
                        })
                    except requests.exceptions.ConnectionError:
                        continue
                else:
                    try:
                        reply = requests.put(f'http://{server}:{serverport}/update', json = {
                            "shard": shard,
                            "Stud_id": stud_id,
                            "data": old_data,
                            "mode": 'log_execute'
                        })
                    except requests.exceptions.ConnectionError:
                        continue
                
                if reply.status_code != 200:
                    print(f'Rollback failed for {server}')
        message = f'<ERROR> {method} failed'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    if write_acks[primary_server] == False:
        conduct_election(shard)
    
    for server in write_acks:
        server_id = int(server[7:])
        if write_acks[server] == False:
            reply = requests.post(f'http://shardmanager:5001/respawn/{server_id}')
            if reply.status_code != 200:
                print(f"Server {server} respawn failed")
                print(reply.json())
                continue
            print(f"Server {server} respawned")
    
    return jsonify({'message': f"Data entry for Stud_id:{stud_id} {method}d", 'status':'success'}), 200

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
    mydb, mycursor = connect_to_db()
    mycursor.execute(f"SELECT Shard_id FROM ShardT WHERE Stud_id_low <= {stud_id} AND Stud_id_low + Shard_size > {stud_id};")
    shard = mycursor.fetchone()
    close_db(mydb, mycursor)
    id = shard[0]

    acquire_writer_lock(id)
    response = modify_shard_data('Update', id, stud_id, new_data)
    release_writer_lock(id)

    return response

@app.route('/del', methods=['DELETE'])
def delete():
    content = request.json

    # check if 'Stud_id' exists in content
    if 'Stud_id' not in content:
        message = '<ERROR> Stud_id not present in request'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    Stud_id = content['Stud_id']

    # Find the shard that contains the data
    mydb, mycursor = connect_to_db()
    mycursor.execute(f"SELECT Shard_id FROM ShardT WHERE Stud_id_low <= {Stud_id} AND Stud_id_low + Shard_size > {Stud_id};")    
    shard = mycursor.fetchone()
    shard = shard[0]
    close_db(mydb, mycursor)
    
    acquire_writer_lock(shard)
    response = modify_shard_data('Delete', shard, Stud_id, '')
    release_writer_lock(shard)
    return response
    
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
    
    mydb, mycursor = connect_to_db()

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
    
    close_db(mydb, mycursor)
    return jsonify({'Data': data, 'status': 'success'}), 200
    
if __name__ == '__main__':
    serverport = 5000

    # Replicas is a list of lists. Each list has two entries: the External Name (user-specified or randomly generated) and the Container Name
    # The Container Name is the name of the container, and is the same as the hostname of the container. It is always of the form Server_<serverid>
    replicas = []

    # Bookkeeping for server IDs
    server_ids = set()
    next_server_id = 1

    studT_schema = ""
        
    # Start the server
    app.run(host='0.0.0.0', port=5000, debug=False)
from flask import Flask, request, jsonify
import json
import requests
from flask_cors import CORS
import os
import threading
import time

app = Flask(__name__)
CORS(app)

shard_data = {}
server_data = {}
shard_lock = threading.Lock()
server_lock = threading.Lock()

def election(shardid):
    print(f'Election started for shard {shardid}', flush=True)
    shard_lock.acquire()
    if shardid not in shard_data:
        add_shard(shardid)
    servers = shard_data[shardid]["servers"]
    shard_lock.release()

    if len(servers) == 0:
        return jsonify({'message': f'No servers available for shard {shardid}', 'status': 'success', 'primary': None}), 200

    global serverport
    votes = {}
    for server in servers:
        server_name = server_data[server]['container_name']
        try:
            reply = requests.get(f'http://{server_name}:{serverport}/vote/{shardid}')
        except requests.exceptions.ConnectionError:
            continue
        else:
            if reply.status_code == 200:
                votes[server] = reply.json()['vote']
    maxIndex = -1
    newPrimary = None
    for server in servers:
        if server not in votes:
            continue
        if votes[server] > maxIndex:
            maxIndex = votes[server]
            newPrimary = server
    
    if newPrimary is None:
        return jsonify({'message': 'No primary elected', 'status': 'success', 'primary': None}), 200
    
    print(f'Election successful for shard {shardid}. New primary: {newPrimary}', flush=True)
    shard_lock.acquire()
    shard_data[shardid]["primary"] = newPrimary
    shard_lock.release()
    return jsonify({'message': 'Election successful','status': 'success', 'primary': newPrimary}), 200

def respawn_server(serverid, server_name, shards_list, schema):
    print(f'Replica {server_name} is down',flush=True)

    for shard in shards_list:
        shard_lock.acquire()
        shard_data[shard]["servers"].remove(serverid)
        primary = shard_data[shard]["primary"]
        shard_lock.release()

        if serverid == primary:
            # conduct election
            print(f'DEBUG RESPAWN: Conducting election for shard {shard}', flush=True)
            reply = requests.get(f'http://shardmanager:5001/election/{shard}')
            status = reply.status_code
            print(f'DEBUG RESPAWN: Election reply: {reply}', flush=True)
            if status != 200:
                print(f'DEBUG RESPAWN: Election failed for shard {shard}', flush=True)
                return 
            
            print(f'DEBUG RESPAWN: Election successful for shard {shard} with primary {reply.json()["primary"]}', flush=True)
            new_primary = reply.json()["primary"]
            new_primary_server_name = f'Server_{new_primary}'
            if new_primary is None:
                new_primary_server_name = None
            reply = requests.post(f'http://loadbalancer:5000/set_new_primary', json = {
                "shard": shard,
                "server": new_primary_server_name
            })
            if reply.status_code != 200:
                print(f'DEBUG RESPAWN: New primary set failed for shard {shard}', flush=True)
                return 
            print(f'DEBUG RESPAWN: New primary set for shard {shard}', flush=True)    
    
    os.system(f'docker stop {server_name} && docker rm {server_name}')
    time.sleep(1)

    if (spawn_container_and_config(server_name, serverid, shards_list, schema) == -2):
        print(f'DEBUG RESPAWN: Server creation failed', flush=True)
        return

    print(f'DEBUG RESPAWN: Data replication started for server {server_name}')
    # first we will complete the data replication to the new server
    for shard in shards_list:
        replicate_data(shard, server_name)

    print(f'DEBUG RESPAWN: Data replication completed for server {server_name}')

    for shard in shards_list:
        shard_lock.acquire()
        shard_data[shard]["servers"].append(serverid)
        shard_lock.release()

    return 
  
def check_heartbeat(server_id, server_name, shards, schema):
    while server_data[server_id]['state'] == 'active':
        try:
            reply = requests.get(f'http://{server_name}:{serverport}/heartbeat')
            if reply.status_code != 200:
                respawn_server(server_id, server_name, shards, schema)
        except requests.exceptions.ConnectionError:
            respawn_server(server_id, server_name, shards, schema)
            
        time.sleep(10)

def add_shard(shardid):
    shard_data[shardid] = {}
    shard_data[shardid]["servers"] = []
    shard_data[shardid]["primary"] = None
    
def spawn_container_and_config(container_name, serverid, shards, schema):
    container = os.popen(f'docker run --name {container_name} --network mynet --network-alias {container_name} -e SERVER_ID={serverid} -d serverim:latest').read()
    time.sleep(5)
    if len(container) == 0:
        return -1
    print(f'DEBUG: Container {container_name} spawned', flush=True)
    try:
        data_payload = {}
        data_payload['schema'] = schema
        data_payload['shards'] = shards
        reply = requests.post(f'http://{container_name}:{serverport}/config', 
                                json = data_payload)
        time.sleep(3)
        if reply.status_code != 200:
            print(f'DEBUG: Server configuration failed', flush=True)
            os.system(f'docker stop {container_name} && docker rm {container_name}')
            return -1
    except requests.exceptions.ConnectionError:
        os.system(f'docker stop {container_name} && docker rm {container_name}')
        print(f'DEBUG: Server configuration failed. Connection Error', flush=True)
        return -2
    print("DEBUG: Server configuration successful", flush=True)
    return 0

def replicate_data(shard, server):
    # Get the data from the primary server
    
    shard_lock.acquire()
    primary = shard_data[shard]["primary"]
    shard_lock.release()

    print(f'Debug: Replicating data for shard {shard} with primary {primary}', flush=True)
    if (primary is None):
        return 
    
    server_lock.acquire()
    primary_server_name = server_data[primary]['container_name']
    server_lock.release()

    shard_logs = []
    try:
        reply = requests.get(f'http://{primary_server_name}:{serverport}/logs/{shard}')
        data = reply.json()
        shard_logs = data['logs']
        if reply.status_code != 200:
            print(f'Debug: Data replication failed', flush=True)
            return 
    except requests.exceptions.ConnectionError:
        print(f'Debug: Server unavailable', flush=True)
        return 
    print(f'Debug: Data fetched from primary {primary_server_name} for shard {shard}', flush=True)
    if len(shard_logs) == 0:
        return
    
    print(f'Debug: Data fetched: {shard_logs}', flush=True)

    try:
        reply = requests.post(f'http://{server}:{serverport}/replicate/{shard}', json = {
            "logs": shard_logs
        })
        if reply.status_code != 200:
            print(f'Debug: Data replication failed', flush=True)
            return 
    except requests.exceptions.ConnectionError:
        print(f'Debug: Server unavailable', flush=True)
        return 
    
    print(f'Debug: Data replication successful for shard {shard}, server {server}', flush=True)

@app.route('/add_server', methods=['POST'])
def add_server():
    global shard_data

    payload = request.json
    serverid = payload['server_id']
    container_name = payload['container_name']
    hostname = payload['hostname']
    shards = payload['shards']
    schema = payload['schema']

    serverid = int(serverid)
    if (spawn_container_and_config(container_name, serverid, shards, schema) == -2):
        return jsonify({'message': 'Server creation failed', 'status': 'failure'}), 400
    
    server_lock.acquire()
    server_data[serverid] = {}
    server_data[serverid]['container_name'] = container_name
    server_data[serverid]['hostname'] = hostname
    server_data[serverid]['shards'] = shards
    server_data[serverid]['schema'] = schema
    server_data[serverid]['thread'] = []
    server_data[serverid]['state'] = 'active'
    server_lock.release()

    # Add the server to the shard_data
    threads = []

    for shard in shards:
        new_shard = False
        shard_lock.acquire()
        if shard not in shard_data:
            new_shard = True
            add_shard(shard)

        shard_data[shard]["servers"].append(serverid)
        shard_lock.release()

        if not new_shard:
            t = threading.Thread(target=replicate_data, args=(shard, container_name))
            threads.append(t)
            t.start()
    
    for t in threads:
        t.join()

    thread = threading.Thread(target=check_heartbeat, args=(serverid, container_name, shards, schema))
    server_lock.acquire()
    server_data[serverid]['thread'] = thread
    server_lock.release()
    thread.start()

    print(f'Debug: Server {container_name} added', flush=True)
    return jsonify({'message': 'Server added successfully', 'status': 'success'}), 200

@app.route('/remove_server', methods=['DELETE'])
def remove_server():
    payload = request.json
    print(payload)
    serverid = payload['server_id']
    serverid = int(serverid)

    server_lock.acquire()
    if serverid not in server_data:
        server_lock.release()
        return jsonify({'message': 'Server not found', 'status': 'failure'}), 400
    
    container_name = server_data[serverid]['container_name']
    shards = server_data[serverid]['shards']
    server_data[serverid]['state'] = 'inactive'
    server_data[serverid]['thread'].join()
    
    shard_lock.acquire()
    for shard in shards:
        print("DEBUG: Removing server from shard {}".format(shard), flush=True)
        shard_data[shard]["servers"].remove(serverid)
    
    # send an abort message to the heartbeat thread

    # remove the server from server_data
    shard_lock.release()
    del server_data[serverid]
    server_lock.release()

    print(f'Debug: Removing server container {container_name}', flush=True)
    os.system(f'docker stop {container_name} && docker rm {container_name}')
    print(f'Debug: Server {container_name} removed', flush=True)
    return jsonify({'message': 'Server removed successfully', 'status': 'success'}), 200
        
@app.route('/respawn/<server_id>', methods=['POST'])
def respawn(server_id):
    server_lock.acquire()
    if server_id not in server_data:
        server_lock.release()
        return jsonify({'message': 'Server not found', 'status': 'failure'}), 400
    server_name = server_data[server_id]['container_name']
    shards = server_data[server_id]['shards']
    schema = server_data[server_id]['schema']
    server_lock.release()
    reply = respawn_server(server_id, server_name, shards, schema)
    reply = json.loads(reply)
    return jsonify(reply), 200
    
@app.route('/election/<shardid>', methods=['GET'])
def start_election(shardid):
    return election(shardid)

if __name__ == '__main__':

    serverport = 5000
    app.run(host='0.0.0.0', port=5001, debug=False)
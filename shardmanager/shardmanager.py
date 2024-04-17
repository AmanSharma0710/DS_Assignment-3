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


app = Flask(__name__)
CORS(app)

shard_data = {}
server_data = {}
shard_lock = threading.Lock()
server_lock = threading.Lock()

def add_shard(shardid):
    global shard_data
    print(f'Debug: Adding shard {shardid}', flush=True)
    shard_data[shardid] = {}
    shard_data[shardid]["servers"] = []
    shard_data[shardid]["primary"] = None
    print(f'Debug: Shard {shardid} added', flush=True)
    
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
            return -1
    except requests.exceptions.ConnectionError:
        print(f'DEBUG: Server configuration failed. Connection Error', flush=True)
        return -2
    print("DEBUG: Server configuration successful", flush=True)
    return 0

def replicate_data(shard, server):
    # Get the data from the primary server
    global shard_data

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
            return jsonify({'message': 'Data replication failed', 'status': 'failure'}), 400
    except requests.exceptions.ConnectionError:
        return jsonify({'message': 'Data replication failed. Connection Error', 'status': 'failure'}), 400
    print(f'Debug: Data fetched from primary {primary_server_name} for shard {shard}', flush=True)
    if len(shard_logs) == 0:
        return
    
    print(f'Debug: Data fetched: {shard_logs}', flush=True)

    # write the data to the new server
    try:
        reply = requests.post(f'http://{server}:{serverport}/replicate/{shard}', json = {
            "logs": shard_logs
        })
        if reply.status_code != 200:
            return jsonify({'message': 'Data replication failed', 'status': 'failure'}), 400
    except requests.exceptions.ConnectionError:
        message = '<ERROR> Server unavailable'
        return jsonify({'message': message, 'status': 'failure'}), 400
    
    print(f'Debug: Data replication successful for shard {shard}', flush=True)


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
    server_lock.release()

    # Add the server to the shard_data
    for shard in shards:
        new_shard = False
        shard_lock.acquire()
        if shard not in shard_data:
            print(f'DEBUG: New shard detected {shard}', flush=True)
            new_shard = True
            add_shard(shard)

        print(f'Debug: Adding server {serverid} to shard {shard}', flush=True)
        shard_data[shard]["servers"].append(serverid)
        shard_lock.release()

        if not new_shard:
            replicate_data(shard, container_name)
            
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
    
    shard_lock.acquire()
    for shard in shards:
        print("DEBUG: Removing server from shard {}".format(shard), flush=True)
        shard_data[shard]["servers"].remove(serverid)
        
    # remove the server from server_data
    del server_data[serverid]
    shard_lock.release()
    server_lock.release()

    print(f'Debug: Removing server container {container_name}', flush=True)
    os.system(f'docker stop {container_name} && docker rm {container_name}')
    print(f'Debug: Server {container_name} removed', flush=True)
    return jsonify({'message': 'Server removed successfully', 'status': 'success'}), 200
        
@app.route('/remove_shard', methods=['DELETE'])
def remove_shard():
    pass

@app.route('/election/<shardid>', methods=['GET'])
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

def respawn_server(server_name):
    print(f'Replica {server_name} is down',flush=True)
    serverid = int(server_name[7:])
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

    os.system(f'docker stop {replica[1]} && docker rm {replica[1]}')

    os.system(f'docker run --name {replica[1]} --network mynet --network-alias {replica[1]} -e SERVER_ID={serverid} -d serverim:latest')
    time.sleep(5)
    print(f'DEBUG RESPAWN: Replica {replica[1]} respawned', flush=True)

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
        server_lock.acquire()
        for server in server_data.items():
            server_name = server[1]['container_name']
            try:
                reply = requests.get(f'http://{server_name}:{serverport}/heartbeat')
            except requests.exceptions.ConnectionError:
                respawn_server(server_name)
            else:
                if reply.status_code != 200:
                    respawn_server(server_name)              
        server_lock.release()
        time.sleep(10)

if __name__ == '__main__':

    serverport = 5000

    # Setting up and spawning the thread that manages the replicas
    thread = threading.Thread(target=manage_replicas)
    thread.start()
    
    app.run(host='0.0.0.0', port=5001, debug=True)
# Class Logger for each server to log the requests for each shard that it is managing
import datetime
import threading
import os

class LogEntry:
    def __init__(self, message):
        self.timestamp = datetime.datetime.now()
        self.message = message
        self.log_id = hash(f'{self.message}')
        self.checksum = hash(f'{self.log_id}: {self.message}')

    def __str__(self):
        return f'{self.timestamp}: {self.log_id}: {self.message}: {self.checksum}'

    def __repr__(self):
        return f'{self.timestamp}: {self.log_id}: {self.message}: {self.checksum}'


class Logger:
    def __init__(self, server_id, shard_id):
        self.server_id = server_id
        self.shard_id = shard_id
        self.electionIndex = 0
        self.file_lock = threading.Lock()
        self.uncommitted_logs = {}
        self.file_name = f'logs/{server_id}_{shard_id}_log.txt'
        file = open(self.file_name, 'w')
    
    def addLogEntry(self, message, rollback_flag=False):
        with self.file_lock:
            logMessage = LogEntry(message)
            self.uncommitted_logs[logMessage.log_id] = message
            with open(self.file_name, 'a') as f:
                f.write(str(logMessage) + '\n')    
    
    def addCommitEntry(self, message):
        with self.file_lock:
            log_id = hash(f'{message}')
            logMessage = LogEntry('COMMIT')
            if log_id not in self.uncommitted_logs:
                print('LOGGER: Log not found in uncommitted logs')
            with open(self.file_name, 'a') as f:
                f.write(str(logMessage) + '\n')
            del self.uncommitted_logs[log_id]
            
    def getElectionIndex(self):
        with self.file_lock:
            return self.electionIndex
    
    def getLogs(self):
        with self.file_lock:
            with open(self.file_name, 'r') as f:
                return f.read()
    
    # define a destructor to delete the log file
    def __del__(self):
        with self.file_lock:
            os.remove(self.file_name)
    
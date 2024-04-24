# Class Logger for each server to log the requests for each shard that it is managing
import datetime
import threading
import os

class LogEntry:
    def __init__(self, message, log_id):
        self.timestamp = datetime.datetime.now()
        self.message = message
        self.log_id = log_id

    def __str__(self):
        return f'{self.timestamp}: {self.log_id}: {self.message}'

    def __repr__(self):
        return f'{self.timestamp}: {self.log_id}: {self.message}'

class Logger:
    def __init__(self, server_id, shard_id):
        self.server_id = server_id
        self.shard_id = shard_id
        self.electionIndex = 0
        self.index = 0
        self.lock = threading.Lock()
        self.uncommitted_logs = {}
        self.file_name = f'logs/{server_id}_{shard_id}_log.txt'
        file = open(self.file_name, 'w').close()
    
    def addLogEntry(self, message):
        with self.lock:
            self.index += 1
            logMessage = LogEntry(message, self.index)
            self.uncommitted_logs[message] = self.index
            print(f"LOGGER ADD LOG ENTRY: {str(logMessage)}", flush=True)
            with open(self.file_name, 'a') as f:
                f.write(str(logMessage) + '\n')
                print(f'LOGGER ADD LOG ENTRY: {str(logMessage)}', flush=True) 
            
    
    def addCommitEntry(self, message):
        with self.lock:
            if message not in self.uncommitted_logs:
                print('LOGGER: Log not found in uncommitted logs', flush=True)
                return
            logMessage = LogEntry('COMMIT', self.uncommitted_logs[message])
            del self.uncommitted_logs[message]
            print(f"LOGGER: COMMIT ENTRY: {message}", flush=True)
            with open(self.file_name, 'a') as f:
                f.write(str(logMessage) + '\n')
            self.electionIndex += 1
            
    def getElectionIndex(self):
        with self.lock:
            return self.electionIndex
    
    def getLogMessage(self, log_id):
        with self.lock:
            # go through uncommited logs
            for message, id in self.uncommitted_logs.items():
                if id == log_id:
                    return message
        return None
    
    def getLogs(self):
        logs = []
        with self.lock:
            with open(self.file_name, 'r') as f:
                for line in f:
                    logs.append(line)
        return logs
                
    def resetLogs(self):
        with self.lock:
            os.remove(self.file_name)
            file = open(self.file_name, 'w')
            self.electionIndex = 0
            self.uncommitted_logs = {}
    

    
# Class Logger for each server to log the requests
import datetime

class Logger:
    def __init__(self, server_id):
        self.server_id = server_id
        current_idx = 0
        # create a log file for the server
        with open('logs/server_{}.log'.format(server_id), 'w') as f:
            # timestamp: server_id: Log file init
            f.write(f'{datetime.now()}: {server_id}: Log file init\n')

    def add_log(self, request):
        with open('logs/server_{}.log'.format(self.server_id), 'a') as f:
            f.write(request + '\n')
            current_idx += 1

    def get_logs(self):
        logs = []
        with open('logs/server_{}.log'.format(self.server_id), 'r') as f:
            for line in f:
                logs.append(line.strip())
        return logs
    
    def get_idx(self):
        return self.current_idx
    
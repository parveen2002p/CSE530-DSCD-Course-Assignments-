import zmq
import threading
import time
from datetime import datetime
import sys

class GroupServer:
    def __init__(self, server_name, message_server_address="tcp://35.223.112.55:50051", address="tcp://0.0.0.0:50052"):
        self.server_name = server_name
        self.context = zmq.Context()
        self.server_socket = self.context.socket(zmq.REQ)
        self.server_socket.connect(message_server_address)
        self.group_address = address
        # Socket for communication with users
        self.own_socket = self.context.socket(zmq.REP)
        self.own_socket.bind("tcp://0.0.0.0:"+self.group_address.split(":")[2])
        #self.own_socket.bind(self.group_address)
        self.user_tele={}
        self.message_data=[]
    
    def register(self):
        request = f"JOIN REQUEST FROM {self.server_name} {self.group_address}"
        self.server_socket.send_string(request)
        response = self.server_socket.recv_string()
        print(f"Registration Response from Message Server: {response}")
        
        if response == "SUCCESS":
            self.run()
            pass

    def run(self):
        print(f"Group Server ({self.server_name}) is running...")        
        poller = zmq.Poller()
        poller.register(self.own_socket, zmq.POLLIN)

        while True:
            socks = dict(poller.poll())

            if self.own_socket in socks and socks[self.own_socket] == zmq.POLLIN:
                message = self.own_socket.recv_string()
                if message.startswith("JOIN REQUEST"):
                    self.handle_join_request(message)
                elif message.startswith("LEAVE REQUEST"):
                    self.handle_leave_request(message)
                elif message.startswith("MESSAGE REQUEST"):
                    self.handle_retrieval_request(message)
                elif message.startswith("MESSAGE SEND"):
                    self.handle_message_request(message)

    def handle_join_request(self, message):
        print(message)
        argList=message.split()
        user_uuid = argList[-1]  
        timestamp = time.time()
        if user_uuid in self.user_tele:
            print("Already joined user")
            self.own_socket.send_string("SUCCESS")
        else:
            self.user_tele[user_uuid] = timestamp
            response = "SUCCESS"
            self.own_socket.send_string(response)

    def handle_leave_request(self, message):
        print(message)
        argList=message.split()
        user_uuid = argList[-1]
        self.user_tele.pop(user_uuid)
        response = "SUCCESS"
        self.own_socket.send_string(response)

    def handle_message_request(self, message):
        argList = message.split()
        user_uuid = argList[-2]
        text_message=argList[-1]
        if user_uuid in self.user_tele:
            #threading.Thread(target=self.process_message, args=(user_uuid,text_message,)).start()
            self.process_message(user_uuid,text_message)
        else:
            response = "FAIL"
            self.own_socket.send_string(response)

    def process_message(self, user_uuid,text_message):
        timestamp = time.time()
        data=[]
        data.append(timestamp)
        data.append(user_uuid)
        data.append(text_message)
        self.message_data.append(data)
        print(f"MESSAGE SEND FROM {user_uuid}")
        response = "SUCCESS"
        self.own_socket.send_string(response)
    
    def handle_retrieval_request(self, message):
        argList = message.split()
        user_uuid = argList[-1]
        timestamp="NO"
        if len(argList)==6:
            user_uuid = argList[-3]
            timestamp=argList[-2]+" "+argList[-1]
        if user_uuid in self.user_tele:
            #threading.Thread(target=self.process_retrieval, args=(user_uuid,timestamp,)).start()
            self.process_retrieval(user_uuid,timestamp)
        else:
            response = "FAIL"
            self.own_socket.send_string(response)

    

    def compare_timestamps(self,t1, t2, format="%Y-%m-%d %H:%M:%S"):
        dt1 = datetime.fromtimestamp(t1)
        dt2 = datetime.strptime(t2, format)

        if dt1 < dt2:
            #return "t1 is older than t2"
            return 1
        elif dt1 == dt2:
            #return "t1 is equal to t2"
            return 0
        else:
            #return "t1 is newer than t2"
            return -1
        

    # # Example usage
    # timestamp1 = "2022-02-03 12:30:00"
    # timestamp2 = "2022-02-03 14:15:00"

    
    def process_retrieval(self, user_uuid,timestamp):
        if(timestamp=="NO"):
            response=""
        
            for item in self.message_data:
                data1=str(datetime.fromtimestamp(item[0]))+" - "+item[1]+" : "+item[2]
                response = response+data1+"\n"
        else:
            response=""
            for item in self.message_data:
                if(self.compare_timestamps(item[0],timestamp)==1):
                    continue
                data1=str(datetime.fromtimestamp(item[0]))+" - "+item[1]+" : "+item[2]
                response = response+data1+"\n"

        print(f"MESSAGE REQUEST FROM {user_uuid}")
        
        self.own_socket.send_string(response)


def create_group_server(server_name, address,message_server_address):
    group_server = GroupServer(server_name=server_name,message_server_address=message_server_address,address=address)
    group_server.register()

if __name__ == "__main__":
    
    server_name = sys.argv[1]
    address = sys.argv[2]
    message_server_address=sys.argv[3]
    create_group_server(server_name,address,message_server_address)

    

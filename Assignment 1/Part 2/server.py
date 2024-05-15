import zmq
import re

class MessageServer:
    def __init__(self, address="tcp://0.0.0.0:50051"):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(address)
        self.group_list=[]

    def run(self):
        print("Message Server is running...")
        while True:
            message = self.socket.recv_string()
            if message.startswith("JOIN REQUEST"):
                
                self.handle_join_request(message)
            elif message.startswith("GROUP LIST REQUEST"):
                
                self.handle_group_list_request(message)

    def handle_join_request(self, message):
        argList=message.split()
        
        if len(argList)>0:
            tempStr=argList[4][6::]
            tempList=tempStr.split(':')
            ip= tempList[0]
            port = tempList[1]
            group_name = argList[3]
            self.register_group_server(group_name, ip, port)
            response = "SUCCESS"
            self.socket.send_string(response)
        else:
            response = "FAILURE"
            self.socket.send_string(response)
    
    def register_group_server(self, group_name, ip, port):
        group_details = {"group_name": group_name, "ip": ip, "port": port}
        self.group_list.append(group_details)
        print(f"JOIN REQUEST FROM {group_name} [{ip}:{port}]")

    def handle_group_list_request(self, message):
        print(message)
        response=""
        count=1
        for item in self.group_list:
            data1=str(count)+" "+item["group_name"] +" - "+item["ip"]+":"+item["port"]
            count+=1
            response = response+data1+"\n"
        self.socket.send_string(response)

if __name__ == "__main__":
    server = MessageServer()
    server.run()

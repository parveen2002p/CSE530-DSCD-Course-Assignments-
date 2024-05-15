import zmq
import uuid
import sys

class User:
    def __init__(self, user_id,  message_server_address="tcp://35.223.112.55:50051"):
        self.user_id = user_id
        self.context = zmq.Context()
        
        self.message_server_socket = self.context.socket(zmq.REQ)
        self.message_server_socket.connect(message_server_address)

        self.group_socket = []
        self.current_available_groups=""
        self.joined_groups=[]
        

    def get_group_list(self):
        request = f"GROUP LIST REQUEST FROM {self.user_id}"
        self.message_server_socket.send_string(request)
        response = self.message_server_socket.recv_string()
        print(f"{self.user_id} received group list:\n{response}")
        self.current_available_groups=response

    def join_group(self, group_server_address):
        self.group_socket.append(self.context.socket(zmq.REQ))
        self.group_socket[-1].connect(group_server_address)
        request = f"JOIN REQUEST FROM {self.user_id}"
        self.group_socket[-1].send_string(request)
        response = self.group_socket[-1].recv_string()
        print(f"{self.user_id} received joining response: {response}")
        if(response=="SUCCESS"):
            self.joined_groups.append(group_server_address)

    def leave_group(self,group_ID):
        request = f"LEAVE REQUEST FROM {self.user_id}"
        self.group_socket[group_ID].send_string(request)
        response = self.group_socket[group_ID].recv_string()
        print(f"{self.user_id} received leave response: {response}")
        if(response=="SUCCESS"):
            self.joined_groups.pop(group_ID)
            self.group_socket.pop(group_ID)
        

    def get_messages(self, group_ID,timestamp=None):
        if timestamp:
            request = f"MESSAGE REQUEST FROM {self.user_id} {timestamp}"
        else:
            request = f"MESSAGE REQUEST FROM {self.user_id}"
        
        self.group_socket[group_ID].send_string(request)
        response = self.group_socket[group_ID].recv_string()
        print(f"{self.user_id} received messages:\n{response}")
    
    def send_message(self,Text,group_ID):
        request = f"MESSAGE SEND FROM {self.user_id} {Text}"
        self.group_socket[group_ID].send_string(request)
        response = self.group_socket[group_ID].recv_string()
        print(f"{self.user_id} received response of sent message : {response}")

if __name__ == "__main__":
    #mainAddr=sys.argv[1]
    UserList=[]
    countUser= 1
    for k in range(0,countUser):
        CustomId=str(uuid.uuid1())
        UserList.append(User((CustomId)))
    flagclose=False
    while flagclose==False:
        print("""
            1. Get Group List:
            2. Join Group
            3. Leave Group
            4. Send Message
            5. Recieve Message
              """)
        inpu=int(input("Enter choice : "))
        if(inpu==1):
            cmdIn=0
            UserList[int(cmdIn)].get_group_list()
        elif(inpu==2):
            # handle duplicate join here
            userID=0
            print(UserList[int(userID)].current_available_groups)
            gno= str(input(("Enter complete address of group to join: ")))
            # tcp://127.0.0.1:5555
            if gno not in UserList[int(userID)].joined_groups:
                try:
                    UserList[int(userID)].join_group(gno)
                except:
                    print("Failed to connect! ")
            else:
                print("Already joined !")
        elif(inpu==3):
            userID=0
            count=1
            for k in UserList[int(userID)].joined_groups:
                print(str(count)+" : "+ k)
                count+=1
            gno=int(input("Enter group no. from above list : "))
            if(gno==-1 or gno==0 or gno>len(UserList[int(userID)].joined_groups)):
                pass
            else:
                UserList[int(userID)].leave_group(gno-1)
        elif(inpu==4):
            userID=0
            count=1
            for k in UserList[int(userID)].joined_groups:
                print(str(count)+" : "+ k)
                count+=1
            gno=int(input("Enter group no. from above list : "))
            text=input("Enter text msg : ")
            if(gno==-1 or gno==0 or gno>len(UserList[int(userID)].joined_groups)):
                pass
            else:
                UserList[int(userID)].send_message(text,gno-1)
        elif(inpu==5):
            userID=0
            count=1
            for k in UserList[int(userID)].joined_groups:
                print(str(count)+" : "+ k)
                count+=1
            gno=int(input("Enter group no. from above list : "))
            text=input("Enter timestamp if any : ")
            if(gno==-1 or gno==0 or gno>len(UserList[int(userID)].joined_groups)):
                pass
            else:
                if(text==""):
                    UserList[int(userID)].get_messages(gno-1,None)
                else:
                    UserList[int(userID)].get_messages(gno-1,text)


# timestamp1 = "2022-02-03 12:30:00"        

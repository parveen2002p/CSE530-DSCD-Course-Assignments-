import grpc
import random
import raft_pb2
import raft_pb2_grpc

if __name__ == '__main__':

    print("\n\nWelcome to the Raft Consensus Algorithm.\n")

    nodes = {1: "127.0.0.1:5001", 2: "127.0.0.1:5002",
             3: "127.0.0.1:5003", 4: "127.0.0.1:5004", 5: "127.0.0.1:5005"}
    node = int(random.uniform(1, len(nodes)))

    while True:
        try:
            print("\nChoose Action to Perform:-")
            print("1. SET Data")
            print("2. GET Data")
            choice = int(input("\nEnter your Choice: "))

            channel = grpc.insecure_channel(nodes[node])
            stub = raft_pb2_grpc.RaftNodeServiceStub(channel)

            if choice == 1:
                key = str(input("\nEnter Key: "))
                value = str(input("Enter Value: "))

                try:
                    response = stub.SET(
                        raft_pb2.SetRequest(key=key, value=value))

                    if response.responseMessage.startswith("I'm not a Leader"):
                        try:
                            node = int(
                                response.responseMessage.split(": ")[-1])

                        except Exception as e:
                            print(
                                "\nCurrently there is no Leader. Please try again later.\n")

                    print("\nResponse: " + response.responseMessage)

                except Exception as e:
                    node = int(random.uniform(1, len(nodes)))
                    print("\nRedirecting to Node: " + str(node))
                    continue

            elif choice == 2:
                key = str(input("\nEnter Key: "))

                try:
                    response = stub.GET(raft_pb2.GetRequest(key=key))
                    if response.error.startswith("I'm not a Leader"):
                        try:
                            node = int(response.error.split(": ")[-1])

                        except Exception as e:
                            print(
                                "\nCurrently there is no Leader. Please try again later.\n")

                    print("\nValue: " + response.value)
                    print(response.error)

                except Exception as e:
                    node = int(random.uniform(1, len(nodes)))
                    print("\nRedirecting to Node: " + str(node))
                    continue

            else:
                print("\nInvalid Choice. Please Try Again.\n")
                continue

        except KeyboardInterrupt:
            print("\n\nExiting the Application.\n")
            break

import os
import sys
import ast
import grpc
import time
import numpy
import random
import traceback
import threading
import subprocess
import KMeans_pb2
import KMeans_pb2_grpc


class Master:
    def __init__(self, M, R, K, max_iters):

        self.M = M
        self.R = R
        self.K = K
        self.centroids = []
        self.chunks_index = []
        self.new_centroids = []
        self.mapper_processes = []
        self.reducer_processes = []
        self.max_iters = max_iters

        if not os.path.exists(f"Data/Mappers"):
            os.makedirs(f"Data/Mappers")

        if not os.path.exists(f"Data/Reducers"):
            os.makedirs(f"Data/Reducers")

        for i in range(self.M):
            self.mapper_processes.append(subprocess.Popen(
                ["python", "Mapper.py"] + [str(5000 + i + 1)]))

        for i in range(self.R):
            self.reducer_processes.append(subprocess.Popen(
                ["python", "Reducer.py"] + [str(50000 + i + 1)]))

        print(f"\n\nStarted {M} Mapper and {R} Reducer Processes.\n\n")

    def run(self):

        try:
            X = []
            centroid_list = []

            with open('Data/Input/points.txt', 'r') as file:
                data = file.readlines()

                for line in data:
                    x, y = line.strip().split(',')
                    X.append([float(x), float(y)])

            X = numpy.array(X)
            self.centroids = [[centroid[0], centroid[1]] for centroid in X[numpy.random.choice(
                X.shape[0], self.K, replace=False)]]
            centroid_list = [centroid for centroid in self.centroids]

            self.split_data()

            with open('Data/Dump.txt', 'w') as file:
                print("Initial Randomly Selected Centroids:- \n")
                file.write("Initial Randomly Selected Centroids:- \n")

                for centroid in self.centroids:
                    file.write(f"{centroid[0]}, {centroid[1]}\n")
                    print(f"{centroid[0]}, {centroid[1]}")
                file.write("\n\n")
                file.close()

            total_iteration = self.max_iters

            while self.max_iters > 0:

                self.max_iters -= 1

                with open('Data/Dump.txt', 'a') as file:
                    file.write(
                        f"Iteration Number: {total_iteration - self.max_iters}\n\n")
                    print(
                        f"\nIteration Number: {total_iteration - self.max_iters} \n")
                    file.close

                threads = []
                response = [-1 for i in range(self.M)]

                def mapper_task(i):

                    channel = grpc.insecure_channel(
                        'localhost:' + str(5000 + i + 1))
                    stub = KMeans_pb2_grpc.MapperStub(channel)

                    with open("Data/Dump.txt", 'a') as file:
                        file.write(
                            f"Master Sending gRPC call to Mapper {i + 1} @ localhost:{5000 + i + 1}\n\n")
                        print(
                            f"Master Sending gRPC call to Mapper {i + 1} @ localhost:{5000 + i + 1}\n")
                        file.close()

                    if i < len(self.chunks_index):
                        response[i] = stub.MapTask(KMeans_pb2.MapRequest(centroid_list=str(
                            centroid_list), data_chunks_index=str(self.chunks_index[i]), R=self.R)).success

                    else:
                        response[i] = stub.MapTask(KMeans_pb2.MapRequest(centroid_list=str(
                            centroid_list), data_chunks_index=str("None"), R=self.R)).success

                    with open("Data/Dump.txt", 'a') as file:
                        file.write(
                            f"Master Got gRPC Response = {response[i]} from Mapper {i + 1} @ localhost:{5000 + i + 1}\n\n")
                        print(
                            f"Master Got gRPC Response = {response[i]} from Mapper {i + 1} @ localhost:{5000 + i + 1}\n")
                        file.close()

                for i in range(self.M):
                    thread = threading.Thread(target=mapper_task, args=(i,))
                    threads.append(thread)
                    thread.start()

                for thread in threads:
                    thread.join()

                indexx = 0
                while True:
                    if str(response[indexx]) == "-1":
                        indexx = 0

                    else:
                        indexx += 1

                    if indexx == len(response):
                        break

                while not all(response):
                    trueList = []
                    firstTrue = -1
                    firstFalse = -1

                    for i in range(len(response)):
                        if response[i] == False and firstFalse == -1:
                            firstFalse = i

                        if response[i] == True:
                            trueList.append(i)

                    if len(trueList) == 0:
                        firstTrue = 0
                        firstFalse = 0

                    else:
                        firstTrue = random.choice(trueList)

                    prev_response = response[firstTrue]
                    response[firstTrue] = -1
                    thread = threading.Thread(
                        target=mapper_task, args=(firstTrue,))
                    thread.start()
                    thread.join()

                    while str(response[firstTrue]) == "-1":
                        continue

                    if firstTrue != firstFalse:
                        response[firstFalse] = response[firstTrue]
                        response[firstTrue] = prev_response

                response = [[-1, ""] for i in range(self.R)]
                threads.clear()

                def reducer_task(i):
                    channel = grpc.insecure_channel(
                        'localhost:' + str(50000 + i + 1))
                    stub = KMeans_pb2_grpc.ReducerStub(channel)

                    with open("Data/Dump.txt", 'a') as file:
                        file.write(
                            f"Master Sending gRPC call to Reducer {i + 1} @ localhost:{50000 + i + 1}\n\n")
                        print(
                            f"Master Sending gRPC call to Reducer {i + 1} @ localhost:{50000 + i + 1}\n")
                        file.close()

                    responsee = stub.ReduceTask(KMeans_pb2.ReduceRequest(
                        mapper_port=[str(5000 + i + 1) for i in range(self.M)], partition=i))
                    response[i] = [responsee.success, responsee.output_files]

                    with open("Data/Dump.txt", 'a') as file:
                        file.write(
                            f"Master Got gRPC Response = {response[i][0]} from Reducer {i + 1} @ localhost:{50000 + i + 1}\n\n")
                        print(
                            f"Master Got gRPC Response = {response[i][0]} from Reducer {i + 1} @ localhost:{50000 + i + 1}\n")
                        file.close()

                for i in range(self.R):
                    thread = threading.Thread(target=reducer_task, args=(i,))
                    threads.append(thread)
                    thread.start()

                for thread in threads:
                    thread.join()

                indexx = 0
                while True:
                    if str(response[indexx][0]) == "-1":
                        indexx = 0

                    else:
                        indexx += 1

                    if indexx == len(response):
                        break

                while not all([response[i][0] for i in range(len(response))]):
                    firstFalse = -1

                    for i in range(len(response)):
                        if response[i][0] == False and firstFalse == -1:
                            firstFalse = i
                            break

                    response[firstFalse][0] = -1

                    thread = threading.Thread(
                        target=reducer_task, args=(firstFalse,))
                    thread.start()
                    thread.join()

                    while str(response[firstFalse][0]) == "-1":
                        continue

                for i in range(self.R):
                    for centroid in [[float(x) for x in sublist] for sublist in ast.literal_eval(response[i][1])]:
                        self.new_centroids.append(centroid)

                self.centroid_list = sorted(self.new_centroids)
                self.new_centroids = sorted(self.new_centroids)

                x_new_centroid = [new_centroid[0]
                                  for new_centroid in self.new_centroids]
                y_new_centroid = [new_centroid[1]
                                  for new_centroid in self.new_centroids]

                x_old_centroid = [old_centroid[0]
                                  for old_centroid in self.centroids]
                y_old_centroid = [old_centroid[1]
                                  for old_centroid in self.centroids]

                if numpy.allclose(x_new_centroid, x_old_centroid) and numpy.allclose(y_new_centroid, y_old_centroid):

                    print("\n\n<---------------CONVERGED--------------->\n\n")

                    with open("Data/Dump.txt", 'a') as file:
                        print("Final Centroids:- \n")
                        file.write(
                            "\n\n<---------------CONVERGED--------------->\n\n")
                        file.write("Final Centroids:-\n")
                        for centroid in self.centroids:
                            file.write(f"{centroid[0]}, {centroid[1]}\n")
                            print(f"{centroid[0]}, {centroid[1]}")
                        file.close()
                    self.write_centroids()
                    break

                self.centroids = [[new_centroid[0], new_centroid[1]]
                                  for new_centroid in self.new_centroids]
                centroid_list = [centroid for centroid in self.centroids]

                with open("Data/Dump.txt", 'a') as file:
                    print("Current Centroids:- ")
                    file.write("Current Centroids:-\n")
                    for centroid in self.centroids:
                        file.write(f"{centroid[0]}, {centroid[1]}\n")
                        print(f"{centroid[0]}, {centroid[1]}")
                    file.write("\n\n")
                    file.close()

                self.new_centroids = []
                self.centroid_list = []
                self.write_centroids()

        except Exception as e:
            print("Error: ", e)
            traceback.print_exc()

            for process in self.mapper_processes:
                process.terminate()

            for process in self.reducer_processes:
                process.terminate()

    def write_centroids(self):

        with open('Data/Centroids.txt', 'w') as file:
            for centroid in self.centroids:
                file.write(f"{centroid[0]}, {centroid[1]}")
            file.close()

    def split_data(self):

        with open('Data/Input/points.txt', 'r') as file:
            points = file.readlines()

            chunks = len(points) // self.M

            if chunks == 0:
                self.chunks_index.append([0, len(points) - 1])
                return

            cnt = 0
            prev_i = 1
            for i in range(len(points)):
                cnt += 1

                if cnt == chunks:
                    self.chunks_index.append([prev_i - 1, i])

                    if len(self.chunks_index) == self.M:
                        self.chunks_index[-1] = [prev_i - 1, len(points) - 1]

                    cnt = 0
                    prev_i = i + 2

            file.close()

    def __del__(self):

        for process in self.mapper_processes:
            process.terminate()

        for process in self.reducer_processes:
            process.terminate()

        print("\n\nMaster Process Terminate and all Mapper and Reducer Processes Terminated.\n\n")
        os._exit(1)


if __name__ == "__main__":

    if len(sys.argv) < 5:
        print("Usage: python Master.py <number_of_mappers> <number_of_reducers> <number_of_centroids> <max_iterations>")
        os._exit(1)

    centroids = None

    with open('Data/Input/points.txt', 'r') as file:
        if len(file.readlines()) < int(sys.argv[3]):
            print(
                "\n\nNumber of Centroids should be less than or equal to number of data points.\n\n")
            file.close()
            os._exit(1)

    try:
        centroids = Master(int(sys.argv[1]), int(sys.argv[2]), int(
            sys.argv[3]), int(sys.argv[4]))

        time.sleep(7)
        centroids.run()

    except Exception as e:
        del centroids

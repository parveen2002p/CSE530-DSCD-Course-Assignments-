import os
import sys
import ast
import time
import grpc
import random
import KMeans_pb2
import KMeans_pb2_grpc
from concurrent import futures
from scipy.spatial.distance import euclidean


class MapperServicer(KMeans_pb2_grpc.MapperServicer):

    def __init__(self, id):

        self.R = 0
        self.id = id
        self.mapping = {}
        self.data_point = []
        self.data_storage = []
        self.centroid_list = []
        self.failure_flag = False
        self.x = 0

    def MapTask(self, request, context):

        if str(self.centroid_list) == request.centroid_list:
            self.data_storage = []

            for i in range(self.R):
                with open(f"Data/Mappers/M{self.id}/partition_{i + 1}.txt", "r") as f:
                    self.data_storage.append(f.readlines())
                    f.close()

            self.x += 1
            self.failure_flag = True

        self.mapping = {}
        self.data_point = []
        self.centroid_list = []

        try:
            self.R = request.R
            if request.data_chunks_index == "None":
                for i in range(self.R):
                    if not os.path.exists(f"Data/Mappers/M{self.id}"):
                        os.makedirs(f"Data/Mappers/M{self.id}")

                    with open(f"Data/Mappers/M{self.id}/partition_{i + 1}.txt", "w") as f:
                        f.close()

                self.failure_flag = False
                self.x = 0
                return KMeans_pb2.MapResponse(success=True)

            self.generate_data_points(
                request.data_chunks_index, request.centroid_list)

            self.mapping = {(centroid[0], centroid[1]): []
                            for centroid in self.centroid_list}

            for data in self.data_point:
                nearest_centroid = None
                min_distance = float("inf")

                for centroid in self.centroid_list:
                    distance = euclidean(data, centroid)
                    if (distance < min_distance):
                        min_distance = distance
                        nearest_centroid = centroid

                self.mapping[(nearest_centroid[0],
                              nearest_centroid[1])].append(data)

            self.partition_intermediate_output()

            self.x = 0
            self.failure_flag = False
            if random.choice(["0", "1", "0", "1", "1", "0", "1", "0", "0", "1"]) == "0":
                return KMeans_pb2.MapResponse(success=True)

            return KMeans_pb2.MapResponse(success=False)

        except Exception:
            pass

    def partition_intermediate_output(self):

        for i in range(self.R):
            if not os.path.exists(f"Data/Mappers/M{self.id}"):
                os.makedirs(f"Data/Mappers/M{self.id}")

            with open(f"Data/Mappers/M{self.id}/partition_{i + 1}.txt", "w") as f:
                f.close()

        if self.failure_flag == False:
            i = 0

            for centroid, data_points in self.mapping.items():
                with open(f"Data/Mappers/M{self.id}/partition_{i + 1}.txt", "a") as f:
                    f.write(f"{centroid} : {data_points}\n")
                    f.close()

                i = (i + 1) % self.R

        else:
            for i in range(self.R):
                for line in self.data_storage[i]:
                    centroid, data_points = line.split(":")
                    centroid = ast.literal_eval(centroid.strip())
                    centroid = (float(centroid[0]), float(centroid[1]))
                    data_points = [[float(x) for x in sublist]
                                   for sublist in ast.literal_eval(data_points)]

                    self.mapping[centroid].extend(data_points)

            i = 0
            for centroid, data_points in self.mapping.items():
                with open(f"Data/Mappers/M{self.id}/partition_{i + 1}.txt", "a") as f:
                    f.write(f"{centroid} : {data_points}\n")
                    f.close()

                i = (i + 1) % self.R

    def ServeData(self, request, context):

        data = None
        with open(f"Data/Mappers/M{self.id}/partition_{request.partition_number}.txt", "r") as f:
            data = f.readlines()
            f.close()

        return KMeans_pb2.ReadResponse(data=data)

    def generate_data_points(self, dataChunksIndex, centroidList):

        dataChunks = []
        dataChunksIndex = ast.literal_eval(dataChunksIndex)

        with open("Data/Input/points.txt", 'r') as file:
            dataChunks = file.readlines(
            )[dataChunksIndex[0]:dataChunksIndex[1] + 1]
            file.close()

        self.data_point = [[float(x) for x in sublist] for sublist in [list(
            map(float, point.strip().split(','))) for point in dataChunks]]
        self.centroid_list = [[float(x) for x in sublist]
                              for sublist in ast.literal_eval(centroidList)]


if __name__ == "__main__":

    print(f"Mapper {int(sys.argv[1]) - 5000} Process Started.\n")

    mapperObjet = MapperServicer(int(sys.argv[1]) - 5000)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    KMeans_pb2_grpc.add_MapperServicer_to_server(mapperObjet, server)
    server.add_insecure_port(f'localhost:{sys.argv[1]}')
    server.start()

    try:
        while True:
            time.sleep(86400)

    except Exception:
        print("\n\n\Mapper Process ENDED!\n\n")
        server.stop(0)

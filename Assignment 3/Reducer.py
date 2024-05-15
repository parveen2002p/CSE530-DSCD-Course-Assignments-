import sys
import ast
import time
import grpc
import random
import KMeans_pb2
import KMeans_pb2_grpc
from concurrent import futures


class ReducerServicer(KMeans_pb2_grpc.ReducerServicer):

    def __init__(self, id):

        self.M = 0
        self.id = id
        self.mapping = {}
        self.partition = 0
        self.new_centroids = []

    def ReduceTask(self, request, context):

        self.mapping = {}
        self.new_centroids = []

        try:
            data = []
            self.M = len(request.mapper_port)
            self.partition = request.partition

            for i in range(self.M):

                channel = grpc.insecure_channel(
                    f'localhost:{request.mapper_port[i]}')
                stub = KMeans_pb2_grpc.MapperStub(channel)

                data.append(stub.ServeData(KMeans_pb2.ReadRequest(
                    partition_number=self.partition + 1)).data)

            try:
                data.sort(key=lambda x: x[0])

            except Exception:
                pass

            for dataa in data:
                for line in dataa:
                    centroid, data_points = line.split(":")
                    centroid = centroid.strip()

                    if self.mapping.get(centroid) is None:
                        self.mapping[centroid] = []

                    self.mapping[centroid].extend(
                        [[float(x) for x in sublist] for sublist in ast.literal_eval(data_points)])

            if random.choice(["0", "1", "0", "1", "1", "0", "1", "0", "0", "1"]) == "0":
                return KMeans_pb2.ReduceResponse(output_files=str(self.UpdateCentroid()), success=True)

            return KMeans_pb2.ReduceResponse(output_files=str(self.UpdateCentroid()), success=False)

        except Exception:
            pass

    def UpdateCentroid(self):

        for centroid, data_points in self.mapping.items():
            xSum = 0
            ySum = 0

            for data in data_points:
                xSum += data[0]
                ySum += data[1]

            self.new_centroids.append(
                [xSum / len(data_points), ySum / len(data_points)])

        with open(f"Data/Reducers/R{self.partition + 1}.txt", "w") as f:
            for centroid in self.new_centroids:
                f.write(f"{centroid[0], centroid[1]}\n")

        return self.new_centroids


if __name__ == "__main__":

    print(f"Reducer {int(sys.argv[1]) - 50000} Process Started.\n")

    reducerObject = ReducerServicer(int(sys.argv[1]) - 50000)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    KMeans_pb2_grpc.add_ReducerServicer_to_server(reducerObject, server)
    server.add_insecure_port(f'localhost:{sys.argv[1]}')
    server.start()

    try:
        while True:
            time.sleep(86400)

    except Exception:
        print("\n\n\Reducer Process ENDED!\n\n")
        server.stop(0)

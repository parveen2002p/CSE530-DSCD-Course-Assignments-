import os
import sys
import time
import random
import threading

import grpc
import raft_pb2
import raft_pb2_grpc
from concurrent import futures


class RaftNodeServiceServicer(raft_pb2_grpc.RaftNodeServiceServicer):

    def __init__(self, node_id, all_node_ids):

        self.candidate = False
        self.dataMap = {}
        self.current_term = 0
        self.commit_length = 1
        self.node_id = node_id
        self.state = 'Follower'
        self.leader_id = None
        self.voted_for = None
        self.lease_timer = None
        self.lease_duration = 0.0
        self.election_timer = None
        self.all_node_ids = all_node_ids
        self.lease_acquire_thread = None
        self.leader_lease_timestamp = 0.0
        self.pre_election_timestamp = 0.0
        self.previous_commit_length = 0
        self.printing_commit_len = 0
        self.election_tuple = [False, 0, 0]
        self.tineout = 0.0 + self.timeout
        self.timeout = random.randint(5, 10)
        self.election_timeout = time.time()
        self.next_index = {node_id: 1 for node_id in all_node_ids}
        self.logs_dir = f"logs_node_{node_id}"
        self.logs_file = os.path.join(self.logs_dir, "logs.txt")
        self.metadata_file = os.path.join(self.logs_dir, "metadata.txt")
        self.dump_file = os.path.join(self.logs_dir, "dump.txt")

        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)

        self.read_metadata()

        if not os.path.exists(self.logs_file):
            with open(self.logs_file, "w") as f:
                f.write("NO-OP 0\n")
                f.close()

        if not os.path.exists(self.dump_file):
            with open(self.dump_file, "w") as f:
                f.close()

        if not os.path.exists(self.metadata_file):
            with open(self.metadata_file, "w") as f:
                f.write(f"CommitLength: {self.commit_length}\n")
                f.write(f"CurrentTerm: {self.current_term}\n")
                f.write(f"VotedFor: {self.voted_for}\n")
                f.close()

        os.chmod(self.logs_dir, 0o777)
        os.chmod(self.logs_file, 0o777)
        os.chmod(self.dump_file, 0o777)
        os.chmod(self.metadata_file, 0o777)

        self.commit_data()

    def read_metadata(self):

        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, "r") as f:
                lines = f.readlines()

                for line in lines:
                    key, value = line.strip().split(": ")

                    if key == "CommitLength":
                        self.commit_length = int(value)

                    elif key == "CurrentTerm":
                        self.current_term = int(value)

                    elif key == "VotedFor":
                        self.voted_for = int(value)

                f.close()

    def write_metadata(self):

        with open(self.metadata_file, "w") as f:
            f.write(f"CommitLength: {self.commit_length}\n")
            f.write(f"CurrentTerm: {self.current_term}\n")
            f.write(f"VotedFor: {self.voted_for}\n")
            f.close()

        os.chmod(self.metadata_file, 0o777)

    def start_election_timer(self):

        print("\nElection Timer Started by Node: " + str(self.node_id) + ".")

        while True:
            if self.check_if_Candidate():
                self.candidate = True
                self.election_timeout = time.time() + random.randint(5, 10)
                self.start_election()

    def reset_election_timer(self):

        self.candidate = False
        self.election_timeout = time.time() + random.randint(5, 10)

    def start_election(self):
        print(
            f"\nNode: {self.node_id}, Election Timer Timed out. Starting Election.")

        with open(self.dump_file, "a") as f:
            f.write(
                f"Node: {self.node_id}, Election Timer Timed out. Starting Election.\n")
            f.close()

        self.leader_id = None
        self.current_term += 1
        self.state = 'Candidate'
        self.voted_for = self.node_id
        self.write_metadata()

        last_log_term = 0
        last_log_index = 0
        with open(self.logs_file, "r") as f:
            lines = f.readlines()

            if len(lines) > 0:
                last_log_term = int(lines[-1].split()[-1])
                last_log_index = len(lines) - 1

            f.close()

        vote_replies = []
        for node_id in self.all_node_ids:
            if (time.time() > self.election_timeout):
                self.transition_to_follower(self.current_term)
                return
            if node_id != self.node_id:
                vote_replies.append(self.send_request_vote(
                    node_id, self.current_term, last_log_index, last_log_term))
                print("\nSending Request Vote to: " + str(node_id))

                try:
                    print("\nVote Received from: " + str(node_id) + ", for term: " +
                          str(self.current_term) + ", Status: " + str(vote_replies[-1][1]))

                except Exception as e:
                    print("\nNone Vote Recieved + NONE")

        votes_received = 1
        max_old_leader_lease_remaining = self.lease_duration

        for vote_reply in vote_replies:
            if vote_reply is not None:
                if vote_reply[0] > self.current_term:
                    self.transition_to_follower(vote_reply[0])
                    return

                if vote_reply[1]:
                    votes_received += 1

                    if vote_reply[2] > max_old_leader_lease_remaining:
                        max_old_leader_lease_remaining = vote_reply[2]

        if votes_received > (len(self.all_node_ids) / 2):
            self.transition_to_leader(max_old_leader_lease_remaining)

        else:
            self.reset_election_timer()

    def reset_timer(self):

        for nodeid in self.all_node_ids:
            if (nodeid != self.node_id):
                self.send_append_entries(nodeid, [], True)

    def transition_to_leader(self, max_old_leader_lease_remaining):

        self.state = 'Leader'
        self.leader_lease_timestamp = time.time()
        print(
            f"\nNode: {self.node_id}, Become Leader for Term: {self.current_term}.")

        with open(self.dump_file, "a") as f:
            f.write(
                f"Node: {self.node_id}, Become Leader for Term: {self.current_term}.\n")
            f.close()

        print(
            f"\nNew Leader: {self.node_id}, Waiting for Old Leader Lease to Timeout.")

        with open(self.dump_file, "a") as f:
            f.write(
                f"New Leader: {self.node_id}, Waiting for Old Leader Lease to Timeout.\n")
            f.close()

        while time.time() - self.leader_lease_timestamp <= max_old_leader_lease_remaining:
            for node_id in self.all_node_ids:
                if node_id != self.node_id:
                    self.send_append_entries(node_id, [], True)

        self.start_leader_lease()

    def check_if_Candidate(self):

        return (self.state != 'Leader' and time.time() > self.election_timeout + self.tineout)

    def start_leader_lease(self):

        self.no_op(self.current_term)
        self.send_heartbeats()

    def start_lease_timer(self):

        self.leader_lease_timestamp = time.time()
        self.lease_timer = threading.Timer(
            10, self.transition_to_follower, [self.current_term])
        self.lease_timer.start()

    def reset_lease_timer(self):

        self.leader_lease_timestamp = time.time()

    def send_heartbeats(self):

        while self.state == 'Leader' and time.time() - self.leader_lease_timestamp <= 10:
            votes = 1
            Negvotes = 0
            lines = []
            with open(self.logs_file, "r") as f:
                lines = f.readlines()
                f.close()

            for node_id in self.all_node_ids:
                if node_id != self.node_id:
                    checkResponse = self.send_append_entries(node_id, lines)
                    if checkResponse:
                        votes += 1

                        if len(lines) > 0:
                            self.next_index[node_id] = len(lines)
                    else:
                        Negvotes += 1
                        self.next_index[node_id] -= 1

            if votes > (len(self.all_node_ids) / 2):
                if len(lines) > 0:
                    for line in lines[self.commit_length - 1:]:
                        if self.previous_commit_length != self.commit_length:
                            print(
                                f"\nNode: {self.node_id} (Leader), Commited the Entry: {line} to the State Machine.")

                            with open(self.dump_file, "a") as f:
                                f.write(
                                    f"Node: {self.node_id} (Leader), Commited the Entry: {line} to the State Machine.\n")
                                f.close()
                            self.previous_commit_length = self.commit_length

                    self.commit_length = len(lines)
                    self.set_commit_length(self.commit_length)
                    self.commit_data()

                print(
                    f"\nLeader: {self.node_id}, Sending Heartbeat & Renewing Lease")

                with open(self.dump_file, "a") as f:
                    f.write(
                        f"Leader: {self.node_id}, Sending Heartbeat & Renewing Lease\n")
                    f.close()

                self.reset_lease_timer()

            print("\nHeartbeat Sent")
            print("\nResponses got: " + str(votes))
            time.sleep(1)

        self.transition_to_follower(self.current_term)

        print(
            f"\nLeader: {self.node_id}, FAILED to Renew Lease. Stepping Down and Transitioning to Follower.")

        with open(self.dump_file, "a") as f:
            f.write(
                f"Leader: {self.node_id}, FAILED to Renew Lease. Stepping Down and Transitioning to Follower.\n")
            f.close()

    def send_request_vote(self, node_id, term, last_log_index, last_log_term):

        channel = grpc.insecure_channel(self.all_node_ids[node_id])
        stub = raft_pb2_grpc.RaftNodeServiceStub(channel)

        try:
            response = stub.RequestVote(raft_pb2.RequestVoteRequest(
                term=term,
                candidate_id=self.node_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term))

            print("\nVote Request Sent to: " + str(node_id))
            return response.term, response.vote_granted, response.old_leader_lease_remaining

        except Exception:
            print(
                f"\nError Occurred while Sending Request Vote RPC to Node: {node_id}.")

            with open(self.dump_file, "a") as f:
                f.write(
                    f"Error Occurred while Sending Request Vote RPC to Node: {node_id}.\n")
                f.close()
            return None

    def RequestVote(self, request, context):

        with threading.Lock():
            if self.state == "Leader":
                return raft_pb2.RequestVoteResponse(term=self.current_term, vote_granted=False, old_leader_lease_remaining=0)

            if self.election_tuple[0] and request.term == self.election_tuple[1] and request.candidate_id != self.election_tuple[2]:
                return raft_pb2.RequestVoteResponse(term=self.current_term, vote_granted=False, old_leader_lease_remaining=0)

            if request.term < self.current_term:
                print(
                    f"\nVote DENIED to: {request.candidate_id} in Term: {request.term}.")

                with open(self.dump_file, "a") as f:
                    f.write(
                        f"Vote DENIED to: {request.candidate_id} in Term: {request.term}.\n")
                    f.close()

                return raft_pb2.RequestVoteResponse(term=self.current_term, vote_granted=False, old_leader_lease_remaining=0)

            if request.term > self.current_term:
                self.transition_to_follower(request.term)

            last_log_term = 0
            last_log_index = 0

            with open(self.logs_file, "r") as f:
                lines = f.readlines()

                if len(lines) > 0:
                    last_log_term = int(lines[-1].split()[-1])
                    last_log_index = len(lines) - 1
                f.close()

            if request.last_log_term > last_log_term or (request.last_log_term == last_log_term and request.last_log_index >= last_log_index):
                self.current_term = request.term
                self.voted_for = request.candidate_id
                self.write_metadata()
                self.election_tuple = [
                    True, request.term, request.candidate_id]

                print(
                    f"\nVote GRANTED to: {request.candidate_id} in Term: {request.term}.")

                with open(self.dump_file, "a") as f:
                    f.write(
                        f"Vote GRANTED to: {request.candidate_id} in Term: {request.term}.\n")
                    f.close()

                self.state = 'Follower'
                self.reset_election_timer()

                return raft_pb2.RequestVoteResponse(term=self.current_term, vote_granted=True, old_leader_lease_remaining=self.lease_duration)

            print(
                f"\nVote DENIED to: {request.candidate_id} in Term: {request.term}.")

            with open(self.dump_file, "a") as f:
                f.write(
                    f"Vote DENIED to: {request.candidate_id} in Term: {request.term}.\n")
                f.close()

            return raft_pb2.RequestVoteResponse(term=self.current_term, vote_granted=False, old_leader_lease_remaining=self.lease_duration)

    def send_append_entries(self, node_id, lines, flag=False):

        channel = grpc.insecure_channel(self.all_node_ids[node_id])
        stub = raft_pb2_grpc.RaftNodeServiceStub(channel)

        try:
            index = 0
            entries = []
            lastLineTerm = 0

            for line in lines:
                if index < self.next_index[node_id]:
                    index += 1
                    continue

                entries.append(str(index) + " " + line)
                index += 1

            if not flag:
                lastLineTerm = int(lines[-1].split()[-1])

            response = stub.AppendEntries(raft_pb2.AppendEntriesRequest(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=self.next_index[node_id] - 1,
                prev_log_term=int(
                    entries[-1].split()[-1]) if len(entries) > 0 else lastLineTerm,
                entries=entries,
                leader_commit=self.commit_length,
                remaining_leader_lease=str(
                    time.time() - self.leader_lease_timestamp),
                flag=flag))

            return response.success

        except Exception as e:
            if not flag:
                print(f"ERROR Sending Append Entries to Node: {node_id}.")
            return False

    def AppendEntries(self, request, context):

        with threading.Lock():
            if request.flag:
                self.reset_election_timer()
                return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)

            if request.term < self.current_term:
                print(
                    f"\nNode: {self.node_id}, REJECTED Append Entries RPC from: {request.leader_id}.")

                with open(self.dump_file, "a") as f:
                    f.write(
                        f"Node: {self.node_id}, REJECTED Append Entries RPC from: {request.leader_id}.\n")
                    f.close()

                return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)

            self.leader_id = request.leader_id
            self.current_term = request.term
            self.voted_for = request.leader_id
            self.lease_duration = float(request.remaining_leader_lease)
            self.write_metadata()

            lines = []
            with open(self.logs_file, "r") as f:
                lines = f.readlines()
                f.close()

            if len(request.entries) > 0:
                if request.prev_log_index <= len(lines) - 1:

                    with open(self.logs_file, "a") as f:
                        for entry in request.entries:
                            index = int(entry.split()[0])
                            if index <= len(lines) - 1:
                                index += 1
                                continue

                            splittedEntry = entry.split()[1:]
                            f.write(" ".join(splittedEntry) + "\n")
                        f.close()

                else:

                    print("\nRequest Sent for Older Log Entry")
                    print(
                        f"\nNode: {self.node_id}, REJECTED Append Entries RPC from: {request.leader_id}.")

                    with open(self.dump_file, "a") as f:
                        f.write(
                            f"Node: {self.node_id}, REJECTED Append Entries RPC from: {request.leader_id}.\n")
                        f.close()

                    return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)

                if (self.commit_length != request.leader_commit):
                    for line in lines[self.commit_length: request.leader_commit]:
                        print(
                            f"\nNode: {self.node_id} (Follower), Commited the Entry: {line} to the State Machine.")

                        with open(self.dump_file, "a") as f:
                            f.write(
                                f"Node: {self.node_id} (Follower), Commited the Entry: {line} to the State Machine.\n")
                            f.close()

                self.set_commit_length(request.leader_commit)
                self.commit_data()

            else:
                if (self.commit_length != request.leader_commit):
                    for line in lines[self.commit_length: request.leader_commit]:
                        print(
                            f"\nNode: {self.node_id} (Follower), Commited the Entry: {line} to the State Machine.")

                        with open(self.dump_file, "a") as f:
                            f.write(
                                f"Node: {self.node_id} (Follower), Commited the Entry: {line} to the State Machine.\n")
                            f.close()

                self.set_commit_length(request.leader_commit)
                self.commit_data()
                print("\nReceived Heartbeat and Resetted Election Timer.")

            self.reset_election_timer()

            print(
                f"\nNode: {self.node_id}, ACCEPTED Append Entries RPC from: {request.leader_id}.")

            with open(self.dump_file, "a") as f:
                f.write(
                    f"Node: {self.node_id}, ACCEPTED Append Entries RPC from: {request.leader_id}.\n")
                f.close()

            return raft_pb2.AppendEntriesResponse(term=self.current_term, success=True)

    def commit_data(self):

        with open(self.logs_file, "r") as f:
            lines = f.readlines()

            for line in lines:
                if line.startswith("SET"):
                    data = line.split()[1:]
                    self.dataMap[data[0]] = data[1]

            f.close()

    def no_op(self, term):

        with open(self.logs_file, "a") as f:
            f.write("NO-OP " + str(term) + "\n")
            f.close()

    def set_commit_length(self, commit_length):

        self.commit_length = commit_length
        self.write_metadata()

    def transition_to_follower(self, term):

        if self.state == 'Leader' or self.state == 'Candidate':
            print(
                f"\nNode: {self.node_id}, Stepping DOWN and Transitioning to Follower")

            with open(self.dump_file, "a") as f:
                f.write(
                    f"Node: {self.node_id}, Stepping DOWN and Transitioning to Follower\n")
                f.close()

        self.state = 'Follower'
        self.current_term = term
        self.voted_for = None
        self.write_metadata()
        self.reset_election_timer()

    def SET(self, request, context):

        if self.state == 'Leader':
            with open(self.logs_file, "a") as f:
                f.write(
                    f"SET {request.key} {request.value} {self.current_term}\n")
                f.close()

            while self.dataMap.get(request.key) is None and self.state == 'Leader':
                continue

            print(
                f"\nNode: {self.node_id} (Leader), Received an Entry SET {request.key} {request.value} Request.")

            with open(self.dump_file, "a") as f:
                f.write(
                    f"Node: {self.node_id} (Leader), Received an Entry SET {request.key} {request.value} Request.\n")
                f.close()

            if self.dataMap.get(request.key) is None:
                return raft_pb2.SetResponse(responseMessage="FAILED to SET Key, Leader is: NONE")

            return raft_pb2.SetResponse(responseMessage="Key Set Successfully") if self.state == 'Leader' else raft_pb2.SetResponse(responseMessage="I'm not a Leader, Leader is: " + str(self.voted_for))

        else:
            if self.leader_id is None:
                return raft_pb2.SetResponse(responseMessage="I'm not a Leader, Leader is: None")

            if self.state == 'Candidate':
                return raft_pb2.SetResponse(responseMessage="I'm not a Leader, Leader is: None")

            if self.voted_for == self.node_id and self.state != 'Leader':
                return raft_pb2.SetResponse(responseMessage="I'm not a Leader, Leader is: None")

            return raft_pb2.SetResponse(responseMessage="I'm not a Leader, Leader is: " + str(self.voted_for))

    def GET(self, request, context):

        if self.state == 'Leader':
            print(
                f"\nNode: {self.node_id} (Leader), Received an Entry GET {request.key} Request.")

            with open(self.dump_file, "a") as f:
                f.write(
                    f"Node: {self.node_id} (Leader), Received an Entry GET {request.key} Request.\n")
                f.close()

            if self.dataMap.get(request.key) is not None:
                return raft_pb2.GetResponse(value=self.dataMap[request.key], error="")

            return raft_pb2.GetResponse(value="None", error="Key not found")

        else:
            if self.leader_id is None:
                return raft_pb2.GetResponse(value="None", error="I'm not a Leader, Leader is: None")

            if self.state == 'Candidate':
                return raft_pb2.GetResponse(value="None", error="I'm not a Leader, Leader is: None")

            if self.voted_for == self.node_id and self.state != 'Leader':
                return raft_pb2.GetResponse(value="None", error="I'm not a Leader, Leader is: None")

            return raft_pb2.GetResponse(value="None", error="I'm not a Leader, Leader is: " + str(self.voted_for))


if __name__ == '__main__':

    if len(sys.argv) < 2:
        print("Usage: python3 raft.py <node_id>")
        os._exit(0)

    nodes = {1: "127.0.0.1:5001", 2: "127.0.0.1:5002",
             3: "127.0.0.1:5003", 4: "127.0.0.1:5004", 5: "127.0.0.1:5005"}

    raftObject = RaftNodeServiceServicer(int(sys.argv[1]), nodes)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    raft_pb2_grpc.add_RaftNodeServiceServicer_to_server(raftObject, server)
    server.add_insecure_port(nodes[int(sys.argv[1])])
    server.start()

    try:
        raftObject.start_election_timer()

    except KeyboardInterrupt:
        server.stop(0)
        print("\n\n\nServer stopped\n\n\n")
        os._exit(0)


from typing import Set, List, Dict
from cs4545.system.da_types import *
from ipv8.community import CommunitySettings
from ipv8.messaging.payload_dataclass import dataclass
from ipv8.types import Peer


@dataclass(msg_id=3)  # Unique message ID for Dolev messages
class DolevMessage:
    msg_id: str
    source: int
    payload: str
    path: List[int]


class DolevAlgorithm(DistributedAlgorithm):
    def __init__(self, settings: CommunitySettings) -> None:
        """
        Initialize the Dolev protocol with Byzantine fault tolerance.

        :param settings: Community settings.
        :param f: The maximum number of Byzantine nodes the network can tolerate.
        """
        super().__init__(settings)
        self.f = 1  # Number of Byzantine nodes hard-coded
        self.delivered_messages: Set[str] = set()  # Tracks delivered messages
        self.received_paths: Dict[str, List[List[int]]] = {}  # Tracks paths for each message ID
        self.add_message_handler(DolevMessage, self.on_message)

    async def on_start_as_starter(self):
        """
        Send the initial message as the starter node.
        """
        print(f"[Node {self.node_id}] Starting Dolev algorithm")
        msg = DolevMessage(
            msg_id=f"{self.node_id}-0",
            source=self.node_id,
            payload=f"Message from node {self.node_id}",
            path=[self.node_id],
        )
        self.broadcast_message(msg)

    def broadcast_message(self, msg: DolevMessage):
        """
        Broadcast a message to all connected peers.

        :param msg: The message to broadcast.
        """
        for peer in self.get_peers():
            print(f"[Node {self.node_id}] Broadcasting message {msg.msg_id} to peer {peer.node_id}")
            self.ez_send(peer, msg)

    @message_wrapper(DolevMessage)
    async def on_message(self, peer: Peer, msg: DolevMessage):
        """
        Handle incoming Dolev messages.

        :param peer: The sender's peer object.
        :param msg: The received DolevMessage.
        """
        sender_id = self.node_id_from_peer(peer)
        print(f"[Node {self.node_id}] Received message {msg.msg_id} from {sender_id}")

        if msg.msg_id in self.delivered_messages:
            print(f"[Node {self.node_id}] Message {msg.msg_id} already delivered, ignoring.")
            return  # Ignore already delivered messages

        # Track received paths for this message
        msg.path.append(self.node_id)
        paths = self.received_paths.setdefault(msg.msg_id, [])
        paths.append(msg.path)
        print(f"[Node {self.node_id}] Updated paths for message {msg.msg_id}: {paths}")

        # Forward the message to other neighbors
        for neighbor in self.get_peers():
            if neighbor.node_id not in msg.path:
                print(f"[Node {self.node_id}] Forwarding message {msg.msg_id} to {neighbor.node_id}")
                self.ez_send(neighbor, msg)

        # Check if the message can be delivered
        if self.is_disjoint_path_set(paths, self.f + 1):
            self.delivered_messages.add(msg.msg_id)
            print(f"[Node {self.node_id}] Delivered message {msg.msg_id}: {msg.payload}")
            self.trigger_dolev_deliver(msg.payload)
        else:
            print(f"[Node {self.node_id}] Not enough disjoint paths for message {msg.msg_id}")

    def is_disjoint_path_set(self, paths: List[List[int]], required_paths: int) -> bool:
        """
        Check if at least `required_paths` node-disjoint paths exist.

        :param paths: The list of paths received for a message.
        :param required_paths: The minimum number of node-disjoint paths required.
        :return: True if the condition is met, False otherwise.
        """
        unique_paths = set(tuple(path) for path in paths)
        node_sets = [set(path) for path in unique_paths]
        disjoint_count = 0
        used_nodes = set()

        for nodes in node_sets:
            if not nodes & used_nodes:  # Check for disjoint set
                disjoint_count += 1
                used_nodes.update(nodes)
                if disjoint_count >= required_paths:
                    print(f"[Node {self.node_id}] Found {disjoint_count} disjoint paths.")
                    return True

        print(f"[Node {self.node_id}] Only {disjoint_count} disjoint paths found.")
        return False

    def trigger_dolev_deliver(self, payload: str):
        """
        Trigger the delivery of a message to the application layer.

        :param payload: The payload of the delivered message.
        """
        print(f"[Node {self.node_id}] Delivered message to application: {payload}")
        self.stop()
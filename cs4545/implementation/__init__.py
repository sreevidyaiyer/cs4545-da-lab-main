from .echo_algorithm import *
from .ring_election import *
from .dolev import *

def get_algorithm(name):
    if name == "echo":
        return EchoAlgorithm
    elif name == "ring":
        return RingElection
    elif name == "dolev":
        # If byzantine_nodes is provided, pass it to the DolevAlgorithm
        return DolevAlgorithm
    else:
        raise ValueError(f"Unknown algorithm: {name}")
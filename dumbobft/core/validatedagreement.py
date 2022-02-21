from gevent import monkey; monkey.patch_all(thread=False)

import copy
import time
import traceback
from datetime import datetime
import gevent
import numpy as np
from collections import namedtuple
from gevent import Greenlet
from gevent.event import Event
from enum import Enum
from collections import defaultdict
from gevent.queue import Queue
from honeybadgerbft.core.commoncoin import shared_coin
from dumbobft.core.baisedbinaryagreement import baisedbinaryagreement
#from dumbobft.core.haltingtwovalueagreement import haltingtwovalueagreement
#from bdtbft.core.twovalueagreement import twovalueagreement
from dumbobft.core.consistentbroadcast import consistentbroadcast
from dumbobft.core.validators import cbc_validate
from honeybadgerbft.exceptions import UnknownTagError



class MessageTag(Enum):
    VABA_COIN = 'VABA_COIN'             # Queue()
    VABA_COMMIT = 'VABA_COMMIT'         # [Queue()] * N
    VABA_VOTE = 'VABA_VOTE'             # [Queue()] * Number_of_ABA_Iterations
    VABA_ABA_COIN = 'VABA_ABA_COIN'     # [Queue()] * Number_of_ABA_Iterations
    VABA_CBC = 'VABA_CBC'               # [Queue()] * N
    VABA_ABA = 'VABA_ABA'               # [Queue()] * Number_of_ABA_Iterations


MessageReceiverQueues = namedtuple(
    'MessageReceiverQueues', ('VABA_COIN', 'VABA_COMMIT', 'VABA_VOTE', 'VABA_ABA_COIN', 'VABA_CBC', 'VABA_ABA'))


def recv_loop(recv_func, recv_queues):
    while True:
        #gevent.sleep(0)
        sender, (tag, j, msg) = recv_func()
        #print("recv2", (sender, (tag, j, msg)))

        if tag not in MessageTag.__members__:
            raise UnknownTagError('Unknown tag: {}! Must be one of {}.'.format(
                tag, MessageTag.__members__.keys()))
        recv_queue = recv_queues._asdict()[tag]

        if tag not in {MessageTag.VABA_COIN.value}:
            recv_queue = recv_queue[j]
        try:
            recv_queue.put_nowait((sender, msg))
        except AttributeError as e:
            # print((sender, msg))
            traceback.print_exc(e)




def validatedagreement(sid, pid, N, f, PK, SK, PK1, SK1, PK2s, SK2, input, decide, receive, send, predicate=lambda x: True, logger=None):
    """Multi-valued Byzantine consensus. It takes an input ``vi`` and will
    finally writes the decided value into ``decide`` channel.

    :param sid: session identifier
    :param pid: my id number
    :param N: the number of parties
    :param f: the number of byzantine parties
    :param PK: ``boldyreva.TBLSPublicKey`` with threshold f+1
    :param SK: ``boldyreva.TBLSPrivateKey`` with threshold f+1
    :param PK1: ``boldyreva.TBLSPublicKey`` with threshold n-f
    :param SK1: ``boldyreva.TBLSPrivateKey`` with threshold n-f
    :param list PK2s: an array of ``coincurve.PublicKey'', i.e., N public keys of ECDSA for all parties
    :param PublicKey SK2: ``coincurve.PrivateKey'', i.e., secret key of ECDSA
    :param input: ``input()`` is called to receive an input
    :param decide: ``decide()`` is eventually called
    :param receive: receive channel
    :param send: send channel
    :param predicate: ``predicate()`` represents the externally validated condition
    """

    #print("Starts to run validated agreement...")

    assert PK.k == f+1
    assert PK.l == N
    assert PK1.k == N-f
    assert PK1.l == N

    """ 
    """
    """ 
    Some instantiations
    """
    """ 
    """

    my_cbc_input = Queue(1)
    my_commit_input = Queue(1)
    aba_inputs = defaultdict(lambda: Queue(1))

    aba_recvs = defaultdict(lambda: Queue())
    aba_coin_recvs = defaultdict(lambda: Queue())
    vote_recvs = defaultdict(lambda: Queue())

    cbc_recvs = [Queue() for _ in range(N)]
    coin_recv = Queue()
    commit_recvs = [Queue() for _ in range(N)]

    cbc_threads = [None] * N
    cbc_outputs = [Queue(1) for _ in range(N)]
    commit_outputs = [Queue(1) for _ in range(N)]
    aba_outputs = defaultdict(lambda: Queue(1))

    is_cbc_delivered = [0] * N
    is_commit_delivered = [0] * N

    recv_queues = MessageReceiverQueues(
        VABA_COIN=coin_recv,
        VABA_COMMIT=commit_recvs,
        VABA_VOTE=vote_recvs,
        VABA_ABA_COIN=aba_coin_recvs,
        VABA_CBC=cbc_recvs,
        VABA_ABA=aba_recvs,
    )
    recv_loop_thred = Greenlet(recv_loop, receive, recv_queues)
    recv_loop_thred.start()

    """ 
    Setup the sub protocols Input Broadcast CBCs"""

    for j in range(N):

        def make_cbc_send(j): # this make will automatically deep copy the enclosed send func
            def cbc_send(k, o):
                """CBC send operation.
                :param k: Node to send.
                :param o: Value to send.
                """
                #print("node", pid, "is sending", o, "to node", k, "with the leader", j)
                send(k, ('VABA_CBC', j, o))
            return cbc_send

        # Only leader gets input
        cbc_input = my_cbc_input.get if j == pid else None
        cbc = gevent.spawn(consistentbroadcast, sid + 'CBC' + str(j), pid, N, f, PK2s, SK2, j,
                           cbc_input, cbc_recvs[j].get, make_cbc_send(j), logger)
        # cbc.get is a blocking function to get cbc output
        #cbc_outputs[j].put_nowait(cbc.get())
        cbc_threads[j] = cbc

    """ 
    Setup the sub protocols Commit CBCs"""

    for j in range(N):

        def make_commit_send(j): # this make will automatically deep copy the enclosed send func
            def commit_send(k, o):
                """COMMIT-CBC send operation.
                :param k: Node to send.
                :param o: Value to send.
                """
                #print("node", pid, "is sending", o, "to node", k, "with the leader", j)
                send(k, ('VABA_COMMIT', j, o))
            return commit_send

        # Only leader gets input
        commit_input = my_commit_input.get if j == pid else None
        commit = gevent.spawn(consistentbroadcast, sid + 'COMMIT-CBC' + str(j), pid, N, f, PK2s, SK2, j,
                           commit_input, commit_recvs[j].get, make_commit_send(j), logger)
        # commit.get is a blocking function to get commit-cbc output
        commit_outputs[j] = commit.get

    """ 
    Setup the sub protocols permutation coins"""

    def coin_bcast(o):
        """Common coin multicast operation.
        :param o: Value to multicast.
        """
        send(-1, ('VABA_COIN', 'leader_election', o))

    permutation_coin = shared_coin(sid + 'PERMUTE', pid, N, f,
                               PK, SK, coin_bcast, coin_recv.get, single_bit=False)
    # False means to get a coin of 256 bits instead of a single bit

    """ 
    """
    """ 
    Start to run consensus
    """
    """ 
    """

    """ 
    Run n CBC instance to consistently broadcast input values
    """

    #cbc_values = [Queue(1) for _ in range(N)]

    def wait_for_input():
        v = input()
        if logger != None:
            logger.info("VABA %s get input at %s" % (sid, datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]))
        #print("node %d gets VABA input" % pid)

        my_cbc_input.put_nowait(v)

    gevent.spawn(wait_for_input)

    wait_cbc_signal = Event()
    wait_cbc_signal.clear()

    def wait_for_cbc_to_continue(leader):
        # Receive output from CBC broadcast for input values
        msg, sigmas = cbc_threads[leader].get()
        if predicate(msg):
            try:
                if cbc_outputs[leader].empty():
                    cbc_outputs[leader].put_nowait((msg, sigmas))
                    is_cbc_delivered[leader] = 1
                    if sum(is_cbc_delivered) >= N - f:
                        wait_cbc_signal.set()
            except:
                pass
            #print("Node %d finishes CBC for Leader %d" % (pid, leader) )
            #print(is_cbc_delivered)

    cbc_out_threads = [gevent.spawn(wait_for_cbc_to_continue, node) for node in range(N)]

    wait_cbc_signal.wait()
    #print("Node %d finishes n-f CBC" % pid)
    #print(is_cbc_delivered)
    #print(cbc_values)

    """
    Run n CBC instance to commit finished CBC IDs
    """

    commit_values = [None] * N

    #assert len(is_cbc_delivered) == N
    #assert sum(is_cbc_delivered) >= N - f
    #assert all(item == 0 or 1 for item in is_cbc_delivered)

    my_commit_input.put_nowait(copy.deepcopy(is_cbc_delivered))  # Deepcopy prevents input changing while executing
    #print("Provide input to commit CBC")

    wait_commit_signal = Event()
    wait_commit_signal.clear()

    def wait_for_commit_to_continue(leader):
        # Receive output from CBC broadcast for commitment
        commit_list, proof = commit_outputs[leader]()
        if (sum(commit_list) >= N - f) and all(item == 0 or 1 for item in commit_list): #
            commit_values[leader] = commit_list # May block
            is_commit_delivered[leader] = 1
            if sum(is_commit_delivered) >= N - f:
                wait_commit_signal.set()
            #print("Leader %d finishes COMMIT_CBC for node %d" % (leader, pid) )
            #print(is_commit_delivered)

    commit_out_threads = [gevent.spawn(wait_for_commit_to_continue, node) for node in range(N)]

    wait_commit_signal.wait()
    #print("Node %d finishes n-f Commit CBC" % pid)
    #print(is_commit_delivered)
    #print(commit_values)

    """
    Run a Coin instance to permute the nodes' IDs to sequentially elect the leaders
    """
    seed = permutation_coin('permutation')  # Block to get a random seed to permute the list of nodes
    # print(seed)
    np.random.seed(seed)
    pi = np.random.permutation(N)

    # print(pi)

    """
    Repeatedly run biased ABA instances until 1 is output 
    """

    r = 0
    a = None
    votes = defaultdict(set)

    while True:
        #gevent.sleep(0)

        a = pi[r]
        if is_cbc_delivered[a] == 1:
            vote = (a, 1, cbc_outputs[a].queue[0])
        else:
            vote = (a, 0, "Bottom")

        send(-1, ('VABA_VOTE', r, vote))

        ballot_counter = 0

        while True:
            #gevent.sleep(0)
            sender, vote = vote_recvs[r].get()
            a, ballot_bit, cbc_out = vote
            if (pi[r] == a) and (ballot_bit == 0 or ballot_bit == 1):
                if ballot_bit == 1:
                    try:
                        (m, sigmas) = cbc_out
                        cbc_sid = sid + 'CBC' + str(a)
                        assert cbc_validate(cbc_sid, N, f, PK2s, m, sigmas)
                        votes[r].add((sender, vote))
                        ballot_counter += 1
                    except:
                        print("Invalid voting ballot")
                        if logger is not None:
                            logger.info("Invalid voting ballot")
                else:
                    if commit_values[sender] is not None and commit_values[sender][a] == 0:
                        votes[r].add((sender, vote))
                        ballot_counter += 1

            if len(votes[r]) >= N - f:
                break

        # print(votes[r])
        aba_r_input = 0
        for vote in votes[r]:
            _, (_, bit, cbc_out) = vote
            if bit == 1:
                aba_r_input = 1
                if is_cbc_delivered[a] == 0:
                    if cbc_outputs[a].empty():
                        cbc_outputs[a].put_nowait(cbc_out)
                        #is_cbc_delivered[a] = 1

        def aba_coin_bcast(o):
            """Common coin multicast operation.
            :param o: Value to multicast.
            """
            send(-1, ('VABA_ABA_COIN', r, o))

        coin = shared_coin(sid + 'COIN' + str(r), pid, N, f,
                           PK, SK,
                           aba_coin_bcast, aba_coin_recvs[r].get, single_bit=True)

        def make_aba_send(rnd): # this make will automatically deep copy the enclosed send func
            def aba_send(k, o):
                """CBC send operation.
                :param k: Node to send.
                :param o: Value to send.
                """
                # print("node", pid, "is sending", o, "to node", k, "with the leader", j)
                send(k, ('VABA_ABA', rnd, o))
            return aba_send

        # Only leader gets input
        aba = gevent.spawn(baisedbinaryagreement, sid + 'ABA' + str(r), pid, N, f, coin,
                     aba_inputs[r].get, aba_outputs[r].put_nowait,
                     aba_recvs[r].get, make_aba_send(r))
        # aba.get is a blocking function to get aba output
        aba_inputs[r].put_nowait(aba_r_input)
        aba_r = aba_outputs[r].get()
        # print("Round", r, "ABA outputs", aba_r)
        if aba_r == 1:
            break
        r += 1
    assert a is not None
    if logger != None:
        logger.info("VABA %s completes at round %d" % (sid, r))
    #print("node %d output in VABA" % pid)
    decide(cbc_outputs[a].get()[0])

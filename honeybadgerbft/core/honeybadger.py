import json
import traceback, time
import gevent
import numpy as np
from collections import namedtuple, deque
from enum import Enum
from gevent.queue import Queue
from honeybadgerbft.core.commoncoin import shared_coin
from honeybadgerbft.core.binaryagreement import binaryagreement
from honeybadgerbft.core.reliablebroadcast import reliablebroadcast
from honeybadgerbft.core.commonsubset import commonsubset
from honeybadgerbft.core.honeybadger_block import honeybadger_block
from honeybadgerbft.exceptions import UnknownTagError


class BroadcastTag(Enum):
    ACS_COIN = 'ACS_COIN'
    ACS_RBC = 'ACS_RBC'
    ACS_ABA = 'ACS_ABA'
    TPKE = 'TPKE'


BroadcastReceiverQueues = namedtuple(
    'BroadcastReceiverQueues', ('ACS_COIN', 'ACS_ABA', 'ACS_RBC', 'TPKE'))


def broadcast_receiver(recv_func, recv_queues):
    sender, (tag, j, msg) = recv_func()
    if tag not in BroadcastTag.__members__:
        # TODO Post python 3 port: Add exception chaining.
        # See https://www.python.org/dev/peps/pep-3134/
        raise UnknownTagError('Unknown tag: {}! Must be one of {}.'.format(
            tag, BroadcastTag.__members__.keys()))
    recv_queue = recv_queues._asdict()[tag]

    if tag != BroadcastTag.TPKE.value:
        recv_queue = recv_queue[j]

    recv_queue.put_nowait((sender, msg))


def broadcast_receiver_loop(recv_func, recv_queues):
    while True:
        gevent.sleep(0)
        time.sleep(0)
        broadcast_receiver(recv_func, recv_queues)


class HoneyBadgerBFT():
    r"""HoneyBadgerBFT object used to run the protocol.

    :param str sid: The base name of the common coin that will be used to
        derive a nonce to uniquely identify the coin.
    :param int pid: Node id.
    :param int B: Batch size of transactions.
    :param int N: Number of nodes in the network.
    :param int f: Number of faulty nodes that can be tolerated.
    :param str sPK: Public key of the threshold signature
        (:math:`\mathsf{TSIG}`) scheme.
    :param str sSK: Signing key of the threshold signature
        (:math:`\mathsf{TSIG}`) scheme.
    :param str ePK: Public key of the threshold encryption
        (:math:`\mathsf{TPKE}`) scheme.
    :param str eSK: Signing key of the threshold encryption
        (:math:`\mathsf{TPKE}`) scheme.
    :param send:
    :param recv:
    :param K: a test parameter to specify break out after K rounds
    """

    def __init__(self, sid, pid, B, N, f, sPK, sSK, ePK, eSK, send, recv, K=3, logger=None, mute=False):
        self.sid = sid
        self.id = pid
        self.B = B
        self.N = N
        self.f = f
        self.sPK = sPK
        self.sSK = sSK
        self.ePK = ePK
        self.eSK = eSK
        self._send = send
        self._recv = recv
        self.logger = logger
        self.round = 0  # Current block number
        self.transaction_buffer = deque()
        self._per_round_recv = {}  # Buffer of incoming messages
        self.K = K

        self.s_time = 0
        self.e_time = 0
        self.txcnt = 0

        self.mute = mute


    def submit_tx(self, tx):
        """Appends the given transaction to the transaction buffer.

        :param tx: Transaction to append to the buffer.
        """
        #print('backlog_tx', self.id, tx)
        #if self.logger != None: self.logger.info('Backlogged tx at Node %d:' % self.id + str(tx))
        self.transaction_buffer.append(tx)

    def run(self):
        """Run the HoneyBadgerBFT protocol."""

        if self.mute:

            def send_blackhole(*args):
                pass

            def recv_blackhole(*args):
                while True:
                    gevent.sleep(1)
                    time.sleep(1)
                    pass

            seed = int.from_bytes(self.sid.encode('utf-8'), 'little')
            if self.id in np.random.RandomState(seed).permutation(self.N)[:int((self.N - 1) / 3)]:
                self._send = send_blackhole
                self._recv = recv_blackhole

        def _recv():
            """Receive messages."""
            while True:

                gevent.sleep(0)
                time.sleep(0)

                (sender, (r, msg)) = self._recv()

                # Maintain an *unbounded* recv queue for each epoch
                if r not in self._per_round_recv:
                    self._per_round_recv[r] = Queue()

                # Buffer this message
                self._per_round_recv[r].put_nowait((sender, msg))

        self._recv_thread = gevent.spawn(_recv)

        self.s_time = time.time()
        if self.logger != None: self.logger.info('Node %d starts to run at time:' % self.id + str(self.s_time))

        while True:
            # For each round...

            gevent.sleep(0)
            time.sleep(0)

            start = time.time()

            r = self.round
            if r not in self._per_round_recv:
                self._per_round_recv[r] = Queue()

            # Select B transactions (TODO: actual random selection)
            tx_to_send = []
            for _ in range(self.B):
                tx_to_send.append(self.transaction_buffer.popleft())

            # TODO: Wait a bit if transaction buffer is not full

            # Run the round
            def _make_send(r):
                def _send(j, o):
                    self._send(j, (r, o))
                return _send

            send_r = _make_send(r)
            recv_r = self._per_round_recv[r].get
            new_tx = self._run_round(r, tx_to_send, send_r, recv_r)

            #print('new block at %d:' % self.id, new_tx)
            if self.logger != None:
                #self.logger.info('Node %d Delivers Block %d: ' % (self.id, self.round) + str(new_tx))
                tx_cnt = str(new_tx).count("Dummy TX")
                self.txcnt += tx_cnt
                self.logger.info(
                'Node %d Delivers ACS Block in Round %d with having %d TXs' % (self.id, r, tx_cnt))

            end = time.time()

            if self.logger != None:
                self.logger.info('ACS Block Delay at Round %d at Node %d: ' % (self.id, r) + str(end - start))

            # Remove output transactions from the backlog buffer
            for _tx in tx_to_send:
                if _tx not in new_tx:
                    self.transaction_buffer.appendleft(_tx)

            #print('buffer at %d:' % self.id, self.transaction_buffer)
            #if self.logger != None:
            #    self.logger.info('Backlog Buffer at Node %d:' % self.id + str(self.transaction_buffer))

            self.round += 1     # Increment the round
            if self.round >= self.K:
                break   # Only run one round for now

        if self.logger != None:
            self.e_time = time.time()
            self.logger.info("node %d breaks in %f seconds with total delivered Txs %d" % (self.id, self.e_time-self.s_time, self.txcnt))
        else:
            print("node %d breaks" % self.id)


    def _run_round(self, r, tx_to_send, send, recv):
        """Run one protocol round.

        :param int r: round id
        :param tx_to_send: Transaction(s) to process.
        :param send:
        :param recv:
        """
        # Unique sid for each round
        sid = self.sid + ':' + str(r)
        pid = self.id
        N = self.N
        f = self.f

        def broadcast(o):
            """Multicast the given input ``o``.

            :param o: Input to multicast.
            """
            for j in range(N):
                send(j, o)

        # Launch ACS, ABA, instances
        coin_recvs = [None] * N
        aba_recvs  = [None] * N  # noqa: E221
        rbc_recvs  = [None] * N  # noqa: E221

        aba_inputs  = [Queue(1) for _ in range(N)]  # noqa: E221
        aba_outputs = [Queue(1) for _ in range(N)]
        rbc_outputs = [Queue(1) for _ in range(N)]

        my_rbc_input = Queue(1)
        #print(pid, r, 'tx_to_send:', tx_to_send)
        #if self.logger != None: self.logger.info('Commit tx at Node %d:' % self.id + str(tx_to_send))

        def _setup(j):
            """Setup the sub protocols RBC, BA and common coin.

            :param int j: Node index for which the setup is being done.
            """
            def coin_bcast(o):
                """Common coin multicast operation.
                :param o: Value to multicast.
                """
                broadcast(('ACS_COIN', j, o))

            coin_recvs[j] = Queue()
            coin = shared_coin(sid + 'COIN' + str(j), pid, N, f,
                               self.sPK, self.sSK,
                               coin_bcast, coin_recvs[j].get)

            def aba_send(k, o):
                """Binary Byzantine Agreement multicast operation.
                :param k: Node to send.
                :param o: Value to send.
                """
                send(k, ('ACS_ABA', j, o))


            aba_recvs[j] = Queue()
            gevent.spawn(binaryagreement, sid+'ABA'+str(j), pid, N, f, coin,
                         aba_inputs[j].get, aba_outputs[j].put_nowait,
                         aba_recvs[j].get, aba_send)

            def rbc_send(k, o):
                """Reliable send operation.
                :param k: Node to send.
                :param o: Value to send.
                """
                send(k, ('ACS_RBC', j, o))

            # Only leader gets input
            rbc_input = my_rbc_input.get if j == pid else None
            rbc_recvs[j] = Queue()
            rbc = gevent.spawn(reliablebroadcast, sid+'RBC'+str(j), pid, N, f, j,
                               rbc_input, rbc_recvs[j].get, rbc_send)
            rbc_outputs[j] = rbc.get  # block for output from rbc

        # N instances of ABA, RBC
        for j in range(N):
            _setup(j)

        # One instance of TPKE
        def tpke_bcast(o):
            """Threshold encryption broadcast."""
            broadcast(('TPKE', '', o))

        tpke_recv = Queue()

        # One instance of ACS
        acs = gevent.spawn(commonsubset, pid, N, f, rbc_outputs,
                           [_.put_nowait for _ in aba_inputs],
                           [_.get for _ in aba_outputs])

        recv_queues = BroadcastReceiverQueues(
            ACS_COIN=coin_recvs,
            ACS_ABA=aba_recvs,
            ACS_RBC=rbc_recvs,
            TPKE=tpke_recv,
        )
        gevent.spawn(broadcast_receiver_loop, recv, recv_queues)

        _input = Queue(1)
        _input.put(json.dumps(tx_to_send))

        _output = honeybadger_block(pid, self.N, self.f, self.ePK, self.eSK,
                          _input.get,
                          acs_in=my_rbc_input.put_nowait, acs_out=acs.get,
                          tpke_bcast=tpke_bcast, tpke_recv=tpke_recv.get)

        block = set()
        for batch in _output:
            decoded_batch = json.loads(batch.decode())
            for tx in decoded_batch:
                block.add(tx)

        return list(block)

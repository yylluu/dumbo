from gevent import monkey; monkey.patch_all(thread=False)

import hashlib
import json
import logging
import os
import pickle
import traceback
import gevent
import time
import numpy as np
from gevent import Greenlet
from gevent.event import Event
from gevent.queue import Queue
from collections import namedtuple
from enum import Enum

from dumbobft.core.validators import prbc_validate
from bdtbft.core.hsfastpath import hsfastpath
from bdtbft.core.twovalueagreement import twovalueagreement
from dumbobft.core.validatedcommonsubset import validatedcommonsubset
from dumbobft.core.provablereliablebroadcast import provablereliablebroadcast
from dumbobft.core.dumbocommonsubset import dumbocommonsubset
from honeybadgerbft.core.honeybadger_block import honeybadger_block
from crypto.threshsig.boldyreva import serialize, deserialize1
from crypto.threshsig.boldyreva import TBLSPrivateKey, TBLSPublicKey
from crypto.ecdsa.ecdsa import PrivateKey
from honeybadgerbft.core.commoncoin import shared_coin
from honeybadgerbft.exceptions import UnknownTagError
from crypto.ecdsa.ecdsa import ecdsa_sign, ecdsa_vrfy, PublicKey


def set_consensus_log(id: int):
    logger = logging.getLogger("consensus-node-"+str(id))
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s %(filename)s [line:%(lineno)d] %(funcName)s %(levelname)s %(message)s ')
    if 'log' not in os.listdir(os.getcwd()):
        os.mkdir(os.getcwd() + '/log')
    full_path = os.path.realpath(os.getcwd()) + '/log/' + "consensus-node-"+str(id) + ".log"
    file_handler = logging.FileHandler(full_path)
    file_handler.setFormatter(formatter) 
    logger.addHandler(file_handler)
    return logger

def hash(x):
    return hashlib.sha256(pickle.dumps(x)).digest()


class BroadcastTag(Enum):
    FAST = 'FAST'
    VIEW_CHANGE = 'VIEW_CHANGE'
    NEW_VIEW = 'NEW_VIEW'



BroadcastReceiverQueues = namedtuple(
    'BroadcastReceiverQueues', ('FAST', 'VIEW_CHANGE', 'NEW_VIEW'))


def broadcast_receiver_loop(recv_func, recv_queues):
    while True:
        #gevent.sleep(0)
        sender, (tag, j, msg) = recv_func()
        if tag not in BroadcastTag.__members__:
            # TODO Post python 3 port: Add exception chaining.
            # See https://www.python.org/dev/peps/pep-3134/
            raise UnknownTagError('Unknown tag: {}! Must be one of {}.'.format(
                tag, BroadcastTag.__members__.keys()))
        recv_queue = recv_queues._asdict()[tag]

        try:
            recv_queue.put_nowait((sender, msg))
        except AttributeError as e:
            print("error", sender, (tag, j, msg))
            traceback.print_exc()


class RotatingLeaderHotstuff():
    """RotatingLeaderHotstuff object used to run the protocol

    :param str sid: The base name of the common coin that will be used to
        derive a nonce to uniquely identify the coin.
    :param int pid: Node id.
    :param int Bfast: Batch size of transactions.
    :param int Bacs: Batch size of transactions.
    :param int N: Number of nodes in the network.
    :param int f: Number of faulty nodes that can be tolerated.
    :param TBLSPublicKey sPK: Public key of the (f, N) threshold signature.
    :param TBLSPrivateKey sSK: Signing key of the (f, N) threshold signature.
    :param TBLSPublicKey sPK1: Public key of the (N-f, N) threshold signature.
    :param TBLSPrivateKey sSK1: Signing key of the (N-f, N) threshold signature.
    :param list sPK2s: Public key(s) of ECDSA signature for all N parties.
    :param PrivateKey sSK2: Signing key of ECDSA signature.
    :param str ePK: Public key of the threshold encryption.
    :param str eSK: Signing key of the threshold encryption.
    :param send:
    :param recv:
    :param K: a test parameter to specify break out after K epochs
    """

    def __init__(self, sid, pid, S, T, Bfast, Bacs, N, f, sPK, sSK, sPK1, sSK1, sPK2s, sSK2, ePK, eSK, send, recv, K=3, mute=False, omitfast=False):

        self.SLOTS_NUM = S
        self.TIMEOUT = T
        self.FAST_BATCH_SIZE = Bfast
        self.FALLBACK_BATCH_SIZE = Bacs

        self.sid = sid
        self.id = pid
        self.N = N
        self.f = f
        self.sPK = sPK
        self.sSK = sSK
        self.sPK1 = sPK1
        self.sSK1 = sSK1
        self.sPK2s = sPK2s
        self.sSK2 = sSK2
        self.ePK = ePK
        self.eSK = eSK
        self._send = send
        self._recv = recv
        self.logger = set_consensus_log(pid)
        self.epoch = 0  # Current block number
        self.transaction_buffer = Queue()
        self._per_epoch_recv = {}  # Buffer of incoming messages

        self.K = K

        self.s_time = 0
        self.e_time = 0

        self.txcnt = 0
        self.txdelay = 0
        self.vcdelay = []

        self.mute = mute
        self.omitfast = omitfast

    def submit_tx(self, tx):
        """Appends the given transaction to the transaction buffer.

        :param tx: Transaction to append to the buffer.
        """
        # print('backlog_tx', self.id, tx)
        #if self.logger != None:
        #    self.logger.info('Backlogged tx at Node %d:' % self.id + str(tx))
        self.transaction_buffer.put_nowait(tx)

    def run_bft(self):
        """Run the Mule protocol."""

        if self.mute:
            muted_nodes = [each * 4 + 1 for each in range(int((self.N-1)/4))]
            if self.id in muted_nodes:
                #T = 0.00001
                while True:
                    time.sleep(10)

        #if self.mute:
        #    muted_nodes = [each * 3 + 1 for each in range(int((self.N-1)/3))]
        #    if self.id in muted_nodes:
        #        self._send = lambda j, o: time.sleep(100)
        #        self._recv = lambda: (time.sleep(100) for i in range(10000))

        def _recv_loop():
            """Receive messages."""
            while True:
                #gevent.sleep(0)
                try:
                    (sender, (r, msg)) = self._recv()
                    # Maintain an *unbounded* recv queue for each epoch
                    if r not in self._per_epoch_recv:
                        self._per_epoch_recv[r] = Queue()
                    # Buffer this message
                    self._per_epoch_recv[r].put_nowait((sender, msg))
                except:
                    continue

        #self._recv_thread = gevent.spawn(_recv_loop)
        self._recv_thread = Greenlet(_recv_loop)
        self._recv_thread.start()

        self.s_time = time.time()
        if self.logger != None:
            self.logger.info('Node %d starts to run at time:' % self.id + str(self.s_time))

        while True:

            # For each epoch
            e = self.epoch

            if e not in self._per_epoch_recv:
                self._per_epoch_recv[e] = Queue()

            def make_epoch_send(e):
                def _send(j, o):
                    self._send(j, (e, o))
                def _send_1(j, o):
                    self._send(j, (e+1, o))
                return _send, _send_1

            send_e, _send_e_1 = make_epoch_send(e)
            recv_e = self._per_epoch_recv[e].get

            self._run_epoch(e, send_e, _send_e_1, recv_e)

            # print('new block at %d:' % self.id, new_tx)
            #if self.logger != None:
            #    self.logger.info('Node %d Delivers Block %d: ' % (self.id, self.epoch) + str(new_tx))

            # print('buffer at %d:' % self.id, self.transaction_buffer)
            #if self.logger != None:
            #    self.logger.info('Backlog Buffer at Node %d:' % self.id + str(self.transaction_buffer))

            self.e_time = time.time()
            if self.logger != None:
                self.logger.info("node %d breaks in %f seconds in epoch %d with total delivered Txs %d and average delay %f" % (self.id, self.e_time-self.s_time, e, self.txcnt, self.txdelay) )
            else:
                print("node %d breaks in %f seconds with total delivered Txs %d and average delay %f" % (self.id, self.e_time-self.s_time, self.txcnt, self.txdelay))

            self.epoch += 1  # Increment the round
            if self.epoch >= self.K:
                break

            #gevent.sleep(0)

    def _recovery(self):
        # TODO: here to implement to recover blocks
        pass

    #
    def _run_epoch(self, e, send, send_1, recv):
        """Run one protocol epoch.

        :param int e: epoch id
        :param send:
        :param recv:
        """
        if self.logger != None:
            self.logger.info('Node enters epoch %d' % e)

        sid = self.sid
        pid = self.id
        N = self.N
        f = self.f
        leader = e % N

        T = self.TIMEOUT
        #if e == 0:
        #    T = 15
        #if self.mute:
        #    muted_nodes = [each * 3 + 1 for each in range(int((N-1)/3))]
        #    if leader in muted_nodes:
        #        T = 0.00001

        #S = self.SLOTS_NUM
        #T = self.TIMEOUT
        #B = self.FAST_BATCH_SIZE

        epoch_id = sid + 'FAST' + str(e)
        hash_genesis = hash(epoch_id)

        fast_recv = Queue()  # The thread-safe queue to receive the messages sent to fast_path of this epoch
        viewchange_recv = Queue()
        newview_recv = Queue()

        recv_queues = BroadcastReceiverQueues(
            FAST=fast_recv,
            VIEW_CHANGE=viewchange_recv,
            NEW_VIEW=newview_recv,
        )
        recv_t = gevent.spawn(broadcast_receiver_loop, recv, recv_queues)


        fast_blocks = Queue(1)  # The blocks that receives

        latest_delivered_block = None
        latest_notarized_block = None
        latest_notarization = None

        newview_counter = 0

        viewchange_counter = 0
        viewchange_max_slot = 0

        def _setup_fastpath(leader):

            def fastpath_send(k, o):
                send(k, ('FAST', '', o))

            def fastpath_output(o):
                nonlocal latest_delivered_block, latest_notarized_block, latest_notarization
                if not fast_blocks.empty():
                    latest_delivered_block = fast_blocks.get()
                    #tx_cnt = str(latest_delivered_block).count("Dummy TX")
                    #self.txcnt += tx_cnt
                    #if self.logger is not None:
                    #    self.logger.info('Node %d Delivers Fastpath Block in Epoch %d at Slot %d with having %d TXs' % (self.id, self.epoch, latest_delivered_block[1], tx_cnt))
                latest_notarized_block, latest_notarization = o
                fast_blocks.put(o)

            fast_thread = gevent.spawn(hsfastpath, epoch_id, pid, N, f, leader,
                                   self.transaction_buffer.get_nowait, fastpath_output,
                                   self.SLOTS_NUM, self.FAST_BATCH_SIZE, T,
                                   hash_genesis, self.sPK2s, self.sSK2,
                                   fast_recv.get, fastpath_send, logger=self.logger, omitfast=self.omitfast)

            return fast_thread


        def wait_newview_msg():
            nonlocal newview_counter
            while True:
                print(newview_counter)
                _, (epoch, last_max_slot, last_max_slot_sig, last_notarized_block_header) = newview_recv.get()
                newview_counter += 1
                # Here skip many verifications etc.....
                if viewchange_counter >= N - f:
                    break
                print(newview_counter)
                gevent.sleep(0)

        def handle_viewchange_msg():
            nonlocal viewchange_counter, viewchange_max_slot

            while True:
                j, (notarized_block_header_j, notarized_block_Sig_j) = viewchange_recv.get()
                if notarized_block_Sig_j is not None:
                    (_, slot_num, Sig_p, _) = notarized_block_header_j
                    notarized_block_hash_j = hash(notarized_block_header_j)
                    try:
                        assert len(notarized_block_Sig_j) >= N-f
                        for item in notarized_block_Sig_j:
                            # print(Sigma_p)
                            (sender, sig_p) = item
                            assert ecdsa_vrfy(self.sPK2s[sender], notarized_block_hash_j, sig_p)
                    except AssertionError:
                        if self.logger is not None: self.logger.info("False view change with invalid notarization")
                        continue  # go to next iteration without counting ViewChange Counter
                else:
                    assert notarized_block_header_j == None
                    slot_num = 0
                gevent.sleep(0)

                viewchange_counter += 1
                if slot_num > viewchange_max_slot:
                    viewchange_max_slot = slot_num

                if viewchange_counter >= N - f:
                    next_leader = (e+1) % N
                    max_slot_sig = ecdsa_sign(self.sSK2, json.dumps(viewchange_max_slot))
                    send_1(next_leader, ('NEW_VIEW', "", (e+1, viewchange_max_slot, max_slot_sig, notarized_block_header_j)))
                    break

        # Start the fast path

        fast_thread = _setup_fastpath(leader)

        #if self.logger is not None:
        #    self.logger.info("epoch %d with fast path leader %d" % (e, leader))


        #if e > 0 and leader == pid:
        #    wait_newview_msg()

        # Setup handler of view change requests
        vc_thread = gevent.spawn(handle_viewchange_msg)


        # Wait either view_change handler done or fast_path done
        vc_ready = gevent.event.Event()
        vc_ready.clear()

        vc_start = gevent.event.Event()
        vc_start.clear()

        def wait_for_fastpath():
            fast_thread.get()
            vc_start.set()
            if self.logger != None:
                self.logger.info('Fastpath of epoch %d completed' % e)

        def wait_for_vc_msg():
            vc_thread.get()
            vc_ready.set()
            if self.logger != None:
                self.logger.info('VC messages of epoch %d collected' % e)

        gevent.spawn(wait_for_fastpath)
        vc_start.wait()

        # Get the returned notarization of the fast path, which contains the combined Signature for the tip of chain
        notarization = latest_notarization
        try:
            if notarization is not None:
                notarized_block = latest_notarized_block
                assert notarized_block is not None
                payload_digest = hash(notarized_block[3])
                notarized_block_header = (notarized_block[0], notarized_block[1], notarized_block[2], payload_digest)
                notarized_block_hash, notarized_block_raw_Sig, (epoch_txcnt, weighted_delay) = notarization
                self.txdelay = (self.txcnt * self.txdelay + epoch_txcnt * weighted_delay) / (self.txcnt + epoch_txcnt)
                self.txcnt += epoch_txcnt
                #assert hash(notarized_block_header) == notarized_block_hash
                o = (notarized_block_header, notarized_block_raw_Sig)
                send(-1, ('VIEW_CHANGE', '', o))
            else:
                notarized_block_header = None
                o = (notarized_block_header, None)
                send(-1, ('VIEW_CHANGE', '', o))
        except AssertionError:
            print("Problematic notarization....")

        gevent.spawn(wait_for_vc_msg)
        vc_ready.wait()

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

from rbcbdtbft.core.rbcfastpath import rbcfastpath
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
from dumbobft.core.validators import prbc_validate


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
    TCVBA = 'TCVBA'
    ABA = 'ABA'
    ABA_COIN = 'ABA_COIN'
    FAST = 'FAST'
    VIEW_CHANGE = 'VIEW_CHANGE'
    VIEW_COIN = 'VIEW_COIN'
    ACS_PRBC = 'ACS_PRBC'
    ACS_VACS = 'ACS_VACS'
    TPKE = 'TPKE'



BroadcastReceiverQueues = namedtuple(
    'BroadcastReceiverQueues', ('TCVBA', 'ABA', 'ABA_COIN', 'FAST', 'VIEW_CHANGE', 'VIEW_COIN', 'ACS_PRBC', 'ACS_VACS', 'TPKE'))


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

        if tag == BroadcastTag.ACS_PRBC.value:
            recv_queue = recv_queue[j]
        try:
            recv_queue.put_nowait((sender, msg))
        except AttributeError as e:
            print("error", sender, (tag, j, msg))
            traceback.print_exc()


class RbcBdt():
    """Mule object used to run the protocol

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

        self.actual_txcnt = 0
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
        self.transaction_buffer.put(tx)

    def run_bft(self):
        """Run the Mule protocol."""

        if self.mute:
            muted_nodes = [each * 4 + 1 for each in range(int((self.N-1)/4))]
            if self.id in muted_nodes:
                #T = 0.00001
                while True:
                    time.sleep(10)

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
                return _send

            send_e = make_epoch_send(e)
            recv_e = self._per_epoch_recv[e].get

            self._run_epoch(e, send_e, recv_e)

            # print('new block at %d:' % self.id, new_tx)
            #if self.logger != None:
            #    self.logger.info('Node %d Delivers Block %d: ' % (self.id, self.epoch) + str(new_tx))

            # print('buffer at %d:' % self.id, self.transaction_buffer)
            #if self.logger != None:
            #    self.logger.info('Backlog Buffer at Node %d:' % self.id + str(self.transaction_buffer))

            self.e_time = time.time()
            if self.logger != None:
                self.logger.info("node %d breaks in %f seconds in epoch %d with total delivered Txs %d (%d) and average delay %f and average VC delay %f" % (self.id, self.e_time-self.s_time, e, self.txcnt, self.actual_txcnt, self.txdelay, sum(self.vcdelay)/len(self.vcdelay)) )
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
    def _run_epoch(self, e, send, recv):
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

        prbc_proofs = dict()

        fast_recv = Queue()
        viewchange_recv = Queue()
        tcvba_recv = Queue()
        coin_recv = Queue()
        aba_coin_recv = Queue()

        prbc_recvs = [Queue() for _ in range(N)]
        vacs_recv = Queue()
        tpke_recv = Queue()

        prbc_outputs = [Queue(1) for _ in range(N)]
        prbc_proofs = dict()

        aba_recv = Queue()

        recv_queues = BroadcastReceiverQueues(
            TCVBA=tcvba_recv,
            FAST=fast_recv,
            ABA=aba_recv,
            ABA_COIN=aba_coin_recv,
            VIEW_CHANGE=viewchange_recv,
            VIEW_COIN=coin_recv,
            ACS_PRBC=prbc_recvs,
            ACS_VACS=vacs_recv,
            TPKE=tpke_recv,
        )
        recv_t = gevent.spawn(broadcast_receiver_loop, recv, recv_queues)

        tcvba_input = Queue(1)
        tcvba_output = Queue(1)

        fast_blocks = Queue(1)  # The blocks that receives

        latest_delivered_block = None
        latest_notarized_block = None
        latest_notarization = None

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

            fast_thread = gevent.spawn(rbcfastpath, epoch_id, pid, N, f, leader,
                                   self.transaction_buffer.get_nowait, fastpath_output,
                                   self.SLOTS_NUM, self.FAST_BATCH_SIZE, T,
                                   hash_genesis, self.sPK2s, self.sSK2,
                                   fast_recv.get, fastpath_send, logger=self.logger, omitfast=self.omitfast)

            return fast_thread

        def _setup_coin():
            def coin_bcast(o):
                """Common coin multicast operation.
                :param o: Value to multicast.
                """
                for k in range(N):
                    send(k, ('VIEW_COIN', '', o))

            coin = shared_coin(epoch_id, pid, N, f,
                               self.sPK, self.sSK,
                               coin_bcast, coin_recv.get, single_bit=True)

            return coin

        def _setup_tcvba(coin):

            def tcvba_send(k, o):
                send(k, ('TCVBA', '', o))

            tcvba = gevent.spawn(twovalueagreement, epoch_id, pid, N, f, coin,
                         tcvba_input.get, tcvba_output.put_nowait,
                         tcvba_recv.get, tcvba_send, logger=self.logger)

            return tcvba

        def handle_viewchange_msg():
            nonlocal viewchange_counter, viewchange_max_slot

            while True:
                #gevent.sleep(0)
                j, (slot_j, proof_j) = viewchange_recv.get()
                if slot_j is not None:
                    prbc_sid_j = epoch_id + 'FAST_PRBC' + str(slot_j) + str(leader)
                    try:
                        if prbc_sid_j in prbc_proofs.keys():
                            assert slot_j == prbc_proofs[prbc_sid_j][0]
                        else:
                            assert prbc_validate(prbc_sid_j, N, f, self.sPK2s, proof_j)
                            prbc_proofs[prbc_sid_j] = (slot_j, proof_j)
                        slot_num = slot_j
                    except AssertionError:
                        if self.logger is not None:
                            self.logger.info("False view change with invalid notarization")
                        continue  # go to next iteration without counting ViewChange Counter
                else:
                    assert slot_j is None
                    slot_num = 0

                viewchange_counter += 1
                if slot_num > viewchange_max_slot:
                    viewchange_max_slot = slot_num

                if viewchange_counter >= N - f:
                    tcvba_input.put_nowait(viewchange_max_slot)
                    break

        # Start the fast path

        fast_thread = _setup_fastpath(leader)

        #if self.logger is not None:
        #    self.logger.info("epoch %d with fast path leader %d" % (e, leader))

        # Setup handler of view change requests
        vc_thread = gevent.spawn(handle_viewchange_msg)

        # Setup view change primitives
        coin_thread = _setup_coin()
        tcvba_thread = _setup_tcvba(coin_thread)



        # Wait either view_change handler done or fast_path done
        vc_ready = gevent.event.Event()
        vc_ready.clear()

        def wait_for_fastpath():
            fast_thread.get()
            vc_ready.set()
            if self.logger != None:
                self.logger.info('Fastpath of epoch %d completed' % e)

        def wait_for_vc_msg():
            vc_thread.get()
            vc_ready.set()
            if self.logger != None:
                self.logger.info('VC messages of epoch %d collected' % e)

        gevent.spawn(wait_for_fastpath)
        gevent.spawn(wait_for_vc_msg)

        #lst = [vc_thread, fast_thread]
        #for g in lst:
        #    g.link(lambda *args: ready.set())

        vc_ready.wait()

        start_vc = time.time()

        # Get the returned notarization of the fast path, which contains the combined Signature for the tip of chain
        notarization = latest_notarization
        try:
            #notarization = fast_thread.get(block=False)
            if notarization is not None:
                notarized_block = latest_notarized_block
                assert notarized_block is not None
                _sid, _slot, _leader, _batch = notarized_block
                slot, proof, (epoch_txcnt, weighted_delay) = notarization
                assert epoch_id == _sid
                assert leader == _leader
                assert _slot == slot
                prbc_sid = epoch_id + 'FAST_PRBC' + str(slot) + str(leader)
                assert prbc_validate(prbc_sid, N, f, self.sPK2s, proof)
                prbc_proofs[prbc_sid] = (slot, proof)
                if (self.txcnt + epoch_txcnt) != 0:
                    self.txdelay = (self.txcnt * self.txdelay + epoch_txcnt * weighted_delay) / (self.txcnt + epoch_txcnt)
                    self.txcnt += epoch_txcnt
                o = (slot, proof)
                send(-1, ('VIEW_CHANGE', '', o))
            else:
                o = (None, None)
                send(-1, ('VIEW_CHANGE', '', o))
        except AssertionError:
            print("Problematic notarization....")
            if self.logger is not None:
                self.logger.info("Problematic notarization....")
        #except gevent.timeout.Timeout:
        #    assert notarization is None
        #    o = (None, None)
        #    send(-1, ('VIEW_CHANGE', '', o))


        #
        delivered_slots = tcvba_output.get()  # Block to receive the output
        #delivered_slots = max(delivered_slots - 1, 0)
        self.actual_txcnt += delivered_slots * self.FAST_BATCH_SIZE
        #

        end_vc = time.time()
        if self.logger is not None:
           self.logger.info('VIEW CHANGE costs time: %f' % (end_vc - start_vc) )
        self.vcdelay.append(end_vc - start_vc)

        #print(("fast blocks: ", fast_blocks))

        if delivered_slots > 0:
            #gevent.joinall([vc_thread, fast_thread])
            #if self.logger != None:
            #    self.logger.info('Fast block tx at Node %d:' % self.id + str(fast_blocks))
            #return delivered_slots
            pass
        else:
            # Select B transactions (TODO: actual random selection)
            tx_to_send = []

            try:
                tx_to_send.append(self.transaction_buffer.get_nowait())
                tx_to_send = tx_to_send * self.FALLBACK_BATCH_SIZE
            except IndexError as e:
                tx_to_send.append("Dummy")

            my_prbc_input = Queue(1)

            vacs_input = Queue(1)
            vacs_output = Queue(1)

            # if self.logger != None: self.logger.info('Commit tx at Node %d:' % self.id + str(tx_to_send))
            start = time.time()

            def _setup_prbc(j):
                """Setup the sub protocols RBC, BA and common coin.

                :param int j: Node index for which the setup is being done.
                """

                def prbc_send(k, o):
                    """Reliable send operation.
                    :param k: Node to send.
                    :param o: Value to send.
                    """
                    send(k, ('ACS_PRBC', j, o))

                # Only leader gets input
                prbc_input = my_prbc_input.get if j == pid else None
                prbc_thread = gevent.spawn(provablereliablebroadcast, epoch_id+'PRBC'+str(j), pid, N, f, self.sPK2s, self.sSK2, j,
                                   prbc_input, prbc_recvs[j].get, prbc_send)

                def wait_for_prbc_output():
                    value, proof = prbc_thread.get()
                    prbc_proofs[epoch_id+'PRBC'+str(j)] = proof
                    prbc_outputs[j].put_nowait((value, proof))

                gevent.spawn(wait_for_prbc_output)

            def _setup_vacs():

                def vacs_send(k, o):
                    """Threshold encryption broadcast."""
                    send(k, ('ACS_VACS', '', o))

                def vacs_predicate(j, vj):
                    prbc_sid = epoch_id+'PRBC'+str(j)
                    try:
                        proof = vj
                        if prbc_sid in prbc_proofs.keys():
                            try:
                                _prbc_sid, _roothash, _ = proof
                                assert prbc_sid == _prbc_sid
                                _, roothash, _ = prbc_proofs[prbc_sid]
                                assert roothash == _roothash
                                return True
                            except AssertionError:
                                print("1 Failed to verify proof for RBC")
                                return False
                        assert prbc_validate(prbc_sid, N, f, self.sPK2s, proof)
                        return True
                    except AssertionError:
                        print("2 Failed to verify proof for RBC")
                        return False

                gevent.spawn(validatedcommonsubset, epoch_id+'VACS', pid, N, f,
                             self.sPK, self.sSK, self.sPK1, self.sSK1, self.sPK2s, self.sSK2,
                             vacs_input.get, vacs_output.put_nowait,
                             vacs_recv.get, vacs_send, vacs_predicate)

            # N instances of ABA, RBC
            for j in range(N):
                _setup_prbc(j)

            # One instance of (validated) ACS
            _setup_vacs()

            # One instance of TPKE
            def tpke_bcast(o):
                """Threshold encryption broadcast."""
                def broadcast(o):
                    """Multicast the given input ``o``.
                    :param o: Input to multicast.
                    """
                    for j in range(N):
                        send(j, o)
                broadcast(('TPKE', '', o))

            # One instance of ACS pid, N, f, prbc_out, vacs_in, vacs_out
            dumboacs_thread = gevent.spawn(dumbocommonsubset, pid, N, f, [prbc_output.get for prbc_output in prbc_outputs],
                               vacs_input.put_nowait,
                               vacs_output.get)

            _output = honeybadger_block(pid, self.N, self.f, self.ePK, self.eSK,
                                        propose=json.dumps(tx_to_send),
                                        acs_put_in=my_prbc_input.put_nowait, acs_get_out=dumboacs_thread.get,
                                        tpke_bcast=tpke_bcast, tpke_recv=tpke_recv.get)

            block = set()
            for batch in _output:
                decoded_batch = json.loads(batch.decode())
                for tx in decoded_batch:
                    block.add(tx)

            end = time.time()

            #if self.logger != None:
            #    blk = str(block)
            #    #self.logger.info('Node %d Delivers ACS Block %d: ' % (self.id, self.epoch) + str(block))
            #    tx_cnt = blk.count("Dummy TX")
            #    self.txcnt += tx_cnt
            #    self.logger.info('Node %d Delivers ACS Block in Epoch %d with having %d TXs' % (self.id, self.epoch, tx_cnt))

            #blkcnt = str(block).count("Dummy TX")
            blkcnt = self.FALLBACK_BATCH_SIZE * (N - f)
            blkdelay = end - start_vc

            self.txdelay = (self.txcnt * self.txdelay + blkcnt * blkdelay) / (self.txcnt + blkcnt)
            self.txcnt += blkcnt

            #if self.logger != None:
            #    self.logger.info('ACS Block Delay at Node %d: ' % self.id + str(end - start))

            #recv_t.kill()
            #return list(block)
            self.actual_txcnt += self.FALLBACK_BATCH_SIZE * (N - f)



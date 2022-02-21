from gevent import monkey; monkey.patch_all(thread=False)

import traceback
import time
from collections import defaultdict
from gevent.event import Event
from gevent.queue import Queue
from gevent import Timeout
from crypto.ecdsa.ecdsa import ecdsa_sign, ecdsa_vrfy, PublicKey
import os
import json
import gevent
import hashlib, pickle
from dumbobft.core.provablereliablebroadcast import provablereliablebroadcast


def hash(x):
    return hashlib.sha256(pickle.dumps(x)).digest()


def rbcfastpath(sid, pid, N, f, leader, get_input, output_notraized_block, Snum, Bsize, Tout, hash_genesis, PK2s, SK2, recv, send, omitfast=False, logger=None):
    """Fast path, Byzantine Safe Broadcast
    :param str sid: ``the string of identifier``
    :param int pid: ``0 <= pid < N``
    :param int N:  at least 3
    :param int f: fault tolerance, ``N >= 3f + 1``
    :param int leader: the pid of leading node
    :param get_input: a function to get input TXs, e.g., input() to get a transaction
    :param output_notraized_block: a function to deliver notraized blocks, e.g., output(block)

    :param list PK2s: an array of ``coincurve.PublicKey'', i.e., N public keys of ECDSA for all parties
    :param PublicKey SK2: ``coincurve.PrivateKey'', i.e., secret key of ECDSA
    :param int Tout: timeout of a slot
    :param int Snum: number of slots in a epoch
    :param int Bsize: batch size, i.e., the number of TXs in a batch
    :param hash_genesis: the hash of genesis block
    :param recv: function to receive incoming messages
    :param send: function to send outgoing messages
    :param bcast: function to broadcast a message
    :return tuple: False to represent timeout, and True to represent success
    """

    if logger is not None:
        logger.info("Entering fast path")

    TIMEOUT = Tout
    SLOTS_NUM = Snum
    BATCH_SIZE = Bsize

    prbc_recvs = [Queue() for _ in range(SLOTS_NUM+1)]

    s_times = [0] * (SLOTS_NUM+1)
    e_times = [0] * (SLOTS_NUM+1)
    txcnt =  [0] * (SLOTS_NUM+1)
    delay =  [0] * (SLOTS_NUM+1)

    epoch_txcnt = 0
    weighted_delay = 0

    fixed_block = None
    notraized_block = None

    def handle_messages():
        while True:
            (sender, msg) = recv()
            tag, slot, o = msg
            if tag == 'FAST_PRBC':
                assert slot in range(SLOTS_NUM + 1)
                prbc_recvs[slot].put_nowait((sender, o))

    recv_thread = gevent.spawn(handle_messages)

    stop = False
    slot_cur = 1

    while slot_cur <= SLOTS_NUM and stop is False:
        try:
            with Timeout(TIMEOUT):

                s_times[slot_cur] = time.time()

                if pid == leader:
                    tx_batch = json.dumps([get_input()] * BATCH_SIZE)
                    slot_prbc_input = Queue(1)
                    slot_prbc_input.put(tx_batch)

                def _setup_prbc():
                    """Setup the sub protocols PRBC.
                    :param int j: Node index for which the setup is being done.
                    """

                    def prbc_send(k, o):
                        """Reliable send operation.
                        :param k: Node to send.
                        :param o: Value to send.
                        """
                        send(k, ('FAST_PRBC', slot_cur, o))

                    # Only leader gets input
                    prbc_input = slot_prbc_input.get if leader == pid  else None

                    return gevent.spawn(provablereliablebroadcast, sid + 'FAST_PRBC' + str(slot_cur) + str(leader), pid, N, f,
                                                PK2s, SK2, leader, prbc_input, prbc_recvs[slot_cur].get, prbc_send)

                if omitfast is False:
                    prbc_thread = _setup_prbc()
                    batches, proof = prbc_thread.get()
                else:
                    while True:
                        gevent.sleep(0.01)
        except Timeout:
            break

        ############################################
        # Timeout up to here
        ############################################

        if notraized_block is not None:
            fixed_block = notraized_block
            assert fixed_block[1] == slot_cur - 1
            slot_prev = slot_cur - 1
            txcnt[slot_prev] = str(fixed_block).count("Dummy TX")
            e_times[slot_prev] = time.time()
            delay[slot_prev] = e_times[slot_prev] - s_times[slot_prev]
            weighted_delay = (epoch_txcnt * weighted_delay + txcnt[slot_prev] * delay[slot_prev]) / (epoch_txcnt + txcnt[slot_prev])
            epoch_txcnt += txcnt[slot_prev]
        ###
        notraized_block = (sid, slot_cur, leader, batches)
        if output_notraized_block is not None:
            output_notraized_block((notraized_block, (slot_cur, proof, (epoch_txcnt, weighted_delay))))

        slot_cur = slot_cur + 1





    if logger is not None:
        logger.info("Leaves fastpath at %d slot" % (slot_cur))
    if notraized_block is not None:
        return notraized_block[1], notraized_block[3], (epoch_txcnt, weighted_delay)  # represents fast_path successes
    else:
        return None

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


def hash(x):
    return hashlib.sha256(pickle.dumps(x)).digest()


def hsfastpath(sid, pid, N, f, leader, get_input, output_notraized_block, Snum, Bsize, Tout, hash_genesis, PK2s, SK2, recv, send, omitfast=False, logger=None):
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
    :param float Tout: timeout of a slot
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

    assert leader in range(N)
    slot_cur = 1

    hash_prev = hash_genesis
    pending_block = None
    notraized_block = None
    fixed_block = None

    # Leader's temp variables
    voters = defaultdict(lambda: set())
    votes = defaultdict(lambda: dict())
    decides = defaultdict(lambda: Queue(1))

    decide_sent = [False] * (SLOTS_NUM + 3)  # The first item of the list is not used

    msg_noncritical_signal = Event()
    msg_noncritical_signal.set()

    slot_noncritical_signal = Event()
    slot_noncritical_signal.set()

    s_times = [0] * (SLOTS_NUM + 3)
    e_times = [0] * (SLOTS_NUM + 3)
    txcnt =  [0] * (SLOTS_NUM + 3)
    delay =  [0] * (SLOTS_NUM + 3)

    epoch_txcnt = 0
    weighted_delay = 0



    def handle_messages():
        nonlocal leader, hash_prev, pending_block, notraized_block, fixed_block, voters, votes, slot_cur

        while True:

            #gevent.sleep(0)

            (sender, msg) = recv()
            #logger.info("receving a fast path msg " + str((sender, msg)))

            assert sender in range(N)

            ########################
            # Enter critical block #
            ########################

            msg_noncritical_signal.clear()

            if msg[0] == 'VOTE' and pid == leader and len(voters[slot_cur]) < N - f:

                _, slot, hash_p, sig_p = msg
                #_, slot, hash_p, raw_sig_p, tx_batch, tx_sig = msg
                #sig_p = deserialize1(raw_sig_p)

                if sender not in voters[slot]:

                    try:
                        assert slot == slot_cur
                    except AssertionError:
                        if logger is not None:
                            # logger.info("vote out of sync from node %d" % sender)
                            pass
                        #if slot < slot_cur:
                        #    if logger is not None: logger.info("Late vote from node %d! Not needed anymore..." % sender)
                        #else:
                        #    if logger is not None: logger.info("Too early vote from node %d! I do not decide earlier block yet..." % sender)
                        msg_noncritical_signal.set()
                        continue

                    try:
                        assert hash_p == hash_prev
                    except AssertionError:
                        if logger is not None:
                            logger.info("False vote from node %d though within the same slot!" % sender)
                        msg_noncritical_signal.set()
                        continue

                    try:
                        assert ecdsa_vrfy(PK2s[sender], hash_p, sig_p)
                    except AssertionError:
                        if logger is not None:
                            logger.info("Vote signature failed!")
                        msg_noncritical_signal.set()
                        continue


                    voters[slot_cur].add(sender)
                    votes[slot_cur][sender] = sig_p

                    if len(voters[slot_cur]) == N - f and not decide_sent[slot_cur]:
                        #print(slot_cur)
                        Sigma = tuple(votes[slot_cur].items())
                        if slot_cur == SLOTS_NUM + 1 or slot_cur == SLOTS_NUM + 2:
                            tx_batch = 'Dummy'
                        else:
                            try:
                                tx_batch = json.dumps([get_input()] * BATCH_SIZE)
                            except Exception as e:
                                tx_batch = json.dumps(['Dummy' for _ in range(BATCH_SIZE)])

                        send(-2, ('DECIDE', slot_cur, hash_prev, Sigma, tx_batch))
                        #if logger is not None: logger.info("Decide made and sent")
                        decide_sent[slot_cur] = True
                        decides[slot_cur].put_nowait((hash_p, Sigma, tx_batch))

            if msg[0] == "DECIDE" and pid != leader:

                _, slot, hash_p, Sigma_p, batches = msg

                try:
                    assert slot == slot_cur
                except AssertionError:
                    if logger is not None:
                        # logger.info("Out of synchronization")
                        pass
                    msg_noncritical_signal.set()
                    continue

                try:
                    assert len(Sigma_p) >= N - f
                except AssertionError:
                    if logger is not None:
                        logger.info("No enough ecdsa signatures!")
                    msg_noncritical_signal.set()
                    continue

                try:
                    for item in Sigma_p:
                        #print(Sigma_p)
                        (sender, sig_p) = item
                        assert ecdsa_vrfy(PK2s[sender], hash_p, sig_p)
                except AssertionError:
                    if logger is not None:
                        logger.info("ecdsa signature failed!")
                    msg_noncritical_signal.set()
                    continue

                decides[slot_cur].put_nowait((hash_p, Sigma_p, batches))


            msg_noncritical_signal.set()

            ########################
            # Leave critical block #
            ########################

    """
    One slot
    """

    def one_slot():
        nonlocal pending_block, notraized_block, fixed_block, hash_prev, slot_cur, epoch_txcnt, delay, e_times, s_times, txcnt, weighted_delay

        #print('3')

        if logger is not None:
            logger.info("Entering slot %d" % slot_cur)

        s_times[slot_cur] = time.time()

        try:
            sig_prev = ecdsa_sign(SK2, hash_prev)
            send(leader, ('VOTE', slot_cur, hash_prev, sig_prev))
        except AttributeError as e:
            if logger is not None:
                logger.info(traceback.print_exc())

        #print('4')

        (h_p, Sigma_p, batches) = decides[slot_cur].get()  # Block to wait for the voted block


        ########################
        # Enter critical block #
        ########################

        slot_noncritical_signal.clear()
        msg_noncritical_signal.wait()

        if pending_block is not None:

            if notraized_block is not None:
                fixed_block = notraized_block
                assert fixed_block[1] + 2 == slot_cur

            notraized_block = (pending_block[0], pending_block[1], pending_block[2], pending_block[4])
            assert notraized_block[1] + 1 == slot_cur

            if fixed_block is not None:
                e_times[fixed_block[1]] = time.time()
                delay[fixed_block[1]] = e_times[fixed_block[1]] - s_times[fixed_block[1]]
                txcnt[fixed_block[1]] = str(fixed_block).count("Dummy TX")
                weighted_delay = (epoch_txcnt * weighted_delay + txcnt[fixed_block[1]] * delay[fixed_block[1]]) / (epoch_txcnt + txcnt[fixed_block[1]])
                epoch_txcnt += txcnt[fixed_block[1]]

                if logger is not None:
                    logger.info('Fast block at Node %d for Epoch %s and Slot %d has delay and TXs: %s, %d' % (pid, sid, fixed_block[1], str(delay[fixed_block[1]]), txcnt[fixed_block[1]]))


            if output_notraized_block is not None:
                output_notraized_block((notraized_block, (h_p, Sigma_p, (epoch_txcnt, weighted_delay))))

        pending_block = (sid, slot_cur, h_p, Sigma_p, batches)
        pending_block_header = (sid, slot_cur, h_p, hash(batches))
        hash_prev = hash(pending_block_header)

        if logger is not None:
            logger.info("Leaving slot %d" % slot_cur)

        slot_cur = slot_cur + 1
        slot_noncritical_signal.set()



        ########################
        # Leave critical block #
        ########################

    """
    Execute the slots
    """

    recv_thread = gevent.spawn(handle_messages)
    #gevent.sleep(0)

    while slot_cur <= SLOTS_NUM + 2:

        #if logger is not None:
        #    logger.info("Enter fastpath's slot %d out of all %d slots" % (slot_cur, SLOTS_NUM))

        #print('0')

        #if logger is not None:
        #    logger.info("entering fastpath slot %d ..." % slot_cur)


        #print('1')

        msg_noncritical_signal.wait()
        slot_noncritical_signal.wait()

        #print('2')

        #timeout = Timeout(TIMEOUT, False)
        #timeout.start()

        timeout = Timeout(TIMEOUT)
        #print(TIMEOUT)
        timeout.start()
        try:
            with gevent.Timeout(TIMEOUT, False):
            #with Timeout(TIMEOUT):
            #gevent.spawn(one_slot).join(timeout=Timeout)
                #if omitfast is False:
                one_slot()
                #else:
                #    while True:
                #        gevent.sleep(0.01)
        except Timeout as e:
            msg_noncritical_signal.wait()
            slot_noncritical_signal.wait()
            gevent.killall([recv_thread])
            print("node " + str(pid) + " error: " + str(e))
            if logger is not None:
                logger.info("Fastpath Timeout!")
            break
        timeout.cancel()
        timeout.close()

    if logger is not None:
        logger.info("Leaves fastpath at %d slot" % (slot_cur))

    if notraized_block != None:
        return pending_block[2], pending_block[3], (epoch_txcnt, weighted_delay)  # represents fast_path successes
    else:
        return None
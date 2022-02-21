from gevent import monkey; monkey.patch_all(thread=False)
import time

import gevent
from gevent.event import Event
from collections import defaultdict
from honeybadgerbft.exceptions import RedundantMessageError, AbandonedNodeError


def handle_conf_messages(*, sender, message, conf_values, pid, bv_signal):
    _, r, v = message
    assert v in ((0,), (1,), (0, 1))
    if sender in conf_values[r][v]:
        # FIXME: Raise for now to simplify things & be consistent
        # with how other TAGs are handled. Will replace the raise
        # with a continue statement as part of
        # https://github.com/initc3/HoneyBadgerBFT-Python/issues/10
        raise RedundantMessageError(
            'Redundant CONF received {}'.format(message))

    conf_values[r][v].add(sender)

    bv_signal.set()


def wait_for_conf_values(*, pid, N, f, epoch, conf_sent, bin_values,
                         values, conf_values, bv_signal, broadcast):
    conf_sent[epoch][tuple(values)] = True

    broadcast(('CONF', epoch, tuple(bin_values[epoch])))

    while True:
        #gevent.sleep(0)

        if 1 in bin_values[epoch] and len(conf_values[epoch][(1,)]) >= N - f:
            return set((1,))
        if 0 in bin_values[epoch] and len(conf_values[epoch][(0,)]) >= N - f:
            return set((0,))
        if (sum(len(senders) for conf_value, senders in
                conf_values[epoch].items() if senders and
                set(conf_value).issubset(bin_values[epoch])) >= N - f):
            return set((0, 1))

        bv_signal.clear()
        bv_signal.wait()


def baisedbinaryagreement(sid, pid, N, f, coin, input, decide, receive, send, logger=None):
    """Binary consensus from [MMR14]. It takes an input ``vi`` and will
    finally write the decided value into ``decide`` channel.

    :param sid: session identifier
    :param pid: my id number
    :param N: the number of parties
    :param f: the number of byzantine parties
    :param coin: a ``common coin(r)`` is called to block until receiving a bit
    :param input: ``input()`` is called to receive an input
    :param decide: ``decide(0)`` or ``output(1)`` is eventually called
    :param send: send channel
    :param receive: receive channel
    """

    # Messages received are routed to either a shared coin, the broadcast, or AUX
    est_values = defaultdict(lambda: [set(), set()])
    aux_values = defaultdict(lambda: [set(), set()])
    conf_values = defaultdict(lambda: {(0,): set(), (1,): set(), (0, 1): set()})
    est_sent = defaultdict(lambda: [False, False])
    conf_sent = defaultdict(lambda: {(0,): False, (1,): False, (0, 1): False})
    bin_values = defaultdict(set)

    # This event is triggered whenever bin_values or aux_values changes
    bv_signal = Event()

    def broadcast(o):
        for i in range(N):
            send(i, o)

    def _recv():
        while True:  # not finished[pid]:
            #gevent.sleep(0)

            (sender, msg) = receive()

            assert sender in range(N)
            if msg[0] == 'EST':
                # BV_Broadcast message
                _, r, v = msg
                assert v in (0, 1)

                est_values[r][v].add(sender)
                # Relay after reaching first threshold
                if len(est_values[r][v]) >= f + 1 and not est_sent[r][v]:
                    est_sent[r][v] = True
                    broadcast(('EST', r, v))

                # Output after reaching second threshold
                if len(est_values[r][v]) >= 2 * f + 1:

                    bin_values[r].add(v)

                    bv_signal.set()

            elif msg[0] == 'AUX':
                # Aux message
                _, r, v = msg
                assert v in (0, 1)

                aux_values[r][v].add(sender)

                bv_signal.set()

            elif msg[0] == 'CONF':
                handle_conf_messages(
                    sender=sender,
                    message=msg,
                    conf_values=conf_values,
                    pid=pid,
                    bv_signal=bv_signal,
                )

    # Translate mmr14 broadcast into coin.broadcast
    # _coin_broadcast = lambda (r, sig): broadcast(('COIN', r, sig))
    # _coin_recv = Queue()
    # coin = shared_coin(sid+'COIN', pid, N, f, _coin_broadcast, _coin_recv.get)

    # Run the receive loop in the background
    _thread_recv = gevent.spawn(_recv)

    # Block waiting for the input
    # print(pid, sid, 'PRE-ENTERING CRITICAL')
    vi = input()

    start = time.time()
    # print(pid, sid, 'PRE-EXITING CRITICAL', vi)

    assert vi in (0, 1)
    est = vi
    r = 0
    already_decided = None

    while True:  # Unbounded number of rounds

        # print("debug", pid, sid, 'deciding', already_decided, "at epoch", r)
        #gevent.sleep(0)

        if not est_sent[r][est]:
            est_sent[r][est] = True
            broadcast(('EST', r, est))

        # print("debug", pid, sid, 'WAITS BIN VAL at epoch', r)

        while len(bin_values[r]) == 0:
            # Block until a value is output
            bv_signal.clear()
            bv_signal.wait()

        # print("debug", pid, sid, 'GETS BIN VAL at epoch', r)

        w = next(iter(bin_values[r]))  # take an element

        broadcast(('AUX', r, w))

        while True:

            #gevent.sleep(0)

            # Block until at least N-f AUX values are received
            if 1 in bin_values[r] and len(aux_values[r][1]) >= N - f:
                values = set((1,))
                # print('[sid:%s] [pid:%d] VALUES 1 %d' % (sid, pid, r))
                break
            if 0 in bin_values[r] and len(aux_values[r][0]) >= N - f:
                values = set((0,))
                # print('[sid:%s] [pid:%d] VALUES 0 %d' % (sid, pid, r))
                break
            if sum(len(aux_values[r][v]) for v in bin_values[r]) >= N - f:
                values = set((0, 1))
                # print('[sid:%s] [pid:%d] VALUES BOTH %d' % (sid, pid, r))
                break
            bv_signal.clear()
            bv_signal.wait()



        # CONF phase

        if not conf_sent[r][tuple(values)]:
            values = wait_for_conf_values(
                pid=pid,
                N=N,
                f=f,
                epoch=r,
                conf_sent=conf_sent,
                bin_values=bin_values,
                values=values,
                conf_values=conf_values,
                bv_signal=bv_signal,
                broadcast=broadcast,
            )


        # Block until receiving the common coin value

        if r == 0:
            s = 1
        else:
            s = coin(r)

        try:
            assert s in (0, 1)
        except AssertionError:
            s = s % 2

        if r > 20:
            print("debug", pid, sid, 'gets a coin', s, 'at epoch', r)



        try:
            est, already_decided = set_new_estimate(
                sid=sid,
                start=start,
                values=values,
                s=s,
                already_decided=already_decided,
                decide=decide,
                logger=logger,
            )
            # print('debug then decided:', already_decided, '%s' % sid)
        except AbandonedNodeError:
            # print('debug node %d quits %s' % (pid, sid))
            # print('[sid:%s] [pid:%d] QUITTING in round %d' % (sid,pid,r)))

            _thread_recv.kill()
            return

        r += 1


def set_new_estimate(*, sid, start, values, s, already_decided, decide, logger):
    if len(values) == 1:
        v = next(iter(values))
        if v == s:
            if already_decided is None:
                already_decided = v
                assert v in (0, 1)
                decide(v)
                end = time.time()
                if logger != None:
                    logger.info("ABA %s spends %f seconds to complete" % (sid, end - start))
            elif already_decided == v:
                # Here corresponds to a proof that if one party
                # decides at round r, then in all the following
                # rounds, everybody will propose r as an
                # estimation. (Lemma 2, Lemma 1) An abandoned
                # party is a party who has decided but no enough
                # peers to help him end the loop.  Lemma: # of
                # abandoned party <= t
                raise AbandonedNodeError
        est = v
    else:
        est = s
    return est, already_decided

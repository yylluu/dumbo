from gevent import monkey; monkey.patch_all(thread=False)
from datetime import datetime
import time
import gevent
from gevent.event import Event
from collections import defaultdict
import hashlib, pickle

def hash(x):
    return hashlib.sha256(pickle.dumps(x)).digest()

def twovalueagreement(sid, pid, N, f, coin, input, decide, receive, send, logger=None):
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
    :return: blocks until
    """
    # Messages received are routed to either a shared coin, the broadcast, or AUX
    est_values = defaultdict(lambda: defaultdict(lambda: set()))
    aux_values = defaultdict(lambda: defaultdict(lambda: set()))
    conf_values = defaultdict(lambda: defaultdict(lambda: set()))
    est_sent = defaultdict(lambda: defaultdict(lambda: False))
    conf_sent = defaultdict(lambda: defaultdict(lambda: False))
    int_values = defaultdict(set)

    input_recived = False

    finish_sent = False
    finish_value = set()
    finish_cnt = 0

    # This event is triggered whenever int_values or aux_values changes
    bv_signal = Event()

    finish_signal = Event()

    def recv():

        nonlocal finish_sent, est_values, est_sent, int_values, aux_values, bv_signal, finish_value, finish_cnt

        while True:  # not finished[pid]:

            #gevent.sleep(0)

            (sender, msg) = receive()

            assert sender in range(N)

            if msg[0] == 'EST':
                # BV_Broadcast message
                _, r, v = msg
                assert type(v) is int
                if sender in est_values[r][v]:
                    # FIXME: raise or continue? For now will raise just
                    # because it appeared first, but maybe the protocol simply
                    # needs to continue.
                    # print(f'Redundant EST received by {sender}', msg)

                    # raise RedundantMessageError(
                    #    'Redundant EST received {}'.format(msg))
                    continue

                est_values[r][v].add(sender)
                # Relay after reaching first threshold
                if len(est_values[r][v]) >= f + 1 and not est_sent[r][v]:
                    est_sent[r][v] = True
                    est_values[r][v].add(sender)
                    send(-2, ('EST', r, v))


                # Output after reaching second threshold
                if len(est_values[r][v]) >= 2 * f + 1:
                    int_values[r].add(v)
                    bv_signal.set()

            elif msg[0] == 'AUX':
                # Aux message
                _, r, v = msg
                assert type(v) is int
                if sender in aux_values[r][v]:
                    # FIXME: raise or continue? For now will raise just
                    # because it appeared first, but maybe the protocol simply
                    # needs to continue.
                    # print('Redundant AUX received', msg)
                    # raise RedundantMessageError(
                    #    'Redundant AUX received {}'.format(msg))
                    continue
                aux_values[r][v].add(sender)
                bv_signal.set()

            elif msg[0] == 'CONF':
                _, r, v = msg
                assert len(v) == 1 or len(v) == 2
                if sender in conf_values[r][v]:
                    # FIXME: Raise for now to simplify things & be consistent
                    # with how other TAGs are handled. Will replace the raise
                    # with a continue statement as part of
                    # https://github.com/initc3/HoneyBadgerBFT-Python/issues/10
                    # raise RedundantMessageError(
                    #    'Redundant CONF received {}'.format(msg))
                    continue
                conf_values[r][v].add(sender)
                bv_signal.set()

            elif msg[0] == 'FINISH':
                _, _, v = msg
                assert type(v) == int
                finish_cnt = finish_cnt + 1
                finish_value.add(v)
                assert len(finish_value) == 1
                if finish_sent is False and finish_cnt >= f + 1:
                    decide(v)
                    send(-2, ('FINISH', '', list(finish_value)[0]))
                    finish_cnt = finish_cnt + 1
                    finish_sent = True
                if finish_cnt >= 2*f + 1:
                    finish_signal.set()

    # Translate mmr14 broadcast into coin.broadcast
    # _coin_broadcast = lambda (r, sig): broadcast(('COIN', r, sig))
    # _coin_recv = Queue()
    # coin = shared_coin(sid+'COIN', pid, N, f, _coin_broadcast, _coin_recv.get)

    finish_signal.clear()

    # Run the receive loop in the background
    _thread_recv = gevent.spawn(recv)

    # Block waiting for the input
    # print(pid, sid, 'PRE-ENTERING CRITICAL')

    vi = input()
    #if logger != None:
    #    logger.info("TCVBA %s gets input" % sid)

    # print(pid, sid, 'PRE-EXITING CRITICAL', vi)
    assert type(vi) is int

    cheap_coins = int.from_bytes(hash(sid), byteorder='big')
    r = 0

    def main_loop():
        nonlocal r, finish_sent, finish_cnt
        est = vi
        while True:  # Unbounded number of rounds
            # print("debug", pid, sid, 'deciding', already_decided, "at epoch", r)

            #gevent.sleep(0)
            #if logger != None:
            #    logger.info("TCVBA %s enters round %d" % (sid, r))

            if not est_sent[r][est]:
                est_sent[r][est] = True
                send(-2, ('EST', r, est))
                est_values[r][est].add(pid)
            # print("debug", pid, sid, 'WAITS BIN VAL at epoch', r)

            while len(int_values[r]) == 0:
                # Block until a value is output
                #gevent.sleep(0)
                bv_signal.clear()
                bv_signal.wait()

            #if logger != None:
            #    logger.info("TCVBA %s gets BIN VAL at epoch %d" % (sid, r))
            # print("debug", pid, sid, 'GETS BIN VAL at epoch', r)

            w = next(iter(int_values[r]))  # take an element

            send(-2, ('AUX', r, w))
            aux_values[r][w].add(pid)
            bv_signal.set()

            while True:
                #gevent.sleep(0)
                len_int_values = len(int_values[r])
                assert len_int_values == 1 or len_int_values == 2
                if len_int_values == 1:
                    if len(aux_values[r][tuple(int_values[r])[0]]) >= N - f:
                        values = set(int_values[r])
                        break
                else:
                    if sum(len(aux_values[r][v]) for v in int_values[r]) >= N - f:
                        values = set(int_values[r])
                        break
                bv_signal.clear()
                bv_signal.wait()

            # CONF phase

            if not conf_sent[r][tuple(values)]:
                send(-2, ('CONF', r, tuple(int_values[r])))
                conf_sent[r][tuple(values)] = True
                conf_values[r][tuple(int_values[r])].add(pid)
                bv_signal.set()

            while True:
                #gevent.sleep(0)
                # len_int_values = len(int_values[r])
                # assert len_int_values == 1 or len_int_values == 2
                if len(conf_values[r][tuple(int_values[r])]) >= N - f:
                    values = set(int_values[r])
                    break
                bv_signal.clear()
                bv_signal.wait()

            # Block until receiving the common coin value

            # print("debug", pid, sid, 'fetchs a coin at epoch', r)
            if r < 10:
                s = (cheap_coins >> r) & 1
            else:
                s = coin(r)
            # print("debug", pid, sid, 'gets a coin', s, 'at epoch', r)

            try:
                assert s in (0, 1)
            except AssertionError:
                s = s % 2

            # Set estimate
            if len(values) == 1:
                v = next(iter(values))
                assert type(v) is int
                if (v % 2) == s:
                    if finish_sent is False:
                        decide(v)
                        send(-2, ('FINISH', '', v))
                        finish_cnt = finish_cnt + 1
                        finish_sent = True
                        if finish_cnt >= 2 * f + 1:
                            finish_signal.set()
                est = v
            else:
                vals = tuple(values)
                assert len(values) == 2
                assert type(vals[0]) is int
                assert type(vals[1]) is int
                assert abs(vals[0] - vals[1]) == 1
                if vals[0] % 2 == s:
                    est = vals[0]
                else:
                    est = vals[1]
            # print('debug then decided:', already_decided, '%s' % sid)

            r += 1

    _thread_main_loop = gevent.spawn(main_loop)

    finish_signal.wait()

    #if logger != None:
    #    logger.info("TCVBA %s completes at round %d" % (sid, r))

    _thread_recv.kill()
    _thread_main_loop.kill()

import random

import gevent
from gevent import Greenlet
from gevent.queue import Queue

from dumbobft.core.validatedcommonsubset import validatedcommonsubset
from crypto.threshsig import dealer

from gevent.monkey import patch_all
patch_all()

# CBC
def simple_router(N, maxdelay=0.01, seed=None):
    """Builds a set of connected channels, with random delay
    @return (receives, sends)
    """
    rnd = random.Random(seed)
    #if seed is not None: print 'ROUTER SEED: %f' % (seed,)

    queues = [Queue() for _ in range(N)]

    def makeSend(i):
        def _send(j, o):
            delay = rnd.random() * maxdelay
            #print 'SEND %8s [%2d -> %2d] %.2f' % (o[0], i, j, delay)
            gevent.spawn_later(delay, queues[j].put, (i,o))
            #queues[j].put((i, o))
        return _send

    def makeRecv(j):
        def _recv():
            (i,o) = queues[j].get()
            #print 'RECV %8s [%2d -> %2d]' % (o[0], i, j)
            return (i,o)
        return _recv

    return ([makeSend(i) for i in range(N)],
            [makeRecv(j) for j in range(N)])


def _test_vacs(N=4, f=1, leader=None, seed=None):
    # Test everything when runs are OK
    sid = 'ISCAS'
    # Note thld siganture for CBC has a threshold different from common coin's
    PK, SKs = dealer(N, f + 1)
    PK1, SK1s = dealer(N, N - f)

    rnd = random.Random(seed)
    router_seed = rnd.random()
    #if leader is None: leader = rnd.randint(0, N-1)
    #print("The leader is: ", leader)
    sends, recvs = simple_router(N, seed=seed)

    threads = []
    inputs = [Queue(1) for _ in range(N)]
    outputs = [Queue(1) for _ in range(N)]

    for i in range(N):
        t = Greenlet(validatedcommonsubset, sid, i, N, f, PK, SKs[i], PK1, SK1s[i],
                     inputs[i].get, outputs[i].put_nowait, recvs[i], sends[i])
        t.start()
        threads.append(t)
        #print("VABA at node %d has been instantiated" % i)

    for i in range(N):
        inputs[i].put_nowait("Hello! This is a test message from node %d" % i)
        #print("Input to node %d has been provided" % i)

    try:
        outs = [outputs[i].get() for i in range(N)]
        try:
            gevent.joinall(threads)
            print(outs)
        except gevent.hub.LoopExit:
            pass
    except KeyboardInterrupt:
        gevent.killall(threads)
        raise

    # Assert the CBC-delivered values are same to the input
    # assert [t.value[0] for t in threads] == [m]*N
    # Assert the CBC-delivered authentications (i.e., signature) are valid
    # digest = PK.hash_message(str((sid, leader, m)))
    # assert [PK.verify_signature(t.value[1], digest) for t in threads] == [True]*N


def test_vacs(N, f, seed):
    _test_vacs(N=N, f=f, seed=seed)


if __name__ == '__main__':
    test_vacs(4, 1, None)

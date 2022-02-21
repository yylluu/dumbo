import logging
import gevent
import random
import time
import queue
from gevent import monkey
from gevent.queue import Queue
from honeybadgerbft.core.commoncoin import shared_coin
from honeybadgerbft.core.binaryagreement import binaryagreement
from crypto.threshsig import dealer


monkey.patch_all(thread=False)

logger = logging.getLogger(__name__)


def simple_aba_router(N, maxdelay=0.001, seed=None):
    """Builds a set of connected channels, with random delay
    @return (receives, sends)
    """
    rnd = random.Random(seed)
    #if seed is not None: print 'ROUTER SEED: %f' % (seed,)
    
    queues = [Queue() for _ in range(N)]
    _threads = []

    def makeSend(i):
        def _send(j, o):
            delay = rnd.random() * maxdelay
            gevent.spawn_later(delay, queues[j].put, (i, o))
        return _send

    def makeRecv(j):
        def _recv():
            (i, o) = queues[j].get()
            #print 'RECV %8s [%2d -> %2d]' % (o[0], i, j)
            return (i,o)
        return _recv
        
    return ([makeSend(i) for i in range(N)],
            [makeRecv(j) for j in range(N)])


def simple_coin_router(N, maxdelay=0.001, seed=None):
    """Builds a set of connected channels, with random delay
     @return (receives, sends)
     """
    rnd = random.Random(seed)
    # if seed is not None: print 'ROUTER SEED: %f' % (seed,)

    queues = [Queue() for _ in range(N)]
    _threads = []

    def makeBcast(i):
        def _send(j, o):
            delay = rnd.random() * maxdelay
            gevent.spawn_later(delay, queues[j].put, (i, o))
        def _bc(o):
            for j in range(N):
                _send(j, o)
        return _bc

    def makeRecv(j):
        def _recv():
            (i, o) = queues[j].get()
            # print 'RECV %8s [%2d -> %2d]' % (o[0], i, j)
            return (i, o)

        return _recv

    return ([makeBcast(i) for i in range(N)],
            [makeRecv(j) for j in range(N)])


### Test binary agreement with boldyreva coin
def _make_coins(sid, N, f):
    # Generate keys
    PK, SKs = dealer(N, f+1)
    coins = [None] * N
    # Router
    rnd = random.Random()
    router_seed = rnd.random()
    bcasts, recvs = simple_coin_router(N, seed=router_seed)
    for i in range(N):
        coins[i] = shared_coin(sid, i, N, f, PK, SKs[i], bcasts[i], recvs[i])
    return coins


def _test_binaryagreement(N=4, f=1, seed=None):
    # Generate keys
    sid = 'sidA'
    rnd = random.Random(seed)

    # Router
    router_seed = rnd.random()
    sends, recvs = simple_aba_router(N, seed=router_seed)

    # Instantiate the common coin
    coins = _make_coins(sid+'COIN', N, f)

    threads = []
    inputs = []
    outputs = []

    for i in range(N):
        inputs.append(Queue())
        outputs.append(Queue())
        
        t = gevent.spawn(binaryagreement, sid, i, N, f, coins[i],
                         inputs[i].get, outputs[i].put_nowait, recvs[i], sends[i])
        threads.append(t)

    for i in range(N):
        inputs[i].put_nowait(random.randint(0, 1))

    try:
        outs = [outputs[i].get() for i in range(N)]
        assert len(set(outs)) == 1
        try:
            gevent.joinall(threads)
            print(outs)
        except gevent.hub.LoopExit:
            pass
    except KeyboardInterrupt:
        gevent.killall(threads)
        raise


def test_binaryagreement():
    latencies = queue.PriorityQueue()
    for i in range(10):
        print('start the test %d ...' % i)
        time_start = time.time()
        _test_binaryagreement(N=4, f=1, seed=i)
        time_end = time.time()
        print('time cost %d: ' % i, time_end - time_start, 's')
        latencies.put(time_end - time_start)


if __name__ == '__main__':
    test_binaryagreement()

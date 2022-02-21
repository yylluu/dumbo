import random
import time
import gevent
from gevent import monkey
from gevent.queue import Queue
from honeybadgerbft.core.honeybadger import HoneyBadgerBFT
from crypto.threshsig import dealer
from honeybadgerbft.crypto.threshenc import tpke


monkey.patch_all(thread=False)

def simple_router(N, maxdelay=0.01, seed=None):
    """Builds a set of connected channels, with random delay

    :return: (receives, sends)
    """
    rnd = random.Random(seed)

    queues = [Queue() for _ in range(N)]
    _threads = []

    def makeSend(i):
        def _send(j, o):
            delay = rnd.random() * maxdelay
            if not i % 3:
                delay *= 0
            gevent.spawn_later(delay, queues[j].put_nowait, (i,o))
        return _send

    def makeRecv(j):
        def _recv():
            (i,o) = queues[j].get()
            print(j, (i,o))
            #print 'RECV %8s [%2d -> %2d]' % (o[0], i, j)
            return (i,o)
        return _recv

    return ([makeSend(i) for i in range(N)],
            [makeRecv(j) for j in range(N)])


### Test asynchronous common subset
def _test_honeybadger(N=5, f=1, seed=None):
    sid = 'sidA'
    # Generate threshold sig keys
    sPK, sSKs = dealer(N, f+1, seed=seed)

    # Generate threshold enc keys
    ePK, eSKs = tpke.dealer(N, f+1)

    rnd = random.Random(seed)
    #print 'SEED:', seed
    router_seed = rnd.random()
    sends, recvs = simple_router(N, seed=router_seed)

    badgers = [None] * N
    threads = [None] * N
    
    # This is an experiment parameter to specify the maximum round number 
    K = 5
    B = 1



    for i in range(N):
        badgers[i] = HoneyBadgerBFT(sid, i, B, N, f,
                                    sPK, sSKs[i], ePK, eSKs[i],
                                    sends[i], recvs[i], K)
        #print(sPK, sSKs[i], ePK, eSKs[i])


    for r in range(K * B):
        for i in range(N):
            #if i == 1: continue
            badgers[i].submit_tx('<[HBBFT Input %d]>' % (i+10*r))

    for i in range(N):
        threads[i] = gevent.spawn(badgers[i].run)



    print('start the test...')
    time_start = time.time()

    #gevent.killall(threads[N-f:])
    #gevent.sleep(3)
    #for i in range(N-f, N):
    #    inputs[i].put(0)
    try:
        outs = [threads[i].get() for i in range(N)]
        print(outs)
        # Consistency check
        assert len(set(outs)) == 1
    except KeyboardInterrupt:
        gevent.killall(threads)
        raise

    time_end = time.time()
    print('complete the test...')
    print('time cost: ', time_end-time_start, 's')


def test_honeybadger():
    _test_honeybadger()


if __name__ == '__main__':
    test_honeybadger()

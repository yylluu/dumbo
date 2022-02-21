import random

import gevent
from gevent import Greenlet
from gevent.queue import Queue

from crypto.threshsig import dealer
from dumbobft.core.provablereliablebroadcast import provablereliablebroadcast, encode, merkleTree


### RBC
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




def _test_rbc1(N=4, f=1, leader=None, seed=None):
    # Test everything when runs are OK
    #if seed is not None: print 'SEED:', seed
    sid = 'sidA'
    # Note thld siganture for CBC has a threshold different from common coin's
    PK, SKs = dealer(N, N - f)

    rnd = random.Random(seed)
    router_seed = rnd.random()
    if leader is None: leader = rnd.randint(0,N-1)
    sends, recvs = simple_router(N, seed=seed)
    threads = []
    leader_input = Queue(1)
    for i in range(N):
        input = leader_input.get if i == leader else None
        t = Greenlet(provablereliablebroadcast, sid, i, N, f, PK, SKs[i], leader, input, recvs[i], sends[i])
        t.start()
        threads.append(t)

    m = b"Hello! This is a test message."
    leader_input.put(m)
    gevent.joinall(threads)
    for t in threads:
        print(t.value)
    assert [t.value[0] for t in threads] == [m]*N

    for i in range(N):
        t = threads[i]
        m = threads[i].value[0]
        Sigma = threads[i].value[1]
        stripes = encode(N - 2 * f, N, m)
        mt = merkleTree(stripes)  # full binary tree
        roothash = mt[1]
        digest = PK.hash_message(str((sid, leader, roothash)))
        assert PK.verify_signature(Sigma, digest)


def test_rbc1(N, f, seed):
    _test_rbc1(N=N, f=f, seed=seed)


if __name__ == '__main__':
    test_rbc1(4, 1, None)

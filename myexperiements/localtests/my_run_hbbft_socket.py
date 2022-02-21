import random
from gevent import monkey

from multiprocessing import Process
from myexperiements.sockettest.socket_server import HoneyBadgerBFTNode
from myexperiements.sockettest.make_key_files import *


monkey.patch_all(thread=False)


def _test_honeybadger_2(N=4, f=1, seed=None):

    def run_hbbft_instance(badger: HoneyBadgerBFTNode):
        badger.run_hbbft_instance()

    rnd = random.Random(seed)

    sid = 'sidA'
    trusted_key_gen(N, f)

    # Nodes list
    host = "127.0.0.1"
    port_base = 10007
    addresses = [(host, port_base + 200 * i) for i in range(N)]
    print(addresses)

    K = 10
    B = 1
    badgers = [None] * N
    processes = [None] * N

    for i in range(N):
        badgers[i] = HoneyBadgerBFTNode(sid, i, B, N, f, addresses_list=addresses, K=K)

    for i in range(N):
        processes[i] = Process(target=run_hbbft_instance, args=(badgers[i], ))
        processes[i].start()
        processes[i].join()

    #while True:
    #    pass


# Test by processes
def test_honeybadger_proc():
    _test_honeybadger_2(7, 2)


if __name__ == '__main__':
    test_honeybadger_proc()

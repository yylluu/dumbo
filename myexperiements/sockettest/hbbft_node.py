import random
import gevent
import os
import pickle

from gevent import time
from honeybadgerbft.core.honeybadger import HoneyBadgerBFT
from myexperiements.sockettest.make_random_tx import tx_generator
from multiprocessing import Value as mpValue, Queue as mpQueue, Process


def load_key(id):

    with open(os.getcwd() + '/keys/' + 'sPK.key', 'rb') as fp:
        sPK = pickle.load(fp)

    with open(os.getcwd() + '/keys/' + 'ePK.key', 'rb') as fp:
        ePK = pickle.load(fp)

    with open(os.getcwd() + '/keys/' + 'sSK-' + str(id) + '.key', 'rb') as fp:
        sSK = pickle.load(fp)

    with open(os.getcwd() + '/keys/' + 'eSK-' + str(id) + '.key', 'rb') as fp:
        eSK = pickle.load(fp)

    return sPK, ePK, sSK, eSK


class HoneyBadgerBFTNode (HoneyBadgerBFT):

    def __init__(self, sid, id, B, N, f, my_address: str, addresses_list: list, K=3, mode='debug', mute=False, tx_buffer=None):
        self.sPK, self.ePK, self.sSK, self.eSK = load_key(id)
        HoneyBadgerBFT.__init__(self, sid, id, B, N, f, self.sPK, self.sSK, self.ePK, self.eSK, send=None, recv=None, K=K, logger=set_logger_of_node(id), mute=mute)
        self.server = Node(id=id, ip=my_address, port=addresses_list[id][1], addresses_list=addresses_list, logger=self.logger)
        self.mode = mode
        self._prepare_bootstrap()

    def _prepare_bootstrap(self):
        if self.mode == 'test' or 'debug':
            for r in range(self.K * self.B):
                tx = tx_generator(250) # Set each dummy TX to be 250 Byte
                HoneyBadgerBFT.submit_tx(self, tx)
        else:
            pass
            # TODO: submit transactions through tx_buffer

    def start_socket_server(self):
        pid = os.getpid()
        #print('pid: ', pid)
        self.logger.info('node id %d is running on pid %d' % (self.id, pid))
        self.server.start()

    def connect_socket_servers(self):
        self.server.connect_and_send_forever()
        self._send = self.server.send
        self._recv = self.server.recv

    def run_hbbft_instance(self):
        self.start_socket_server()
        time.sleep(3)
        gevent.sleep(3)
        self.connect_socket_servers()
        time.sleep(3)
        gevent.sleep(3)
        self.run()
        time.sleep(3)
        gevent.sleep(3)
        self.server.stop_service()


def main(sid, i, B, N, f, addresses, K):
    badger = HoneyBadgerBFTNode(sid, i, B, N, f, addresses, K)
    badger.run_hbbft_instance()


if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--sid', metavar='sid', required=True,
                        help='identifier of node', type=str)
    parser.add_argument('--id', metavar='id', required=True,
                        help='identifier of node', type=int)
    parser.add_argument('--N', metavar='N', required=True,
                        help='number of parties', type=int)
    parser.add_argument('--f', metavar='f', required=True,
                        help='number of faulties', type=int)
    parser.add_argument('--B', metavar='B', required=True,
                        help='size of batch', type=int)
    parser.add_argument('--K', metavar='K', required=True,
                        help='rounds to execute', type=int)
    args = parser.parse_args()

    # Some parameters
    sid = args.sid
    i = args.id
    N = args.N
    f = args.f
    B = args.B
    K = args.K

    # Random generator
    rnd = random.Random(sid)

    # Nodes list
    host = "127.0.0.1"
    port_base = int(rnd.random() * 5 + 1) * 10000
    addresses = [(host, port_base + 200 * i) for i in range(N)]
    print(addresses)

    main(sid, i, B, N, f, addresses, K)

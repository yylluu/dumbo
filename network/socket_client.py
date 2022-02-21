from gevent import monkey; monkey.patch_all(thread=False)

import time
import pickle
from typing import List, Callable
import gevent
import os
from multiprocessing import Value as mpValue, Process
from gevent import socket, lock
from gevent.queue import Queue
import logging
import traceback
from ctypes import c_bool

# Network node class: deal with socket communications
class NetworkClient (Process):

    SEP = '\r\nSEP\r\nSEP\r\nSEP\r\n'.encode('utf-8')

    def __init__(self, port: int, my_ip: str, id: int, addresses_list: list, client_from_bft: Callable, client_ready: mpValue, stop: mpValue, bft_running: mpValue = mpValue(c_bool, False), dynamic=True):

        self.bft_running = bft_running

        self.client_from_bft = client_from_bft
        self.ready = client_ready
        self.stop = stop

        self.ip = my_ip
        self.port = port
        self.id = id
        self.addresses_list = addresses_list
        self.N = len(self.addresses_list)

        self.is_out_sock_connected = [False] * self.N

        self.socks = [None for _ in self.addresses_list]
        self.sock_queues = [Queue() for _ in self.addresses_list]
        self.sock_locks = [lock.Semaphore() for _ in self.addresses_list]

        self.TIME = 1000
        self.BYTES = 625_000
        self.DELAY = 100

        self.network_condition = True
        self.DYNAMIC = dynamic

        super().__init__()


    def _connect_and_send_forever(self):
        pid = os.getpid()
        self.logger.info('node %d\'s socket client starts to make outgoing connections on process id %d' % (self.id, pid))
        while not self.stop.value:
            try:
                for j in range(self.N):
                    if not self.is_out_sock_connected[j]:
                        self.is_out_sock_connected[j] = self._connect(j)
                if all(self.is_out_sock_connected):
                    with self.ready.get_lock():
                        self.ready.value = True
                    break
            except Exception as e:
                self.logger.info(str((e, traceback.print_exc())))
        if self.DYNAMIC:
            gevent.spawn(self._change_network)
            send_threads = [gevent.spawn(self._dynamic_send, j) for j in range(self.N)]
        else:
            send_threads = [gevent.spawn(self._send, j) for j in range(self.N)]
        partten_thread = gevent.spawn(self._partten)
        self._handle_send_loop()
        #gevent.joinall(send_threads)


    def _connect(self, j: int):
        sock = socket.socket()
        if self.ip == '127.0.0.1':
            sock.bind((self.ip, self.port + j + 1))
        try:
            sock.connect(self.addresses_list[j])
            self.socks[j] = sock
            return True
        except Exception as e1:
            return False


    def _partten(self):
        while not self.stop.value:
            if self.network_condition:  # True="50ms-200Mbps"
                self.TIME = 100
                self.BYTES = 2_500_000
                self.DELAY = 50
            if not self.network_condition:  # False="300ms-50Mbps"
                self.TIME = 100
                self.BYTES = 625_000
                self.DELAY = 300
            gevent.sleep(1)
            #print(self.pattern.value)


    def _dynamic_send(self, j: int):
        #  100kbps - 12.5 kB : 1 sec
        #  500kbps - 62.5 kB : 1 sec
        #  1Mbps - 125 kB : 1 sec
        #  5Mbps - 625 kB : 1 sec
        #  50Mbps - 6250 kB : 1 sec

        cnt = self.BYTES  # 1000 bytes
        msg = None

        while not self.stop.value:

            if cnt == self.BYTES:
                start = time.time() * 1000

            if msg is None:
                o = self.sock_queues[j].get()
                msg = pickle.dumps(o) + self.SEP
                gevent.sleep(self.DELAY / 1000)

            if len(msg) <= cnt:
                cnt = cnt - len(msg)
                try:
                    self.socks[j].sendall(msg)
                    msg = None
                except:
                    self.logger.error("fail to send msg")
                    # self.logger.error(str((e1, traceback.print_exc())))
                    self.socks[j].close()
                    break
            else:
                msg1 = msg[0:cnt]
                msg = msg[cnt:]
                try:
                    self.socks[j].sendall(msg1)
                    cnt = 0
                except:
                    self.logger.error("fail to send msg")
                    # self.logger.error(str((e1, traceback.print_exc())))
                    self.socks[j].close()
                    break

            if cnt == 0:
                end = time.time() * 1000
                duration = end - start
                # print(duration)
                cnt = self.BYTES
                gevent.sleep(max((self.TIME - duration) / 1000, 0))


    def _send(self, j:int):
        while not self.stop.value:
            o = self.sock_queues[j].get()
            try:
                self.socks[j].sendall(pickle.dumps(o) + self.SEP)
            except:
                self.logger.error("fail to send msg")
                self.socks[j].close()
                break


    ##
    def _handle_send_loop(self):
        while not self.stop.value:
            try:
                j, o = self.client_from_bft()
                #o = self.send_queue[j].get_nowait()
                #print('send' + str((j, o)))
                #self.logger.info('send' + str((j, o)))
                try:
                    #self._send(j, pickle.dumps(o))
                    if j == -1: # -1 means broadcast
                        for i in range(self.N):
                            self.sock_queues[i].put_nowait(o)
                    elif j == -2: # -2 means broadcast except myself
                        for i in range(self.N):
                            if i != self.pid:
                                self.sock_queues[i].put_nowait(o)
                    else:
                        self.sock_queues[j].put_nowait(o)
                except Exception as e:
                    self.logger.error(str(("problem objective when sending", o)))
                    traceback.print_exc()
            except:
                pass

        #print("sending loop quits ...")

    def _change_network(self):
        seconds = 0
        self.network_condition = True
        while not self.bft_running.value:
            gevent.sleep(0.0001)
        while seconds < 59:
            seconds += 1
            gevent.sleep(1)
        self.network_condition = False
        self.logger.info("change to bad network....")
        while seconds < 61:
            seconds += 1
            gevent.sleep(1)
        self.network_condition = True
        self.logger.info("change to good network....")
        while not self.stop.value:
            seconds += 1
            if seconds % 120 == 0:
                if int(seconds / 120) % 2 == 1:
                    self.network_condition = False
                    self.logger.info("change to bad network....")
                else:
                    self.network_condition = True
                    self.logger.info("change to good network....")
            gevent.sleep(1)

    #Greenlet(_change_network).start()


    def run(self):
        self.logger = self._set_client_logger(self.id)
        pid = os.getpid()
        self.logger.info('node id %d is running on pid %d' % (self.id, pid))
        with self.ready.get_lock():
            self.ready.value = False
        self._connect_and_send_forever()

    def stop_service(self):
        with self.stop.get_lock():
            self.stop.value = True

    def _set_client_logger(self, id: int):
        logger = logging.getLogger("node-" + str(id))
        logger.setLevel(logging.DEBUG)
        # logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s %(filename)s [line:%(lineno)d] %(funcName)s %(levelname)s %(message)s ')
        if 'log' not in os.listdir(os.getcwd()):
            os.mkdir(os.getcwd() + '/log')
        full_path = os.path.realpath(os.getcwd()) + '/log/' + "node-net-client-" + str(id) + ".log"
        file_handler = logging.FileHandler(full_path)
        file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
        logger.addHandler(file_handler)
        return logger

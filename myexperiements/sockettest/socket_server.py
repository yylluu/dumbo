import time
import pickle
from typing import List

import gevent
import os

from multiprocessing import Value as mpValue, Queue as mpQueue, Process, Semaphore as mpSemaphore
from gevent import socket, monkey, lock

import logging
import traceback

monkey.patch_all(thread=False)



# Network node class: deal with socket communications
class NetworkServer (Process):

    SEP = '\r\nSEP\r\nSEP\r\nSEP\r\n'

    def __init__(self, port: int, my_ip: str, id: int, addresses_list: list, recv_q: mpQueue, send_q: List[mpQueue], ready: mpValue, stop: mpValue):

        self.recv_queue = recv_q
        self.send_queues = send_q
        self.ready = ready
        self.stop = stop

        self.ip = my_ip
        self.port = port
        self.id = id
        self.addresses_list = addresses_list
        self.N = len(self.addresses_list)

        self.logger = self._set_network_logger(self.id)

        self.is_out_sock_connected = [False] * self.N
        self.is_in_sock_connected = [False] * self.N

        self.socks = [None for _ in self.addresses_list]
        self.sock_locks = [lock.Semaphore() for _ in self.addresses_list]

        super().__init__()

    def _handle_ingoing_msg(self, sock, address):

        jid = self._address_to_id(address)
        buf = b''
        try:
            while not self.stop.value:
                leep(0)
                time.sleep(0)
                buf += sock.recv(5000)
                tmp = buf.split(self.SEP.encode('utf-8'), 1)
                while len(tmp) == 2:
                    buf = tmp[1]
                    data = tmp[0]
                    if data != '' and data:
                        if data == 'ping'.encode('utf-8'):
                            sock.sendall('pong'.encode('utf-8'))
                            self.logger.info("node {} is pinging node {}...".format(jid, self.id))
                            self.is_in_sock_connected[jid] = True
                        else:
                            (j, o) = (jid, pickle.loads(data))
                            assert j in range(self.N)
                            self.recv_queue.put_nowait((j, o))
                            #self.logger.info('recv' + str((j, o)))
                            #print('recv' + str((j, o)))
                    else:
                        self.logger.error('syntax error messages')
                        raise ValueError
                    tmp = buf.split(self.SEP.encode('utf-8'), 1)
                gevent.sleep(0)
                time.sleep(0)
        except Exception as e:
            self.logger.error(str((e, traceback.print_exc())))

    def _listen_and_recv_forever(self):
        pid = os.getpid()
        self.logger.info('node %d\'s socket server starts to listen ingoing connections on process id %d' % (self.id, pid))
        print("my IP is " + self.ip)
        self.server_sock = socket.socket()
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.ip, self.port))
        self.server_sock.listen(5)
        handle_msg_threads = []
        while not self.stop.value:
            gevent.sleep(0)
            time.sleep(0)
            sock, address = self.server_sock.accept()
            msg_t = gevent.spawn(self._handle_ingoing_msg, sock, address)
            handle_msg_threads.append(msg_t)
            self.logger.info('node id %d accepts a new socket from node %d' % (self.id, self._address_to_id(address)))
            gevent.sleep(0)
            time.sleep(0)
        #gevent.joinall(handle_msg_threads)
        #gevent.sleep(5)
        #time.sleep(5)

    def _connect_and_send_forever(self):
        pid = os.getpid()
        self.logger.info('node %d\'s socket client starts to make outgoing connections on process id %d' % (self.id, pid))
        while not self.stop.value:
            gevent.sleep(0)
            time.sleep(0)
            try:
                for j in range(self.N):
                    if not self.is_out_sock_connected[j]:
                        self.is_out_sock_connected[j] = self._connect(j)
                if all(self.is_out_sock_connected) and all(self.is_in_sock_connected):
                    with self.ready.get_lock():
                        self.ready.value = True
                    break
            except Exception as e:
                self.logger.info(str((e, traceback.print_exc())))
            gevent.sleep(0)
        send_threads = [gevent.spawn(self._send_loop, j) for j in range(self.N)]
        gevent.joinall(send_threads)


    def _connect(self, j: int):
        sock = socket.socket()
        if self.ip == '127.0.0.1':
            sock.bind((self.ip, self.port + j + 1))
        try:
            sock.connect(self.addresses_list[j])
            sock.sendall(('ping' + self.SEP).encode('utf-8'))
            pong = sock.recv(5000)
        except Exception as e1:
            return False
        if pong.decode('utf-8') == 'pong':
            self.logger.info("node {} is ponging node {}...".format(j, self.id))
            self.socks[j] = sock
        else:
            self.logger.info("fails to build connect from {} to {}".format(self.id, j))
            return False
        return True

    def _send(self, j: int, o: bytes):
        msg = b''.join([o, self.SEP.encode('utf-8')])
        self.sock_locks[j].acquire()
        for _ in range(3):
            try:
                self.socks[j].sendall(msg)
                break
            except Exception as e1:
                self.logger.error("fail to send msg")
                self.logger.error(str((e1, traceback.print_exc())))
                continue
        self.sock_locks[j].release()

    ##
    ##
    def _send_loop(self, j: int):

        while not self.stop.value:

            gevent.sleep(0)
            time.sleep(0)

            try:
                gevent.sleep(0)
                time.sleep(0)
                #j, o = self.send_queue.get_nowait()
                o = self.send_queues[j].get_nowait()
                #print('send1' + str((j, o)))
                try:
                    self._send(j, pickle.dumps(o))
                except Exception as e:
                    self.logger.error(str(("problem objective when sending", o)))
                    traceback.print_exc()
            except:
                pass

            gevent.sleep(0)
            time.sleep(0)
        #print("sending loop quits ...")

    def run(self):
        pid = os.getpid()

        self.logger.info('node id %d is running on pid %d' % (self.id, pid))
        with self.ready.get_lock():
            self.ready.value = False

        send_thread = gevent.spawn(self._listen_and_recv_forever)
        recv_thread = gevent.spawn(self._connect_and_send_forever)
        gevent.joinall([send_thread, recv_thread])

    def stop_service(self):
        with self.stop.get_lock():
            self.stop.value = True

    def _address_to_id(self, address: tuple):
        for i in range(self.N):
            if address[0] != '127.0.0.1' and address[0] == self.addresses_list[i][0]:
                return i
        return int((address[1] - 10000) / 200)
    def _set_network_logger(self, id: int):
        logger = logging.getLogger("node-" + str(id))
        logger.setLevel(logging.DEBUG)
        # logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s %(filename)s [line:%(lineno)d] %(funcName)s %(levelname)s %(message)s ')
        if 'log' not in os.listdir(os.getcwd()):
            os.mkdir(os.getcwd() + '/log')
        full_path = os.path.realpath(os.getcwd()) + '/log/' + "node-" + str(id) + ".log"
        file_handler = logging.FileHandler(full_path)
        file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
        logger.addHandler(file_handler)
        return logger

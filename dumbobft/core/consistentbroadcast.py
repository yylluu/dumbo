from queue import Queue

from gevent import monkey; monkey.patch_all(thread=False)

from datetime import datetime
from collections import defaultdict
from crypto.ecdsa.ecdsa import ecdsa_vrfy, ecdsa_sign
import hashlib, pickle


def hash(x):
    return hashlib.sha256(pickle.dumps(x)).digest()

def consistentbroadcast(sid, pid, N, f, PK2s, SK2, leader, input, receive, send, logger=None):
    """Consistent broadcast
    :param str sid: session identifier
    :param int pid: ``0 <= pid < N``
    :param int N:  at least 3
    :param int f: fault tolerance, ``N >= 3f + 1``
    :param list PK2s: an array of ``coincurve.PublicKey'', i.e., N public keys of ECDSA for all parties
    :param PublicKey SK2: ``coincurve.PrivateKey'', i.e., secret key of ECDSA
    :param int leader: ``0 <= leader < N``
    :param input: if ``pid == leader``, then :func:`input()` is called
        to wait for the input value
    :param receive: :func:`receive()` blocks until a message is
        received; message is of the form::

            (i, (tag, ...)) = receive()

        where ``tag`` is one of ``{"VAL", "ECHO", "READY"}``
    :param send: sends (without blocking) a message to a designed
        recipient ``send(i, (tag, ...))``

    :return str: ``m`` after receiving ``CBC-FINAL`` message
        from the leader

        .. important:: **Messages**

            ``CBC_VAL( m )``
                sent from ``leader`` to each other party
            ``CBC_ECHO( m, sigma )``
                sent to leader after receiving ``CBC-VAL`` message
            ``CBC_FINAL( m, Sigma )``
                sent from ``leader`` after receiving :math:``N-f`` ``CBC_ECHO`` messages
                where Sigma is computed over {sigma_i} in these ``CBC_ECHO`` messages
    """

    #assert N >= 3*f + 1
    #assert f >= 0
    #assert 0 <= leader < N
    #assert 0 <= pid < N


    EchoThreshold = N - f      # Wait for this many CBC_ECHO to send CBC_FINAL
    m = None
    digestFromLeader = None
    finalSent = False
    cbc_echo_sshares = defaultdict(lambda: None)

    #print("CBC starts...")

    if pid == leader:
        # The leader sends the input to each participant
        #print("block to wait for CBC input")

        m = input() # block until an input is received

        #print("CBC input received: ", m)
        assert isinstance(m, (str, bytes, list, tuple))
        digestFromLeader = hash((sid, m))
        # print("leader", pid, "has digest:", digestFromLeader)
        cbc_echo_sshares[pid] = ecdsa_sign(SK2, digestFromLeader)
        send(-1, ('CBC_SEND', m))
        #print("Leader %d broadcasts CBC SEND messages" % leader)


    # Handle all consensus messages
    while True:
        #gevent.sleep(0)

        (j, msg) = receive()
        #print("recv3", (j, msg))

        if msg[0] == 'CBC_SEND' and digestFromLeader is None:
            # CBC_SEND message
            (_, m) = msg
            if j != leader:
                print("Node %d receives a CBC_SEND message from node %d other than leader %d" % (pid, j, leader), msg)
                continue
            digestFromLeader = hash((sid, m))
            #print("Node", pid, "has digest:", digestFromLeader, "for leader", leader, "session id", sid, "message", m)
            send(leader, ('CBC_ECHO', ecdsa_sign(SK2, digestFromLeader)))

        elif msg[0] == 'CBC_ECHO':
            # CBC_READY message
            #print("I receive CBC_ECHO from node %d" % j)
            if pid != leader:
                print("I reject CBC_ECHO from %d as I am not CBC leader:", j)
                continue
            (_, sig) = msg
            try:
                assert ecdsa_vrfy(PK2s[j], digestFromLeader, sig)
            except AssertionError:
                print("Signature share failed in CBC!", (sid, pid, j, msg))
                continue
            #print("I accept CBC_ECHO from node %d" % j)
            cbc_echo_sshares[j] = sig
            if len(cbc_echo_sshares) >= EchoThreshold and not finalSent:
                sigmas = tuple(list(cbc_echo_sshares.items())[:N - f])
                # assert PK.verify_signature(Sigma, digestFromLeader)
                finalSent = True
                send(-1, ('CBC_FINAL', m, sigmas))
                #print("Leader %d broadcasts CBC FINAL messages" % leader)

        elif msg[0] == 'CBC_FINAL':
            # CBC_FINAL message
            #print("I receive CBC_FINAL from node %d" % j)
            if j != leader:
                print("Node %d receives a CBC_FINAL message from node %d other than leader %d" % (pid, j, leader), msg)
                continue
            (_, m, sigmas) = msg
            try:
                assert len(sigmas) == N - f and len(set(sigmas)) == N - f
                digest = hash((sid, m))
                for (i, sig_i) in sigmas:
                    assert ecdsa_vrfy(PK2s[i], digest, sig_i)
            except AssertionError:
                print("Signature failed!", (sid, pid, j, msg))
                continue
            #print("CBC finished for leader", leader)

            return m, sigmas
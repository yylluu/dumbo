from gevent import monkey; monkey.patch_all(thread=False)

from datetime import datetime
import time
from collections import defaultdict
from crypto.ecdsa.ecdsa import ecdsa_vrfy, ecdsa_sign
from honeybadgerbft.core.reliablebroadcast import merkleTree, getMerkleBranch, merkleVerify
from honeybadgerbft.core.reliablebroadcast import encode, decode
import hashlib, pickle

def hash(x):
    return hashlib.sha256(pickle.dumps(x)).digest()

def provablereliablebroadcast(sid, pid, N, f,  PK2s, SK2, leader, input, receive, send, logger=None):
    """Reliable broadcastdef hash(x):
    return hashlib.sha256(pickle.dumps(x)).digest()

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

    :return str: ``m`` after receiving :math:`2f+1` ``READY`` messages
        and :math:`N-2f` ``ECHO`` messages

        .. important:: **Messages**

            ``VAL( roothash, branch[i], stripe[i] )``
                sent from ``leader`` to each other party
            ``ECHO( roothash, branch[i], stripe[i] )``
                sent after receiving ``VAL`` message
            ``READY( roothash, sigma )``
                sent after receiving :math:`N-f` ``ECHO`` messages
                or after receiving :math:`f+1` ``READY`` messages

    .. todo::
        **Accountability**

        A large computational expense occurs when attempting to
        decode the value from erasure codes, and recomputing to check it
        is formed correctly. By transmitting a signature along with
        ``VAL`` and ``ECHO``, we can ensure that if the value is decoded
        but not necessarily reconstructed, then evidence incriminates
        the leader.

    """

    #assert N >= 3*f + 1
    #assert f >= 0
    #assert 0 <= leader < N
    #assert 0 <= pid < N

    #print("RBC starts...")

    K               = N - 2 * f  # Need this many to reconstruct. (# noqa: E221)
    EchoThreshold   = N - f      # Wait for this many ECHO to send READY. (# noqa: E221)
    ReadyThreshold  = f + 1      # Wait for this many READY to amplify READY. (# noqa: E221)
    OutputThreshold = N - f      # Wait for this many READY to output
    # NOTE: The above thresholds  are chosen to minimize the size
    # of the erasure coding stripes, i.e. to maximize K.
    # The following alternative thresholds are more canonical
    # (e.g., in Bracha '86) and require larger stripes, but must wait
    # for fewer nodes to respond
    #   EchoThreshold = ceil((N + f + 1.)/2)
    #   K = EchoThreshold - f

    def broadcast(o):
        send(-1, o)

    start = time.time()

    if pid == leader:
        # The leader erasure encodes the input, sending one strip to each participant
        #print("block to wait for RBC input")
        m = input()  # block until an input is received
        #print("RBC input received: ", m)
        # assert isinstance(m, (str, bytes))
        # print('Input received: %d bytes' % (len(m),))
        stripes = encode(K, N, m)
        mt = merkleTree(stripes)  # full binary tree
        roothash = mt[1]
        for i in range(N):
            branch = getMerkleBranch(i, mt)
            send(i, ('VAL', roothash, branch, stripes[i]))
        #print("encoding time: " + str(end - start))


    # TODO: filter policy: if leader, discard all messages until sending VAL

    fromLeader = None
    stripes = defaultdict(lambda: [None for _ in range(N)])
    echoCounter = defaultdict(lambda: 0)
    echoSenders = set()  # Peers that have sent us ECHO messages
    ready = defaultdict(set)
    readySent = False
    readySenders = set()  # Peers that have sent us READY messages
    readySigShares = defaultdict(lambda: None)

    def decode_output(roothash):
        # Rebuild the merkle tree to guarantee decoding is correct
        m = decode(K, N, stripes[roothash])
        _stripes = encode(K, N, m)
        _mt = merkleTree(_stripes)
        _roothash = _mt[1]
        # TODO: Accountability: If this fails, incriminate leader
        assert _roothash == roothash
        return m

    while True:  # main receive loop
        #gevent.sleep(0)
        sender, msg = receive()
        if msg[0] == 'VAL' and fromLeader is None:
            # Validation
            (_, roothash, branch, stripe) = msg
            if sender != leader:
                print("VAL message from other than leader:", sender)
                continue
            try:
                assert merkleVerify(N, stripe, roothash, branch, pid)
            except Exception as e:
                print("Failed to validate VAL message:", e)
                continue
            # Update
            fromLeader = roothash
            broadcast(('ECHO', roothash, branch, stripe))

        elif msg[0] == 'ECHO':
            (_, roothash, branch, stripe) = msg
            # Validation
            if roothash in stripes and stripes[roothash][sender] is not None \
               or sender in echoSenders:
                print("Redundant ECHO")
                continue
            try:
                assert merkleVerify(N, stripe, roothash, branch, sender)
            except AssertionError as e:
                print("Failed to validate ECHO message:", e)
                continue

            # Update
            stripes[roothash][sender] = stripe
            echoSenders.add(sender)
            echoCounter[roothash] += 1

            if echoCounter[roothash] >= EchoThreshold and not readySent:
                readySent = True
                digest = hash((sid, roothash))
                sig = ecdsa_sign(SK2, digest)
                send(-1, ('READY', roothash, sig))

            #if len(ready[roothash]) >= OutputThreshold and echoCounter[roothash] >= K:
            #    return decode_output(roothash)

        elif msg[0] == 'READY':
            (_, roothash, sig) = msg
            # Validation
            if sender in ready[roothash] or sender in readySenders:
                print("Redundant READY")
                continue
            try:
                digest = hash((sid, roothash))
                assert ecdsa_vrfy(PK2s[sender], digest, sig)
            except AssertionError:
                print("Signature share failed in PRBC!", (sid, pid, sender, msg))
                continue

            # Update
            ready[roothash].add(sender)
            readySenders.add(sender)
            readySigShares[sender] = sig

            # Amplify ready messages
            if len(ready[roothash]) >= ReadyThreshold and not readySent:
                readySent = True
                digest = hash((sid, roothash))
                sig = ecdsa_sign(SK2, digest)
                send(-1, ('READY', roothash, sig))

            if len(ready[roothash]) >= OutputThreshold and echoCounter[roothash] >= K:
                sigmas = tuple(list(readySigShares.items())[:OutputThreshold])
                value = decode_output(roothash)
                proof = (sid, roothash, sigmas)
                #print("RBC finished for leader", leader)
                end = time.time()
                if logger != None:
                    logger.info("ABA %d completes in %f seconds" % (leader, end-start))
                return value, proof

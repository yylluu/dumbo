from gevent import monkey; monkey.patch_all(thread=False)

import logging
from crypto.threshsig.boldyreva import g12deserialize, g12serialize
from collections import defaultdict
from gevent import Greenlet
from gevent.queue import Queue
import hashlib


logger = logging.getLogger(__name__)


class CommonCoinFailureException(Exception):
    """Raised for common coin failures."""
    pass


def hash(x):
    return hashlib.sha256(x).digest()


def shared_coin(sid, pid, N, f, PK, SK, broadcast, receive, single_bit=True, logger=None):
    """A shared coin based on threshold signatures

    :param sid: a unique instance id
    :param pid: my id number
    :param N: number of parties
    :param f: fault tolerance, :math:`f+1` shares needed to get the coin
    :param PK: ``boldyreva.TBLSPublicKey``
    :param SK: ``boldyreva.TBLSPrivateKey``
    :param broadcast: broadcast channel
    :param receive: receive channel
    :param single_bit: is the output coin a single bit or not ?
    :return: a function ``getCoin()``, where ``getCoin(r)`` blocks
    """
    assert PK.k == f+1
    assert PK.l == N    # noqa: E741
    received = defaultdict(dict)
    outputQueue = defaultdict(lambda: Queue(1))

    def _recv():
        while True:     # main receive loop


            # New shares for some round r, from sender i
            (i, (_, r, raw_sig)) = receive()
            sig = g12deserialize(raw_sig)

            assert i in range(N)
            # assert r >= 0  ### Comment this line since round r can be a string
            if i in received[r]:
                print("redundant coin sig received", (sid, pid, i, r))
                continue

            h = PK.hash_message(str((sid, r)))

            # TODO: Accountability: Optimistically skip verifying
            # each share, knowing evidence available later
            try:
                if i != pid:
                    assert PK.verify_share(sig, i, h)
            except AssertionError:
                #print("Signature share failed!")
                #print("Signature share failed!", (sid, pid, i, r, sig, h))
                #print('debug', sig, h)
                #print('debug', type(sig), type(h))
                continue
                #pass

            received[r][i] = sig

            # After reaching the threshold, compute the output and
            # make it available locally

            if len(received[r]) == f + 1:

                # Verify and get the combined signature
                sigs = dict(list(received[r].items())[:f+1])
                sig = PK.combine_shares(sigs)
                assert PK.verify_signature(sig, h)

                # Compute the bit from the least bit of the hash
                coin = hash(g12serialize(sig))[0]
                if single_bit:
                    bit = coin % 2

                    outputQueue[r].put_nowait(bit)
                else:

                    outputQueue[r].put_nowait(coin)

    #greenletPacker(Greenlet(_recv), 'shared_coin', (pid, N, f, broadcast, receive)).start()
    Greenlet(_recv).start()

    def getCoin(round):
        """Gets a coin.

        :param round: the epoch/round.
        :returns: a coin.

        """
        # I have to do mapping to 1..l
        h = PK.hash_message(str((sid, round)))
        h.initPP()
        # print('debug', pid, SK.sign(h), h)
        # print('debug-SK', pid, SK.SK, SK.l, SK.k, SK.i)
        # print('debug-PK', pid, PK.VKs[pid], PK.l, PK.k, PK.VK)
        # print('debug', pid, type(SK.sign(h)), type(h), type(SK.SK), type(PK.VKs[pid]))
        # print('debug', pid, ismember(SK.sign(h)), ismember(h), ismember(SK.SK), ismember(PK.VKs[pid]))

        sig = SK.sign(h)
        sig.initPP()
        broadcast(('COIN', round, g12serialize(sig)))
        PK.verify_share(sig, pid, h)
        coin = outputQueue[round].get()
        #print('debug', 'node %d gets a coin %d for round %d in %s' % (pid, coin, round, sid))
        return coin

    return getCoin

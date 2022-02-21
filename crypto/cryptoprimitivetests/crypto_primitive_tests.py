import math
from typing import List

import zfec
import hashlib
import string
import random
import time
import pickle
from py_ecc.secp256k1 import privtopub, ecdsa_raw_sign, ecdsa_raw_recover

from pyeclib.ec_iface import ECDriver
from coincurve import PrivateKey as ECDSAPrivateKey, PublicKey as ECDSAPublicKey, verify_signature as ecdsa_vrify

from hashlib import sha256
import binascii

def encode(K, N, m):
    """Erasure encodes string ``m`` into ``N`` blocks, such that any ``K``
    can reconstruct.

    :param int K: K
    :param int N: number of blocks to encode string ``m`` into.
    :param bytes m: bytestring to encode.

    :return list: Erasure codes resulting from encoding ``m`` into
        ``N`` blocks using ``zfec`` lib.

    """
    try:
        m = m.encode()
    except AttributeError:
        pass
    encoder = zfec.Encoder(K, N)
    assert K <= 256  # TODO: Record this assumption!
    # pad m to a multiple of K bytes
    padlen = K - (len(m) % K)
    m += padlen * chr(K-padlen).encode()
    step = len(m)//K
    blocks = [m[i*step: (i+1)*step] for i in range(K)]
    stripes = encoder.encode(blocks)
    assert len(stripes[0]) == len(stripes[-1])
    print(str(len(stripes[0])))
    return stripes


def decode(K, N, stripes):
    """Decodes an erasure-encoded string from a subset of stripes

    :param list stripes: a container of :math:`N` elements,
        each of which is either a string or ``None``
        at least :math:`K` elements are strings
        all string elements are the same length

    """
    assert len(stripes) == N
    blocks = []
    blocknums = []
    for i, block in enumerate(stripes):
        if block is None:
            continue
        blocks.append(block)
        blocknums.append(i)
        if len(blocks) == K:
            break
    else:
        raise ValueError("Too few to recover")
    decoder = zfec.Decoder(K, N)
    rec = decoder.decode(blocks, blocknums)
    m = b''.join(rec)
    padlen = K - m[-1]
    m = m[:-padlen]
    return m


def encode1(K: int, N: int, m, coder: ECDriver):

    assert coder.k == K
    assert coder.m == N - K

    try:
        m = m.encode()
    except AttributeError:
        pass

    stripes = [_ for _ in coder.encode(m)]

    assert len(stripes[0]) == len(stripes[-1])
    print(str(len(stripes[0])))
    return stripes

def decode1(K: int, N: int, stripes: List[bytes], coder: ECDriver):

    assert len(stripes) == N
    assert len(stripes) == coder.k + coder.m

    blocks = []
    for block in stripes:
        if block is None:
            continue
        blocks.append(block)
        if len(blocks) == K:
            break
    else:
        raise ValueError("Too few to recover")
    rec = coder.decode(blocks)
    return rec


#####################
#    Merkle tree    #
#####################
def hash(x):
    assert isinstance(x, (str, bytes))
    try:
        x = x.encode()
    except AttributeError:
        pass
    return hashlib.sha256(x).digest()


def ceil(x): return int(math.ceil(x))


def merkleTree(strList):
    """Builds a merkle tree from a list of :math:`N` strings (:math:`N`
    at least 1)

    :return list: Merkle tree, a list of ``2*ceil(N)`` strings. The root
         digest is at ``tree[1]``, ``tree[0]`` is blank.
    """
    N = len(strList)
    assert N >= 1
    bottomrow = 2 ** ceil(math.log(N, 2))
    mt = [b''] * (2 * bottomrow)
    for i in range(N):
        mt[bottomrow + i] = hash(strList[i])
    for i in range(bottomrow - 1, 0, -1):
        mt[i] = hash(mt[i*2] + mt[i*2+1])
    return mt


def getMerkleBranch(index, mt):
    """Computes a merkle tree from a list of leaves.
    """
    res = []
    t = index + (len(mt) >> 1)
    while t > 1:
        res.append(mt[t ^ 1])  # we are picking up the sibling
        t //= 2
    return res


def merkleVerify(N, val, roothash, branch, index):
    """Verify a merkle tree branch proof
    """
    assert 0 <= index < N
    # XXX Python 3 related issue, for now let's tolerate both bytes and
    # strings
    assert isinstance(val, (str, bytes))
    assert len(branch) == ceil(math.log(N, 2))
    # Index has information on whether we are facing a left sibling or a right sibling
    tmp = hash(val)
    tindex = index
    for br in branch:
        tmp = hash((tindex & 1) and br + tmp or tmp + br)
        tindex >>= 1
    if tmp != roothash:
        print("Verification failed with", hash(val), roothash, branch, tmp == roothash)
        return False
    return True


def test_encoding(B):


    for f in range(1, 2):

        K = f + 1
        N = 3 * f + 1

        coder = ECDriver(k=K, m=N-K, ec_type='isa_l_rs_vand')

        m = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(256*B)).encode()

        print(K)

        start = time.time()
        stripes = encode(K, N, m)
        middle = time.time()
        print((middle - start) * 1000)

        m1 = decode(K, N, stripes)

        end = time.time()
        print((end - middle) * 1000)

        stripes = encode1(K, N, m, coder)
        m1 = decode1(K, N, stripes, coder)

        middle1 = time.time()
        print((middle1 - middle) * 1000)

        mt = merkleTree(stripes)
        getMerkleBranch(1, mt)
        end = time.time()
        print((end - middle) * 1000)
        print()


priv = binascii.unhexlify('792eca682b890b31356247f2b04662bff448b6bb19ea1c8ab48da222c894ef9b')
pub = (20033694065814990006010338153307081985267967222430278129327181081381512401190, 72089573118161052907088366229362685603474623289048716349537937839432544970413)

def fastecdsa_sign(priv, msg):
    digest = hash(msg)
    v, r, s = ecdsa_raw_sign(digest, priv)
    return (v, r, s)


def fastecdsa_vrfy(pub, msg, sig):
    digest = hash(msg)
    return ecdsa_raw_recover(digest, sig) == pub

def generate_ecdsa(seed=None):
    return ECDSAPrivateKey(seed)

def ecdsa_sign(SK, msg):
    return SK.sign(hash(msg))

def ecdsa_vrfy(PK, msg, sig):
    return ecdsa_vrify(sig, hash(msg), PK.format())


def test_ecdsa():
    t1 = time.time()
    SK = generate_ecdsa()
    for K in range(1, 1000):
        sig = ecdsa_sign(SK, "This is a test msg")
        ecdsa_vrfy(SK.public_key, "This is a test msg", sig)
    t2 = time.time()
    print((t2 - t1) * 1000)

    t1 = time.time()
    for K in range(1, 1000):
        sig = fastecdsa_sign(priv, "This is a test msg")
        fastecdsa_vrfy(pub, "This is a test msg", sig)
    t2 = time.time()
    print((t2 - t1) * 1000)


if __name__ == "__main__":
    #test_encoding(1000000)
    test_ecdsa()
    #hash(None)
    #test_ecdsa()


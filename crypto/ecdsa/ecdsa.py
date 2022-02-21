import hashlib
from coincurve import PrivateKey, PublicKey, verify_signature


def hash(x):
    assert isinstance(x, (str, bytes))
    try:
        x = x.encode()
    except AttributeError:
        pass
    return hashlib.sha256(x).digest()


def ecdsa_sign(SK, msg):
    return SK.sign(hash(msg))


def ecdsa_vrfy(PK, msg, sig):
    return verify_signature(sig, hash(msg), PK.format())


def pki(N):
    SKs = [PrivateKey() for _ in range(N)]
    PKs = [SK.public_key for SK in SKs]
    return PKs, SKs

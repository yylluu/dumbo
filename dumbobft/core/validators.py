import hashlib, pickle
from crypto.ecdsa.ecdsa import ecdsa_vrfy

def hash(x):
    return hashlib.sha256(pickle.dumps(x)).digest()

def prbc_validate(sid, N, f, PK2s, proof):
    try:
        _sid, roothash, sigmas = proof
        assert _sid == sid
        assert len(sigmas) == N - f and len(set(sigmas)) == N - f
        digest = hash((sid, roothash))
        for (i, sig_i) in sigmas:
            assert ecdsa_vrfy(PK2s[i], digest, sig_i)
        return True
    except:
        return False

def cbc_validate(sid, N, f, PK2s, value, proof):
    try:
        sigmas = proof
        assert len(sigmas) == N - f and len(set(sigmas)) == N - f
        digest = hash((sid, value))
        for (i, sig_i) in sigmas:
            assert ecdsa_vrfy(PK2s[i], digest, sig_i)
        return True
    except:
        return False
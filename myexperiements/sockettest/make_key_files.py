from crypto.threshsig import dealer
from honeybadgerbft.crypto.threshenc import tpke
import pickle
import os


def trusted_key_gen(N=4, f=1, seed=None):

    # Generate threshold sig keys
    sPK, sSKs = dealer(N, f+1, seed=seed)

    # Generate threshold enc keys
    ePK, eSKs = tpke.dealer(N, f+1)

    # Save all keys to files
    if 'keys' not in os.listdir(os.getcwd()):
        os.mkdir(os.getcwd() + '/keys')

    with open(os.getcwd() + '/keys/' + 'sPK.key', 'wb') as fp:
        pickle.dump(sPK, fp)

    with open(os.getcwd() + '/keys/' + 'ePK.key', 'wb') as fp:
        pickle.dump(ePK, fp)

    for i in range(N):
        with open(os.getcwd() + '/keys/' + 'sSK-' + str(i) + '.key', 'wb') as fp:
            pickle.dump(sSKs[i], fp)

    for i in range(N):
        with open(os.getcwd() + '/keys/' + 'eSK-' + str(i) + '.key', 'wb') as fp:
            pickle.dump(eSKs[i], fp)


if __name__ == '__main__':
    trusted_key_gen()


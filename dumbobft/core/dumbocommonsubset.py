from gevent import monkey; monkey.patch_all(thread=False)

from datetime import datetime
import gevent


def dumbocommonsubset(pid, N, f, prbc_out, vacs_in, vacs_out, logger=None):
    """The BKR93 algorithm for asynchronous common subset.

    :param pid: my identifier
    :param N: number of nodes
    :param f: fault tolerance
    :param rbc_out: an array of :math:`N` (blocking) output functions,
        returning a string
    :param aba_in: an array of :math:`N` (non-blocking) functions that
        accept an input bit
    :param aba_out: an array of :math:`N` (blocking) output functions,
        returning a bit
    :return: an :math:`N`-element array, each element either ``None`` or a
        string
    """

    #print("Starts to run dumbo ACS...")

    #assert len(prbc_out) == N
    #assert len(vacs_in) == 1
    #assert len(vacs_out) == 1

    prbc_values = [None] * N
    prbc_proofs = [None] * N
    is_prbc_delivered = [0] * N

    def wait_for_prbc_to_continue(leader):#
        # Receive output from reliable broadcast
        msg, (prbc_sid, roothash, Sigma) = prbc_out[leader]()
        prbc_values[leader] = msg
        prbc_proofs[leader] = (prbc_sid, roothash, Sigma)
        is_prbc_delivered[leader] = 1
        if leader == pid:
            vacs_in(prbc_proofs[leader])
            if logger != None:
                logger.info("DumboACS transfers prbc out to vacs in at %s" % datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    prbc_threads = [gevent.spawn(wait_for_prbc_to_continue, j) for j in range(N)]

    prbc_proofs_vector = vacs_out()

    if prbc_proofs_vector is not None:
        #assert type(prbc_proofs_vector) == list and len(prbc_proofs_vector) == N
        for j in range(N):
            if prbc_proofs_vector[j] is not None:
                prbc_threads[j].join()
                assert prbc_values[j] is not None
            else:
                prbc_threads[j].kill()
                prbc_values[j] = None

    return tuple(prbc_values)

from crypto.threshsig.boldyreva import dealer, serialize, deserialize0, deserialize2
import argparse
import pickle


def _generate_keys(players, k):
    if k:
        k = int(k)
    else:
        k = players // 2  # N - 2 * t
    PK, SKs = dealer(players=players, k=k)

    sk0 = serialize(SKs[0].SK)
    print(sk0)
    SK0 = deserialize0(sk0)
    print(serialize(SK0))

    vk = serialize(PK.VK)
    print(vk)
    VK = deserialize2(vk)
    print(serialize(VK))

    vk0 = serialize(PK.VKs[0])
    print(vk0)
    VK0 = deserialize2(vk0)
    print(serialize(VK0))

    return (PK.l, PK.k, serialize(PK.VK), [serialize(VKp) for VKp in PK.VKs],
            [(SK.i, serialize(SK.SK)) for SK in SKs])


def main():
    """ """
    parser = argparse.ArgumentParser()
    parser.add_argument('players', help='The number of players')
    parser.add_argument('k', help='k')
    args = parser.parse_args()
    keys = _generate_keys(int(args.players), args.k)
    print(pickle.dumps(keys))


if __name__ == '__main__':
    main()

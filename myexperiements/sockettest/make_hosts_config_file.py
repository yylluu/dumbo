import os

host = "127.0.0.1"
port = 10007

with open('hosts.config', 'a') as hosts:
    for i in range(161):
        hosts.write(str(i) + " " + host + " " + str(port) + os.linesep)
        port += 200

# print("hosts.config is not correctly read... ")
# host = "127.0.0.1"
# port_base = int(rnd.random() * 5 + 1) * 10000
# addresses = [(host, port_base + 200 * i) for i in range(N)]
# print(addresses)
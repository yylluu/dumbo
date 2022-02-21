Proof-of-Concept implementation for Bolt-Dumbo Transformer. 
The code is forked from the implementation of Honeybadger-BFT protocol.
The pessimistic fallback path is instantiated by Dumbo-2 (Guo et al. CCS'2020).
So this codebase also includes a PoC implementation for Dumbo-2 as a by-product.

1. To run the benchmarks at your machine (with Ubuntu 18.84 LTS), first install all dependencies as follows:
    ```
    sudo apt-get update
    sudo apt-get -y install make bison flex libgmp-dev libmpc-dev python3 python3-dev python3-pip libssl-dev
    
    wget https://crypto.stanford.edu/pbc/files/pbc-0.5.14.tar.gz
    tar -xvf pbc-0.5.14.tar.gz
    cd pbc-0.5.14
    sudo ./configure
    sudo make
    sudo make install
    cd ..
    
    sudo ldconfig /usr/local/lib
    
    cat <<EOF >/home/ubuntu/.profile
    export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/lib
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
    EOF
    
    source /home/ubuntu/.profile
    export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/lib
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
     
    git clone https://github.com/JHUISI/charm.git
    cd charm
    sudo ./configure.sh
    sudo make
    sudo make install
    sudo make test
    cd ..
    
    python3 -m pip install --upgrade pip
    sudo pip3 install gevent setuptools gevent numpy ecdsa pysocks gmpy2 zfec gipc pycrypto coincurve
    ```

2. A quick start to run Dumbo-2 for 20 epochs with a batch size of 1000 tx can be:
   ```
   ./run_local_network_test.sh 4 1 1000 20
   ```
   
   To run BDT variants instead of Dumbo-2, edit line-12 in run_local_network_test.sh to replace "dumbo" by "bdt" or "rbc-bdt" before executing the shell script.

3. If you would like to test the code among AWS cloud servers (with Ubuntu 18.84 LTS). You can follow the commands inside run_local_network_test.sh to remotely start the protocols at all servers. An example to conduct the WAN tests from your PC side terminal can be:
   ```
   # the number of remove AWS servers
   N = 4
   
   # public IPs --- This is the public IPs of AWS servers
    pubIPsVar=([0]='3.236.98.149'
    [1]='3.250.230.5'
    [2]='13.236.193.178'
    [3]='18.181.208.49')
    
   # private IPs --- This is the private IPs of AWS servers
    priIPsVar=([0]='172.31.71.134'
    [1]='172.31.7.198'
    [2]='172.31.6.250'
    [3]='172.31.2.176')
   
   # Clone code to all remote AWS servers from github
    i=0; while [ $i -le $(( N-1 )) ]; do
    ssh -i "/home/your-name/your-key-dir/your-sk.pem" -o StrictHostKeyChecking=no ubuntu@${pubIPsVar[i]} "git clone --branch master https://github.com/yylluu/dumbo.git" &
    i=$(( i+1 ))
    done
   
   # Update IP addresses to all remote AWS servers 
    rm tmp_hosts.config
    i=0; while [ $i -le $(( N-1 )) ]; do
      echo $i ${priIPsVar[$i]} ${pubIPsVar[$i]} $(( $((200 * $i)) + 10000 )) >> tmp_hosts.config
      i=$(( i+1 ))
    done
    i=0; while [ $i -le $(( N-1 )) ]; do
      ssh -o "StrictHostKeyChecking no" -i "/home/your-name/your-key-dir/your-sk.pem" ubuntu@${pubIPsVar[i]} "rm /home/ubuntu/dumbo/hosts.config"
      scp -i "/home/your-name/your-key-dir/your-sk.pem" tmp_hosts.config ubuntu@${pubIPsVar[i]}:/home/ubuntu/dumbo/hosts.config &
      i=$(( i+1 ))
    done
    
    # Start Protocols at all remote AWS servers
    i=0; while [ $i -le $(( N-1 )) ]; do   ssh -i "/home/your-name/your-key-dir/your-sk.pem" ubuntu@${pubIPsVar[i]} "export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/lib; export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib; cd dumbo; nohup python3 run_socket_node.py --sid 'sidA' --id $i --N $N --f $(( (N-1)/3 )) --B 10000 --K 11 --S 50 --T 2 --P "bdt" --F 1000000 > node-$i.out" &   i=$(( i+1 )); done
 
    # Download logs from all remote AWS servers to your local PC
    i=0
    while [ $i -le $(( N-1 )) ]
    do
      scp -i "/home/your-name/your-key-dir/your-sk.pem" ubuntu@${pubIPsVar[i]}:/home/ubuntu/dumbo/log/node-$i.log node-$i.log &
      i=$(( i+1 ))
    done
 
   ```

Here down below is the original README.md of HoneyBadgerBFT

# HoneyBadgerBFT
The Honey Badger of BFT Protocols.

<img width=200 src="http://i.imgur.com/wqzdYl4.png"/>

[![Travis branch](https://img.shields.io/travis/initc3/HoneyBadgerBFT-Python/dev.svg)](https://travis-ci.org/initc3/HoneyBadgerBFT-Python)
[![Codecov branch](https://img.shields.io/codecov/c/github/initc3/honeybadgerbft-python/dev.svg)](https://codecov.io/github/initc3/honeybadgerbft-python?branch=dev)

HoneyBadgerBFT is a leaderless and completely asynchronous BFT consensus protocols.
This makes it a good fit for blockchains deployed over wide area networks
or when adversarial conditions are expected.
HoneyBadger nodes can even stay hidden behind anonymizing relays like Tor, and
the purely-asynchronous protocol will make progress at whatever rate the
network supports.

This repository contains a Python implementation of the HoneyBadgerBFT protocol.
It is still a prototype, and is not approved for production use. It is intended
to serve as a useful reference and alternative implementations for other projects.

## Development Activities

Since its initial implementation, the project has gone through a substantial
refactoring, and is currently under active development.

At the moment, the following three milestones are being focused on:

* [Bounded Badger](https://github.com/initc3/HoneyBadgerBFT-Python/milestone/3)
* [Test Network](https://github.com/initc3/HoneyBadgerBFT-Python/milestone/2<Paste>)
* [Release 1.0](https://github.com/initc3/HoneyBadgerBFT-Python/milestone/1)

A roadmap of the project can be found in [ROADMAP.rst](./ROADMAP.rst).


### Contributing
Contributions are welcomed! To quickly get setup for development:

1. Fork the repository and clone your fork. (See the Github Guide
   [Forking Projects](https://guides.github.com/activities/forking/) if
   needed.)

2. Install [`Docker`](https://docs.docker.com/install/). (For Linux, see
   [Manage Docker as a non-root user](https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user)
   to run `docker` without `sudo`.)

3. Install [`docker-compose`](https://docs.docker.com/compose/install/).

4. Run the tests (the first time will take longer as the image will be built):

   ```bash
   $ docker-compose run --rm honeybadger
   ```

   The tests should pass, and you should also see a small code coverage report
   output to the terminal.

If the above went all well, you should be setup for developing
**HoneyBadgerBFT-Python**!

## License
This is released under the CRAPL academic license. See ./CRAPL-LICENSE.txt
Other licenses may be issued at the authors' discretion.

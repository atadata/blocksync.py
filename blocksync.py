#!/usr/bin/env python
"""
Synchronise block devices over the network

Copyright 2006-2008 Justin Azoff <justin@bouncybouncy.net>
Copyright 2011 Robert Coup <robert@coup.net.nz>
Copyright 2016 ATADATA <support@atadata.com>
License: GPL

Getting started:

* Copy blocksync.py to the home directory on the remote host
* Make sure your remote user can either sudo or is root itself.
* Make sure your local user can ssh to the remote host
* Invoke:
    sudo python blocksync.py /dev/source user@remotehost /dev/dest
"""

import sys
from sha import sha
import subprocess
import time


SAME = "same\n"
DIFF = "diff\n"


def do_open(f, mode):
    f = open(f, mode)
    f.seek(0, 2)
    size = f.tell()
    f.seek(0)
    return f, size


def getblocks(f, blocksize):
    while 1:
        block = f.read(blocksize)
        if not block:
            break
        yield block


def server(dev, blocksize, position=0):
    print dev, blocksize
    f, size = do_open(dev, 'r+')
    print size
    sys.stdout.flush()

    if position != 0:
        f.seek(position)

    for block in getblocks(f, blocksize):
        print sha(block).hexdigest()
        sys.stdout.flush()
        res = sys.stdin.readline()
        if res != SAME:
            newblock = sys.stdin.read(blocksize)
            f.seek(-len(newblock), 1)
            f.write(newblock)


def sync(srcdev, dsthost, dstdev=None, blocksize=1024 * 1024, cmd='/tmp/atatemp/blocksync', port=22):

    if not dstdev:
        dstdev = srcdev

    print "Block size is %0.1f MB" % (float(blocksize) / (1024 * 1024))
    cmd = [
        'ssh', '-i', '/tmp/atatemp/atakey', '-o', 'StrictHostKeyChecking=no', '-o', 'PasswordAuthentication=no', '-c', 'aes128-ctr', dsthost, '-p', str(port),
        cmd, 'target', dstdev, '-b', str(blocksize)
    ]
    print "Running: %s" % " ".join(cmd)

    attempts = 0
    max_attempts = 3
    while attempts < max_attempts:
        if attempts > 0:
            print "Retrying ({} of {})".format(attempts + 1, max_attempts)
        p = subprocess.Popen(
            cmd, bufsize=0, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, close_fds=True)
        p_in, p_out = p.stdin, p.stdout

        line = p_out.readline()
        p.poll()
        if p.returncode is not None:
            attempts += 1
            print "Error connecting to or invoking blocksync on the remote host!"
            sys.exit(1)
        else:
            attempts = max_attempts

    if not line:
        print p.stderr.read()
        sys.exit(1)

    a, b = line.split()
    if a != dstdev:
        print "Dest device (%s) doesn't match with the remote host (%s)!" % (
            dstdev, a)
        sys.exit(1)
    if int(b) != blocksize:
        print "Source block size (%d) doesn't match with" +\
            " the remote host (%d)!" % (blocksize, int(b))
        sys.exit(1)

    try:
        f, size = do_open(srcdev, 'r')
    except Exception, e:
        print "Error accessing source device! %s" % e
        sys.exit(1)

    line = p_out.readline()
    p.poll()
    if p.returncode is not None:
        print "Error accessing device on remote host!"
        sys.exit(1)

    remote_size = int(line)
    if size > remote_size:
        print "Source device size (%d) is bigger than" +\
            " target device size (%d)!" % (size, remote_size)
        sys.exit(1)

    same_blocks = diff_blocks = 0

    print "Starting sync..."
    t0 = time.time()
    t_last = t0
    size_blocks = size / blocksize
    for i, l_block in enumerate(getblocks(f, blocksize)):
        l_sum = sha(l_block).hexdigest()
        r_sum = p_out.readline().strip()

        if l_sum == r_sum:
            p_in.write(SAME)
            p_in.flush()
            same_blocks += 1
        else:
            p_in.write(DIFF)
            p_in.flush()
            p_in.write(l_block)
            p_in.flush()
            diff_blocks += 1

        t1 = time.time()
        if t1 - t_last > 1 or (same_blocks + diff_blocks) >= size_blocks:
            rate = (i + 1.0) * blocksize / (1024.0 * 1024.0) / (t1 - t0)
            print "\rsame: %d, diff: %d, %d/%d, %5.1f MB/s" % (
                same_blocks, diff_blocks, same_blocks + diff_blocks,
                size_blocks, rate),
            t_last = t1

    print "\n\nCompleted in %d seconds" % (time.time() - t0)

    return same_blocks, diff_blocks


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser("Blocksync")
    subparsers = parser.add_subparsers(dest="mode")
    target = subparsers.add_parser("target")
    target.add_argument("device")
    target.add_argument(
        "-b", "--blocksize", type=int,
        default=1024 * 1024, help="block size (bytes)")

    source = subparsers.add_parser("source")
    source.add_argument("srcdev", help="source device")
    source.add_argument("uhost", help="user@hostname")
    source.add_argument("dstdev", help="destination device")
    source.add_argument(
        "-b", "--blocksize", type=int,
        default=1024 * 1024, help="block size (bytes)")
    source.add_argument(
        "-p", "--port", type=int,
        default=22, help="ssh port")
    source.add_argument(
        "-c", "--command",
        default='/tmp/atatemp/blocksync', help="path to sync script")

    args = parser.parse_args()

    if args.mode == 'target':
        server(args.device, args.blocksize)
    elif args.mode == 'source':
        sync(
            args.srcdev,
            args.uhost,
            args.dstdev,
            args.blocksize,
            cmd=args.command,
            port=args.port)

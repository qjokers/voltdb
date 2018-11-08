#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.

# Author: Phil Rosegay

# VOLTDB K8S node startup controller
#
# Uses DNS SRV records to discover nodes and edit the VoltDB configuration when nodes startup.
# Copy "pristine" voltdbroot from docker image to persistent storage on first run.
#
# args are voltdb ... (ie. voltdb start command)
# Invoke from the container ENTRYPOINT ex.
#
#   voltdbk8s.py voltdb start <parms>

#
# voltdb start cmd with -H will be ignored, -H will be set appropriately.
#


import sys, os
import socket
import subprocess
import httplib2
from shutil import copytree


# mount point for persistent volume voltdbroot
PV_VOLTDBROOT = "/voltdbroot"


# TODO: need to implement internal-interface option (voltdb command) support
VOLTDB_INTERNAL_INTERFACE=3021
VOLTDB_HTTP_PORT = 8080


def query_dns_srv(query):
    m_list = []
    try:
        # SRV gives us records for each node in the cluster like ...
        # _service._proto.name.  TTL   class SRV priority weight port target.
        #nginx.default.svc.cluster.local    service = 10 100 0 voltdb-0.nginx.default.svc.cluster.local.
        # the fqdn is structured as ... headless-service-name.namespace.svc.cluster.local
        # this is similar to etcd
        answers = subprocess.check_output(("nslookup -type=SRV %s" % query).split(' ')).split('\n')[2:]
    except:
        return m_list
    for rdata in answers:
        if len(rdata):
            m_list.append(rdata.split(' ')[-1][:-1])  # drop the trailing '.'
            print rdata
    print m_list
    # return a list of fq hostnames of pods in the service domain
    return sorted(m_list)


def try_to_connect(host, port):
    s = socket.socket()
    try:
        s.connect((host, port))
        return True
    except Exception as e:
        print str(e)
        return False
    finally:
        s.close()


def http_get(url, host, port):
    # TODO: need to support clusters with externssl
    proto = "http"
    admin = "true"
    urlp = "%s://%s:%d/api/1.0/?%s&admin=%s" % (proto, host, port, url, admin)
    print urlp
    h = httplib2.Http(".cache")
    return h.request(urlp, "GET")


def get_system_information(host, port, section='OVERVIEW'):
    try:
        resource = "Procedure=@SystemInformation&Parameters=[\"" + section + "\"]"
        print resource
        resp_headers, content = http_get(resource, host, port)
        return resp_headers, content
    except:
        raise


def find_arg_index(args, arg):
    for i in range(0, len(args)):
        if args[i] == arg:
            return i+1
    return None


def fork_voltdb(host, voltdbroot):
    # before we fork over, see if /voltdbroot (persistent storage mount) is empty
    # if it is, initialize a new database there from our assets
    if not os.path.exists(PV_VOLTDBROOT):
        print "ERROR: '%s' is not mounted!!!!" % PV_VOLTDBROOT
        sys.exit(-1)
    assets_dir = os.path.join(os.getenv('VOLTDB_INIT_VOLUME', '/etc/voltdb'))
    working_voltdbroot = os.path.join(PV_VOLTDBROOT, voltdbroot)
    try:
        pv = os.listdir(working_voltdbroot)
    except OSError:
        pv = []
    print pv
    if len(pv) == 0:
        print "Initializing a new voltdb database at '%s'" % working_voltdbroot
        try:
            os.mkdir(working_voltdbroot)
        except OSError:
            pass
        os.chdir(working_voltdbroot)
        cmd = ['voltdb',  'init']
        """
        These asset files are mounted from a configmap
        !!!Updating the configmap will have NO EFFECT on a running or restarted instance!!!
        The command to create the configmap is typically:
        
        kubectl create configmap --from-file=deployment=mydeployment.xml [--from-file=classes=myclasses.jar] [--from-file=schema=myschema.sql]
        
        The deployment file node count is ignored, node count is specified in the runtime environment variable $NODECOUNT
        """
        if os.path.isdir(assets_dir):
            deployment_file = os.path.join(assets_dir, 'deployment')
            if os.path.isfile(deployment_file):
                cmd.extend(['--config', deployment_file])
            classes_file = os.path.join(assets_dir, 'classes')
            if os.path.isfile(classes_file):
                cmd.extend(['--classes', classes_file])
            schema_file = os.path.join(assets_dir, 'schema')
            if os.path.isfile(schema_file):
                cmd.extend(['--schema', schema_file])
        extra_init_args = os.getenv('VOLTDB_INIT_ARGS')
        if extra_init_args:
            cmd.extend(extra_init_args.split(' '))
        print "Init command: " + str(cmd)
        from subprocess import Popen
        sp = Popen(cmd, shell=False)
        sp.wait()
        if sp.returncode != 0:
            print "ERROR failed Initializing voltdb database at '%s' (did you forget --force?)" % working_voltdbroot
            sys.exit(-1)
        print "Initialize new voltdb succeeded on node '%s'" % host
        os.chdir(PV_VOLTDBROOT)
        os.system("rm -f " + ssname)
        os.system("ln -sf " + voltdbroot + " " + ssname)

    os.chdir(working_voltdbroot)
    args = sys.argv[:]  # copy
    import shlex
    args = shlex.split(' '.join(args))
    if os.path.isdir(assets_dir):
        license_file = os.path.join(assets_dir, 'license')
        if os.path.isfile(license_file):
            li = find_arg_index(args, '-L') or find_arg_index(args, '--license')
            if li is None:
                li = len(args)
                args[li] = "-L"
                li += 1
            args[li+1] = license_file
    if os.path.isfile(deployment_file):
        cmd.extend(['--config', deployment_file])
    di = find_arg_index(args, '-D') or find_arg_index(args, '--directory')
    if di:
        args[di+1] = working_voltdbroot
    hni = find_arg_index(args, '-H') or find_arg_index(args, '--host')
    if hni is None:
        hni = len(args)
        args[hni] = "-H"
        hni += 1
    args[hni] = host
    print "VoltDB cmd is '%s'" % args[1:]
    # flush so we see our output in k8s logs
    sys.stdout.flush()
    sys.stderr.flush()
    d = os.path.dirname(args[0])
    print "Starting VoltDB..."
    os.execv(os.path.join(d, args[1]), args[1:])
    sys.exit(0)


def get_hostname_tuple(fqdn):
    hostname, domain = fqdn.split('.', 1)
    ssp = hostname.split('-')
    hn = ('-'.join(ssp[0:-1]), ssp[-1], hostname, domain)  # statefulset hostnames are podname-ordinal
    return hn   # returns a tuple (ss-name, ordinal, hostname, domain)


if __name__ == "__main__":

    print sys.argv
    print os.environ

    # check that our args look like a voltdb start command line and only that
    if not (sys.argv[1] == 'voltdb' and sys.argv[2] == 'start'):
        print "ERROR: expected voltdb start command but found '%s'" % sys.argv[1]
        sys.exit(-1)

    fqhostname = socket.getfqdn()
    print fqhostname

    # for maintenance mode, don't bring up the database just hang
    # nb. in maintenance mode, liveness checks will probably timeout if enabled
    if '--k8s-maintenance' in sys.argv:
        while True:
            from time import sleep
            sleep(10000)
        #os.execv('tail' '-f', '/dev/null')

    # use the domain of the leader address to find other pods in our cluster
    hn = get_hostname_tuple(fqhostname)
    print hn

    ssname, ordinal, my_hostname, domain = hn

    if len(hn) != 4 or not ordinal.isdigit():
        # TODO: need a better check for running with k8s statfulset???
        # we don't know what do with this, just fork
        fork_voltdb(ssname+"-0."+domain, None)

    # if there are some pods up in our cluster, connect to the first one we find
    # if we fail to form/rejoin/join a cluster, k8s will likely just restart the pod

    # get a list of fq hostnames of pods in the domain from DNS SRV records
    my_cluster_members = query_dns_srv(domain)

    din = find_arg_index(sys.argv, '-D')
    if din:
        voltdbroot = '.'.join(my_hostname, sys.argv[din])
    else:
        voltdbroot = fqhostname

    # nodes may be "published before they are ready to receive traffic"
    for host in my_cluster_members:
        print "Connecting to '%s'" % host
        if try_to_connect(host, VOLTDB_INTERNAL_INTERFACE):
            # we may have found a running node, get voltdb SYSTEMINFORMATION
            if try_to_connect(host, VOLTDB_HTTP_PORT):
                sys_info = None
                try:
                    sys_info = get_system_information(host, VOLTDB_HTTP_PORT)
                except:
                    raise
                print "sysinfo: " + str(sys_info)
            # try to connect to mesh
            fork_voltdb(host, voltdbroot)

    fork_voltdb(ssname+"-0."+domain, voltdbroot)

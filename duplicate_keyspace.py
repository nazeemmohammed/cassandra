#!/usr/bin/python

import os, re, glob, time, getpass, datetime, sys, json, pdb, csv
import sys, shlex, subprocess
from cassandra.cluster import Cluster

nodes=[]
snapshot_ids={}
cluster_name=""
src_ks=""
tgt_ks=""
table_ids={}

def frame(text):
    """
    Put the text in a pretty frame.
    """
    result = """
+{h}+
|{{ts{13/08/18 13:54}}}|
+{h}+
""".format(h='-' * len(text), t=text).strip()
    return result

def get_exitcode_stdout_stderr(cmd):
    args = shlex.split(cmd)
    proc = subprocess.Popen(args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    out, err = proc.communicate()
    exitcode = proc.returncode
    return exitcode, out, err

def call_oscmd(cmd):
    exitcode, out, err = get_exitcode_stdout_stderr(cmd)
    if exitcode != 0:
       print(frame("EXITCODE"))
       print(exitcode)
       print(frame("STDERR"))
       print(err)
    return out

def get_nodes_list(cluster_name):
    cmd = 'ccm status ' + cluster_name
    out=call_oscmd(cmd)
    ccmstatout=out.split('\n')[2:-1]
    for row in ccmstatout:
        if ': ' in row:
            key, value=row.split(': ')
            nodes.append(key)
    print "Nodes:"
    print nodes
    print '*'*100

def nodetool_flush():
    for node in nodes:
        cmd='ccm ' + node + ' nodetool flush'
        print cmd
        out=call_oscmd(cmd)
        print out
        print '*'*100

def nodetool_refresh():
#    pdb.set_trace()
    for node in nodes:
        for tbls in table_ids[tgt_ks].iterkeys():
            cmd='ccm ' + node + ' nodetool refresh ' + tgt_ks + " " + tbls
            print cmd
            out=call_oscmd(cmd)
            print out
            print '*'*100

def nodetool_repair():
    for node in nodes:
        cmd='ccm ' + node + ' nodetool repair'
        print cmd
        out=call_oscmd(cmd)
        print out
        print '*'*100

def nodetool_snapshot():
    for node in nodes:
        cmd='ccm ' + node + ' nodetool snapshot'
        print cmd
        out=call_oscmd(cmd)
        print out
        for row in out.split('\n'):
            if ': ' in row:
                key,value=row.split(': ')
                snapshot_ids[node]=value
        print snapshot_ids
        print '*'*100

def create_tgt_keyspace():
    cmd='ccm node1 cqlsh -e "desc keyspace ' + src_ks + """;" | grep 'CREATE KEYSPACE' | sed 's/CREATE KEYSPACE """ + src_ks + '/CREATE KEYSPACE ' + tgt_ks + """/' | ccm node1 cqlsh"""
#    out=call_oscmd(cmd)  -- for some reason this is throwing error
#    os.system(cmd)
    print cmd
    proc=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out, err = proc.communicate()
    print out
    cmd='ccm node1 cqlsh -e "desc keyspace ' + src_ks + """;" | grep -v 'CREATE KEYSPACE' | sed 's/CREATE TABLE """ + src_ks + '/CREATE TABLE ' + tgt_ks + """/' | ccm node1 cqlsh -k """ + tgt_ks
    print cmd
#    out=call_oscmd(cmd)
#    os.system(cmd)
    proc=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out, err = proc.communicate()
    print out
    print '*'*100

def get_table_ids():
    cluster=Cluster(['127.0.0.1','127.0.0.1','127.0.0.2'])
    session=cluster.connect()
    session.set_keyspace('system_schema')
    session_recs=session.execute('select keyspace_name, table_name, id from tables where keyspace_name in (%s,%s)',(src_ks,tgt_ks))
    table_ids[src_ks]={}
    table_ids[tgt_ks]={}
    for rec in session_recs:
        table_ids[str(rec.keyspace_name)][str(rec.table_name)]=re.sub(r'(-)',"",str(rec.id))
    print table_ids
    print '*'*100

def copy_keyspace():
    global cluster_name
    global src_ks
    global tgt_ks
    for node in nodes:
        for tbl in table_ids[src_ks].iterkeys():
            src_path="/home/ubuntu/.ccm/" + cluster_name + "/" + node + "/data0/" + src_ks + "/" + tbl + "-" + table_ids[src_ks][tbl] + "/"
            tgt_path="/home/ubuntu/.ccm/" + cluster_name + "/" + node + "/data0/" + tgt_ks + "/" + tbl + "-" + table_ids[tgt_ks][tbl] + "/"
            cmd="rsync -av --progress " + src_path + " " + tgt_path + " --exclude snapshots --exclude backups"
            print cmd
            out=call_oscmd(cmd)
            print out
    print '*'*100

def init():
    global cluster_name
    global src_ks
    global tgt_ks
    if len(sys.argv) == 4:
       try:
           cluster_name=sys.argv[1]
           src_ks=sys.argv[2]
           tgt_ks=sys.argv[3]
       except ValueError:
           print("All the required params are not passed - cluster_name src_ks tgt_ks")
           sys.exit()

def main():
    init()
    get_nodes_list(cluster_name)
    nodetool_flush()
    nodetool_snapshot()
    create_tgt_keyspace()
    get_table_ids()
    copy_keyspace()
    nodetool_refresh()
    nodetool_repair()

if __name__ == "__main__":
   main()

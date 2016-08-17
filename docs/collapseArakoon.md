# Easy way to collapse an Arakoon on a cluster

Applies to: Eugene-updates & Fargo

## 1. Description

Collapse an Arakoon on a cluster.

## 2. Possible error when collapsing

```
(client_protocol|info): ... Start collapsing ... (n=2)
(main|info): Starting collapse
(main|info): Creating local store at /opt/OpenvStorage/db/arakoon/ovsdb/tlogs/head.db (lcnum=16384) (ncnum=8192)
(main|info): returning assuming no I "/opt/OpenvStorage/db/arakoon/ovsdb/tlogs/head.db": Failure("success")
(main|info): Going to collapse 5917 tlogs
...
(client_protocol|error): Exception during client request (Failure("success")) => rc:ff msg:unknown failure
```

## 3. Multiple methods to collapse
### 3.1 Arakoon catch up
<div style="background-color:orange; color:black">
  <strong>Warning!</strong><br />Use this method only when the 2 other Arakoons are properly collapsed.
</div>

Lets Arakoon do the catch up for you.

#### 3.1.1 Steps
Stop your Arakoon services. ( stop ovs-arakoon-..)

Remove every file in the correct tlogs directory. (check the config from the Arakoon where tlogs directory is located)

Remove the <node_id>.db file in the db directory. (check the config from the Arakoon where the home directory is located)

Start your Arakoon services. (start ovs-arakoon-..)

Verify the log file and check if he download the head.db from another node and reply the tlogs.
```
(main|info): CATCHUP start: I'm @ 0 and T3BpM2jJy5qloYS5 is more recent
(main|info): catchup_tlog 0
...
(main|info): catchup_tlog completed
(main|info): CATCHUP phase 1 done (until_i = 29630399); now the store
(main|info): replaying log to store
(main|info): Replaying tlog file: 5921.tlx (1/6)
(main|info): Completed replay of 5921.tlx, took 0.080827 seconds, 5 to go
(main|info): Replaying tlog file: 5922.tlx (2/6)
(main|info): Completed replay of 5922.tlx, took 0.067129 seconds, 4 to go
(main|info): Replaying tlog file: 5923.tlog (3/6)
... 
(main|info): T3BpM2jJy5qloYS5 is master

```

### 3.2 Remote collapse
Collapse the Arakoon manually with collapse-remote

#### 3.2.1 Steps
Check the IP and PORT where your Arakoon is running and collapse-remote the Arakoon database.

**Arakoon config (Fargo):**

* Get the Arakoon config location:
```
etcdctl ls --recursive | grep arakoon | grep config

/ovs/arakoon/mybackend-abm/config
/ovs/arakoon/mybackend-nsm_0/config
/ovs/arakoon/voldrv/config
/ovs/arakoon/ovsdb/config
```
* Print the correct Arakoon config:
```
etcdctl get /ovs/arakoon/ovsdb/config
```

**Collapse remote**

* Command:
```
arakoon --collapse-remote <cluster_id> <ip> <port> <n> tells node to collapse all but <n> tlogs into its head database
```
The cluster_id, ip and port can be found in the previous printed Arakoon config

* Example:
```
arakoon --collapse-remote ovsdb 10.100.1.2 26400 2
```

### 3.3 Manually
<div style="background-color:orange; color:black">
  <strong>Warning!</strong><br />Use this method only when <strong>one</strong> Arakoon is properly collapsed.
</div>

Manually copy the files from one Arakoon to another.

#### 3.3.1 Steps
You can try this method if 3.1 is not working and you received errors in the log file.

Stop your Arakoon services. ( stop ovs-arakoon-..)

Remove every file in the correct tlogs directory. (check the config from the Arakoon where tlogs directory is located)

Remove the <node_id>.db file in the db directory. (check the config from the Arakoon where the home directory is located)

Copy the head.db, *.tlx, *.tlog files from the Arakoon that is properly collapsed to the tlogs directory. (use scp for this)

Change the owner and group for all these files. (chown ovs:ovs *)

Start your Arakoon services. ( start ovs-arakoon-..)

Verify the log file if the Arakoon find a master.
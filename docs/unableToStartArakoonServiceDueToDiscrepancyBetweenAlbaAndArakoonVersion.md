# Unable to start Arakoon service due to discrepancy between Alba and Arakoon version

Applies to: Eugene-updates & Fargo

## 1. Description
Due to an upgrade of ALBA the Arakoon went down. It was impossible to restart the Arakoon. 

Possible error message:
```
less /var/log/upstart/ovs-arakoon-du-conv-1-nsm_2.log

Jun 20 10:15:20 7743: (main|fatal): nsm_host_plugin: Dynlink.Error "error loading shared library: /mnt/ovs-db-write/arakoon/du-conv-1-nsm_2/db/nsm_host_plugin.cmxs: undefined symbol: camlArakoon_config_url"
```
In the error message above there was a missing config file. After checking the version of Alba and Arakoon this was due to a discrepancy of the version.
 
## 2. Steps

### 2.1 Check the version of Alba and Arakoon

```
root@du-conv-1-02:~# alba version
0.9.7
```

```
root@du-conv-1-02:~# arakoon --version
version: 1.8.12
```

### 2.2 Check if it's the latest version of Alba and Arakoon

```
root@du-conv-1-02:~# apt-cache policy alba
alba:
  Installed: 0.9.7
  Candidate: 0.9.7
```

```
root@du-conv-1-02:~# apt-cache policy arakoon
arakoon:
  Installed: 1.8.12
  Candidate: 1.9.5
```

As you can see, the Arakoon version isn't the latest one.

<div style="color:black;background-color:#cccccc;font-size:150%">
  <strong>Warning!</strong> When you upgrade Alba you need to upgrade Arakoon as well.
</div>

### 2.3 Upgrade arakoon
```
apt-get install --only-upgrade arakoon
```

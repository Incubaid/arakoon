# Easy way to retrieve the Arakoon masters on a cluster

Applies to: Eugene-updates & Fargo

## 1. Description

Retrieve the Arakoon master on a cluster.

## 2. Steps
### 2.1 Eugene-updates
```
echo -e "##########################\nWho is the Arakoon master?\n##########################" ; for i in $(pgrep -a arakoon | cut -d ' ' -f 6); do arakoon --who-master -config $i 2>/dev/null ; echo -ne "     => " ; echo $i | cut -d '/' -f 6 ; done ; echo -e "\nThere should be $(find /var/log/arakoon/ -mindepth 1 -maxdepth 1 -type d | wc -l) arakoons available!"
```
### 2.2 Fargo and unstable
```
echo -e "##########################\nWho is the Arakoon master?\n##########################" ; for i in $(pgrep -a arakoon | cut -d ' ' -f 6); do arakoon --who-master -config $i 2>/dev/null ; echo -ne "     => " ; echo $i | cut -d '/' -f 6 ; done ; echo -e "\nThere should be $(find /var/log/upstart/ovs-arakoon-* | grep -v gz | wc -l) arakoons available!"
```

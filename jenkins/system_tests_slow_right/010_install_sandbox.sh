whoami
sudo -l
echo $PATH
which rm
sudo rm -rf /opt/qbase3
sudo tar xfz /home/incubaid/qbaseSandbox_3.2-small-linux64.tar.gz -C /opt
sudo cp /home/incubaid/sources/publish.cfg
/opt/qbase3/cfg/qpackages4/sources.cfg
sudo cp /home/incubaid/hgconnection.cfg /opt/qbase3/cfg/qconfig/hgconnection.cfg

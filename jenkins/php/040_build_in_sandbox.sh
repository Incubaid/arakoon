cd /opt/qbase3/var/tmp/arakoon-1.0
sudo echo "export PATH=/opt/qbase3/bin:$PATH" > /tmp/buildInSB.sh
sudo cat buildInSandbox.sh >> /tmp/buildInSB.sh
sudo chmod a+x /tmp/buildInSB.sh
sudo  /tmp/buildInSB.sh -tag small_tlogs
sudo cp
/opt/qbase3/var/tmp/arakoon-1.0/_build/3rd-party/tokyocabinet-1.4.47/tcbmgr
/opt/qbase3/apps/arakoon/bin/tcbmgr
cd -

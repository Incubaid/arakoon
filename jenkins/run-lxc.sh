#!/bin/bash -xue

JOB=$1
BASE=$2

if [ -z $JOB ];
then
    echo "\$JOB not set"
    exit 1
fi
if [ -z $BASE ];
then
    echo "\$BASE not set"
    exit 1
fi

if [ ! -r jenkins/$JOB/seed ];
then
    echo "Seed file jenkins/$JOB/seed not readable"
    exit 1
fi

SEED=`cat jenkins/$JOB/seed`
# Relative to $BASE
SEEDS_BASE=seeds
JOBS_BASE=jobs
ROOTFS=$BASE/$JOBS_BASE/$BUILD_TAG
CONTAINER_USER=jenkinslxc

if [ ! -d $BASE/$SEEDS_BASE/$SEED ];
then
    echo "Seed subvolume '$SEED' not found"
    exit 1
fi

echo "Create seed snapshot"
pushd $BASE
sudo /sbin/btrfs subvolume snapshot $SEEDS_BASE/$SEED $JOBS_BASE/$BUILD_TAG
popd

echo "Fixup clone files"
pushd $ROOTFS
sudo /bin/chown jenkins etc/hosts
sudo /bin/chown jenkins etc
sed -i "s/localhost/$BUILD_TAG localhost/" etc/hosts
sudo /bin/chown root:root etc/hosts
sudo /bin/chown root:root etc
popd

LXC_CONF_NAME=lxc-$BUILD_TAG.conf
LXC_CONF=$ROOTFS/$LXC_CONF_NAME

echo "Creating LXC configuration file"
cat > $LXC_CONF_NAME << EOF
lxc.utsname=$BUILD_TAG

lxc.network.type=veth
lxc.network.flags=down
lxc.network.link=lxcbr0
lxc.network.name=eth0

lxc.rootfs=$ROOTFS

lxc.mount.entry=$WORKSPACE $ROOTFS/home/$CONTAINER_USER/workspace bind bind,nodev,nosuid 0 0

lxc.tty = 4
lxc.pts = 1024
lxc.cgroup.devices.deny = a
# /dev/null and zero
lxc.cgroup.devices.allow = c 1:3 rwm
lxc.cgroup.devices.allow = c 1:5 rwm
# consoles
lxc.cgroup.devices.allow = c 5:1 rwm
lxc.cgroup.devices.allow = c 5:0 rwm
lxc.cgroup.devices.allow = c 4:0 rwm
lxc.cgroup.devices.allow = c 4:1 rwm
# /dev/{,u}random
lxc.cgroup.devices.allow = c 1:9 rwm
lxc.cgroup.devices.allow = c 1:8 rwm
lxc.cgroup.devices.allow = c 136:* rwm
lxc.cgroup.devices.allow = c 5:2 rwm
# rtc
lxc.cgroup.devices.allow = c 254:0 rwm

# mounts point
lxc.mount.entry=proc $ROOTFS/proc proc nodev,noexec,nosuid 0 0
lxc.mount.entry=devpts $ROOTFS/dev/pts devpts defaults 0 0
lxc.mount.entry=sysfs $ROOTFS/sys sysfs defaults  0 0
EOF

sudo chown root:root $LXC_CONF_NAME
sudo mv $LXC_CONF_NAME $LXC_CONF

echo "Write job execution script"
JOB_SCRIPT_NAME=job-$BUILD_TAG.sh

cat > $JOB_SCRIPT_NAME << EOF
#!/bin/bash -xue

sudo /sbin/ifconfig lo up
# Make sure no daemon keeps running!
sudo /sbin/dhclient eth0
trap "{ sudo /sbin/dhclient -r eth0; }" EXIT

/sbin/ifconfig
/sbin/route -n

# Re-export some of the Jenkins env
export BUILD_NUMBER=$BUILD_NUMBER
export BUILD_ID=$BUILD_ID
export JOB_NAME=$JOB_NAME
export BUILD_TAG=$BUILD_TAG

export WORKSPACE=\$HOME/workspace

export JOB=$JOB

cd \$HOME/workspace

for script in \`ls jenkins/\$JOB/* | grep -e "\/[0-9]\{3\}_"\`; do
    cd "\$WORKSPACE"
    \$script
done

ps afx

EOF

echo "Move job execution script into container"
sudo /bin/chown root:root $JOB_SCRIPT_NAME
sudo /bin/mv $JOB_SCRIPT_NAME $ROOTFS/home/$CONTAINER_USER

echo "Run job"
sudo /usr/bin/lxc-execute -n $BUILD_TAG -f $LXC_CONF -- su - $CONTAINER_USER -c "/bin/bash -xue /home/$CONTAINER_USER/$JOB_SCRIPT_NAME"

exit $?

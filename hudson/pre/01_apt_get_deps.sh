set -e
set -u

echo "Installing all packages needed to build Arakoon (TODO: validate packages)"
sudo apt-get install build-essential autoconf libtool pkg-config \
	libglib2.0-dev uuid-dev liblog4c-dev automake  libncurses5-dev -y

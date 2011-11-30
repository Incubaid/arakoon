set -e
set -u

echo "Installing all packages needed to build Arakoon (TODO: validate packages)"
sudo apt-get install build-essential autoconf libtool libfuse-dev pkg-config \
	libglib2.0-dev uuid-dev liblog4c-dev python-setuptools liblog4cxx10-dev \
	automake python-nose -y

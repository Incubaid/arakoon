Summary: Arakoon
Name: arakoon
Version: 1.7.4
Release: 1
License: Apache 2.0

%description
Arakoon, a key-value store

%prep
cd ../SOURCES/arakoon-%{version}
make clean

%build
cd ../SOURCES/arakoon-%{version}
make

%install
cd ../SOURCES/arakoon-%{version}
mkdir -p $RPM_BUILD_ROOT/usr/bin/
cp arakoon.native $RPM_BUILD_ROOT/usr/bin/arakoon

%files
/usr/bin/arakoon

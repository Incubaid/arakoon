Summary: Arakoon
Name: arakoon
Version: 1.7.4
Release: 3%{?dist}
License: Apache 2.0
Requires: libev >= 4
Source: https://github.com/Incubaid/arakoon-%{version}
URL: http://www.arakoon.org
ExclusiveArch: x86_64

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
mkdir -p %{buildroot}%{_bindir}
cp LICENSE %{_builddir}
cp README.md %{_builddir}/README
cp arakoon.native %{buildroot}%{_bindir}/arakoon

%files
%doc README LICENSE
%{_bindir}/arakoon

%changelog
* Thu Oct 02 2014 Kenneth Henderick <kenneth.henderick@cloudfounders.com> - 1.7.4-3
- Create RPM packages containing more information

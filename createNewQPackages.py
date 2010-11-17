#! /opt/qbase3/bin/python

from pymonkey import InitBase
from pymonkey import q, i
fs = q.system.fs
import sys

required_arg_count = 1
def print_useage(error_msg=""):
    print 
    print error_msg
    print
    print "Sample usage: "
    print "\t ./createNewQPackages 1.2.4"
    print 
    print
    sys.exit(0)

if len(sys.argv) != required_arg_count + 1:
    print_useage( "Script requires %d command line parameters" % required_arg_count )

def validate_new_version( new_version ):
    msg_invalid_version = 'Invalid version: "%s". Version string should be composed of 3 integers, separated by dots'  % new_version
    try :
        version_parts = new_version.split('.')
        version_part_cnt = len ( version_parts )
        if version_part_cnt != 3:
            print_usage( msg_invalid_version )
        for i in range( version_part_cnt ) :
            if int( version_part_cnt ) != version_part_cnt:
                print_usage( msg_invalid_version ) 
    except:
        print_useage( msg_invalid_version )

new_version = sys.argv[1]
validate_new_version( new_version )

# i.qp.updateMetadataAll()


versions_dir = q.qp.getMetadataPath( 'pylabs.org', 'arakoon', '' )
version_dirs = fs.listDirsInDir ( versions_dir )
versions = map( fs.getBaseName, version_dirs )
prev_version = '0.0.0'

prev_version_dir_server = None
for v in versions:
    if v > prev_version and v < new_version:
        prev_version_dir_server = fs.joinPaths ( versions_dir , v )
        prev_version = v

if prev_version_dir_server is None:
    print '\n\nNo previous versions of arakoon found in metadata repo. Aborting...\n\n'
    sys.exit(255) 



def createNewPackage( package_name, package_description, old_version, new_version) :
    pkg = q.qp.createNewQPackage( 'pylabs.org', package_name, new_version, package_description , ['linux64'] )
    pkg_meta_dir = pkg.getPathMetadata()

    prev_pkg = i.qp.find( package_name, version=old_version ).qpackage
    prev_pkg_meta_dir = prev_pkg.getPathMetadata()

    prev_tasklet_dir = fs.joinPaths(prev_pkg_meta_dir, 'tasklets')
    tasklet_dir = fs.joinPaths(pkg_meta_dir,'tasklets')

    fs.createDir( tasklet_dir )
    for f in fs.listFilesInDir ( prev_tasklet_dir, filter="*.py" ) :
        fs.copyFile( f, tasklet_dir )

    p = i.qp.find( package_name, version=new_version )
    return p

p = i.qp.find( 'arakoon_dev', version='0.8.0' )
p.install()

packages = list()
p = createNewPackage('arakoon', 'Version %s of the arakoon key value store' % new_version, prev_version, new_version )
packages.append(p)
createNewPackage('arakoon_client', 'Version %s of the arakoon client' % new_version, prev_version, new_version )
packages.append(p)
p = createNewPackage('arakoon_system_tests', 'Version %s of the arakoon system tests' % new_version, prev_version, new_version )
deps = p.qpackage.dependencies
deps_to_replace = ['arakoon','arakoon_client']
for dep in deps:
    if dep.name in deps_to_replace:
        p.qpackage.removeDependency( dep )
for dep in deps_to_replace:
    p.qpackage.addDependency( 'pylabs.org', dep, [q.enumerators.PlatformType.LINUX64], minversion=new_version, maxversion=new_version )
packages.append(p)

for p in packages:
    p.quickPackage()

#! /opt/qbase3/bin/python

from pymonkey import InitBase
from pymonkey import q, i
fs = q.system.fs
import sys

required_arg_count = 2
def print_useage(error_msg=""):
    print 
    print error_msg
    print
    print "Sample usage: "
    print "\t ./createNewQPackages 1.2.4 1.2.4"
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
branch = sys.argv[2]
validate_new_version( new_version )

i.qp.updateMetaDataAll()

versions_dir = q.qp.getMetadataPath( 'pylabs.org', 'arakoon', '' )
version_dirs = fs.listDirsInDir ( versions_dir )
versions = map( fs.getBaseName, version_dirs )
prev_version = '0.0.0'

prev_version_dir_server = None

def is_better_version ( version, new_version, best_version ) :

    def split_version ( v ) :
        parts = v.split('.')
        if len(parts) < 3 :
            return (0, 0, 0)
        return ( int(parts[0]), int(parts[1]), int(parts[2]) )

    def compare_version (v, ov) :
        (v1, v2, v3) = split_version(v)
        (ov1, ov2, ov3) = split_version(ov)
        nv = v1 * 1000000 + v2 * 1000 + v3
        nov = ov1 * 1000000 + ov2 * 1000 + ov3
        if nv > nov :
            return 1
        if nv < nov :
            return -1
        return 0

    return compare_version( version, best_version ) == 1 and compare_version(version,new_version) == -1
  
for v in versions:
    if is_better_version(v, new_version, prev_version) :
        prev_version_dir_server = fs.joinPaths ( versions_dir , v )
        prev_version = v

if prev_version_dir_server is None:
    print '\n\nNo previous versions of arakoon found in metadata repo. Aborting...\n\n'
    sys.exit(255) 

template = """
import pymonkey.config.IConfigBase


    
__author__ = 'incubaid'
__tags__   = 'codemanagement',

def main(q, i, params, tags):

    from pymonkey.clients.hg.HgRecipe import HgRecipe

    srcRepoUrl = "http://bitbucket.org/despiegk/arakoon"
    srcRepoBranch = "%(branch)s"
    
    qpackage = params["qpackage"]
    
    q.logger.log( "Checking out %%s %%s from bitbucket.org" %% (qpackage.name, qpackage.version) )
    
    packageDir = "%%s-%%s" %% (qpackage.name, qpackage.version)
    targetDirectory = q.system.fs.joinPaths( q.dirs.tmpDir, packageDir )
    
    recipe = HgRecipe()
    
#    connection = i.hg.connections.findByUrl( "http://bitbucket.org/despiegk/arakoon")
    
    try :
        connection = i.hg.connections.findByUrl( srcRepoUrl )
    except pymonkey.config.IConfigBase.ConfigError , ex :
        if not q.qshellconfig.interactive :
            q.eventhandler.raiseCritical( "Please checkout the %%s package from within an interactive qshell. Cannot continue without your bitbucket credentials." %% (qpackage.name))
    
    
    
    if q.system.fs.exists( targetDirectory )  :
        q.logger.log( "Removing directory %%s" %% targetDirectory )
        q.system.fs.removeDirTree ( targetDirectory )
    
    recipe.addRepository( connection )    
    recipe.addSource( connection, "src/client/python", q.system.fs.joinPaths(targetDirectory, 'src'), srcRepoBranch )
    recipe.addSource( connection, "extension/client", q.system.fs.joinPaths(targetDirectory, 'extension'), srcRepoBranch )
    recipe.executeTaskletAction( params['action'] )
    
    q.logger.log( "Checkout complete" ) 

"""

def get_arakoon_codemanagement_tasklet(branch):
    tasklet = template % {'branch' : branch}
    return tasklet

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

    print 'B', pkg.dependencies
    for d in prev_pkg.dependencies:
        pkg.addDependency(d.domain, d.name, d.supportedPlatforms, \
            d.minversion, d.maxversion, d.dependencytype)

    print 'A', pkg.dependencies

    p = i.qp.find( package_name, version=new_version )
    return p

p = i.qp.find( 'arakoon_dev', version='0.10.0' )
p.install()

def replace_deps( p, deps_to_replace):
    deps = p.qpackage.dependencies
    for dep in deps:
        if dep.name in deps_to_replace:
            p.qpackage.removeDependency( dep )
    for dep in deps_to_replace:
        p.qpackage.addDependency( 'pylabs.org', dep, [q.enumerators.PlatformType.LINUX64], minversion=new_version, maxversion=new_version, dependencytype=q.enumerators.DependencyType4.RUNTIME)


packages = list()

tasklet_code = get_arakoon_codemanagement_tasklet(branch)
p = createNewPackage('arakoon_client', 'Version %s of the arakoon client' % new_version, prev_version, new_version )
packages.append(p)
# Update codemanagement.py
tasklet_path = fs.joinPaths(p.qpackage.getPathMetadata(), 'tasklets', 'codemanagement.py')
q.system.fs.writeFile(tasklet_path, tasklet_code)

p = createNewPackage('arakoon', 'Version %s of the arakoon key value store' % new_version, prev_version, new_version )
replace_deps(p, ['arakoon_client'])
packages.append(p)
# Update codemanagement.py
tasklet_path = fs.joinPaths(p.qpackage.getPathMetadata(), 'tasklets', 'codemanagement.py')
q.system.fs.writeFile(tasklet_path, get_arakoon_codemanagement_tasklet(branch))


p = createNewPackage('arakoon_system_tests', 'Version %s of the arakoon system tests' % new_version, prev_version, new_version )
deps_to_replace = ['arakoon']
replace_deps(p, deps_to_replace)
#p.qpackage.addDependency( 'qpackages.org', 'testrunner', [q.enumerators.PlatformType.LINUX64], dependencytype=q.enumerators.DependencyType4.RUNTIME)
packages.append(p)

for p in packages:
    p.quickPackage()

msg = 'Releasing Arakoon %s (branch %s)' % (new_version, branch)
q.qp.publish(commitMessage=msg, domain='pylabs.org')

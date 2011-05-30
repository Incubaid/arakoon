from pylabs.InitBase import q
import sys
import optparse

parser = optparse.OptionParser()
parser.add_option("-s", "--stop", dest="stop", action="store_true", default=False)
parser.add_option("-p", "--port", dest="port", type="int", default=15500)
parser.add_option("-c", "--cluster", dest="cluster", default="phpclient")
(options, args) = parser.parse_args()

if options.stop:
    clu = q.manage.arakoon.getCluster(options.cluster)
    if "%s_0"%(options.cluster) in clu.getStatus():
        clu.tearDown()
        print "Cluster tear down successfully!"
    else:
        print "Cluster is not running!"
else:
    clu = q.manage.arakoon.getCluster(options.cluster)
    if "%s_0"%(options.cluster) not in clu.getStatus():
        clu.setUp(1, options.port)
        clu.start()
        print "Arakoon Server started!"
    else:
        print "Cluster Already running!"


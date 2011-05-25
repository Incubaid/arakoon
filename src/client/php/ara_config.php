<?php

include_once 'ara_def.php';

function arakoonLog($function, $message)
{
    echo "$function | $message <br>";
}


class ArakoonClientConfig
{


    private $clusterId = null;
    private $nodes = null;

    /*
        Constructor of an ArakoonClientConfig object

        The constructor takes one optional parameter 'nodes'.
        This is a dictionary containing info on the arakoon server nodes. It contains:
          - nodeids as keys
          - (hostname/ip, tcp port) tuples as value
        e.g. ::
            cfg = ArakoonClientConfig ('ricky',
                { "myFirstNode" : ( "127.0.0.1", 4000 ),
                  "mySecondNode" : ( "127.0.0.1", 5000 ) ,
                  "myThirdNode"  : ( "127.0.0.1", 6000 ) } )

        @type clusterId: string
        @param clusterId: name of the cluster
        @type nodes: dict
        @param nodes: A dictionary containing the locations for the server nodes
    */
    public function __construct($clusterId, $nodes) {
       $this->clusterId = $clusterId;
       $this->nodes = $nodes;
    }

    /*   
        Retrieve the period messages to the master should be retried when a master re-election occurs
        
        This period is specified in seconds
        
        @rtype: integer
        @return: Returns the retry period in seconds
    */
    static function getNoMasterRetryPeriod(){
        return ARA_CFG_NO_MASTER_RETRY;
    }

    /*
        Retrieve the tcp connection timeout

        Can be controlled by changing the global variable L{ARA_CFG_CONN_TIMEOUT}

        @rtype: integer
        @return: Returns the tcp connection timeout
    */
    static function getConnectionTimeout(){
        return ARA_CFG_CONN_TIMEOUT;
    }

    /*
        Retrieves the backoff interval.

        If an attempt to send a message to the server fails,
        the client will wait a random number of seconds. The maximum wait time is n*getBackoffInterVal()
        with n being the attempt counter.
        Can be controlled by changing the global variable L{ARA_CFG_CONN_BACKOFF}

        @rtype: integer
        @return: The maximum backoff interval
    */
   
    static function getBackoffInterval(){
        return ARA_CFG_CONN_BACKOFF;
    }
    /*Retrieve location of the server node with give node identifier

        A location is a pair consisting of a hostname or ip address as first element.
        The second element of the pair is the tcp port

        @type nodeId: string
        @param nodeId: The node identifier whose location you are interested in

        @rtype: pair(string,int)
        @return: Returns a pair with the nodes hostname or ip and the tcp port, e.g. ("127.0.0.1", 4000)
    */ 
    function getNodeLocation($nodeId){
        return $this->nodes[$nodeId ];

   
    }

    /*
        Retrieve the number of attempts a message should be tried before giving up

        Can be controlled by changing the global variable L{ARA_CFG_TRY_CNT}

        @rtype: integer
        @return: Returns the max retry count.
    */
    function getTryCount (){
        return ARA_CFG_TRY_CNT;
    }

    /*
        Retrieve the dictionary with node locations

        @rtype: dict
        @return: Returns a dictionary mapping the node identifiers (string) to its location ( pair<string,integer> )
    */
    function getNodes(){
        return $this->nodes;
    }
        
    function getClusterId(){
        return $this->clusterId;
    }

}


?>

from system_tests_common import *
import logging

@with_custom_setup(setup_2_nodes, basic_teardown)
def test_learner():
    iterate_n_times(54321, simple_set)
    cluster = q.manage.arakoon.getCluster(cluster_id)
    logging.info("adding learner")
    name = node_names[2]
    (db_dir, log_dir) = build_node_dir_names(name)
    cluster.addNode(name,
                    node_ips[2],
                    clientPort = node_client_base_port + 2,
                    messagingPort = node_msg_base_port + 2,
                    logDir = log_dir,
                    logLevel = 'debug',
                    home = db_dir,
                    isLearner = True,
                    targets = [node_names[0]])

    cluster.addLocalNode(name)
    cluster.createDirs(name)
    cluster.startOne(name)
    
    assert_running_nodes(3)
    time.sleep(60.0) # 1000/s in catchup should be no problem
    #use a client ??"
    stop_all()
    assert_last_i_in_sync( node_names[0], name )

    

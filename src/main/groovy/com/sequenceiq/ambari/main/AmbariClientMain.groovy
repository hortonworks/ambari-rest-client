package com.sequenceiq.ambari.main

import com.sequenceiq.ambari.client.AmbariClient

class AmbariClientMain {
    public static void main(String[] args) {
        def host = 'localhost'
        def port = '49156'
        if (args.size == 2) {
            host = args[0]
            port = args[1]
        }

        AmbariClient client = new AmbariClient(host, port)
        println "\n  clusterList: \n${client.clusterList()}"
        println "\n  hostsList: \n${client.hostList()}"
        println "\n  tasksList: \n${client.taskList()}"
        println "\n  serviceList: \n${client.serviceList()}"
        println "\n  blueprintList: \n${client.blueprintList()}"
        println "\n  getClusterBlueprint: \n${client.getClusterBlueprint()}"

    }
}

package com.sequenceiq.ambari.client

import groovy.json.JsonSlurper
import groovyx.net.http.RESTClient

class AmbariClient {

	def slurper = new JsonSlurper();
	def ambari
	
	AmbariClient(host = 'localhost', port = '8080', user = 'admin', password = 'admin') {
		ambari = new RESTClient( "http://${host}:${port}/api/v1/" as String )
		ambari.headers['Authorization'] = 'Basic '+"$user:$password".getBytes('iso-8859-1').encodeBase64()
	}
	
	def clusters() {
		slurper.parseText(ambari.get( path : "clusters", query: ['fields': 'Clusters/*' ] ).data.text)
	}
	
	def String clusterList() {
		clusters().items.collect{"[$it.Clusters.cluster_id] $it.Clusters.cluster_name:$it.Clusters.version"}.join("\n")
	}
	
	
	def tasks(task=1) {		
		slurper.parseText(ambari.get( path : "clusters/MySingleNodeCluster/requests/$task" , params: ['fields': 'tasks/Tasks/*'] ).data.text)
	}
	
	def tasksList(task=1) {
		tasks(task).tasks.collect{ "${it.Tasks.command_detail.padRight(30)} [${it.Tasks.status}]"}.join("\n")
		
	}
	
	def hosts() {
		slurper.parseText(ambari.get( path : "clusters/MySingleNodeCluster/hosts", query: ['fields':'Hosts/*']).data.text)
	}
	
	def hostsList() {
		hosts().items.collect{"$it.Hosts.host_name [$it.Hosts.host_status] $it.Hosts.ip $it.Hosts.os_type:$it.Hosts.os_arch"}.join("\n")
	}

	public static void main(String[] args) {
		
		
		def host = 'localhost'
		def port = '49156'
		if (args.size == 2) {
			host = args[0]
			port = args[1]
		}
		
		AmbariClient client = new AmbariClient(host, port)
		println "\n  clusterList: \n ${client.clusterList()}"
		println "\n  hostsList: \n ${client.hostsList()}"
		println "\n  tasksList: \n ${client.tasksList()}"
	}	
}

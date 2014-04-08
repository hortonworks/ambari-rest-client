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
	
	def String[] clusters() {
		def clusters = slurper.parseText(ambari.get( path : "clusters", params: ['fields': 'Clusters/*' ] ).data.text)
		clusters.items.collect{"[$it.Clusters.cluster_id] $it.Clusters.cluster_name:$it.Clusters.version"}
	}
	
	def String clusterList() {
		clusters().join("\n")
	}
	
	def String[] tasks(task=1) {		
		try {
		  def tt=slurper.parseText(ambari.get( path : "clusters/MySingleNodeCluster/requests/$task" , params: ['fields': 'tasks/Tasks/*'] ).data.text)
		  println "# of Tasks: ${tt.tasks.size}";
		  def taskList = tt.tasks.collect{ "${it.Tasks.command_detail.padRight(30)} [${it.Tasks.status}]"}
		} catch (Exception e) {
		  e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		
		
		def host = 'localhost'
		def port = '49156'
		if (args.size == 2) {
			host = args[0]
			port = args[1]
		}
		
		AmbariClient client = new AmbariClient(host, port)
		def tasks = client.tasks()
		tasks.each{
			println "# next task: $it"
		}
	}	
}

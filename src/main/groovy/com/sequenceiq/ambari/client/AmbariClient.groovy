package com.sequenceiq.ambari.client

import groovy.json.JsonSlurper
import groovyx.net.http.RESTClient

class AmbariClient {

	def slurper = new JsonSlurper();
	def RESTClient ambari
	def clusterName
	boolean debugEnabled = false;
	
	AmbariClient(host = 'localhost', port = '8080', user = 'admin', password = 'admin') {
		ambari = new RESTClient( "http://${host}:${port}/api/v1/" as String )
		ambari.headers['Authorization'] = 'Basic '+"$user:$password".getBytes('iso-8859-1').encodeBase64()
		clusterName = clusters().items[0].Clusters.cluster_name
	}
	
	def setDebugEnabled(boolean enabled) {
		debugEnabled = enabled;
	} 
	
	def String getClusterName() {
		clusterName
	}
	
	def slurp(path, fields) {
		if (debugEnabled) {
			def baseUri=ambari.getUri();
			println "[DEBUG] ${baseUri}${path}?fields=$fields"
		}
		
		slurper.parseText(ambari.get( path : "$path", query: ['fields':"$fields"]).data.text)
	}
	
	def getAllResources(resourceName, fields) {
		slurp("clusters/$clusterName/$resourceName", "$fields/*")
	}	
	
	def blueprints() {
		slurp("blueprints", "Blueprints")
	}
	
	def String getClusterBlueprint() {
		ambari.get( path : "clusters/$clusterName", query: ['format':"blueprint"]).data.text
	}
	
	def String blueprintList() {
		blueprints().items.collect{"${it.Blueprints.blueprint_name.padRight(30)} [${it.Blueprints.stack_name}:${it.Blueprints.stack_version}]"}.join("\n")
	}
	
	def clusters() {
		slurp("clusters", "Clusters")
	}
	
	def String clusterList() {
		clusters().items.collect{"[$it.Clusters.cluster_id] $it.Clusters.cluster_name:$it.Clusters.version"}.join("\n")
	}

	def tasks(request=1) {	
		getAllResources("requests/$request","tasks/Tasks")
	}
	
	def String taskList(request=1) {
		tasks(request).tasks.collect{ "${it.Tasks.command_detail.padRight(30)} [${it.Tasks.status}]"}.join("\n")
	}
	
	def hosts() {
		getAllResources("hosts", "Hosts")
	}
	
	def String hostList() {
		hosts().items.collect{"$it.Hosts.host_name [$it.Hosts.host_status] $it.Hosts.ip $it.Hosts.os_type:$it.Hosts.os_arch"}.join("\n")
	}
	
	def serviceComponents(service) {
		getAllResources("services/$service/components", "ServiceComponentInfo")
	}

	def String allServiceComponents() {
		services().items.collect{
			def name = it.ServiceInfo.service_name
			def state = it.ServiceInfo.state
			def componentList = serviceComponents(name).items.collect{"    ${it.ServiceComponentInfo.component_name.padRight(30)}  [$it.ServiceComponentInfo.state]"}.join("\n")
			"${name.padRight(30)} [$state]\n$componentList"
		}.join("\n")
	}
	
	def services() {
		getAllResources("services", "ServiceInfo")
	}
	
	def String serviceList() {
		services().items.collect{"${it.ServiceInfo.service_name.padRight(30)} [$it.ServiceInfo.state]"}.join("\n")
	}

	def hostComponents(host) {
		getAllResources("hosts/$host/host_components", "HostRoles")
	}
	
	def String hostComponentList(host) {
		hostComponents(host).items.collect{"${it.HostRoles.component_name.padRight(30)} [$it.HostRoles.state]"}.join("\n")
	}
	
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

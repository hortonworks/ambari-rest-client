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

	def calcMaxLength(table, fields) {
		def maxLength = [:]
		fields.each{field -> maxLength[field]=field.length()}
		
		table.each{ row ->
			fields.each{ field ->
				def actualFieldLength = row[field].toString().length()
				maxLength[field] = Math.max(maxLength[field], actualFieldLength)
			}
		}
		
		return maxLength
	}
		
	def resourceTable(table, fields) {
		def maxLength = calcMaxLength(table, fields)
		
		def lines = []
		lines << fields.collect{field-> field.center(maxLength[field]) }.join(" | ")
		lines << fields.collect{field-> "-".center(maxLength[field], '-') }.join("-+-")
		
		table.each{ row ->
			lines << fields.collect{ field -> row[field].toString().padRight(maxLength[field])}.join( " | ")
		}
		
		lines.collect{ line ->
			"| ${line} |"
		}.join("\n")
	}
	
	def resourceList(collection, resourceName, fields, itemListName = 'items') {
		def table = collection[itemListName].collect{ item->
			def row = [:]
			fields.each{ field-> 
				row[field] = item[resourceName][field]
			}
			return row
		}
		
		resourceTable(table, fields)
	}

	def blueprints() {
		slurp("blueprints", "Blueprints")
	}
	
	def String getClusterBlueprint() {
		ambari.get( path : "clusters/$clusterName", query: ['format':"blueprint"]).data.text
	}
	
	def String blueprintList() {
		resourceList(blueprints(), "Blueprints", ["blueprint_name", "stack_name", "stack_version"])
	}
	
	def clusters() {
		slurp("clusters", "Clusters")
	}
	
	def String clusterList() {
		resourceList(clusters(), "Clusters", ["cluster_id", "cluster_name", "version"])
	}

	def tasks(request=1) {	
		getAllResources("requests/$request","tasks/Tasks")
	}
	
	def String taskList(request=1) {
		resourceList(tasks(request), "Tasks", ["command_detail", "status"], "tasks")
	}
	
	def hosts() {
		getAllResources("hosts", "Hosts")
	}
	
	def String hostList() {
		resourceList(hosts(), "Hosts", ["host_name", "host_status", "os_type", "os_arch"])
	}
	
	def serviceComponents(service) {
		getAllResources("services/$service/components", "ServiceComponentInfo")
	}

	def String allServiceComponents() {
		def allComponents = services().items.collect{
			def name = it.ServiceInfo.service_name
			def componentList = serviceComponents(name).items
		}.flatten()

		def temp=[items: allComponents]
		resourceList(temp, "ServiceComponentInfo", ["service_name", "component_name", "state"])		
	}
	
	def services() {
		getAllResources("services", "ServiceInfo")
	}
	
	def String serviceList() {
		resourceList(services(), "ServiceInfo", ["service_name", "state"])
	}

	def hostComponents(host) {
		getAllResources("hosts/$host/host_components", "HostRoles")
	}
	
	def String hostComponentList(host) {
		resourceList(hostComponents(host), "HostRoles", ["component_name", "state"])
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
		println "\n allServiceComponents: ${client.allServiceComponents()}"
		
	}	
}

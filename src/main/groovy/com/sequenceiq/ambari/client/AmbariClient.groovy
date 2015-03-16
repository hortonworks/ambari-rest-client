/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sequenceiq.ambari.client

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import groovyx.net.http.ContentType
import groovyx.net.http.HttpResponseException
import groovyx.net.http.RESTClient
import org.apache.commons.io.IOUtils
import org.apache.http.NoHttpResponseException
import org.apache.http.client.ClientProtocolException
import java.net.ConnectException
import java.net.NoRouteToHostException
import java.net.UnknownHostException

/**
 * Basic client to send requests to the Ambari server.
 */
@Slf4j
class AmbariClient {

  private static final int PAD = 30
  private static final int OK_RESPONSE = 200
  private static final String SLAVE = "slave_"
  boolean debugEnabled = false;
  def RESTClient ambari
  def slurper = new JsonSlurper()
  def clusterName

  /**
   * Connects to the ambari server.
   *
   * @param host host of the Ambari server; default value is localhost
   * @param port port of the Ambari server; default value is 8080
   * @param user username of the Ambari server; default is admin
   * @param password password fom the Ambari server; default is admin
   */
  AmbariClient(host = 'localhost', port = '8080', user = 'admin', password = 'admin') {
    ambari = new RESTClient("http://${host}:${port}/api/v1/" as String)
    ambari.headers['Authorization'] = 'Basic ' + "$user:$password".getBytes('iso-8859-1').encodeBase64()
    ambari.headers['X-Requested-By'] = 'ambari'
  }

  /**
   * Connects to the ambari server.
   *
   * @param restClient underlying client
   * @param slurper slurper to parse responses
   */
  AmbariClient(RESTClient restClient, JsonSlurper slurper) {
    this.ambari = restClient
    this.slurper = slurper
  }

  /**
   * Sets the debug variable. Used by printing the API calls for the Ambari Shell.
   *
   * @param enabled enable or disable
   */
  def setDebugEnabled(boolean enabled) {
    debugEnabled = enabled;
  }

  /**
   * Returns the name of the cluster.
   *
   * @return the name of the cluster of null if no cluster yet
   */
  def String getClusterName() {
    if (!clusterName) {
      def clusters = getClusters();
      if (clusters) {
        clusterName = clusters.items[0]?.Clusters?.cluster_name
      }
    }
    return clusterName
  }

  /**
   * Adds the registered nodes to the cluster.
   *
   * @param hosts list of hostname to add
   * @throws HttpResponseException if a node is not registered with ambari
   */
  def addHosts(List<String> hosts) throws HttpResponseException {
    def requestBody = hosts.collectAll { ["Hosts": ["host_name": it]] }
    ambari.post(path: "clusters/${getClusterName()}/hosts", body: new JsonBuilder(requestBody).toPrettyString(), { it })
  }

  /**
   * Returns the state of the host.
   *
   * @param host host's internal hostname
   * @return state of the host
   */
  String getHostState(String host) {
    getAllResources("hosts/$host").Hosts.host_status
  }

  /**
   * Create a new Ambari user.
   */
  def createUser(String user, String password, boolean admin) {
    def context = ["Users/active": true, "Users/admin": admin, "Users/password": password, "Users/user_name": user]
    ambari.post(path: "users", body: new JsonBuilder(context).toPrettyString(), { it })
  }

  /**
   * Delete an Ambari user.
   */
  def deleteUser(String user) {
    ambari.delete(path: "users/$user")
  }

  /**
   * Change the password of an Ambari user.
   */
  def changePassword(String user, String oldPassword, String newPassword, boolean admin) {
    def roles = ["user"]
    if (admin) {
      roles << "admin"
    }
    def context = ["Users/password": newPassword, "Users/old_password": oldPassword]
    ambari.put(path: "users/$user", body: new JsonBuilder(context).toPrettyString(), requestContentType: ContentType.URLENC)
  }

  /**
   * Runs a MapReduce service check which is a simple WordCount.
   * @return id of the request
   */
  def int runMRServiceCheck() {
    runServiceCheck("MAPREDUCE2_SERVICE_CHECK", "MAPREDUCE2")
  }

  /**
   * Run a service check.
   *
   * @param command command to run
   * @param serviceName name of the service
   * @return id of the request
   */
  def int runServiceCheck(String command, String serviceName) {
    Map bodyMap = [
      "RequestInfo"              : [command: command, context: command],
      "Requests/resource_filters": [[service_name: serviceName]]
    ]
    ambari.post(path: "clusters/${getClusterName()}/requests", body: new JsonBuilder(bodyMap).toPrettyString(), {
      getRequestId(it)
    })
  }

  /**
   * Decommission and remove a host from the cluster.
   * NOTE: this is a synchronous call, it wont return until all
   * requests are finished
   *
   * Steps:
   *  1, decommission services
   *  2, stop services
   *  3, delete host components
   *  4, delete host
   *  5, restart services
   *
   * @param hostName host to be deleted
   */
  def removeHost(String hostName) {
    def components = getHostComponentsMap(hostName).keySet() as List

    // decommission
    if (components.contains("NODEMANAGER")) {
      decommissionNodeManagers([hostName])
    }
    if (components.contains("DATANODE")) {
      decommissionDataNodes([hostName])
    }

    // stop services
    def requests = stopComponentsOnHost(hostName, components)
    waitForRequestsToFinish(requests.values() as List)

    // delete host components
    deleteHostComponents(hostName, components)

    // delete host
    deleteHost(hostName)

    // restart zookeper
    def id = restartServiceComponents("ZOOKEEPER", ["ZOOKEEPER_SERVER"])
    waitForRequestsToFinish([id])

    // restart nagios
    if (getServiceComponentsMap().containsKey("NAGIOS")) {
      id = restartServiceComponents("NAGIOS", ["NAGIOS_SERVER"])
      waitForRequestsToFinish([id])
    }
  }

  /**
   * Does not return until all the requests are finished.
   * @param requestIds ids of the requests
   */
  def waitForRequestsToFinish(List<Integer> requestIds) {
    def stopped = false
    while (!stopped) {
      def state = true
      for (int id : requestIds) {
        if (getRequestProgress(id) != 100.0) {
          state = false;
          break;
        }
      }
      stopped = state
      Thread.sleep(2000)
    }
  }

  /**
   * Decommission the data node on the given hosts.
   */
  def int decommissionDataNodes(List<String> hosts) {
    decommission(hosts, "DATANODE", "HDFS", "NAMENODE")
  }

  /**
   * Decommission the node manager on the given hosts.
   */
  def int decommissionNodeManagers(List<String> hosts) {
    decommission(hosts, "NODEMANAGER", "YARN", "RESOURCEMANAGER")
  }

  /**
   * Decommission the HBase Region Server on the given hosts.
   */
  def int decommissionHBaseRegionServers(List<String> hosts) {
    decommission(hosts, "HBASE_REGIONSERVER", "HBASE", "HBASE_MASTER")
  }

  /**
   * Decommission a host component on a given host.
   * @param host hostName where the component is installed to
   * @param slaveName slave to be decommissioned
   * @param serviceName where the slave belongs to
   * @param componentName where the slave belongs to
   */
  def int decommission(List<String> hosts, String slaveName, String serviceName, String componentName) {
    def requestInfo = [
      command   : "DECOMMISSION",
      context   : "Decommission $slaveName",
      parameters: ["slave_type": slaveName, "excluded_hosts": hosts.join(',')]
    ]
    def filter = [
      ["service_name": serviceName, "component_name": componentName]
    ]
    Map bodyMap = [
      "RequestInfo"              : requestInfo,
      "Requests/resource_filters": filter
    ]
    ambari.post(path: "clusters/${getClusterName()}/requests", body: new JsonBuilder(bodyMap).toPrettyString(), {
      getRequestId(it)
    })
  }

  /**
   * Returns the name of the node and the size of the replicated block that are remaining.
   *
   * @return Map where the key is the internal host name of the node and the value
   *         is size of the replicated block
   */
  def Map<String, Long> getDecommissioningDataNodes() {
    def result = [:]
    def response = slurp("clusters/${getClusterName()}/services/HDFS/components/NAMENODE", "metrics/dfs/namenode/DecomNodes")
    def nodes = slurper.parseText(response?.metrics?.dfs?.namenode?.DecomNodes)
    if (nodes) {
      nodes.each {
        result << [(it.key): it.value.underReplicatedBlocks as Long]
      }
    }
    result
  }

  /**
   * Creates a new alert definition.
   *
   * @param definition alert definition as json
   */
  def void createAlert(String definition) {
    ambari.post(path: "clusters/${getClusterName()}/alert_definitions", body: definition, { it })
  }

  /**
   * Returns the alert definitions.
   *
   * @return collection of definitions
   */
  def List<Map<String, String>> getAlertDefinitions() {
    getAllResources("alert_definitions", "AlertDefinition").items.collect {
      def details = [:]
      def definition = it.AlertDefinition
      details << ["enabled": definition.enabled]
      details << ["scope":  definition.scope]
      details << ["interval": definition.interval as String]
      details << ["description": definition.description]
      details << ["name": definition.name]
      details << ["label": definition.label]
      details << ["service_name": definition.service_name]
      details
    }
  }

  /**
   * Returns all the defined alerts grouped by alert definition name.
   * A filter can be applied for scopes: HOST, ANY, SERVICE
   *
   * @return alert properties
   */
  def Map<String, List<Map<String, Object>>> getAlerts(List<String> scope = []) {
    getAllResources("alerts", "Alert").items.findAll {
      scope == [] || scope.contains(it.Alert.scope)
    }.collect { it.Alert }.groupBy { it.definition_name }
  }

  /**
   * Get a specified alert by id.
   *
   * @param id id of the alert
   * @return alert or empty collection
   */
  Map<String, Object> getAlert(int id) {
    def response = getAllResources("alerts/$id")
    response ? response.Alert : [:]
  }

  /**
   * Get a specified alert by name. Can return multiple
   * values if the alert is HOST based.
   *
   * @param definitionName alert definition name
   * @return list of alerts or empty collection
   */
  List<Map<String, Object>> getAlert(String definitionName) {
    getAlerts()[definitionName] ?: []
  }

  /**
   * Get a specified alert history by id.
   *
   * @param id id of the alert history
   * @return history or empty collection
   */
  Map<String, Object> getAlertHistory(int id) {
    def response = getAllResources("alert_history/$id")
    response ? response.AlertHistory : [:]
  }

  /**
   * Returns the history of a certain alert.
   *
   * @param alertDefinition alert definition name
   * @param count desired number of result from the latest one
   * @return list of alert properties or empty collection
   */
  def List<Map<String, Object>> getAlertHistory(String alertDefinition, int count) {
    def result = []
    def prePredicate = ["AlertHistory/definition_name": alertDefinition]
    def items = getAllPredictedResources("alert_history", prePredicate).items
    if (items) {
      def itemSize = items.size
      def from = itemSize - count > -1 ? itemSize - count : 0
      from = from == itemSize ? itemSize - 1 : from
      items[from..itemSize - 1].collect { it.AlertHistory.id }.each {
        result << getAlertHistory(it)
      }
    }
    result
  }

  /**
   * Returns the nodes by DFS an their data allocations.
   *
   * @return a Map where the key is the internal host name and the value
   *         is a Map where the key is the remaining space and the value is the used space in bytes
   */
  def Map<String, Map<Long, Long>> getDFSSpace() {
    def result = [:]
    def response = slurp("clusters/${getClusterName()}/services/HDFS/components/NAMENODE", "metrics/dfs")
    def liveNodes = slurper.parseText(response?.metrics?.dfs?.namenode?.LiveNodes)
    if (liveNodes) {
      liveNodes.each {
        if (it.value.adminState == "In Service") {
          result << [(it.key): [(it.value.remaining as Long): it.value.usedSpace as Long]]
        }
      }
    }
    result
  }

  /**
   * Deletes the components from the host.
   */
  def deleteHostComponents(String hostName, List<String> components) {
    components.each {
      ambari.delete(path: "clusters/${getClusterName()}/hosts/$hostName/host_components/$it")
    }
  }

  /**
   * Deletes the host from the cluster.
   * Note: Deleting a host from a cluster does not mean it is also
   * deleted/unregistered from Ambari. It will remain there with UNKNOWN state.
   */
  def deleteHost(String hostName) {
    ambari.delete(path: "clusters/${getClusterName()}/hosts/$hostName")
  }

  /**
   * Deletes the host from Ambari.
   * Note: Deleting a host from a cluster does not mean it is also
   * deleted/unregistered from Ambari. It will remain there with UNKNOWN state.
   */
  def unregisterHost(String hostName) {
    ambari.delete(path: "hosts/$hostName")
  }

  /**
   * Install all the components from a given blueprint's host group. The services must be installed
   * in order to install its components. It is recommended to use the same blueprint's host group from which
   * the cluster was created.
   *
   * @param hostNames components will be installed on these hosts
   * @param blueprint id of the blueprint
   * @param hostGroup host group of the blueprint
   * @return request id since its an async call
   */
  def int installComponentsToHosts(List<String> hostNames, String blueprint, String hostGroup) throws HttpResponseException {
    def bpMap = getBlueprint(blueprint)
    def components = bpMap?.host_groups?.find { it.name.equals(hostGroup) }?.components?.collect { it.name }
    components ? installComponentsToHosts(hostNames, components) : -1
  }

  /**
   * Installs the given components to the given hosts.
   * Only existing service components can be installed.
   *
   * @param hostNames hosts to install the component to
   * @param components components to be installed
   * @throws HttpResponseException in case the component's service is not installed
   * @return request id since its an async call
   */
  def int installComponentsToHosts(List<String> hostNames, List<String> components) throws HttpResponseException {
    def clusterName = getClusterName()
    def commaSepHostNames = hostNames.join(',')
    def addRequest = [
      "RequestInfo": ["query": "Hosts/host_name.in($commaSepHostNames)"],
      "Body"       : ["host_components": components.collectAll { ["HostRoles": ["component_name": it]] }]
    ]
    ambari.post(path: "clusters/${getClusterName()}/hosts", body: new JsonBuilder(addRequest).toPrettyString(), { it })
    setAllComponentsState(clusterName, hostNames, "INSTALLED", "Install components")
  }

  /**
   * Starts the given components on a host.
   * To start all components on hosts use the {@link #startAllServices} method.
   *
   * @return map of the component names and their request id since its an async call
   * @throws HttpResponseException in case the component is not found
   */
  def Map<String, Integer> startComponentsOnHost(String hostName, List<String> components) throws HttpResponseException {
    setComponentsState(hostName, components, "STARTED")
  }

  /**
   * Stops the given components on a host.
   *
   * @return map of the component names and their request id since its an async call
   * @throws HttpResponseException in case the component is not found
   */
  def Map<String, Integer> stopComponentsOnHost(String hostName, List<String> components) throws HttpResponseException {
    setComponentsState(hostName, components, "INSTALLED")
  }

  /**
   * Stops all the components on a host.
   *
   * @return request id since its an async call
   * @throws HttpResponseException in case the component is not found
   */
  def int stopAllComponentsOnHosts(List<String> hostNames) throws HttpResponseException {
    setAllComponentsState(getClusterName(), hostNames, "INSTALLED", "Stop all components")
  }

  /**
   * Checks whether the blueprint exists or not.
   *
   * @param id id of the blueprint
   * @return true if exists false otherwise
   */
  def boolean doesBlueprintExist(String id) {
    def result = false
    try {
      def Map resourceRequest = getResourceRequestMap("blueprints/$id", ['fields': "Blueprints"])
      def jsonResponse = getSlurpedResource(resourceRequest)
      result = !(jsonResponse.status)
    } catch (e) {
      log.info("Blueprint does not exist", e)
    }
    return result
  }

  /**
   * Checks whether there are available blueprints or not.
   *
   * @return true if blueprints are available false otherwise
   */
  def boolean isBlueprintAvailable() {
    return getBlueprints().items?.size > 0
  }

  /**
   * Returns a pre-formatted String of the blueprint.
   *
   * @param id id of the blueprint
   * @return formatted String
   */
  def String showBlueprint(String id) {
    def resp = getBlueprint(id)
    if (resp) {
      def groups = resp.host_groups.collect {
        def name = it.name
        def comps = it.components.collect { it.name.padRight(PAD).padLeft(PAD + 10) }.join("\n")
        return "HOSTGROUP: $name\n$comps"
      }.join("\n")
      return "[${resp.Blueprints.stack_name}:${resp.Blueprints.stack_version}]\n$groups"
    }
    return "Not found"
  }

  /**
   * Returns the host group - components mapping of the blueprint.
   *
   * @param id id of the blueprint
   * @return Map where the key is the host group and the value is the list of components
   */
  def Map<String, List<String>> getBlueprintMap(String id) {
    def result = getBlueprint(id)?.host_groups?.collectEntries { [(it.name): it.components.collect { it.name }] }
    result ?: [:]
  }

  /**
   * Returns a pre-formatted String of the blueprints.
   *
   * @return formatted blueprint list
   */
  def String showBlueprints() {
    getBlueprints().items.collect {
      "${it.Blueprints.blueprint_name.padRight(PAD)} [${it.Blueprints.stack_name}:${it.Blueprints.stack_version}]"
    }.join("\n")
  }

  /**
   * Returns a Map containing the blueprint name - stack association.
   *
   * @return Map where the key is the blueprint's name value is the used stack
   */
  def Map<String, String> getBlueprintsMap() {
    def result = getBlueprints().items?.collectEntries {
      [(it.Blueprints.blueprint_name): it.Blueprints.stack_name + ":" + it.Blueprints.stack_version]
    }
    result ?: new HashMap()
  }

  /**
   * Recommends a host - host group assignment based on the provided blueprint
   * and the available hosts.
   *
   * @param blueprint id of the blueprint
   * @return recommended assignments
   */
  def Map<String, List<String>> recommendAssignments(String blueprint) throws InvalidHostGroupHostAssociation {
    def result = [:]
    def hostNames = getHostStatuses().keySet() as List
    def groups = getBlueprint(blueprint)?.host_groups?.collect { ["name": it.name, "cardinality": it.cardinality] }
    if (hostNames && groups) {
      def groupSize = groups.size()
      def hostSize = hostNames.size()
      if (hostSize == 1 && groupSize == 1) {
        result = [(groups[0].name): [hostNames[0]]]
      } else if (hostSize >= groupSize) {
        int i = 0
        groups.findAll { !it.name.toLowerCase().startsWith(SLAVE) }.each {
          result << [(it.name): [hostNames[i++]]]
        }
        def slaves = groups.findAll { it.name.toLowerCase().startsWith(SLAVE) }
        if (slaves) {
          int k = 0
          for (int j = i; j < hostSize; j++) {
            result[slaves[k].name] = result[slaves[k].name] ?: []
            result[slaves[k].name] << hostNames[j]
            result << [(slaves[k].name): result[slaves[k++].name]]
            k = k == slaves.size ? 0 : k
          }
        } else {
          throw new InvalidHostGroupHostAssociation("At least one '$SLAVE' is required", groupSize)
        }
      } else {
        throw new InvalidHostGroupHostAssociation("At least $groupSize host is required", groupSize)
      }
    }
    return result
  }

  /**
   * Returns the name of the host groups for a given blueprint.
   *
   * @param blueprint id of the blueprint
   * @return host group list or empty list
   */
  def List<String> getHostGroups(String blueprint) {
    def result = getBlueprint(blueprint)
    result ? result.host_groups.collect { it.name } : new ArrayList<String>()
  }

  /**
   * Returns a pre-formatted String of a blueprint used by the cluster.
   *
   * @return formatted String
   */
  def String showClusterBlueprint() {
    ambari.get(path: "clusters/${getClusterName()}", query: ['format': "blueprint"]).data.text
  }

  /**
   * Adds a blueprint to the Ambari server. Exception is thrown if fails.
   *
   * @param json blueprint as json
   * @throws HttpResponseException in case of error
   */
  def String addBlueprint(String json) throws HttpResponseException {
    addBlueprint(json, [:])
  }

  def String addBlueprint(String json, Map<String, Map<String, String>> configurations) throws HttpResponseException {
    if (json) {
      def text = slurper.parseText(json)
      def bpMap = extendBlueprintConfiguration(text, configurations)
      def builder = new JsonBuilder(bpMap)
      def resultJson = builder.toPrettyString()
      postBlueprint(resultJson)
      resultJson
    }
  }

  def String addBlueprintWithHostgroupConfiguration(String json, Map<String, Map<String, Map<String, String>>> configurations) throws HttpResponseException {
    if (json) {
      def text = slurper.parseText(json)
      def bpMap = extendBlueprintHostGroupConfiguration(text, configurations)
      def builder = new JsonBuilder(bpMap)
      def resultJson = builder.toPrettyString()
      postBlueprint(resultJson)
      resultJson
    }
  }

  /**
   * Only validates the multinode blueprints, at least 1 slave host group must exist.
   * Throws an exception if the blueprint is not valid.
   *
   * @param json blueprint json
   * @throws InvalidBlueprintException if the blueprint is not valid
   */
  def void validateBlueprint(String json) throws InvalidBlueprintException {
    if (json) {
      def bpMap = slurper.parseText(json)
      if (bpMap?.host_groups?.size > 1) {
        def find = bpMap.host_groups.find { it.name.toLowerCase().startsWith(SLAVE) }
        if (!find) {
          throw new InvalidBlueprintException("At least one '$SLAVE' host group is required.")
        }
      }
      if (isComponentPresent(bpMap, "NAGIOS_SERVER")) {
        if (bpMap?.configurations?.findAll { it?."nagios-env"?.nagios_contact }?.size() != 1) {
          throw new InvalidBlueprintException("We are no longer supporting 1.6 blueprints");
        }
      }
    } else {
      throw new InvalidBlueprintException("No blueprint specified")
    }
  }

  /**
   * Adds the default blueprints.
   *
   * @throws HttpResponseException in case of error
   */
  def void addDefaultBlueprints() throws HttpResponseException {
    addBlueprint(getResourceContent("blueprints/multi-node-hdfs-yarn"))
    addBlueprint(getResourceContent("blueprints/single-node-hdfs-yarn"))
    addBlueprint(getResourceContent("blueprints/lambda-architecture"))
    addBlueprint(getResourceContent("blueprints/hdp-singlenode-default"))
    addBlueprint(getResourceContent("blueprints/hdp-multinode-default"))
  }

  /**
   * Creates a cluster with the given blueprint and host group - host association.
   *
   * @param clusterName name of the cluster
   * @param blueprintName blueprint id used to create this cluster
   * @param hostGroups Map<String, List<String> key - host group, value - host list
   * @return true if the creation was successful false otherwise
   * @throws HttpResponseException in case of error
   */
  def void createCluster(String clusterName, String blueprintName, Map<String, List<String>> hostGroups) throws HttpResponseException {
    if (debugEnabled) {
      println "[DEBUG] POST ${ambari.getUri()}clusters/$clusterName"
    }
    ambari.post(path: "clusters/$clusterName", body: createClusterJson(blueprintName, hostGroups), { it })
  }

  /**
   * Deletes the cluster.
   *
   * @param clusterName name of the cluster
   * @throws HttpResponseException in case of error
   */
  def void deleteCluster(String clusterName) throws HttpResponseException {
    if (debugEnabled) {
      println "[DEBUG] DELETE ${ambari.getUri()}clusters/$clusterName"
    }
    ambari.delete(path: "clusters/$clusterName")
  }

  /**
   * Returns the active cluster as json
   *
   * @return cluster as json String
   * @throws HttpResponseException in case of error
   */
  def String getClusterAsJson() throws HttpResponseException {
    String path = "clusters/" + getClusterName();
    Map resourceRequestMap = getResourceRequestMap(path, null)
    return getRawResource(resourceRequestMap)
  }

  /**
   * Returns all clusters as json
   *
   * @return json String
   * @throws HttpResponseException in case of error
   */
  def getClustersAsJson() throws HttpResponseException {
    Map resourceRequestMap = getResourceRequestMap("clusters", null)
    return getRawResource(resourceRequestMap)
  }

  /**
   * Modify an existing configuration. Be ware you'll have to provide the whole configuration
   * otherwise properties might get lost.
   *
   * @param type type of the configuration e.g capacity-scheduler
   * @param properties properties to be used
   */
  def modifyConfiguration(String type, Map<String, String> properties) {
    Map bodyMap = [
      "Clusters": ["desired_config": ["type": type, "tag": "version${System.currentTimeMillis()}", "properties": properties]]
    ]
    def Map<String, ?> putRequestMap = [:]
    putRequestMap.put('requestContentType', ContentType.URLENC)
    putRequestMap.put('path', "clusters/${getClusterName()}")
    putRequestMap.put('body', new JsonBuilder(bodyMap).toPrettyString());
    ambari.put(putRequestMap)
  }

  /**
   * Returns a pre-formatted String of the clusters.
   *
   * @return pre-formatted cluster list
   */
  def String showClusterList() {
    getClusters().items.collect {
      "[$it.Clusters.cluster_id] $it.Clusters.cluster_name:$it.Clusters.version"
    }.join("\n")
  }

  /**
   * Returns the task properties as Map.
   *
   * @param request request id; default is 1
   * @return property Map or empty Map
   */
  def getTasks(request = 1) {
    getAllResources("requests/$request", "tasks/Tasks")
  }

  /**
   * Returns the install progress state. If the install failed -1 returned.
   *
   * @param request request id; default is 1
   * @return progress in percentage
   */
  def BigDecimal getRequestProgress(request = 1) {
    def response = getAllResources("requests/$request", "Requests")
    def String status = response?.Requests?.request_status
    if (status && status.equals("FAILED")) {
      return new BigDecimal(-1)
    }
    return response?.Requests?.progress_percent
  }

  /**
   * Returns a pre-formatted task list.
   *
   * @param request request id; default is 1
   * @return pre-formatted task list
   */
  def String showTaskList(request = 1) {
    getTasks(request)?.tasks.collect { "${it.Tasks.command_detail.padRight(PAD)} [${it.Tasks.status}]" }.join("\n")
  }

  /**
   * Returns a Map containing the task's command detail as key and the task's status as value.
   *
   * @param request request id; default is 1
   * @return key task command detail; task value status
   */
  def Map<String, String> getTaskMap(request = 1) {
    def result = getTasks(request).tasks?.collectEntries { [(it.Tasks.command_detail): it.Tasks.status] }
    result ?: new HashMap()
  }

  /**
   * Returns the available host names and their states. It also
   * contains hosts which are not part of the cluster, but are connected
   * to ambari.
   *
   * @return hostname state association
   */
  def Map<String, String> getHostStatuses() {
    getHosts().items.collectEntries { [(it.Hosts.host_name): it.Hosts.host_status] }
  }

  def Map<String, String> getHostNames() {
    getHosts().items.collectEntries { [(it.Hosts.public_host_name): it.Hosts.host_name] }
  }

  /**
   * Returns the names of the hosts which have the given state. It also
   * contains hosts which are not part of the cluster, but are connected
   * to ambari.
   */
  def Map<String, String> getHostNamesByState(String state) {
    getHostStatuses().findAll { it.value == state }
  }

  /**
   * Returns a pre-formatted list of the hosts.
   *
   * @return pre-formatted String
   */
  def String showHostList() {
    getHosts().items.collect {
      "$it.Hosts.host_name [$it.Hosts.host_status] $it.Hosts.ip $it.Hosts.os_type:$it.Hosts.os_arch"
    }.join("\n")
  }

  /**
   * Returns a pre-formatted list of the service components.
   *
   * @return pre-formatted String
   */
  def String showServiceComponents() {
    getServices().items.collect {
      def name = it.ServiceInfo.service_name
      def state = it.ServiceInfo.state
      def componentList = getServiceComponents(name).items.collect {
        "    ${it.ServiceComponentInfo.component_name.padRight(PAD)}  [$it.ServiceComponentInfo.state]"
      }.join("\n")
      "${name.padRight(PAD)} [$state]\n$componentList"
    }.join("\n")
  }

  /**
   * Returns the service components properties as Map where the key is the service name and
   * value is a component - state association.
   *
   * @return service name - [component name - status]
   */
  def Map<String, Map<String, String>> getServiceComponentsMap() {
    def result = getServices().items.collectEntries {
      def name = it.ServiceInfo.service_name
      def componentList = getServiceComponents(name).items.collectEntries {
        [(it.ServiceComponentInfo.component_name): it.ServiceComponentInfo.state]
      }
      [(name): componentList]
    }
    result ?: new HashMap()
  }

  /**
   * Returns all the components states grouped by hosts.
   *
   * @return host name - [component name - state]
   */
  def Map<String, Map<String, String>> getHostComponentsStates() {
    getAllResources("hosts", "host_components/HostRoles/state").items.collectEntries {
      def hostName = it.Hosts.host_name
      def components = it.host_components.collectEntries {
        [(it.HostRoles.component_name): it.HostRoles.state]
      }
      [(hostName): components]
    }
  }

  /**
   * Returns the type of the components of a given host group in a given blueprint.
   * There are 3 types: SLAVE, CLIENT, MASTER
   *
   * @param blueprintId if of the blueprint
   * @param hostGroup host group's name in the blueprint
   * @return map where the key is the component's name and the value is the category
   */
  def Map<String, String> getComponentsCategory(String blueprintId, String hostGroup) {
    def bpMap = getBlueprint(blueprintId)
    def components = bpMap?.host_groups?.find { it.name.equals(hostGroup) }?.components?.collect { it.name }
    getComponentsCategory(components)
  }

  /**
   * Returns the type of the components of a given blueprint.
   * There are 3 types: SLAVE, CLIENT, MASTER
   *
   * @param blueprintId if of the blueprint
   * @return map where the key is the component's name and the value is the category
   */
  def Map<String, String> getComponentsCategory(String blueprintId) {
    def bpMap = getBlueprint(blueprintId)
    def components = bpMap?.host_groups?.components?.name?.flatten()
    getComponentsCategory(components)
  }

  /**
   * Returns the type of the components.
   * There are 3 types: SLAVE, CLIENT, MASTER
   *
   * @param components list of the components
   * @return map where the key is the component's name and the value is the category
   */
  def Map<String, String> getComponentsCategory(List<String> components) {
    def result = [:]
    components.each {
      def json = slurp("clusters/${getClusterName()}/components/$it", "ServiceComponentInfo")
      def category = json?.ServiceComponentInfo?.category
      if (category) {
        result << [(it): category]
      }
    }
    result
  }

  /**
   * Performs a health check on the Ambari server.
   *
   * @return status
   */
  def String healthCheck() {
    ambari.get(path: "check", headers: ["Accept": ContentType.TEXT]).data.text
  }

  /**
   * Returns a pre-formatted service list.
   *
   * @return formatted String
   */
  def String showServiceList() {
    getServices().items.collect { "${it.ServiceInfo.service_name.padRight(PAD)} [$it.ServiceInfo.state]" }.join("\n")
  }

  /**
   * Returns the services properties as Map where the key is the service's name and the values is the service's state.
   *
   * @return service name - service state association as Map
   */
  def Map<String, String> getServicesMap() {
    def result = getServices().items.collectEntries { [(it.ServiceInfo.service_name): it.ServiceInfo.state] }
    result ?: new HashMap()
  }

  /**
   * Returns a pre-formatted component list of a host.
   *
   * @param host which host's components are requested
   * @return formatted String
   */
  def String showHostComponentList(host) {
    getHostComponents(host).items.collect {
      "${it.HostRoles.component_name.padRight(PAD)} [$it.HostRoles.state]"
    }.join("\n")
  }

  /**
   * Returns the host's components as Map where the key is the component's name and values is its state.
   *
   * @param host which host's components are requested
   * @return component name - state association
   */
  def Map<String, String> getHostComponentsMap(host) {
    def result = getHostComponents(host)?.items?.collectEntries { [(it.HostRoles.component_name): it.HostRoles.state] }
    result ?: [:]
  }

  /**
   * Returns the blueprint json as String.
   *
   * @param id id of the blueprint
   * @return json as String, exception if thrown is it fails
   */
  def String getBlueprintAsJson(id) {
    Map resourceRequestMap = getResourceRequestMap("blueprints/$id", ['fields': "host_groups,Blueprints"])
    return getRawResource(resourceRequestMap)
  }

  /**
   * Returns a map with service configurations. The keys are the service names, values are maps with <propertyName, propertyValue> entries
   *
   * @return a Map with entries of format <servicename, Map<property, value>>
   */
  def Map<String, Map<String, String>> getServiceConfigMap(String type = "") {
    def Map<String, Integer> serviceToTags = new HashMap<>()

    //get services and last versions configurations
    def path = "clusters/${getClusterName()}/configurations"
    Map<String, ?> configsResourceRequestMap = getResourceRequestMap(path, type ? ["type": type] : [:])
    def rawConfigs = getSlurpedResource(configsResourceRequestMap)

    rawConfigs?.items.collect { object ->
      // selecting the latest versions
      if (object.tag.isLong()) {
        processServiceVersions(serviceToTags, object.type, object.tag)
      }
    }

    // collect properties for every service
    def finalMap = serviceToTags.collectEntries { entry ->
      // collect config props for every service
      def propsMap = collectConfigPropertiesForService(entry.getKey(), entry.getValue())
      // put them in the final map
      [(entry.key): propsMap]
    }
    return finalMap
  }

  def Map<String, Map<String, String>> getServiceConfigMapByHostGroup(String hostGroup) {
    def Map<String, List<String>> serviceToTags = new HashMap<>()

    //get services and last versions configurations
    def path = "clusters/${getClusterName()}/configurations"
    Map<String, ?> configsResourceRequestMap = getResourceRequestMap(path, [:])
    def rawConfigs = getSlurpedResource(configsResourceRequestMap)

    rawConfigs?.items.collect { object ->
      // selecting the latest versions
      processServiceVersionsByHostGroup(serviceToTags, object.type.toString(), object.version, object.tag, hostGroup)
    }

    // collect properties for every service
    def finalMap = serviceToTags.collectEntries { entry ->
      // collect config props for every service
      def propsMap = collectConfigPropertiesForServiceByList(entry.getKey(), entry.getValue())
      // put them in the final map
      [(entry.key): propsMap]
    }
    return finalMap
    return finalMap
  }

  /**
   * Starts all the services.
   *
   * @return id of the request since its an async call
   */
  def int startAllServices() {
    log.debug("Starting all services ...")
    manageService("Start All Services", "STARTED")
  }

  /**
   * Stops all the services.
   *
   * @return id of the request since its an async call
   */
  def int stopAllServices() {
    log.debug("Stopping all services ...")
    manageService("Stop All Services", "INSTALLED")
  }

  /**
   * Starts the given service.
   *
   * @param service name of the service
   * @return id of the request
   */
  def int startService(String service) {
    manageService("Starting $service", "STARTED", service)
  }

  /**
   * Stops the given service.
   *
   * @param service name of the service
   * @return id of the request
   */
  def int stopService(String service) {
    manageService("Stopping $service", "INSTALLED", service)
  }

  def boolean servicesStarted() {
    return servicesStatus(true)
  }

  def boolean servicesStopped() {
    return servicesStatus(false)
  }

  /**
   * Returns the public hostnames of the hosts which the host components are installed to.
   */
  def List<String> getPublicHostNames(String hostComponent) {
    def hosts = getHostNamesByComponent(hostComponent)
    if (hosts) {
      return hosts.collect() { resolveInternalHostName(it) }
    } else {
      return []
    }
  }

  /**
   * Returns the internal host names of the hosts which the host components are installed to.
   */
  def List<String> getHostNamesByComponent(String component) {
    def hostRoles = getAllResources("host_components", "HostRoles/component_name")
    def hosts = hostRoles?.items?.findAll { it.HostRoles.component_name.equals(component) }?.HostRoles?.host_name
    hosts ?: []
  }

  /**
   * Restarts the given components of a service.
   */
  def int restartServiceComponents(String service, List<String> components) {
    def filter = components.collect {
      ["service_name": service, "component_name": it, "hosts": getHostNamesByComponent(it).join(",")]
    }
    Map bodyMap = [
      "RequestInfo"              : [command: "RESTART", context: "Restart $service components $components"],
      "Requests/resource_filters": filter
    ]
    ambari.post(path: "clusters/${getClusterName()}/requests", body: new JsonBuilder(bodyMap).toPrettyString(), {
      getRequestId(it)
    })
  }

  /**
   * Returns the names of the hosts which are in the cluster.
   */
  def List<String> getClusterHosts() {
    slurp("clusters/${getClusterName()}")?.hosts?.Hosts?.host_name
  }

  /**
   * Resolves an internal hostname to a public one.
   */
  def String resolveInternalHostName(String internalHostName) {
    slurp("clusters/${getClusterName()}/hosts/$internalHostName")?.Hosts?.public_host_name
  }

  def private boolean servicesStatus(boolean starting) {
    def String status = (starting) ? "STARTED" : "INSTALLED"
    Map serviceComponents = getServicesMap();
    boolean allInState = true;
    serviceComponents.values().each { val ->
      log.debug("Service: {}", val)
      allInState = allInState && val.equals(status)
    }
    return allInState;
  }

  def private manageService(String context, String state, String service = "") {
    Map bodyMap = [
      RequestInfo: [context: context],
      ServiceInfo: [state: state]
    ]
    JsonBuilder builder = new JsonBuilder(bodyMap)
    def path = "${ambari.getUri()}clusters/${getClusterName()}/services"
    if (service) {
      path += "/$service"
    }
    def Map<String, ?> putRequestMap = [:]
    putRequestMap.put('requestContentType', ContentType.URLENC)
    putRequestMap.put('path', path)
    putRequestMap.put('query', ['params/run_smoke_test': 'false'])
    putRequestMap.put('body', builder.toPrettyString());

    def reponse = ambari.put(putRequestMap)
    slurper.parseText(reponse.getAt("responseData")?.getAt("str"))?.Requests?.id
  }

  private def processServiceVersions(Map<String, Integer> serviceToVersions, String service, def version) {
    boolean change = false
    log.debug("Handling service version <{}:{}>", service, version)
    if (serviceToVersions.containsKey(service)) {
      log.debug("Entry already added, checking versions ...")
      def newVersion = Long.valueOf(version.minus("version")).longValue()
      def oldVersion = Long.valueOf(serviceToVersions.get(service).minus("version")).longValue()
      change = oldVersion < newVersion
    } else {
      change = true;
    }
    if (change) {
      log.debug("Adding / updating service version <{}:{}>", service, version)
      serviceToVersions.put(service, version);
    }
  }

  private def processServiceVersionsByHostGroup(Map<String, List<String>> serviceToVersions, String service,
                                                def version, def tag, String hostGroup) {
    boolean change = false
    log.debug("Handling service version <{}:{}>", service, version)
    if (tag.isLong() || tag.toString().equals(hostGroup)) {
      if (serviceToVersions.containsKey(service)) {
        if (serviceToVersions.get(service).get(0).isLong()) {
          if (tag.toString().equals(hostGroup)) {
            change = true;
          } else {
            log.debug("Entry already added, checking versions ...")
            def newVersion = version.longValue()
            def oldVersion = Long.valueOf(serviceToVersions.get(service).get(0)).longValue()
            change = oldVersion < newVersion
          }
        } else {
          change = true;
        }
      } else {
        change = true;
      }
    }
    if (change) {
      log.debug("Adding / updating service version <{}:{}>", service, version)
      if (!serviceToVersions.containsKey(service)) {
        serviceToVersions.put(service, new ArrayList<String>());
      }
      if (tag.isLong()) {
        serviceToVersions.get(service).add(0, tag.toString());
      } else {
        serviceToVersions.get(service).add(tag.toString());
      }
    }
  }

  private def Map<String, String> collectConfigPropertiesForServiceByList(String service, List<String> tag) {
    Map<String, String> serviceConfigProperties = new HashMap<>();

    for (String actualTag : tag) {
      def Map resourceRequestMap = getResourceRequestMap("clusters/${getClusterName()}/configurations",
        ['type': "$service", 'tag': "$actualTag"])
      def rawResource = getSlurpedResource(resourceRequestMap);

      if (rawResource) {
        Map<String, String> tmpConfigs = rawResource.items?.collectEntries { it -> it.properties }
        serviceConfigProperties.putAll(tmpConfigs);
      } else {
        log.debug("No resource object has been returned for the resource request map: {}", resourceRequestMap)
      }
    }
    return serviceConfigProperties
  }

  private def Map<String, String> collectConfigPropertiesForService(String service, def tag) {
    Map<String, String> serviceConfigProperties

    def Map resourceRequestMap = getResourceRequestMap("clusters/${getClusterName()}/configurations",
      ['type': "$service", 'tag': "$tag"])
    def rawResource = getSlurpedResource(resourceRequestMap);

    if (rawResource) {
      serviceConfigProperties = rawResource.items?.collectEntries { it -> it.properties }
    } else {
      log.debug("No resource object has been returned for the resource request map: {}", resourceRequestMap)
    }
    return serviceConfigProperties
  }

  private Map<String, ?> getResourceRequestMap(String path, Map<String, String> queryParams) {
    def Map requestMap = [:]
    if (queryParams) {
      requestMap = ['path': "${ambari.getUri()}" + path, 'query': queryParams]
    } else {
      requestMap = ['path': "${ambari.getUri()}" + path]
    }
    return requestMap
  }

  /**
   * Gets the resource as a text as it;s returned by the server.
   *
   * @param resourceRequestMap
   */
  private getRawResource(Map resourceRequestMap) {
    def rawResource = null;
    try {
      if (debugEnabled) {
        println "[DEBUG] GET ${resourceRequestMap.get('path')}"
      }
      rawResource = ambari.get(resourceRequestMap)?.data?.text
    } catch (e) {
      def clazz = e.class
      log.error("Error occurred during GET request to {}, exception: ", resourceRequestMap.get('path'), e)
      if (clazz == NoHttpResponseException.class || clazz == ConnectException.class
        || clazz == ClientProtocolException.class || clazz == NoRouteToHostException.class
        || clazz == UnknownHostException.class || (clazz == HttpResponseException.class && e.message == "Bad credentials")) {
        throw new AmbariConnectionException("Cannot connect to Ambari ${ambari.getUri()}")
      }
    }
    return rawResource
  }

  /**
   * Slurps the response text.
   *
   * @param resourceRequestMap a map wrapping the resource request components
   * @return an Object as it's created by the JsonSlurper
   */
  private getSlurpedResource(Map resourceRequestMap) {
    def rawResource = getRawResource(resourceRequestMap)
    def slurpedResource = (rawResource != null) ? slurper.parseText(rawResource) : rawResource
    return slurpedResource
  }


  private def getAllResources(resourceName, fields = "") {
    slurp("clusters/${getClusterName()}/$resourceName", fields ? "$fields/*" : "")
  }

  private def getAllPredictedResources(resourceName, predicate = [:]) {
    slurpPredicate("clusters/${getClusterName()}/$resourceName", predicate)
  }

  /**
   * Posts the blueprint JSON to Ambari with name 'bp' in the URL
   * because it does not matter here. The blueprint's name is
   * provided in the JSON.
   *
   * @param blueprint json
   * @return response message
   */
  private void postBlueprint(String blueprint) {
    if (debugEnabled) {
      println "[DEBUG] POST ${ambari.getUri()}blueprints/bp"
    }
    ambari.post(path: "blueprints/bp", body: blueprint, { it })
  }

  private def createClusterJson(String name, Map hostGroups) {
    def builder = new JsonBuilder()
    def groups = hostGroups.collect {
      def hostList = it.value.collect { ['fqdn': it] }
      [name: it.key, hosts: hostList]
    }
    builder { "blueprint" name; "default_password" "admin"; "host_groups" groups }
    builder.toPrettyString()
  }

  private def slurp(path, fields = "") {

    def fieldsMap = fields ? ['fields': fields] : [:]
    def Map resourceReqMap = getResourceRequestMap(path, fieldsMap)
    def result = getSlurpedResource(resourceReqMap)

    return result
  }

  private def slurpPredicate(path, predicate) {
    def Map resourceReqMap = getResourceRequestMap(path, predicate)
    getSlurpedResource(resourceReqMap)
  }

  /**
   * Return the blueprint's properties as a Map.
   *
   * @param id id of the blueprint
   * @return properties as Map
   */
  private def getBlueprint(id) {
    slurp("blueprints/$id", "host_groups,Blueprints")
  }

  /**
   * Returns a Map containing the blueprint's properties parsed from the Ambari response json.
   *
   * @return blueprint's properties as Map or empty Map
   */
  private def getBlueprints() {
    slurp("blueprints", "Blueprints")
  }

  /**
   * Returns a Map containing the cluster's properties parsed from the Ambari response json.
   *
   * @return cluster's properties as Map or empty Map
   */
  private def getClusters() {
    slurp("clusters", "Clusters")
  }

  /**
   * Returns the available hosts properties as a Map.
   *
   * @return Map containing the hosts properties
   */
  private def getHosts() {
    slurp("hosts", "Hosts")
  }

  /**
   * Returns the service components properties as Map.
   *
   * @param service id of the service
   * @return service component properties as Map
   */
  private def getServiceComponents(service) {
    getAllResources("services/$service/components", "ServiceComponentInfo")
  }

  /**
   * Returns the services properties as Map parsed from Ambari response json.
   *
   * @return service properties as Map
   */
  private def getServices() {
    getAllResources("services", "ServiceInfo")
  }

  private def addComponentToHost(String hostName, String component) {
    if (debugEnabled) {
      println "[DEBUG] POST ${ambari.getUri()}clusters/${getClusterName()}/hosts/$hostName/host_components"
    }
    ambari.post(path: "clusters/${getClusterName()}/hosts/$hostName/host_components/${component.toUpperCase()}", { it })
  }

  private def Map<String, Integer> setComponentsState(String hostName, List<String> components, String state)
    throws HttpResponseException {
    def resp = [:]
    components.each {
      def id = setComponentState(hostName, it, state)
      if (id) {
        resp << [(it): id]
      }
    }
    return resp
  }

  private def setComponentState(String hostName, String component, String state) {
    if (debugEnabled) {
      println "[DEBUG] PUT ${ambari.getUri()}clusters/${getClusterName()}/hosts/$hostName/host_components/$component"
    }
    Map bodyMap = [
      HostRoles  : [state: state.toUpperCase()],
      RequestInfo: [context: "${component.toUpperCase()} ${state.toUpperCase()}"]
    ]
    def Map<String, ?> putRequestMap = [:]
    putRequestMap.put('requestContentType', ContentType.URLENC)
    putRequestMap.put('path', "clusters/${getClusterName()}/hosts/$hostName/host_components/${component.toUpperCase()}")
    putRequestMap.put('body', new JsonBuilder(bodyMap).toPrettyString());
    def response = ambari.put(putRequestMap).getAt("responseData")?.getAt("str")
    if (response) {
      slurper.parseText(response)?.Requests?.id
    }
  }

  private int setAllComponentsState(String clusterName, List<String> hostNames, String state, String context)
    throws HttpResponseException {
    def reqInfo = [
      "context"        : context,
      "operation_level": ["level": "HOST_COMPONENT", "cluster_name": clusterName],
      "query"          : "HostRoles/host_name.in(${hostNames.join(',')})"
    ]
    def reqBody = ["HostRoles": ["state": state]]
    def Map<String, ?> putRequestMap = [:]
    putRequestMap.put('requestContentType', ContentType.URLENC)
    putRequestMap.put('path', "clusters/$clusterName/host_components")
    putRequestMap.put('body', new JsonBuilder(["RequestInfo": reqInfo, "Body": reqBody]).toPrettyString())
    if (state == "INSTALLED") {
      putRequestMap.put('query', ['state': 'INIT'])
    }
    def response = ambari.put(putRequestMap).getAt("responseData")?.getAt("str")
    response ? slurper.parseText(response)?.Requests?.id : -1
  }

  /**
   * Returns the properties of the host components as a Map parsed from the Ambari response json.
   *
   * @param host which host's components are requested
   * @return component properties as Map
   */
  private def getHostComponents(host) {
    getAllResources("hosts/$host/host_components", "HostRoles")
  }

  private String getResourceContent(name) {
    getClass().getClassLoader().getResourceAsStream(name)?.text
  }

  private def extendBlueprintConfiguration(Map blueprintMap, Map newConfigs) {
    def configurations = blueprintMap.configurations
    if (!configurations) {
      if (newConfigs) {
        def conf = []
        newConfigs.each { conf << [(it.key): it.value] }
        blueprintMap << ["configurations": conf]
      }
      return blueprintMap
    }
    newConfigs.each {
      def site = it.key
      def index = indexOfConfig(configurations, site)
      if (index == -1) {
        configurations << ["$site": it.value]
      } else {
        def existingConf = configurations.get(index)
        existingConf."$site" << it.value
      }
    }
    blueprintMap
  }

  private def extendBlueprintHostGroupConfiguration(Map blueprintMap, Map newConfigs) {
    for (int j = 0; j < newConfigs.size(); j++) {
      def configurations = blueprintMap.host_groups.find { it.name == newConfigs.keySet().getAt(j) }.configurations
      if (!configurations) {
        if (newConfigs) {
          def conf = []
          newConfigs.get(newConfigs.keySet().getAt(j)).each { conf << [(it.key): it.value] }
          blueprintMap.host_groups.find { it.name == newConfigs.keySet().getAt(j) } << ["configurations": conf]
        }
      } else {
        newConfigs.get(newConfigs.keySet().getAt(j)).each {
          def site = it.key
          def index = indexOfConfig(configurations, site)
          if (index == -1) {
            configurations << ["$site": it.value]
          } else {
            def existingConf = configurations.get(index)
            existingConf."$site" << it.value
          }
        }
      }
    }
    blueprintMap
  }

  private int indexOfConfig(List<Map> configurations, String site) {
    def index = 0
    for (Map conf : configurations) {
      if (conf.containsKey(site)) {
        return index;
      }
      index++
    }
    return -1;
  }

  private def int getRequestId(def responseDecorator) {
    def resp = IOUtils.toString(new InputStreamReader(responseDecorator.entity.content.wrappedStream))
    slurper.parseText(resp)?.Requests?.id
  }

  private def boolean isComponentPresent(def bpMap, def component) {
    bpMap?.host_groups?.collectAll { it?.components?.name }.flatten().contains(component)
  }

}

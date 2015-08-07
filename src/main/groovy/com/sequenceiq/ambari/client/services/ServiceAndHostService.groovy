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
package com.sequenceiq.ambari.client.services

import groovy.json.JsonBuilder
import groovy.util.logging.Slf4j
import groovyx.net.http.ContentType
import groovyx.net.http.HttpResponseException
import static CommonService.PAD

@Slf4j
trait ServiceAndHostService extends ClusterService {

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
   * Returns the names of the hosts which are in the cluster.
   */
  def List<String> getClusterHosts() {
    utils.slurp("clusters/${getClusterName()}")?.hosts?.Hosts?.host_name
  }

  /**
   * Resolves an internal hostname to a public one.
   */
  def String resolveInternalHostName(String internalHostName) {
    utils.slurp("clusters/${getClusterName()}/hosts/$internalHostName")?.Hosts?.public_host_name
  }

  /**
   * Returns the internal host names of the hosts which the host components are installed to.
   */
  def List<String> getHostNamesByComponent(String component) {
    def hostRoles = utils.getAllResources('host_components', 'HostRoles/component_name')
    def hosts = hostRoles?.items?.findAll { it.HostRoles.component_name.equals(component) }?.HostRoles?.host_name
    hosts ?: []
  }

  /**
   * Adds the registered nodes to the cluster.
   *
   * @param hosts list of hostname to add
   * @throws groovyx.net.http.HttpResponseException if a node is not registered with ambari
   */
  def addHosts(List<String> hosts) throws HttpResponseException {
    def requestBody = hosts.collectNested { ['Hosts': ['host_name': it]] }
    ambari.post(path: "clusters/${getClusterName()}/hosts", body: new JsonBuilder(requestBody).toPrettyString(), { it })
  }

  /**
   * Returns the state of the host.
   *
   * @param host host's internal hostname
   * @return state of the host
   */
  String getHostState(String host) {
    utils.getAllResources("hosts/$host").Hosts.host_status
  }

  /**
   * Returns the available host names and their states. It also
   * contains hosts which are not part of the cluster, but are connected
   * to ambari.
   *
   * @return hostname state association
   */
  def Map<String, String> getHostStatuses() {
    utils.getHosts().items.collectEntries { [(it.Hosts.host_name): it.Hosts.host_status] }
  }

  def Map<String, String> getHostNames() {
    utils.getHosts().items.collectEntries { [(it.Hosts.public_host_name): it.Hosts.host_name] }
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
    if (components.contains('NODEMANAGER')) {
      decommissionNodeManagers([hostName])
    }
    if (components.contains('DATANODE')) {
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
    def id = restartServiceComponents('ZOOKEEPER', ['ZOOKEEPER_SERVER'])
    waitForRequestsToFinish([id])

    // restart nagios
    if (getServiceComponentsMap().containsKey('NAGIOS')) {
      id = restartServiceComponents('NAGIOS', ['NAGIOS_SERVER'])
      waitForRequestsToFinish([id])
    }
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
   * Returns a pre-formatted component list of a host.
   *
   * @param host which host's components are requested
   * @return formatted String
   */
  def String showHostComponentList(host) {
    utils.getHostComponents(host).items.collect {
      "${it.HostRoles.component_name.padRight(PAD)} [$it.HostRoles.state]"
    }.join('\n')
  }

  /**
   * Returns the host's components as Map where the key is the component's name and values is its state.
   *
   * @param host which host's components are requested
   * @return component name - state association
   */
  def Map<String, String> getHostComponentsMap(host) {
    def result = utils.getHostComponents(host)?.items?.collectEntries { [(it.HostRoles.component_name): it.HostRoles.state] }
    result ?: [:]
  }

  /**
   * Decommission the node manager on the given hosts.
   */
  def int decommissionNodeManagers(List<String> hosts) {
    decommission(hosts, 'NODEMANAGER', 'YARN', 'RESOURCEMANAGER')
  }

  /**
   * Decommission the data node on the given hosts.
   */
  def int decommissionDataNodes(List<String> hosts) {
    decommission(hosts, 'DATANODE', 'HDFS', 'NAMENODE')
  }

  /**
   * Returns the name of the node and the size of the replicated block that are remaining.
   *
   * @return Map where the key is the internal host name of the node and the value
   *         is size of the replicated block
   */
  def Map<String, Long> getDecommissioningDataNodes() {
    def result = [:]
    def response = utils.slurp("clusters/${getClusterName()}/services/HDFS/components/NAMENODE", 'metrics/dfs/namenode/DecomNodes')
    def nodes = slurper.parseText(response?.metrics?.dfs?.namenode?.DecomNodes)
    if (nodes) {
      nodes.each {
        result << [(it.key): it.value.underReplicatedBlocks as Long]
      }
    }
    result
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
    def response = ambari.put(putRequestMap).getAt('responseData')?.getAt('str')
    if (response) {
      slurper.parseText(response)?.Requests?.id
    }
  }

  /**
   * Starts the given components on a host.
   * To start all components on hosts use the {@link #startAllServices} method.
   *
   * @return map of the component names and their request id since its an async call
   * @throws HttpResponseException in case the component is not found
   */
  def Map<String, Integer> startComponentsOnHost(String hostName, List<String> components) throws HttpResponseException {
    setComponentsState(hostName, components, 'STARTED')
  }

  /**
   * Stops the given components on a host.
   *
   * @return map of the component names and their request id since its an async call
   * @throws HttpResponseException in case the component is not found
   */
  def Map<String, Integer> stopComponentsOnHost(String hostName, List<String> components) throws HttpResponseException {
    setComponentsState(hostName, components, 'INSTALLED')
  }

  /**
   * Restarts the given components of a service.
   */
  def int restartServiceComponents(String service, List<String> components) {
    def filter = components.collect {
      ['service_name': service, 'component_name': it, 'hosts': getHostNamesByComponent(it).join(',')]
    }
    Map bodyMap = [
            'RequestInfo'              : [command: 'RESTART', context: "Restart $service components $components"],
            'Requests/resource_filters': filter
    ]
    ambari.post(path: "clusters/${getClusterName()}/requests", body: new JsonBuilder(bodyMap).toPrettyString(), {
      utils.getRequestId(it)
    })
  }

  /**
   * Returns a pre-formatted list of the hosts.
   *
   * @return pre-formatted String
   */
  def String showHostList() {
    utils.getHosts().items.collect {
      "$it.Hosts.host_name [$it.Hosts.host_status] $it.Hosts.ip $it.Hosts.os_type:$it.Hosts.os_arch"
    }.join('\n')
  }

  /**
   * Returns the service components properties as Map.
   *
   * @param service id of the service
   * @return service component properties as Map
   */
  protected Closure getServiceComponents = { String service ->
    utils.getAllResources("services/$service/components", 'ServiceComponentInfo')
  }

  /**
   * Returns a pre-formatted list of the service components.
   *
   * @return pre-formatted String
   */
  def String showServiceComponents() {
    Closure getServiceComponents = this.getServiceComponents
    getServices().items.collect {
      String name = it.ServiceInfo.service_name
      def state = it.ServiceInfo.state
      def componentList = getServiceComponents(name).items.collect {
        "    ${it.ServiceComponentInfo.component_name.padRight(PAD)}  [$it.ServiceComponentInfo.state]"
      }.join('\n')
      "${name.padRight(PAD)} [$state]\n$componentList"
    }.join('\n')
  }

  /**
   * Returns the service components properties as Map where the key is the service name and
   * value is a component - state association.
   *
   * @return service name - [component name - status]
   */
  def Map<String, Map<String, String>> getServiceComponentsMap() {
    Closure getServiceComponents = this.getServiceComponents
    def result = getServices().items.collectEntries {
      String name = it.ServiceInfo.service_name
      def componentList = getServiceComponents(name).items.collectEntries {
        [(it.ServiceComponentInfo.component_name): it.ServiceComponentInfo.state]
      }
      [(name): componentList]
    }
    result ?: new HashMap()
  }

  private addComponentToHost(String hostName, String component) {
    if (debugEnabled) {
      println "[DEBUG] POST ${ambari.getUri()}clusters/${getClusterName()}/hosts/$hostName/host_components"
    }
    ambari.post(path: "clusters/${getClusterName()}/hosts/$hostName/host_components/${component.toUpperCase()}", { it })
  }

  /**
   * Returns all the components states grouped by hosts.
   *
   * @return host name - [component name - state]
   */
  def Map<String, Map<String, String>> getHostComponentsStates() {
    utils.getAllResources('hosts', 'host_components/HostRoles/state').items.collectEntries {
      def hostName = it.Hosts.host_name
      def components = it.host_components.collectEntries {
        [(it.HostRoles.component_name): it.HostRoles.state]
      }
      [(hostName): components]
    }
  }

  /**
   * Stops all the components on the specified hosts.
   *
   * @param hostNames list of FQDN
   * @return request id since its an async call
   * @throws HttpResponseException in case of any rest exception
   */
  def int stopAllComponentsOnHosts(List<String> hostNames) throws HttpResponseException {
    setAllComponentsStateToInstalled(hostNames, 'Stop all components on hosts')
  }

  /**
   * Installs the added components on the specified hosts.
   *
   * @param hostNames list of FQDN
   * @return request id since its an async call
   * @throws HttpResponseException in case of any rest exception
   */
  def int installAllComponentsOnHosts(List<String> hostNames) throws HttpResponseException {
    setAllComponentsStateToInstalled(hostNames, 'Install all components on hosts')
  }

  private int setAllComponentsStateToInstalled(List<String> hostNames, String context) {
    setAllComponentsState(getClusterName(), 'INSTALLED', context, "HostRoles/host_name.in(${hostNames.join(',')})")
  }

  /**
   * Sets the state of all components to the desired state.
   *
   * @param clusterName name of the cluster
   * @param state desired state
   * @param context context shown on the UI
   * @return id of the request
   */
  private def int setAllComponentsState(clusterName, state, context, query) throws HttpResponseException {
    def reqInfo = [
            'context'        : context,
            'operation_level': ['level': 'HOST_COMPONENT', 'cluster_name': clusterName],
            'query'          : query
    ]
    def reqBody = ['HostRoles': ['state': state]]
    Map<String, ?> putRequestMap = [:]
    putRequestMap.put('requestContentType', ContentType.URLENC)
    putRequestMap.put('path', "clusters/$clusterName/host_components")
    putRequestMap.put('body', new JsonBuilder(['RequestInfo': reqInfo, 'Body': reqBody]).toPrettyString())
    if (state == 'INSTALLED') {
      putRequestMap.put('query', ['state': 'INIT'])
    }
    def response = ambari.put(putRequestMap).getAt('responseData')?.getAt('str')
    response ? slurper.parseText(response)?.Requests?.id : -1
  }

  /**
   * Adds the given components to the given hosts. It does not install them. To install the components use
   * the {@link #installAllComponentsOnHosts(java.util.List)}.
   * Only existing service components can be added.
   *
   * @param hostNames hosts to install the component to
   * @param components components to be installed
   * @throws HttpResponseException in case the component's service is not installed
   */
  def void addComponentsToHosts(List<String> hostNames, List<String> components) throws HttpResponseException {
    def commaSepHostNames = hostNames.join(',')
    def addRequest = [
            'RequestInfo': ['query': "Hosts/host_name.in($commaSepHostNames)"],
            'Body'       : ['host_components': components.collectNested { ['HostRoles': ['component_name': it]] }]
    ]
    ambari.post(path: "clusters/${getClusterName()}/hosts", body: new JsonBuilder(addRequest).toPrettyString(), { it })
  }

  /**
   * Returns the services properties as Map parsed from Ambari response json.
   *
   * @return service properties as Map
   */
  private getServices() {
    utils.getAllResources('services', 'ServiceInfo')
  }

  /**
   * Returns a pre-formatted service list.
   *
   * @return formatted String
   */
  def String showServiceList() {
    getServices().items.collect { "${it.ServiceInfo.service_name.padRight(PAD)} [$it.ServiceInfo.state]" }.join('\n')
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
   * Add a service to the cluster.
   *
   * @param serviceName name of the service
   */
  def void addService(String serviceName) {
    ambari.post(path: "clusters/${getClusterName()}/services",
            body: new JsonBuilder(['ServiceInfo': ['service_name': serviceName]]).toPrettyString(), { it })
  }

  /**
   * Add a service component to the cluster.
   *
   * @param serviceName name of the service
   * @param component component name
   */
  def void addServiceComponent(String serviceName, String component) {
    def body = ['components': [['ServiceComponentInfo': ['component_name': component]]]]
    def Map<String, ?> requestMap = [:]
    requestMap.put('path', "clusters/${getClusterName()}/services")
    requestMap.put('query', ['ServiceInfo/service_name': serviceName])
    requestMap.put('body', new JsonBuilder(body).toPrettyString());
    ambari.post(requestMap, { it })
  }

  /**
   * Sets the given service's state to the desired value.
   *
   * @param serviceName name of the service
   * @param state desired state
   * @return id of the request
   */
  def int setServiceState(String serviceName, String state) {
    def body = ['RequestInfo': ['context'        : "Set service: $serviceName state to: $state",
                                'operation_level': ['level': 'CLUSTER', 'cluster_name': getClusterName()]],
                'Body'       : ['ServiceInfo': ['state': state]]]
    def Map<String, ?> requestMap = [:]
    requestMap.put('requestContentType', ContentType.URLENC)
    requestMap.put('path', "clusters/${getClusterName()}/services")
    requestMap.put('query', ['ServiceInfo/service_name': serviceName, 'ServiceInfo/state': state])
    requestMap.put('body', new JsonBuilder(body).toPrettyString());

    utils.putAndGetId(requestMap)
  }

  /**
   * Starts all the services.
   *
   * @return id of the request since its an async call
   */
  def int startAllServices() {
    log.debug('Starting all services ...')
    manageService('Start All Services', 'STARTED')
  }

  /**
   * Stops all the services.
   *
   * @return id of the request since its an async call
   */
  def int stopAllServices() {
    log.debug('Stopping all services ...')
    manageService('Stop All Services', 'INSTALLED')
  }

  /**
   * Starts the given service.
   *
   * @param service name of the service
   * @return id of the request
   */
  def int startService(String service) {
    manageService("Starting $service", 'STARTED', service)
  }

  /**
   * Stops the given service.
   *
   * @param service name of the service
   * @return id of the request
   */
  def int stopService(String service) {
    manageService("Stopping $service", 'INSTALLED', service)
  }

  def boolean servicesStarted() {
    return servicesStatus(true)
  }

  def boolean servicesStopped() {
    return servicesStatus(false)
  }

  private boolean servicesStatus(boolean starting) {
    def String status = (starting) ? 'STARTED' : 'INSTALLED'
    Map serviceComponents = getServicesMap()
    boolean allInState = true
    serviceComponents.values().each { val ->
      log.debug('Service: {}', val)
      allInState = allInState && val.equals(status)
    }
    return allInState;
  }

  private int manageService(String context, String state) {
    return manageService(context, state, '')
  }

  private int manageService(String context, String state, String service) {
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

    utils.putAndGetId(putRequestMap)
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
            'RequestInfo'              : [command: command, context: command],
            'Requests/resource_filters': [[service_name: serviceName]]
    ]
    ambari.post(path: "clusters/${getClusterName()}/requests", body: new JsonBuilder(bodyMap).toPrettyString(), {
      utils.getRequestId(it)
    })
  }

  def Map<String, Map<String, String>> getServiceConfigMap() {
    return getServiceConfigMap('')
  }

  /**
   * Returns a map with service configurations of the 'default' config group.
   * The keys are the config-types, values are maps with <propertyName, propertyValue> entries
   *
   * @return a Map with entries of format <config-type, Map<property, value>>
   */
  def Map<String, Map<String, String>> getServiceConfigMap(String type) {
    def configs = getServiceConfigMapByHostGroup(null)
    type ? [(type): configs.get(type)] : configs
  }

  /**
   * Returns the currently active host group level configurations combined with the 'default' config group.
   *
   * @param hostGroup name of the host group, in Ambari the config group's name is the same
   * @return return the configurations in the form of <config-type, <property_key, property_value>>
   */
  def Map<String, Map<String, String>> getServiceConfigMapByHostGroup(String hostGroup) {
    Map<String, Map<String, String>> result = new HashMap<>()
    def rawConfigs = utils.getAllPredictedResources('configurations/service_config_versions',
            ['is_current': 'true', 'fields': '*']).items
    rawConfigs.each {
      if (it.group_name == 'default' || (hostGroup && it.group_name.contains(hostGroup))) {
        it.configurations.each {
          def type = it.type
          Map props = result.get(type)
          if (props) {
            props.putAll(it.properties)
          } else {
            result << [(it.type): it.properties]
          }
        }
      }
    }
    result
  }
}

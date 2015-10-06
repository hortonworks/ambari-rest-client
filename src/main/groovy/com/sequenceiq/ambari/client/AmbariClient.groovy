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
import com.sequenceiq.ambari.client.services.AlertService
import com.sequenceiq.ambari.client.services.BlueprintService
import com.sequenceiq.ambari.client.services.ConfigService
import com.sequenceiq.ambari.client.services.HBaseService
import com.sequenceiq.ambari.client.services.KerberosService
import com.sequenceiq.ambari.client.services.ServiceAndHostService
import com.sequenceiq.ambari.client.services.StackService
import com.sequenceiq.ambari.client.services.TaskService
import com.sequenceiq.ambari.client.services.UserService
import com.sequenceiq.ambari.client.services.ViewService
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import groovyx.net.http.ContentType
import groovyx.net.http.HttpResponseException
import groovyx.net.http.RESTClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager

import javax.net.ssl.SSLContext
/**
 * Basic client to send requests to the Ambari server.
 */
@Slf4j
class AmbariClient implements AlertService, BlueprintService, ConfigService, HBaseService, ServiceAndHostService, KerberosService, StackService, TaskService, UserService, ViewService {

  private static final String SLAVE = 'slave_'

  final RESTClient ambari

  /**
   * Connects to the ambari server.
   *
   * @param host host of the Ambari server; default value is localhost
   * @param port port of the Ambari server; default value is 8080
   * @param user username of the Ambari server; default is admin
   * @param password password fom the Ambari server; default is admin
   * @param clientCertPath client certificate path, used in 2-way-ssl
   * @param clientKeyPath client key path, used in 2-way-ssl
   * @param serverCertPath server certificate path, used in 2-way-ssl
   */
  AmbariClient(String host = 'localhost', String port = '8080', String user = 'admin', String password = 'admin',
               String clientCertPath = null, String clientKeyPath = null, String serverCertPath = null) {
    def http = clientCertPath == null ? 'http' : 'https';
    ambari = new RESTClient("$http://${host}:${port}/api/v1/" as String)

    if (clientCertPath) {
      SSLContext sslContext = utils.setupSSLContext(clientCertPath, clientKeyPath, serverCertPath);
      PoolingHttpClientConnectionManager connectionManager =
        new PoolingHttpClientConnectionManager(utils.setupSchemeRegistry(sslContext));
      connectionManager.setMaxTotal(1000);
      connectionManager.setDefaultMaxPerRoute(500);
      def httpClient = HttpClientBuilder.create().setConnectionManager(connectionManager)
        .setDefaultRequestConfig().build();
      ambari.setClient(httpClient)
    }

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
   * Runs a MapReduce service check which is a simple WordCount.
   * @return id of the request
   */
  def int runMRServiceCheck() {
    runServiceCheck('MAPREDUCE2_SERVICE_CHECK', 'MAPREDUCE2')
  }

  /**
   * Returns the nodes by DFS an their data allocations.
   *
   * @return a Map where the key is the internal host name and the value
   *         is a Map where the key is the remaining space and the value is the used space in bytes
   */
  def Map<String, Map<Long, Long>> getDFSSpace() {
    def result = [:]
    def response = utils.slurp("clusters/${getClusterName()}/services/HDFS/components/NAMENODE", 'metrics/dfs')
    def liveNodes = slurper.parseText(response?.metrics?.dfs?.namenode?.LiveNodes as String)
    if (liveNodes) {
      liveNodes.each {
        if (it.value.adminState == 'In Service') {
          result << [(it.key.split(':')[0]): [(it.value.remaining as Long): it.value.usedSpace as Long]]
        }
      }
    }
    result
  }

  def boolean isComponentPresent(def bpMap, def component) {
    bpMap?.host_groups?.collectNested { it?.components?.name }?.flatten()?.contains(component)
  }

  /**
   * Adds all the components from a given blueprint's host group. The services must be installed
   * in order to add its components. It is recommended to use the same blueprint's host group from which
   * the cluster was created.
   *
   * @param hostNames components will be installed on these hosts
   * @param blueprint id of the blueprint
   * @param hostGroup host group of the blueprint
   */
  def void addComponentsToHosts(List<String> hostNames, String blueprint, String hostGroup) throws HttpResponseException {
    def bpMap = utils.getBlueprint(blueprint)
    def components = bpMap?.host_groups?.find { it.name.equals(hostGroup) }?.components?.collect { it.name }
    if (components) {
      addComponentsToHosts(hostNames, components)
    }
  }

  /**
   * Starts all the components of the specified hostgroup of a blueprint.
   *
   * @param blueprint id of the blueprint
   * @param hostGroup hostgroup of the blueprint
   * @return request id since its an async call
   * @throws HttpResponseException in case of any rest exception
   */
  def int startAllComponents(String blueprint, String hostGroup) throws HttpResponseException {
    def categories = getComponentsCategory(blueprint, hostGroup)
    def components = categories.findAll { it.value.equals('SLAVE') || it.value.equals('MASTER') }.keySet()
    def query = "HostRoles/component_name.in(${components.join(',')})"
    setAllComponentsState(getClusterName(), 'STARTED', 'Start all components', query)
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
    def groups = utils.getBlueprint(blueprint)?.host_groups?.collect { ['name': it.name, 'cardinality': it.cardinality] }
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
   * Returns the type of the components of a given host group in a given blueprint.
   * There are 3 types: SLAVE, CLIENT, MASTER
   *
   * @param blueprintId if of the blueprint
   * @param hostGroup host group's name in the blueprint
   * @return map where the key is the component's name and the value is the category
   */
  def Map<String, String> getComponentsCategory(String blueprintId, String hostGroup) {
    def bpMap = utils.getBlueprint(blueprintId)
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
    def bpMap = utils.getBlueprint(blueprintId)
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
      def json = utils.slurp("clusters/${getClusterName()}/components/$it", 'ServiceComponentInfo')
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
    ambari.get(path: 'check', headers: ['Accept': ContentType.TEXT]).data.text
  }
}

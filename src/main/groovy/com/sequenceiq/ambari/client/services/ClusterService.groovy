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
import groovyx.net.http.HttpResponseException

@Slf4j
trait ClusterService extends CommonService {

  /**
   * Returns the name of the cluster.
   *
   * @return the name of the cluster of null if no cluster yet
   */
  def String getClusterName() {
    if (!clusterNameCache) {
      def clusters = utils.getClusters();
      if (clusters) {
        clusterNameCache = clusters.items[0]?.Clusters?.cluster_name
      }
    }
    return clusterNameCache
  }

  /**
   * Returns a pre-formatted String of the clusters.
   *
   * @return pre-formatted cluster list
   */
  def String showClusterList() {
    utils.getClusters().items.collect {
      "[$it.Clusters.cluster_id] $it.Clusters.cluster_name:$it.Clusters.version"
    }.join('\n')
  }

  /**
   * Returns all clusters as json
   *
   * @return json String
   * @throws HttpResponseException in case of error
   */
  def getClustersAsJson() throws HttpResponseException {
    Map resourceRequestMap = utils.getResourceRequestMap('clusters', null)
    return utils.getRawResource(resourceRequestMap)
  }

  /**
   * Returns the active cluster as json
   *
   * @return cluster as json String
   * @throws HttpResponseException in case of error
   */
  def String getClusterAsJson() throws HttpResponseException {
    String path = 'clusters/' + getClusterName();
    Map resourceRequestMap = utils.getResourceRequestMap(path, null)
    return utils.getRawResource(resourceRequestMap)
  }

  private String createClusterJson(String name, Map hostGroups) {
    def builder = new JsonBuilder()
    def groups = hostGroups.collect {
      def hostList = it.value.collect { ['fqdn': it] }
      [name: it.key, hosts: hostList]
    }
    builder { "blueprint" name; "default_password" "admin"; "host_groups" groups }
    builder.toPrettyString()
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
   * Decommission a host component on a given host.
   * @param host hostName where the component is installed to
   * @param slaveName slave to be decommissioned
   * @param serviceName where the slave belongs to
   * @param componentName where the slave belongs to
   */
  def int decommission(List<String> hosts, String slaveName, String serviceName, String componentName) {
    def requestInfo = [
            command   : 'DECOMMISSION',
            context   : "Decommission $slaveName",
            parameters: ['slave_type': slaveName, 'excluded_hosts': hosts.join(',')]
    ]
    def filter = [
            ['service_name': serviceName, 'component_name': componentName]
    ]
    Map bodyMap = [
            'RequestInfo'              : requestInfo,
            'Requests/resource_filters': filter
    ]
    ambari.post(path: "clusters/${getClusterName()}/requests", body: new JsonBuilder(bodyMap).toPrettyString(), {
      utils.getRequestId(it)
    })
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

  def BigDecimal getRequestProgress() {
    return getRequestProgress(1)
  }

  /**
   * Returns the install progress state. If the install failed -1 returned.
   *
   * @param request request id; default is 1
   * @return progress in percentage
   */
  def BigDecimal getRequestProgress(request) {
    def response = utils.getAllResources("requests/$request", 'Requests')
    def String status = response?.Requests?.request_status
    if (status && status.equals('FAILED')) {
      return new BigDecimal(-1)
    }
    return response?.Requests?.progress_percent
  }

  /**
   * Returns a map with <status, list of request ids> based on statuses parameter.
   * @param statuses the relevant statuses
   * @return the status map
   */
  def Map<String, List<Integer>> getRequests(String... statuses) {
    def reqs = utils.getAllResources('requests', 'Requests/request_status,Requests/id')?.items?.Requests
    def resp = [:]
    reqs.each {
      def reqStatus = it?.request_status
      if (!statuses || reqStatus in statuses) {
        def reqlist = resp[reqStatus]
        if (reqlist == null) {
          reqlist = []
          resp[reqStatus] = reqlist
        }
        reqlist << it?.id
      }
    }
    return resp
  }
}

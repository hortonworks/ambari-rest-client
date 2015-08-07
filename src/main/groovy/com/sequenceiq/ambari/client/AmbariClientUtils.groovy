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
import com.sequenceiq.ambari.client.services.CommonService
import groovy.text.SimpleTemplateEngine
import groovy.util.logging.Slf4j
import groovyx.net.http.HttpResponseException
import org.apache.commons.io.IOUtils
import org.apache.http.NoHttpResponseException
import org.apache.http.client.ClientProtocolException
import org.apache.http.config.Registry
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.socket.ConnectionSocketFactory
import org.apache.http.conn.socket.PlainConnectionSocketFactory
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.conn.ssl.SSLContexts
import org.bouncycastle.jce.provider.BouncyCastleProvider

import javax.net.ssl.SSLContext
import java.security.Security

@Slf4j
class AmbariClientUtils {

  private final CommonService ambariClient
  private final SimpleTemplateEngine templateProcessor = new SimpleTemplateEngine()

  AmbariClientUtils(CommonService ambariClient) {
    this.ambariClient = ambariClient
  }

  String getResourceContent(String name) {
    getClass().getClassLoader().getResourceAsStream(name)?.text
  }

  int indexOfConfig(List<Map> configurations, String site) {
    def index = 0
    for (Map conf : configurations) {
      if (conf.containsKey(site)) {
        return index;
      }
      index++
    }
    return -1;
  }

  def int getRequestId(def responseDecorator) {
    def resp = IOUtils.toString(new InputStreamReader(responseDecorator.entity.content))
    ambariClient.slurper.parseText(resp)?.Requests?.id
  }

  /**
   * Gets the resource as a text as it;s returned by the server.
   *
   * @param resourceRequestMap
   */
  def getRawResource(Map resourceRequestMap) {
    def rawResource = null;
    try {
      if (ambariClient.debugEnabled) {
        println "[DEBUG] GET ${resourceRequestMap.get('path')}"
      }
      rawResource = ambariClient.ambari.get(resourceRequestMap)?.data?.text
    } catch (e) {
      def clazz = e.class
      log.error('Error occurred during GET request to {}, exception: ', resourceRequestMap?.get('path'), e)
      if (clazz == NoHttpResponseException.class || clazz == ConnectException.class
              || clazz == ClientProtocolException.class || clazz == NoRouteToHostException.class
              || clazz == UnknownHostException.class || (clazz == HttpResponseException.class && e.message == 'Bad credentials')) {
        throw new AmbariConnectionException("Cannot connect to Ambari ${ambariClient.ambari.getUri()}")
      }
    }
    return rawResource
  }

  def getAllResources(resourceName, fields = '') {
    slurp("clusters/${ambariClient.getClusterName()}/$resourceName", fields ? "$fields/*" : '')
  }

  def getAllPredictedResources(resourceName, predicate) {
    slurpPredicate("clusters/${ambariClient.getClusterName()}/$resourceName", predicate)
  }

  def slurpPredicate(String path, Map predicate) {
    def Map resourceReqMap = getResourceRequestMap(path, predicate)
    getSlurpedResource(resourceReqMap)
  }

  def slurp(path, fields = '') {
    def fieldsMap = fields ? ['fields': fields] : [:]
    def Map resourceReqMap = getResourceRequestMap(path, fieldsMap)
    def result = getSlurpedResource(resourceReqMap)
    return result
  }

  /**
   * Slurps the response text.
   *
   * @param resourceRequestMap a map wrapping the resource request components
   * @return an Object as it's created by the JsonSlurper
   */
  def getSlurpedResource(Map resourceRequestMap) {
    def rawResource = getRawResource(resourceRequestMap)
    def slurpedResource = (rawResource != null) ? ambariClient.slurper.parseText(rawResource) : rawResource
    return slurpedResource
  }

  /**
   * Return the blueprint's properties as a Map.
   *
   * @param id id of the blueprint
   * @return properties as Map
   */
  def getBlueprint(id) {
    slurp("blueprints/$id", 'host_groups,Blueprints')
  }

  /**
   * Returns a Map containing the blueprint's properties parsed from the Ambari response json.
   *
   * @return blueprint's properties as Map or empty Map
   */
  def getBlueprints() {
    slurp('blueprints', 'Blueprints')
  }

  /**
   * Returns a Map containing the cluster's properties parsed from the Ambari response json.
   *
   * @return cluster's properties as Map or empty Map
   */
  def getClusters() {
    slurp('clusters', 'Clusters')
  }

  /**
   * Returns the available hosts properties as a Map.
   *
   * @return Map containing the hosts properties
   */
  def getHosts() {
    slurp('hosts', 'Hosts')
  }

  /**
   * Returns the properties of the host components as a Map parsed from the Ambari response json.
   *
   * @param host which host's components are requested
   * @return component properties as Map
   */
  def getHostComponents(host) {
    getAllResources("hosts/$host/host_components", 'HostRoles')
  }

  Map<String, ?> getResourceRequestMap(String path, Map<String, String> queryParams) {
    def Map requestMap
    if (queryParams) {
      requestMap = ['path': "${ambariClient.ambari.getUri()}" + path, 'query': queryParams]
    } else {
      requestMap = ['path': "${ambariClient.ambari.getUri()}" + path]
    }
    return requestMap
  }

  def String createJson(String templateName, Map bindings) throws Exception {
    def InputStream inPut = this.getClass().getClassLoader().getResourceAsStream("templates/$templateName");
    templateProcessor.createTemplate(new InputStreamReader(inPut)).make(bindings);
  }

  def SSLContext setupSSLContext(String clientCertPath, String clientKeyPath, String serverCertPath) {
    Security.addProvider(new BouncyCastleProvider());
    SSLContext context = SSLContexts.custom()
            .loadTrustMaterial(KeystoreUtils.createTrustStore(serverCertPath))
            .loadKeyMaterial(KeystoreUtils.createKeyStore(clientCertPath, clientKeyPath), 'consul'.toCharArray())
            .build();
    return context;
  }

  def Registry<ConnectionSocketFactory> setupSchemeRegistry(SSLContext sslContext) {
    RegistryBuilder<ConnectionSocketFactory> registryBuilder = RegistryBuilder.create();
    registryBuilder.register('http', PlainConnectionSocketFactory.getSocketFactory());
    if (sslContext != null) {
      registryBuilder.register('https', new SSLConnectionSocketFactory(sslContext));
    }
    return registryBuilder.build();
  }

  def int putAndGetId(def Map<String, ?> putRequestMap) {
    def response = ambariClient.ambari.put(putRequestMap)
    ambariClient.slurper.parseText(response.getAt('responseData')?.getAt('str') as String)?.Requests?.id
  }
}

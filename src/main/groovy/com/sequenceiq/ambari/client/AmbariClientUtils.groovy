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
import org.apache.commons.io.IOUtils
import org.apache.http.client.ClientProtocolException
import org.apache.http.config.Registry
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.socket.ConnectionSocketFactory
import org.apache.http.conn.socket.PlainConnectionSocketFactory
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.conn.ssl.SSLContexts
import org.apache.http.conn.ssl.X509HostnameVerifier
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.codehaus.groovy.control.CompilationFailedException

import javax.net.ssl.SSLContext
import javax.net.ssl.SSLException
import javax.net.ssl.SSLSession
import javax.net.ssl.SSLSocket
import java.security.Security
import java.security.cert.X509Certificate

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
   * @trhows AmbariConnectionException
   */
  def getRawResource(Map resourceRequestMap) throws AmbariConnectionException {
    def responseData = null;
    try {
      if (ambariClient.debugEnabled) {
        println "[DEBUG] GET ${resourceRequestMap.get('path')}"
      }
      log.info('AmbariClient getRawResource, resourceRequestMap: {}', resourceRequestMap)
      def responseDecorator = ambariClient.ambari.get(resourceRequestMap)
      def statusLine = responseDecorator?.statusLine
      responseData = responseDecorator?.data?.text
      log.debug('AmbariClient statusLine: {}, responseData: {}', statusLine, responseData)
    } catch (e) {
      log.info('Error occurred during GET request to {}, exception: ', resourceRequestMap, e)
      throw e
    }
    return responseData
  }

  def getAllResources(resourceName, fields = '') throws AmbariConnectionException {
    slurp("clusters/${ambariClient.getClusterName()}/$resourceName", fields ? "$fields/*" : '')
  }

  def getAllPredictedResources(resourceName, predicate) throws AmbariConnectionException {
    slurpPredicate("clusters/${ambariClient.getClusterName()}/$resourceName", predicate)
  }

  def slurpPredicate(String path, Map predicate) throws AmbariConnectionException {
    def Map resourceReqMap = getResourceRequestMap(path, predicate)
    getSlurpedResource(resourceReqMap)
  }

  def slurp(path, fields = '', queryString = '') throws AmbariConnectionException {
    def fieldsMap = fields ? ['fields': fields] : [:]
    def Map resourceReqMap = getResourceRequestMap(path, fieldsMap, queryString)
    def result = getSlurpedResource(resourceReqMap)
    return result
  }

  /**
   * Slurps the response text.
   *
   * @param resourceRequestMap a map wrapping the resource request components
   * @return an Object as it's created by the JsonSlurper
   */
  def getSlurpedResource(Map resourceRequestMap) throws AmbariConnectionException {
    def rawResource = getRawResource(resourceRequestMap)
    log.info("getSlurpedResource() rawResource value: {}", rawResource)
    def slurpedResource = (rawResource != null) ? ambariClient.slurper.parseText(rawResource) : rawResource
    log.info("getSlurpedResource() return value: {}", slurpedResource)
    return slurpedResource
  }

  /**
   * Return the blueprint's properties as a Map.
   *
   * @param id id of the blueprint
   * @return properties as Map
   */
  def getBlueprint(id) throws AmbariConnectionException {
    slurp("blueprints/$id", 'host_groups,Blueprints')
  }

  /**
   * Returns a Map containing the blueprint's properties parsed from the Ambari response json.
   *
   * @return blueprint's properties as Map or empty Map
   */
  def getBlueprints() throws AmbariConnectionException {
    slurp('blueprints', 'Blueprints')
  }

  /**
   * Returns a Map containing the cluster's properties parsed from the Ambari response json.
   *
   * @return cluster's properties as Map or empty Map
   */
  def getClusters() throws AmbariConnectionException {
    slurp('clusters', 'Clusters')
  }

  /**
   * Returns a Map containing the cluster's properties parsed from the Ambari response json.
   *
   * @return cluster's properties as Map or empty Map
   */
  def getClustersFields(List<String> fields) throws AmbariConnectionException {
    slurp('clusters', fields.join(','))
  }

  /**
   * Returns the available hosts properties as a Map.
   *
   * @return Map containing the hosts properties
   */
  def getHosts() throws AmbariConnectionException {
    slurp('hosts', 'Hosts')
  }

  /**
   * Returns the filtered parameters of all hosts as a Map
   * @param hosts the list of the hosts
   * @param fields the list of the parameter names
   * @throws AmbariConnectionException
   */
  def Map<String, Object> getFilteredParamsForAllHosts(String clusterName, List<String> fields) throws AmbariConnectionException {
    slurp("clusters/${clusterName}/hosts", fields.join(','))
  }

  /**
   * Returns the filtered parameters of the given hosts as a Map
   * @param hosts the list of the hosts
   * @param fields the list of the parameter names
   * @throws AmbariConnectionException
   */
  def Map<String, Object> getFilteredHostsParams(String clusterName, List<String> hosts, List<String> fields) throws AmbariConnectionException {
    slurp("clusters/${clusterName}/hosts", fields.join(','), "Hosts/host_name.in(${hosts.join(',')})")
  }

  /**
   * Returns the filtered parameters of the given hosts and components as a Map
   * @param hosts the list of the hosts
   * @param fields the list of the parameter names
   * @throws AmbariConnectionException
   */
  def Map<String, Object> getFilteredHostsParams(String clusterName, List<String> hosts, List<String> components, List<String> fields)
          throws AmbariConnectionException {
    slurp("clusters/${clusterName}/hosts", fields.join(','),
            "Hosts/host_name.in(${hosts.join(',')})&host_components/HostRoles/component_name.in(${components.join(',')})")
  }

  /**
   * Returns the filtered parameters of components for all the hosts as a Map
   * @param hosts the list of the hosts
   * @param fields the list of the parameter names
   * @throws AmbariConnectionException
   */
  def Map<String, Object> getFilteredComponentParamsForAllHosts(String clusterName, List<String> components, List<String> fields)
          throws AmbariConnectionException {
    slurp("clusters/${clusterName}/hosts", fields.join(','), "host_components/HostRoles/component_name.in(${components.join(',')})")
  }

  /**
   * Returns the properties of the host components as a Map parsed from the Ambari response json.
   *
   * @param host which host's components are requested
   * @return component properties as Map
   */
  def getHostComponents(host) throws AmbariConnectionException {
    getAllResources("hosts/$host/host_components", 'HostRoles')
  }

  Map<String, ?> getResourceRequestMap(String path, Map<String, String> queryParams, String queryString = '') {
    def Map requestMap
    if (queryParams) {
      requestMap = ['path': "${ambariClient.ambari.getUri()}" + path, 'query': queryParams]
    } else {
      requestMap = ['path': "${ambariClient.ambari.getUri()}" + path]
    }
    if (queryString) {
      requestMap << ['queryString': queryString]
    }
    return requestMap
  }

  def String createJson(String templateName, Map bindings) throws CompilationFailedException, IOException {
    def InputStream inPut = this.getClass().getClassLoader().getResourceAsStream("templates/$templateName");
    templateProcessor.createTemplate(new InputStreamReader(inPut)).make(bindings);
  }

  def SSLContext setupSSLContext(String clientCert, String clientKey, String serverCert) throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    SSLContext context = SSLContexts.custom()
            .loadTrustMaterial(KeystoreUtils.createTrustStore(serverCert))
            .loadKeyMaterial(KeystoreUtils.createKeyStore(clientCert, clientKey), 'consul'.toCharArray())
            .build();
    return context;
  }

  public static X509HostnameVerifier hostnameVerifier() {
    return new X509HostnameVerifier() {
      @Override
      void verify(String s, SSLSocket sslSocket) throws IOException {
      }

      @Override
      void verify(String s, X509Certificate x509Certificate) throws SSLException {
      }

      @Override
      void verify(String s, String[] strings, String[] strings1) throws SSLException {
      }

      @Override
      boolean verify(String s, SSLSession sslSession) {
        return true
      }
    }
  }

  def Registry<ConnectionSocketFactory> setupSchemeRegistry(SSLContext sslContext) {
    RegistryBuilder<ConnectionSocketFactory> registryBuilder = RegistryBuilder.create();
    registryBuilder.register('http', PlainConnectionSocketFactory.getSocketFactory());
    if (sslContext != null) {
      SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(sslContext, hostnameVerifier());
      registryBuilder.register('https', socketFactory);
    }
    return registryBuilder.build();
  }

  def int putAndGetId(def Map<String, ?> putRequestMap) throws URISyntaxException, ClientProtocolException, IOException {
    log.info('AmbariClient putAndGetId, requestMap: {}', putRequestMap)
    def response = ambariClient.ambari.put(putRequestMap)
    def responseData = response.getAt('responseData')?.getAt('str') as String
    log.info('AmbariClient statusLine: {}, responseData: {}', response.getAt('statusLine'), responseData)
    responseData == null ? -1 : ambariClient.slurper.parseText(responseData)?.Requests?.id
  }
}

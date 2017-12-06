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

@Slf4j
trait KerberosService extends BlueprintService {

  /**
   * Create a default kerberos config (same as with the wizard)
   *
   * @param kdcHost key distribution center's host
   * @param realm kerberos realm
   * @param domain kerberos domain
   */
  def void createKerberosConfig(String kdcHosts, String realm, String domain) {
    def model = [
      'KDC_HOSTS'     : kdcHosts,
      'REALM'        : realm,
      'DOMAIN'       : domain,
      'TAG'          : "version${System.currentTimeMillis()}",
      'ATTR_TEMPLATE': utils.getResourceContent('templates/kerberos-attr-template'),
      'CONTENT'      : utils.getResourceContent('templates/kerberos-content-template'),]
    def Map<String, ?> putRequestMap = [:]
    putRequestMap.put('requestContentType', ContentType.URLENC)
    putRequestMap.put('path', "clusters/${getClusterName()}")
    putRequestMap.put('body', utils.createJson('kerberos-config.json', model));
    ambari.put(putRequestMap)
  }

  /**
   * Creates a default kerberos descriptor for the different Hadoop services.
   *
   * @param realm kerberos realm
   */
  def void createKerberosDescriptor(String realm) {
    def json = utils.getResourceContent('templates/kerberos-descriptor').replaceAll('actual-realm', realm)
    ambari.post(path: "clusters/${getClusterName()}/artifacts/kerberos_descriptor", body: json, { it })
  }

  /**
   * In order to add/remove or change any service or host related config in a kerberos secure cluster you need
   * to set the session attributes which contains an admin principal.
   *
   * @param principal admin principal
   * @param password password for the admin principal
   */
  def void setKerberosSession(String principal, String password) {
    def session = ['session_attributes': ['kerberos_admin': ['principal': principal, 'password': password]]]
    def Map<String, ?> kdcPut = [:]
    kdcPut.put('requestContentType', ContentType.URLENC)
    kdcPut.put('path', "clusters/${getClusterName()}")
    kdcPut.put('body', new JsonBuilder(session).toPrettyString());
    ambari.put(kdcPut)
  }

  /**
   * Enables kerberos security on the cluster. It will generate the necessary keytabs using the previously
   * provided krb5-conf.
   *
   * @return id of the request
   */
  def int enableKerberos() {
    changeKerberosState('KERBEROS')
  }

  /**
   * Disbles kerberos security on the cluster.
   *
   * @return id of the request
   */
  def int disableKerberos() {
    changeKerberosState('NONE')
  }

  private int changeKerberosState(String state) {
    Map<String, ?> putRequestMap = [:]
    putRequestMap.put('requestContentType', ContentType.URLENC)
    putRequestMap.put('path', "clusters/${getClusterName()}")
    putRequestMap.put('body', new JsonBuilder(['Clusters': ['security_type': state]]).toPrettyString())

    utils.putAndGetId(putRequestMap)
  }

  /**
   * Generate the keytabs.
   *
   * @param missingOnly if set to true, keytabs will be generated only for missing hosts and services
   * @return id of the request
   */
  def int generateKeytabs(boolean missingOnly) {
    def Map<String, ?> putRequestMap = [:]
    putRequestMap.put('requestContentType', ContentType.URLENC)
    putRequestMap.put('path', "clusters/${getClusterName()}")
    putRequestMap.put('body', new JsonBuilder(['Clusters': ['security_type': 'KERBEROS']]).toPrettyString())
    putRequestMap.put('query', ['regenerate_keytabs': missingOnly ? 'missing' : 'all'])

    utils.putAndGetId(putRequestMap)
  }

  /**
   * Adds the required kerberos configurations to a blueprint and the KERBEROS_CLIENT for each host group.
   *
   * @param blueprint blueprint in JSON format
   * @param kdcHost KDC server address
   * @param realm REALM for KDC
   * @param domains comma separated domain names
   * @return Returns the blueprint in JSON format with the extended kerberos configuration
   */
  def String extendBlueprintWithKerberos(String blueprint,String kdcType, String kdcHosts, String realm, String domains, String ldapUrl, String containerDn,
                                         Boolean useUdp, Integer kpropPort) {
    String krb5_conf_content = utils.getResourceContent('templates/krb5-conf-template.conf').replaceAll("udp_preference_limit_content", useUdp ? "0" : "1");
    if (kpropPort != null) {
      krb5_conf_content = krb5_conf_content.replaceAll("iprop_enable_content", "true")
      krb5_conf_content = krb5_conf_content.replaceAll("iprop_port_content", kpropPort.toString())
    } else {
      krb5_conf_content = krb5_conf_content.replaceAll("iprop_enable_content", "false")
      krb5_conf_content = krb5_conf_content.replaceAll("iprop_port_content", "8888")
    }
    def config = [
      "kerberos-env": ["realm"           : realm, "kdc_type": kdcType, "kdc_hosts": kdcHosts, "admin_server_host": kdcHosts,
                       "encryption_types": "aes des3-cbc-sha1 rc4 des-cbc-md5", "ldap_url": ldapUrl == null ? "" : ldapUrl,
                       "container_dn": containerDn == null ? "" : containerDn],
    ]
    if (!useUdp || kpropPort != null) {
      config["krb5-conf"] = ["domains": domains, "manage_krb5_conf": "true", "content": krb5_conf_content.toString()]
    } else {
      config["krb5-conf"] = ["domains": domains, "manage_krb5_conf": "true"]
    }
    def blueprintMap = slurper.parseText(blueprint)
    blueprintMap.host_groups.each {
      def kbClient = ["name": "KERBEROS_CLIENT"]
      def comps = it.components
      comps.remove(kbClient)
      comps << kbClient
    }
    def bpDetails = blueprintMap.Blueprints
    def kerbType = ["type": "KERBEROS"]
    if (bpDetails.security) {
      bpDetails.security << kerbType
    } else {
      bpDetails << ["security": kerbType]
    }
    extendBlueprintGlobalConfiguration(new JsonBuilder(blueprintMap).toPrettyString(), config)
  }
}

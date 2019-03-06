/*
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

import com.sequenceiq.ambari.client.AmbariConnectionException
import groovy.json.JsonBuilder
import groovy.util.logging.Slf4j
import groovyx.net.http.HttpResponseException
import org.apache.http.client.ClientProtocolException

@Slf4j
trait GroupService extends CommonService {

  private final String JSON_KEY = 'Groups'
  private final String GROUPS = 'groups'
  private final String MEMBERS = 'members'
  private final String MEMBER_INFO = 'MemberInfo'

  /**
   * Create a new group.
   */
  def createGroup(String group) throws URISyntaxException, ClientProtocolException, HttpResponseException, IOException {
    def context = ["$JSON_KEY/group_name": group]
    ambari.post(path: GROUPS, body: new JsonBuilder(context).toPrettyString(), { it })
  }

  /**
   * Delete a group.
   */
  def deleteGroup(String group) throws URISyntaxException, ClientProtocolException, HttpResponseException, IOException {
    ambari.delete(path: "$GROUPS/$group")
  }

  /**
   * Get the details of a group.
   */
  def getGroup(String group) throws AmbariConnectionException {
    def result = utils.slurp("$GROUPS/$group", JSON_KEY)
    result ? result[JSON_KEY] : [:]
  }

  /**
   * Get the names of users that belong to the group.
   */
  def getGroupMembers(String group) throws AmbariConnectionException {
    def result = utils.slurp("$GROUPS/$group/$MEMBERS", MEMBER_INFO)
    result ? result.items?.collect { it[MEMBER_INFO]['user_name'] } : []
  }

  /**
   * Add a user to the group.
   */
  def addMemberToGroup(String group, String user) throws URISyntaxException, ClientProtocolException, HttpResponseException, IOException {
    ambari.post(path: "$GROUPS/$group/$MEMBERS/$user")
  }

  /**
   * Remove a user from the group.
   */
  def removeMemberFromGroup(String group, String user) throws URISyntaxException, ClientProtocolException, HttpResponseException, IOException {
    ambari.delete(path: "$GROUPS/$group/$MEMBERS/$user")
  }
}
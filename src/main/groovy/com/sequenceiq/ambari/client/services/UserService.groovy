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
trait UserService extends CommonService {

  private final String JSON_KEY = 'Users'
  private final String USERS = 'users'
  private final String ACTIVE = 'active'
  private final String ADMIN = 'admin'
  private final String PASSWORD = 'password'

  /**
   * Create a new Ambari user.
   */
  def createUser(String user, String password, boolean admin) {
    def context = ["$JSON_KEY/$ACTIVE": true, "$JSON_KEY/$ADMIN": admin, "$JSON_KEY/$PASSWORD": password, "$JSON_KEY/user_name": user]
    ambari.post(path: USERS, body: new JsonBuilder(context).toPrettyString(), { it })
  }

  /**
   * Delete an Ambari user.
   */
  def deleteUser(String user) {
    ambari.delete(path: "$USERS/$user")
  }

  /**
   * Get the details of a user.
   */
  def getUser(String user) {
    def result = utils.slurp("$USERS/$user", JSON_KEY)
    result ? result[JSON_KEY] : [:]
  }

  /**
   * Change the password of an Ambari user.
   *
   * @param oldPassword the password of AmbariClient's user, not necessarily the password being changed
   * @param admin ignored, kept for compatibility
   */
  def changePassword(String user, String oldPassword, String newPassword, boolean admin = false) {
    def context = ["$JSON_KEY/$PASSWORD": newPassword, "$JSON_KEY/old_password": oldPassword]
    ambari.put(path: "$USERS/$user", body: new JsonBuilder(context).toPrettyString(), requestContentType: ContentType.URLENC)
  }

  /**
   * Grant or revoke admin privilege to a user.
   * @param grant if true, privilege will be granted, otherwise it will be revoked
   */
  def grantAdminPrivilege(String user, boolean grant) {
    def context = ["$JSON_KEY/$ADMIN": grant]
    ambari.put(path: "$USERS/$user", body: new JsonBuilder(context).toPrettyString(), requestContentType: ContentType.URLENC)
  }

  /**
   * Activate or inactivate a user.
   * @param active if true, user will be active, otherwise it will be inactive
   */
  def setUserActive(String user, boolean active) {
    def context = ["$JSON_KEY/$ACTIVE": active]
    ambari.put(path: "$USERS/$user", body: new JsonBuilder(context).toPrettyString(), requestContentType: ContentType.URLENC)
  }

}
/*
 * Copyright 2013 Maurício Linhares
 *
 * Maurício Linhares licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */


package com.github.mauricio.async.db.postgresql.exceptions

import com.github.mauricio.async.db.exceptions.DatabaseException
import com.github.mauricio.async.db.postgresql.messages.backend.AuthenticationResponseType

class MissingCredentialInformationException(
                                             val username: String,
                                             val password: String?,
                                             val authenticationResponseType: AuthenticationResponseType)
  : DatabaseException(
    "Username and password were required by auth type %s but are not available (username=[%s] password=[%s]".format(
      authenticationResponseType,
      username,
      password
    )
  )
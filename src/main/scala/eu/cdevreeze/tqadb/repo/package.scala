/*
 * Copyright 2019-2020 Chris de Vreeze
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.cdevreeze.tqadb

/**
 * Transactional repositories, encapsulating transactional access to the database.
 *
 * The idea of a transactional data repository is consistent with the same idea in Spring Data JDBC
 * (https://docs.spring.io/spring-data/jdbc/docs/1.1.1.RELEASE/reference/html/#jdbc.transactions). Also note that if we sometimes need a
 * transactional service or facade spanning multiple repositories, we can simply create one.
 *
 * This is preferable to the old best practice and heavy-handed approach of having separate transactional "service" and non-transactional
 * DAO layers where in most cases both would have the same public APIs, yet would not even share these public APIs.
 *
 * Unlike Spring Data JDBC, here we have 2 co-operating implementations of each type of repository, one non-transactional (using the JdbcTemplate)
 * and one transactional (using the transaction manager), where the second one invokes the first one. The non-transactional
 * one can easily be re-used in other (transactional) services/facades. The transactional one is used by higher layers of the application
 * (presentation/integration layer, or console program).
 *
 * Although the (mockable) JDBC template is passed as one of the repository constructor parameters, in theory a DataSource could have been passed
 * instead. After all, the PlatformTransactionManager and DataSource must be created only once and they must live as long as the application,
 * whereas the TransactionTemplate and JdbcTemplate objects created from them may be short-lived objects created as many times
 * as desired. The TransactionTemplate and JdbcTemplate, through the PlatformTransactionManager and utilities like DataSourceUtils,
 * manage transactions and connections in co-operation (keeping the connection open long enough for the transaction to finish), through
 * a thread-bound JDBC Connection. As a consequence, there can be only one thread-bound Connection per DataSource.
 *
 * Keep this use of thread-bound JDBC Connections in mind, especially when building reactive applications using these repositories.
 * For example, when using Play in combination with JDBC (with or without the Spring JdbcTemplate), see
 * https://www.playframework.com/documentation/2.7.x/ScalaDatabase.
 */
package object repo

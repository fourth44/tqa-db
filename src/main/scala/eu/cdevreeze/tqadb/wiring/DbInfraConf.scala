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

package eu.cdevreeze.tqadb.wiring

import javax.sql.DataSource
import org.springframework.transaction.PlatformTransactionManager

/**
 * Database "infrastructure" configuration without any magic, and fully understood by the Scala compiler.
 * This configuration holds a DataSource and PlatformTransactionManager. Both must be created only once, and then
 * live for the duration of the application.
 *
 * It can be used to create JdbcTemplate and TransactionTemplate instances, which may be created as often as desired.
 *
 * @author Chris de Vreeze
 */
trait DbInfraConf {

  def dataSource: DataSource

  def transactionManager: PlatformTransactionManager
}

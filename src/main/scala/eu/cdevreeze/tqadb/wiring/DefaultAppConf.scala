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

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import eu.cdevreeze.tqadb.internal.TransactionTemplates
import eu.cdevreeze.tqadb.internal.TransactionTemplates._
import eu.cdevreeze.tqadb.repo.DefaultDtsRepo
import eu.cdevreeze.tqadb.repo.DefaultEntrypointRepo
import eu.cdevreeze.tqadb.repo.DtsRepo
import eu.cdevreeze.tqadb.repo.EntrypointRepo
import org.springframework.jdbc.core.JdbcTemplate

/**
 * Default application configuration without any magic, and fully understood by the Scala compiler.
 *
 * Note that choosing a transaction timeout for "long transactions" at runtime is possible, and that this would not
 * be possible when using the Transactional annotation instead of a TransactionTemplate.
 *
 * @author Chris de Vreeze
 */
final class DefaultAppConf(val dbInfraConf: DbInfraConf, val longTransactionTimeoutInSeconds: Int) extends AppConf {

  def dtsRepo: DtsRepo = {
    val txManager = dbInfraConf.transactionManager

    val txTemplate = TransactionTemplates(txManager).withIsolationLevel(IsolationSerializable)
      .withTimeoutInSeconds(longTransactionTimeoutInSeconds)

    val readOnlyTxTemplate = TransactionTemplates(txManager).withReadOnly(true).withIsolationLevel(IsolationSerializable)
      .withTimeoutInSeconds(longTransactionTimeoutInSeconds)

    new DefaultDtsRepo(new JdbcTemplate(dbInfraConf.dataSource), txTemplate, readOnlyTxTemplate)
  }

  def entrypointRepo: EntrypointRepo = {
    val txManager = dbInfraConf.transactionManager

    val txTemplate = TransactionTemplates(txManager).withIsolationLevel(IsolationSerializable)

    val readOnlyTxTemplate = TransactionTemplates(txManager).withReadOnly(true).withIsolationLevel(IsolationSerializable)

    new DefaultEntrypointRepo(new JdbcTemplate(dbInfraConf.dataSource), txTemplate, readOnlyTxTemplate)
  }
}

object DefaultAppConf {

  def getInstance(dbInfraConf: DbInfraConf): DefaultAppConf = {
    getInstance(dbInfraConf, ConfigFactory.load())
  }

  def getInstance(dbInfraConf: DbInfraConf, config: Config): DefaultAppConf = {
    val longTransactionTimeoutInSeconds: Int =
      if (config.hasPath("longTransactionTimeoutInSeconds")) config.getInt("longTransactionTimeoutInSeconds") else 600 // scalastyle:off

    new DefaultAppConf(dbInfraConf, longTransactionTimeoutInSeconds)
  }
}

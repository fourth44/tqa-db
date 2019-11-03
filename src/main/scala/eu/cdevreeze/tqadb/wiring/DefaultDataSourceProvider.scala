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
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource
import org.postgresql.ds.PGSimpleDataSource

/**
 * Default DataSource provider without any magic, and fully understood by the Scala compiler.
 *
 * @author Chris de Vreeze
 */
final class DefaultDataSourceProvider(
  val host: String,
  val port: Int,
  val user: String,
  val password: String,
  val database: String) extends DataSourceProvider {

  val simpleDataSource: DataSource = {
    val ds = new PGSimpleDataSource()
    ds.setUrl(s"jdbc:postgresql://${host}:${port}/$database")
    ds.setUser(user)
    ds.setPassword(password)
    ds
  }

  val dataSource: DataSource = {
    // Uses connection pool, so closing a connection means giving it back to the connection pool

    // See https://github.com/brettwooldridge/HikariCP#configuration-knobs-baby

    val ds = new HikariDataSource()
    ds.setDataSource(simpleDataSource)
    ds.setAutoCommit(false)
    ds
  }
}

object DefaultDataSourceProvider {

  def getInstance(): DefaultDataSourceProvider = {
    getInstance(ConfigFactory.load())
  }

  def getInstance(config: Config): DefaultDataSourceProvider = {
    new DefaultDataSourceProvider(
      config.getString("db.host"),
      config.getInt("db.port"),
      config.getString("db.user"),
      config.getString("db.password"),
      config.getString("db.database")
    )
  }
}

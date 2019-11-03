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
  val password: String) extends DataSourceProvider {

  val simpleDataSource: DataSource = {
    val ds = new PGSimpleDataSource()
    ds.setUrl(s"jdbc:postgresql://${host}:${port}/taxo") // Assuming database taxo
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

  def getInstanceFromSysProps(): DefaultDataSourceProvider = {
    new DefaultDataSourceProvider(
      System.getProperty("db.host", "localhost"),
      System.getProperty("db.port", "5432").toInt,
      System.getProperty("db.user").ensuring(Option(_).nonEmpty),
      System.getProperty("db.password").ensuring(Option(_).nonEmpty)
    )
  }
}

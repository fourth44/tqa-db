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

package eu.cdevreeze.tqadb.repo

import java.net.URI
import java.sql.ResultSet

import scala.jdk.CollectionConverters._

import eu.cdevreeze.tqadb.data.Entrypoint
import eu.cdevreeze.tqadb.repo.DefaultEntrypointRepo.NonTransactionalEntrypointRepo
import org.jooq.impl.DSL.field
import org.jooq.impl.DSL.select
import org.jooq.impl.DSL.table
import org.springframework.jdbc.core.JdbcOperations
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionStatus
import org.springframework.transaction.support.TransactionTemplate

/**
 * Transactional entrypoint repository.
 *
 * @author Chris de Vreeze
 */
final class DefaultEntrypointRepo(val txManager: PlatformTransactionManager, val jdbcTemplate: JdbcOperations) extends EntrypointRepo {

  private val delegateRepo: EntrypointRepo = new NonTransactionalEntrypointRepo(jdbcTemplate)

  // TODO Consider using the RetryTemplate for read-write transactions with isolation level serializable

  def findAllEntrypoints(): Seq[Entrypoint] = {
    val txTemplate = new TransactionTemplate(txManager)
    txTemplate.setReadOnly(true)

    txTemplate.execute { _: TransactionStatus =>
      delegateRepo.findAllEntrypoints()
    }
  }
}

object DefaultEntrypointRepo {

  final class NonTransactionalEntrypointRepo(val jdbcTemplate: JdbcOperations) extends EntrypointRepo {

    private def namedParameterJdbcTemplate: NamedParameterJdbcOperations = new NamedParameterJdbcTemplate(jdbcTemplate)

    def findAllEntrypoints(): Seq[Entrypoint] = {
      namedParameterJdbcTemplate.query(findAllEntrypointDocUrisSql, entrypointDocUriRowMapper)
        .asScala.groupBy(_.name).toSeq.map { case (name, rows) => Entrypoint(name, rows.map(_.docUri).toSet) }
    }
  }

  private case class EntrypointDocUri(name: String, docUri: URI)

  val findAllEntrypointDocUrisSql: String =
    select(field("entrypoints.name"), field("entrypoint_docuris.docuri"))
      .from(table("entrypoints"))
      .join(table("entrypoint_docuris"))
      .on(field("entrypoints.name").eq(field("entrypoint_docuris.entrypoint_name")))
      .getSQL()

  private val entrypointDocUriRowMapper: RowMapper[EntrypointDocUri] = { (rs: ResultSet, rowNum: Int) =>
    EntrypointDocUri(
      name = rs.getString("name"),
      docUri = URI.create(rs.getString("docuri"))
    )
  }
}

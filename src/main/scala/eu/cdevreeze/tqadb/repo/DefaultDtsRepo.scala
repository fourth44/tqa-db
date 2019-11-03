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

import java.sql.PreparedStatement
import java.sql.SQLXML

import eu.cdevreeze.tqa.base.dom.TaxonomyDocument
import eu.cdevreeze.tqa.base.taxonomy.BasicTaxonomy
import eu.cdevreeze.tqadb.data.Entrypoint
import eu.cdevreeze.yaidom.saxon.SaxonDocument
import javax.xml.transform.sax.SAXResult
import net.sf.saxon.query.QueryResult
import net.sf.saxon.serialize.SerializationProperties
// import org.jooq.util.postgres.PostgresDSL._
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.JdbcOperations
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.TransactionStatus
import org.springframework.transaction.support.TransactionTemplate

/**
 * Transactional DTS repository.
 *
 * @author Chris de Vreeze
 */
final class DefaultDtsRepo(val txManager: PlatformTransactionManager, val jdbcTemplate: JdbcOperations) extends DtsRepo {

  // private def namedParameterJdbcTemplate: NamedParameterJdbcOperations = new NamedParameterJdbcTemplate(jdbcTemplate)

  import DefaultDtsRepo._

  // TODO Consider using the RetryTemplate for read-write transactions with isolation level serializable

  def insertTaxo(entrypoint: Entrypoint, taxo: BasicTaxonomy): Unit = {
    require(
      entrypoint.docUris.subsetOf(taxo.taxonomyBase.taxonomyDocUriMap.keySet),
      s"Not an entrypoint for the given taxonomy: $entrypoint")

    val txTemplate: TransactionTemplate = new TransactionTemplate(txManager)
    txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE)
    txTemplate.setTimeout(600) // scalastyle:off

    txTemplate.executeWithoutResult { _: TransactionStatus =>
      val batch: Map[Int, TaxonomyDocument] = taxo.taxonomyDocs.zipWithIndex.map { case (d, i) => i -> d }.toMap
      val batchSize = batch.size

      val psSetterInsertDoc: BatchPreparedStatementSetter = new BatchPreparedStatementSetter {
        override def setValues(ps: PreparedStatement, i: Int): Unit = {
          val taxoDoc = batch(i)
          ps.setString(1, taxoDoc.uri.toString)
          val sqlXml = convertDocToSQLXML(taxoDoc, ps)
          ps.setSQLXML(2, sqlXml)
          sqlXml.free()
        }

        override def getBatchSize: Int = batchSize
      }

      jdbcTemplate.batchUpdate(insertDocSql, psSetterInsertDoc)

      jdbcTemplate.update(insertEntrypointSql, entrypoint.name)

      entrypoint.docUris.foreach { epDocUri =>
        jdbcTemplate.update(insertEntrypointDocUrisSql, entrypoint.name, epDocUri.toString)
      }

      val psSetterInsertDtsUri: BatchPreparedStatementSetter = new BatchPreparedStatementSetter {
        override def setValues(ps: PreparedStatement, i: Int): Unit = {
          val taxoDoc = batch(i)
          ps.setString(1, entrypoint.name)
          ps.setString(2, taxoDoc.uri.toString)
        }

        override def getBatchSize: Int = batchSize
      }

      jdbcTemplate.batchUpdate(insertDtsDocUrisSql, psSetterInsertDtsUri)
    }
  }

  private def convertDocToSQLXML(doc: TaxonomyDocument, ps: PreparedStatement): SQLXML = {
    val sqlXml: SQLXML = ps.getConnection().createSQLXML()
    val saxResult = sqlXml.setResult(classOf[SAXResult])
    val saxonNodeInfo = doc.backingDocument.asInstanceOf[SaxonDocument].wrappedTreeInfo.getRootNode
    QueryResult.serialize(saxonNodeInfo, saxResult, new SerializationProperties())
    sqlXml
  }
}

object DefaultDtsRepo {

  val insertDocSql: String =
    "INSERT INTO taxo_documents (docuri, doc) VALUES (?, ?) ON CONFLICT (docuri) DO NOTHING"

  val insertEntrypointSql: String =
    "INSERT INTO entrypoints (name) VALUES (?)"

  val insertEntrypointDocUrisSql: String =
    "INSERT INTO entrypoint_docuris (entrypoint_name, docuri) VALUES (?, ?)"

  val insertDtsDocUrisSql: String =
    "INSERT INTO dts_docuris (entrypoint_name, docuri) VALUES (?, ?)"
}

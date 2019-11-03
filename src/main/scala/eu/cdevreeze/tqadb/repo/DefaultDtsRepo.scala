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
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLXML

import scala.jdk.CollectionConverters._

import eu.cdevreeze.tqa.base.dom.TaxonomyDocument
import eu.cdevreeze.tqa.base.relationship.DefaultRelationshipFactory
import eu.cdevreeze.tqa.base.relationship.RelationshipFactory
import eu.cdevreeze.tqa.base.taxonomy.BasicTaxonomy
import eu.cdevreeze.tqa.base.taxonomybuilder.DocumentCollector
import eu.cdevreeze.tqa.base.taxonomybuilder.TaxonomyBuilder
import eu.cdevreeze.tqa.base.taxonomybuilder.TrivialDocumentCollector
import eu.cdevreeze.tqa.docbuilder.DocumentBuilder
import eu.cdevreeze.tqadb.data.Entrypoint
import eu.cdevreeze.tqadb.repo.DefaultDtsRepo.NonTransactionalDtsRepo
import eu.cdevreeze.yaidom.saxon.SaxonDocument
import javax.xml.transform.sax.SAXResult
import javax.xml.transform.sax.SAXSource
import net.sf.saxon.query.QueryResult
import net.sf.saxon.s9api.Processor
import net.sf.saxon.serialize.SerializationProperties
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.jooq.impl.DSL.field
import org.jooq.impl.DSL.select
import org.jooq.impl.DSL.table
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

  private val delegateRepo: DtsRepo = new NonTransactionalDtsRepo(jdbcTemplate)

  // TODO Consider using the RetryTemplate for read-write transactions with isolation level serializable

  def insertTaxo(entrypoint: Entrypoint, taxo: BasicTaxonomy): Unit = {
    require(
      entrypoint.docUris.subsetOf(taxo.taxonomyBase.taxonomyDocUriMap.keySet),
      s"Not an entrypoint for the given taxonomy: $entrypoint")

    val txTemplate: TransactionTemplate = new TransactionTemplate(txManager)
    txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE)
    txTemplate.setTimeout(600) // scalastyle:off

    txTemplate.executeWithoutResult { _: TransactionStatus =>
      delegateRepo.insertTaxo(entrypoint, taxo)
    }
  }

  def insertOrUpdateTaxo(entrypoint: Entrypoint, taxo: BasicTaxonomy): Unit = {
    require(
      entrypoint.docUris.subsetOf(taxo.taxonomyBase.taxonomyDocUriMap.keySet),
      s"Not an entrypoint for the given taxonomy: $entrypoint")

    val txTemplate: TransactionTemplate = new TransactionTemplate(txManager)
    txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE)
    txTemplate.setTimeout(600) // scalastyle:off

    txTemplate.executeWithoutResult { _: TransactionStatus =>
      delegateRepo.insertOrUpdateTaxo(entrypoint, taxo)
    }
  }

  def deleteTaxo(entrypoint: Entrypoint): Unit = {
    val txTemplate: TransactionTemplate = new TransactionTemplate(txManager)
    txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE)

    txTemplate.executeWithoutResult { _: TransactionStatus =>
      delegateRepo.deleteTaxo(entrypoint)
    }
  }

  def getTaxonomy(entrypointName: String): BasicTaxonomy = {
    val txTemplate = new TransactionTemplate(txManager)
    txTemplate.setReadOnly(true)
    txTemplate.setTimeout(300) // scalastyle:off

    txTemplate.execute { _: TransactionStatus =>
      delegateRepo.getTaxonomy(entrypointName)
    }
  }
}

object DefaultDtsRepo {

  final class NonTransactionalDtsRepo(val jdbcTemplate: JdbcOperations) extends DtsRepo {

    private def namedParameterJdbcTemplate: NamedParameterJdbcOperations = new NamedParameterJdbcTemplate(jdbcTemplate)

    def insertTaxo(entrypoint: Entrypoint, taxo: BasicTaxonomy): Unit = {
      require(
        entrypoint.docUris.subsetOf(taxo.taxonomyBase.taxonomyDocUriMap.keySet),
        s"Not an entrypoint for the given taxonomy: $entrypoint")

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

    def insertOrUpdateTaxo(entrypoint: Entrypoint, taxo: BasicTaxonomy): Unit = {
      require(
        entrypoint.docUris.subsetOf(taxo.taxonomyBase.taxonomyDocUriMap.keySet),
        s"Not an entrypoint for the given taxonomy: $entrypoint")

      deleteTaxo(entrypoint)
      insertTaxo(entrypoint, taxo)
    }

    def deleteTaxo(entrypoint: Entrypoint): Unit = {
      jdbcTemplate.update(deleteDtsDocUrisSql, entrypoint.name)

      jdbcTemplate.update(deleteEntrypointDocUrisSql, entrypoint.name)

      jdbcTemplate.update(deleteEntrypointSql, entrypoint.name)

      jdbcTemplate.update(deleteDocsForEntrypointSql)
    }

    def getTaxonomy(entrypointName: String): BasicTaxonomy = {
      val processor = new Processor(false)
      val taxonomyDocRowMapper = getTaxonomyDocRowMapper(processor)

      val docs: Seq[TaxonomyDocument] =
        namedParameterJdbcTemplate.query(getTaxonomyDocSql, Map("name" -> entrypointName).asJava, taxonomyDocRowMapper).asScala.toSeq

      getTaxonomy(docs, processor)
    }

    private def convertDocToSQLXML(doc: TaxonomyDocument, ps: PreparedStatement): SQLXML = {
      val sqlXml: SQLXML = ps.getConnection().createSQLXML()
      val saxResult = sqlXml.setResult(classOf[SAXResult])
      saxResult.setSystemId(doc.uri.toString) // Trying to retain the document URI, but we cannot count on it.

      val saxonNodeInfo = doc.backingDocument.asInstanceOf[SaxonDocument].wrappedTreeInfo.getRootNode
      saxonNodeInfo.setSystemId(doc.uri.toString)

      QueryResult.serialize(saxonNodeInfo, saxResult, new SerializationProperties())
      sqlXml
    }

    private def getTaxonomy(taxonomyDocs: Seq[TaxonomyDocument], processor: Processor): BasicTaxonomy = {
      val docsByUri: Map[URI, TaxonomyDocument] = taxonomyDocs.groupBy(_.uri).view.mapValues(_.head).toMap
      val docUris: Set[URI] = docsByUri.keySet

      val docBuilder: DocumentBuilder = new DocumentBuilder {
        override type BackingDoc = SaxonDocument

        override def build(uri: URI): SaxonDocument = {
          docsByUri.getOrElse(uri, sys.error(s"Missing document $uri")).backingDocument.asInstanceOf[SaxonDocument]
            .ensuring(_.uriOption.nonEmpty)
        }
      }

      val docCollector: DocumentCollector = TrivialDocumentCollector
      val relationshipFactory: RelationshipFactory = DefaultRelationshipFactory.StrictInstance

      TaxonomyBuilder.withDocumentBuilder(docBuilder)
        .withDocumentCollector(docCollector).withRelationshipFactory(relationshipFactory).build(docUris)
    }
  }

  val insertDocSql: String =
    "INSERT INTO taxo_documents (docuri, doc) VALUES (?, ?) ON CONFLICT (docuri) DO NOTHING"

  val insertEntrypointSql: String =
    "INSERT INTO entrypoints (name) VALUES (?)"

  val insertEntrypointDocUrisSql: String =
    "INSERT INTO entrypoint_docuris (entrypoint_name, docuri) VALUES (?, ?)"

  val insertDtsDocUrisSql: String =
    "INSERT INTO dts_docuris (entrypoint_name, docuri) VALUES (?, ?)"

  val deleteDtsDocUrisSql: String =
    "DELETE FROM dts_docuris WHERE entrypoint_name = ?"

  val deleteEntrypointDocUrisSql: String =
    "DELETE FROM entrypoint_docuris WHERE entrypoint_name = ?"

  val deleteEntrypointSql: String =
    "DELETE FROM entrypoints WHERE name = ?"

  val deleteDocsForEntrypointSql: String =
    "DELETE FROM taxo_documents AS td WHERE NOT EXISTS (SELECT * FROM dts_docuris AS dd WHERE td.docuri = dd.docuri)"

  val getTaxonomyDocSql: String = {
    select(field("taxo_documents.docuri"), field("taxo_documents.doc"))
      .from(table("taxo_documents"))
      .join(table("dts_docuris"))
      .on(field("taxo_documents.docuri").eq(field("dts_docuris.docuri")))
      .join(table("entrypoints"))
      .on(field("dts_docuris.entrypoint_name").eq(field("entrypoints.name")))
      .where("entrypoints.name = :name")
      .getSQL()
  }

  private def getTaxonomyDocRowMapper(processor: Processor): RowMapper[TaxonomyDocument] = { (rs: ResultSet, rowNum: Int) =>
    val docUri: URI = URI.create(rs.getString("docuri"))
    val docSqlXml: SQLXML = rs.getSQLXML("doc")

    convertSQLXMLToTaxonomyDoc(docSqlXml, docUri, processor)
  }

  private def convertSQLXMLToTaxonomyDoc(sqlXml: SQLXML, docUri: URI, processor: Processor): TaxonomyDocument = {
    val source = sqlXml.getSource(classOf[SAXSource])
    source.setSystemId(docUri.toString) // Trying to make sure that the resulting TaxonomyDocument has its URI filled in

    val treeInfo = processor.newDocumentBuilder().build(source).getUnderlyingNode.getTreeInfo

    val saxonDoc: SaxonDocument = SaxonDocument.wrapDocument(treeInfo).ensuring(_.uriOption.nonEmpty)
    TaxonomyDocument.build(saxonDoc)
  }
}

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

package eu.cdevreeze.tqadb.console

import java.io.File
import java.net.URI
import java.sql.PreparedStatement
import java.sql.SQLXML

import eu.cdevreeze.tqa.base.dom.TaxonomyDocument
import eu.cdevreeze.tqa.base.relationship.DefaultRelationshipFactory
import eu.cdevreeze.tqa.base.relationship.RelationshipFactory
import eu.cdevreeze.tqa.base.taxonomy.BasicTaxonomy
import eu.cdevreeze.tqa.base.taxonomybuilder.DefaultDtsCollector
import eu.cdevreeze.tqa.base.taxonomybuilder.DocumentCollector
import eu.cdevreeze.tqa.base.taxonomybuilder.TaxonomyBuilder
import eu.cdevreeze.tqa.docbuilder.DocumentBuilder
import eu.cdevreeze.tqa.docbuilder.jvm.UriResolvers
import eu.cdevreeze.tqa.docbuilder.saxon.SaxonDocumentBuilder
import eu.cdevreeze.tqadb.data.Entrypoint
import eu.cdevreeze.tqadb.wiring.DefaultAppConf
import eu.cdevreeze.tqadb.wiring.DefaultDataSourceProvider
import eu.cdevreeze.yaidom.saxon.SaxonDocument
import javax.sql.DataSource
import javax.xml.transform.sax.SAXResult
import net.sf.saxon.query.QueryResult
import net.sf.saxon.s9api.Processor
import net.sf.saxon.serialize.SerializationProperties
import org.springframework.jdbc.core.BatchPreparedStatementSetter
// import org.jooq.util.postgres.PostgresDSL._
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.TransactionStatus
import org.springframework.transaction.support.TransactionTemplate
import org.xml.sax.InputSource

/**
 * Program that inserts a DTS into the database. Documents that have already been added are not added again (so they
 * are not overwritten in the database). Both the entrypoint-related tables and the taxonomy document table are filled
 * with the DTS.
 *
 * @author Chris de Vreeze
 */
object DtsInserter {

  def insertDts(taxoRootDir: File, entrypointDocUris: Set[URI]): Unit = {
    val dts: BasicTaxonomy = getTaxonomy(taxoRootDir, entrypointDocUris)

    val entrypointName = entrypointDocUris.head.toString
    val entrypoint = Entrypoint(entrypointName, entrypointDocUris)

    val ds = DefaultDataSourceProvider.getInstanceFromSysProps().dataSource
    val appConf = new DefaultAppConf(ds)

    insertTaxo(entrypoint, dts)(appConf.transactionManager, ds)
  }

  private def getTaxonomy(taxoRootDir: File, entrypointDocUris: Set[URI]): BasicTaxonomy = {
    val processor = new Processor(false)

    val saxonDocBuilder = processor.newDocumentBuilder()
    val uriResolver: URI => InputSource = UriResolvers.fromLocalMirrorRootDirectory(taxoRootDir)

    val docBuilder: DocumentBuilder = SaxonDocumentBuilder(saxonDocBuilder, uriResolver)
    val docCollector: DocumentCollector = DefaultDtsCollector()
    val relationshipFactory: RelationshipFactory = DefaultRelationshipFactory.StrictInstance

    TaxonomyBuilder.withDocumentBuilder(docBuilder)
      .withDocumentCollector(docCollector).withRelationshipFactory(relationshipFactory).build(entrypointDocUris)
  }

  private def insertTaxo(entrypoint: Entrypoint, taxo: BasicTaxonomy)(txManager: PlatformTransactionManager, ds: DataSource): Unit = {
    val txTemplate: TransactionTemplate = new TransactionTemplate(txManager)
    txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE)
    txTemplate.setTimeout(300) // scalastyle:off

    val jdbcTemplate = new JdbcTemplate(ds)

    txTemplate.executeWithoutResult { _: TransactionStatus =>
      val batch: Map[Int, TaxonomyDocument] = taxo.taxonomyDocs.zipWithIndex.map { case (d, i) => i -> d }.toMap
      val batchSize = batch.size

      val psSetter: BatchPreparedStatementSetter = new BatchPreparedStatementSetter {
        override def setValues(ps: PreparedStatement, i: Int): Unit = {
          val taxoDoc = batch(i)
          ps.setString(1, taxoDoc.uri.toString)
          val sqlXml = convertDocToSQLXML(taxoDoc, ps)
          ps.setSQLXML(2, sqlXml)
          sqlXml.free()
        }
        override def getBatchSize: Int = batchSize
      }

      jdbcTemplate.batchUpdate(insertDocSql, psSetter)

      // TODO Other tables
    }
  }

  private def convertDocToSQLXML(doc: TaxonomyDocument, ps: PreparedStatement): SQLXML = {
    val sqlXml: SQLXML = ps.getConnection().createSQLXML()
    val saxResult = sqlXml.setResult(classOf[SAXResult])
    val saxonNodeInfo = doc.backingDocument.asInstanceOf[SaxonDocument].wrappedTreeInfo.getRootNode
    QueryResult.serialize(saxonNodeInfo, saxResult, new SerializationProperties())
    sqlXml
  }

  def main(args: Array[String]): Unit = {
    require(args.sizeIs >= 2, s"Usage: DtsInserter <root dir> <entrypoint doc URI> ...")

    val rootDir = new File(args(0)).ensuring(_.isDirectory)

    insertDts(rootDir, args.drop(1).map(u => URI.create(u)).toSet)
  }

  private val insertDocSql: String =
    "INSERT INTO taxo_documents (docuri, doc) VALUES (?, ?) ON CONFLICT (docuri) DO NOTHING"
}

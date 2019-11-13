package eu.cdevreeze.tqadb.repo

import java.net.URI
import java.sql.PreparedStatement
import java.sql.SQLXML

import cats.data.NonEmptyList
import cats.effect.IO
import doobie.util.transactor.Transactor
import eu.cdevreeze.tqa.base.taxonomy.BasicTaxonomy
import eu.cdevreeze.tqadb.data.Entrypoint
import doobie.implicits._
import doobie._
import cats.implicits._
import eu.cdevreeze.tqa.base.dom.TaxonomyDocument
import eu.cdevreeze.tqa.base.relationship.DefaultRelationshipFactory
import eu.cdevreeze.tqa.base.relationship.RelationshipFactory
import eu.cdevreeze.tqa.base.taxonomybuilder.DocumentCollector
import eu.cdevreeze.tqa.base.taxonomybuilder.TaxonomyBuilder
import eu.cdevreeze.tqa.base.taxonomybuilder.TrivialDocumentCollector
import eu.cdevreeze.tqa.docbuilder.DocumentBuilder
import eu.cdevreeze.yaidom.saxon.SaxonDocument
import javax.xml.transform.sax.SAXSource
import net.sf.saxon.s9api.Processor
import doobie.enum.JdbcType.Other
import doobie.util.log.LogHandler
import javax.xml.transform.sax.SAXResult
import net.sf.saxon.query.QueryResult
import net.sf.saxon.serialize.SerializationProperties

class DoobieDtsRepo extends DtsRepoF[ConnectionIO] {

  import DoobieDtsRepo._

  /**
   * Inserts a DTS into the database. The given entrypoint must belong to the given taxonomy.
   */
  override def insertTaxo(entrypoint: Entrypoint, taxo: BasicTaxonomy): doobie.ConnectionIO[Unit] = {

    for {
      _ <- insertTaxoDocumentsSql.updateMany(taxo.taxonomyDocs.toList)
      _ <- insertEntrypointSql(entrypoint.name).run
      _ <- entrypoint.docUris.toList.traverse { docUri =>
        insertEntrypointDocUrisSql(entrypoint.name, docUri).run
      }
      _ <- insertDtsDocUrisSql.updateMany(taxo.taxonomyDocs.toList.map(td => (entrypoint.name, td.uri)))
    } yield {
      ()
    }
  }

  /**
   * Inserts a DTS into the database, or updates it if it is already in the database. The given entrypoint must belong to the given taxonomy.
   * Updating here means, within the same transaction: first delete, then insert.
   */
  override def insertOrUpdateTaxo(entrypoint: Entrypoint, taxo: BasicTaxonomy): doobie.ConnectionIO[Unit] = {
    val valid = entrypoint.docUris.subsetOf(taxo.taxonomyBase.taxonomyDocUriMap.keySet)
    if (!valid) {
      FC.raiseError(new IllegalArgumentException(s"Not an entrypoint for the given taxonomy: $entrypoint"))
    } else {
      deleteTaxo(entrypoint) *> insertTaxo(entrypoint, taxo)
    }
  }

  /**
   * Removes a DTS from the database. If the entrypoint does not occur in the database, this is a no-op.
   */
  override def deleteTaxo(entrypoint: Entrypoint): doobie.ConnectionIO[Unit] = {
    for {
      _ <- deleteDtsDocUrisSql(entrypoint.name).run
      _ <- deleteEntrypointDocUrisSql(entrypoint.name).run
      _ <- deleteEntrypointSql(entrypoint.name).run
      _ <- deleteDocsForEntrypointSql.run
    } yield ()
  }

  /**
   * Loads the taxonomy with the given entrypoint from the database. The taxonomy is returned as a TQA taxonomy.
   * An exception is thrown if the entrypoint does not occur in the database.
   */
  override def getTaxonomy(entrypointName: String): doobie.ConnectionIO[BasicTaxonomy] = {
    val processor = new Processor(false)

    implicit val saxonRd = saxonRead(processor)

    getTaxonomyDocumentsSql(entrypointName).to[List]
      .map(taxonomyDocuments => DoobieDtsRepo.getTaxonomy(taxonomyDocuments, processor))
  }

}

object DoobieDtsRepo {

  implicit val han: LogHandler = LogHandler.jdkLogHandler // not for production use

  // convenience repo with transactions
  final class TxDtsRepo(transactor: Transactor[IO]) extends DtsRepoF[IO] {
    private val delegateRepo: DtsRepoF[ConnectionIO] = new DoobieDtsRepo

    override def insertTaxo(entrypoint: Entrypoint, taxo: BasicTaxonomy): IO[Unit] =
      delegateRepo.insertTaxo(entrypoint, taxo).transact(transactor)

    override def insertOrUpdateTaxo(entrypoint: Entrypoint, taxo: BasicTaxonomy): IO[Unit] =
      delegateRepo.insertOrUpdateTaxo(entrypoint, taxo).transact(transactor)

    override def deleteTaxo(entrypoint: Entrypoint): IO[Unit] =
      delegateRepo.deleteTaxo(entrypoint).transact(transactor)

    override def getTaxonomy(entrypointName: String): IO[BasicTaxonomy] =
      delegateRepo.getTaxonomy(entrypointName).transact(transactor)

  }

  val insertTaxoDocumentsSql = Update[TaxonomyDocument]("INSERT INTO taxo_documents (docuri, doc) VALUES (?, ?) ON CONFLICT (docuri) DO NOTHING")

  val insertDtsDocUrisSql = Update[(String, URI)]("INSERT INTO dts_docuris (entrypoint_name, docuri) VALUES (?, ?)")

  def insertEntrypointSql(entrypointName: String) = sql"INSERT INTO entrypoints (name) VALUES (${entrypointName})".update

  def insertEntrypointDocUrisSql(entrypointName: String, docUri: URI) =
    sql"INSERT INTO entrypoint_docuris (entrypoint_name, docuri) VALUES (${entrypointName}, ${docUri})".update

  def getTaxonomyDocumentsSql(entrypointName: String)(implicit r: Read[TaxonomyDocument]) =
      sql"""select taxo_documents.docuri, taxo_documents.doc
            from taxo_documents t
            join dts_docuris on taxo_documents.docuri = taxo_documents.docuri
            join entrypoints on dts_docuris.entrypoint_name = entrypoints.name
            where entrypoints.name = ${entrypointName}
            """.query[TaxonomyDocument]


  def deleteDtsDocUrisSql(entrypointName: String) =
    sql"DELETE FROM dts_docuris WHERE entrypoint_name = ${entrypointName}".update

  def deleteEntrypointDocUrisSql(entrypointName: String) =
    sql"DELETE FROM entrypoint_docuris WHERE entrypoint_name = ${entrypointName}".update

  def deleteEntrypointSql(entrypointName: String) =
    sql"DELETE FROM entrypoints WHERE name = ${entrypointName}".update

  val deleteDocsForEntrypointSql =
    sql"DELETE FROM taxo_documents AS td WHERE NOT EXISTS (SELECT * FROM dts_docuris AS dd WHERE td.docuri = dd.docuri)".update

  ///////////////////////////

  def saxonPut: Put[TaxonomyDocument] = Put.Advanced.one(Other, NonEmptyList.of("xml"),
    put = { case (ps, n, td) =>
      val sqlXml = convertDocToSQLXML(td, ps)
      ps.setObject(n, sqlXml)
      // sqlXml.free() ???
    },
    update = { (_, _, _) => sys.error("update not supported, sorry") }
  )

  def saxonGet(processor: Processor): Get[String => TaxonomyDocument] = Get.Advanced.one(Other, NonEmptyList.of("xml"),
    get = { (rs, n) => uri: String =>
      val sqlXml = rs.getObject(n + 1).asInstanceOf[SQLXML]
      convertSQLXMLToTaxonomyDoc(sqlXml, URI.create(uri), processor)
    }
  )

  def saxonRead(processor: Processor): Read[TaxonomyDocument] = Read[String].ap(Read.fromGet(saxonGet(processor)))

  implicit def saxonWrite: Write[TaxonomyDocument] = Write[String].product(Write.fromPut(saxonPut)).contramap[TaxonomyDocument](td => (td.uri.toString, td))

  // copied
  private def convertSQLXMLToTaxonomyDoc(sqlXml: SQLXML, docUri: URI, processor: Processor): TaxonomyDocument = {
    val source = sqlXml.getSource(classOf[SAXSource])
    source.setSystemId(docUri.toString) // Trying to make sure that the resulting TaxonomyDocument has its URI filled in

    val treeInfo = processor.newDocumentBuilder().build(source).getUnderlyingNode.getTreeInfo

    val saxonDoc: SaxonDocument = SaxonDocument.wrapDocument(treeInfo).ensuring(_.uriOption.nonEmpty)
    TaxonomyDocument.build(saxonDoc)
  }

  // copied
  private def convertDocToSQLXML(doc: TaxonomyDocument, ps: PreparedStatement): SQLXML = {
    val sqlXml: SQLXML = ps.getConnection().createSQLXML()
    val saxResult = sqlXml.setResult(classOf[SAXResult])
    saxResult.setSystemId(doc.uri.toString) // Trying to retain the document URI, but we cannot count on it.

    val saxonNodeInfo = doc.backingDocument.asInstanceOf[SaxonDocument].wrappedTreeInfo.getRootNode
    saxonNodeInfo.setSystemId(doc.uri.toString)

    QueryResult.serialize(saxonNodeInfo, saxResult, new SerializationProperties())
    sqlXml
  }

  // copied
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
package eu.cdevreeze.tqadb.console

import java.io.File
import java.net.URI

import cats.effect._
import cats.syntax.all._
import doobie.util.ExecutionContexts
import doobie.util.transactor.Strategy
import doobie.util.transactor.Transactor
import doobie._
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
import eu.cdevreeze.tqadb.repo
import eu.cdevreeze.tqadb.repo.DtsRepoF
import eu.cdevreeze.tqadb.wiring.DefaultDataSourceProvider
import javax.sql.DataSource
import net.sf.saxon.s9api.Processor
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.xml.sax.InputSource

object DtsInserterIO extends IOApp {

  private val logger: Logger = LoggerFactory.getLogger("DtsInserter")

  val ds = DefaultDataSourceProvider.getInstance().simpleDataSource

  def run(args: List[String]): IO[ExitCode] = {
    args match {
      case Seq(one, two@_*) =>
        (for {
          rootDir <- IO(new File(one).ensuring(_.isDirectory))
          _ <- transactor(ds).use { tx =>
            val dtsRepo: DtsRepoF[IO] = new repo.DoobieDtsRepo.TxDtsRepo(tx)
            insertOrUpdateDts(rootDir, two.map(URI.create).toSet, dtsRepo)
          }
        } yield {
          ()
        }).as(ExitCode.Success)
      case _ => IO(System.err.println("Usage: DtsInserter <root dir> <entrypoint doc URI> ...")).as(ExitCode(2))
    }
  }


  def transactor(ds: DataSource): Resource[IO, Transactor[IO]] =
    for {
      ce <- ExecutionContexts.fixedThreadPool[IO](32) // our connect EC
      be <- Blocker[IO]    // our blocking EC
    } yield {
      val xa = Transactor.fromDataSource[IO](ds, ce, be)
      Transactor.strategy.set(xa, Strategy.default.copy(
        after = Strategy.default.after <* FC.delay(logger.info("after commit")),
        before = Strategy.default.before *> FC.delay(logger.info("begin transaction"))
      ))
    }


  def insertOrUpdateDts(taxoRootDir: File, entrypointDocUris: Set[URI], dtsRepo: DtsRepoF[IO]): IO[Unit] = {
    for {
      _               <- IO(logger.info(s"Loading taxonomy"))
      dts             <- IO(getTaxonomy(taxoRootDir, entrypointDocUris))
      entrypointName  <- IO(entrypointDocUris.head.toString)
      entrypoint      <- IO(Entrypoint(entrypointName, entrypointDocUris))
      _               <- IO(logger.info(s"Storing taxonomy"))
      _               <- dtsRepo.insertOrUpdateTaxo(entrypoint, dts)
      _               <- IO(logger.info(s"Stored taxonomy"))
    } yield {
      ()
    }
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

}

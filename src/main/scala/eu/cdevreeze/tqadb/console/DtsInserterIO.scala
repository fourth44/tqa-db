package eu.cdevreeze.tqadb.console

import java.io.File
import java.net.URI

import cats.effect._
import cats.syntax.all._
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor
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

  // We need a ContextShift[IO] before we can construct a Transactor[IO]. The passed ExecutionContext
  // is where nonblocking operations will be executed. For testing here we're using a synchronous EC.
  implicit val cs = IO.contextShift(ExecutionContexts.synchronous)

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

    val rootDir = new File(args(0)).ensuring(_.isDirectory)

    insertOrUpdateDts(rootDir, args.drop(1).map(u => URI.create(u)).toSet)
  }


  def transactor(ds: DataSource)(implicit ev: ContextShift[IO]): Resource[IO, Transactor.Aux[IO, DataSource]] =
    for {
      ce <- ExecutionContexts.fixedThreadPool[IO](32) // our connect EC
      be <- Blocker[IO]    // our blocking EC
    } yield Transactor.fromDataSource[IO](ds, ce, be)


  def insertOrUpdateDts(taxoRootDir: File, entrypointDocUris: Set[URI], dtsRepo: DtsRepoF[IO]): IO[Unit] = {
    logger.info(s"Loading taxonomy")
    val dts: BasicTaxonomy = getTaxonomy(taxoRootDir, entrypointDocUris)

    val entrypointName = entrypointDocUris.head.toString
    val entrypoint = Entrypoint(entrypointName, entrypointDocUris)

    logger.info(s"Storing taxonomy")

    // TODO IO wrapper around stuff above

    dtsRepo.insertOrUpdateTaxo(entrypoint, dts) <* IO(logger.info(s"Stored taxonomy"))

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

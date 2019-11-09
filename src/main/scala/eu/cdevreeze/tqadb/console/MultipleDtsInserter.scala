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
import java.util.regex.Pattern

import scala.util.Try
import scala.util.chaining._

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
import eu.cdevreeze.tqadb.repo.DtsRepo
import eu.cdevreeze.tqadb.wiring.DefaultAppConf
import eu.cdevreeze.tqadb.wiring.DefaultDataSourceProvider
import eu.cdevreeze.tqadb.wiring.DefaultDbInfraConf
import net.sf.saxon.s9api.Processor
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.xml.sax.InputSource

/**
 * Program that inserts multiple single-document DTSes into the database. Documents that have already been added are not added again (so they
 * are not overwritten in the database). Both the entrypoint-related tables and the taxonomy document table are filled
 * with the DTS.
 *
 * @author Chris de Vreeze
 */
object MultipleDtsInserter {

  private val logger: Logger = LoggerFactory.getLogger("MultipleDtsInserter")

  def insertOrUpdateDtses(taxoRootDir: File, httpsHosts: Set[String], entrypointRegexes: Set[Pattern]): Unit = {
    val entrypointDocUris: Set[URI] = findAllEntrypointUris(taxoRootDir, httpsHosts, entrypointRegexes)

    logger.info(s"Found ${entrypointDocUris.size} entrypoints")

    val ds = DefaultDataSourceProvider.getInstance().dataSource
    ds.setMaximumPoolSize(5) // scalastyle:off
    val appConf = DefaultAppConf.getInstance(new DefaultDbInfraConf(ds))
    val dtsRepo: DtsRepo = appConf.dtsRepo

    entrypointDocUris.toSeq.foreach { entrypointDocUri =>
      logger.info(s"Processing taxonomy. Entrypoint: $entrypointDocUri")

      Try {
        insertOrUpdateDts(taxoRootDir, entrypointDocUri, dtsRepo)
      }.recover { case t =>
        logger.warn(s"Could not process entrypoint $entrypointDocUri. Exception: $t")
      }
    }
    logger.info(s"Ready processing ${entrypointDocUris.size} entrypoints")
  }

  private def insertOrUpdateDts(taxoRootDir: File, entrypointDocUri: URI, dtsRepo: DtsRepo): Unit = {
    logger.info(s"Loading taxonomy. Entrypoint: $entrypointDocUri")
    val dts: BasicTaxonomy = getTaxonomy(taxoRootDir, entrypointDocUri)

    val entrypointName = entrypointDocUri.toString
    val entrypoint = Entrypoint(entrypointName, Set(entrypointDocUri))

    logger.info(s"Storing taxonomy. Entrypoint: $entrypointDocUri")

    dtsRepo.insertOrUpdateTaxo(entrypoint, dts)
    logger.info(s"Stored taxonomy. Entrypoint: $entrypointDocUri")
  }

  private def findAllEntrypointUris(taxoRootDir: File, httpsHosts: Set[String], entrypointRegexes: Set[Pattern]): Set[URI] = {
    findFiles(taxoRootDir, f => entrypointRegexes.exists(regex => regex.matcher(f.toURI.toString).matches()))
      .map(f => convertFileToOriginalUri(f, taxoRootDir, httpsHosts)).toSet
  }

  private def getTaxonomy(taxoRootDir: File, entrypointDocUri: URI): BasicTaxonomy = {
    val processor = new Processor(false)

    val saxonDocBuilder = processor.newDocumentBuilder()
    val uriResolver: URI => InputSource = UriResolvers.fromLocalMirrorRootDirectory(taxoRootDir)

    val docBuilder: DocumentBuilder = SaxonDocumentBuilder(saxonDocBuilder, uriResolver)
    val docCollector: DocumentCollector = DefaultDtsCollector()
    val relationshipFactory: RelationshipFactory = DefaultRelationshipFactory.StrictInstance

    TaxonomyBuilder.withDocumentBuilder(docBuilder)
      .withDocumentCollector(docCollector).withRelationshipFactory(relationshipFactory).build(Set(entrypointDocUri))
  }

  private def findFiles(rootDir: File, fileFilter: File => Boolean): Seq[File] = {
    require(rootDir.isDirectory)

    rootDir.listFiles().toSeq.flatMap { file =>
      file match {
        case d if d.isDirectory =>
          // Recursive call
          findFiles(d, fileFilter)
        case f if f.isFile && fileFilter(f) =>
          Seq(f)
        case _ =>
          Seq.empty
      }
    }
  }

  private def convertFileToOriginalUri(f: File, rootDir: File, httpsHosts: Set[String]): URI = {
    require(rootDir.isDirectory)

    // A bit expensive to do for each file URI conversion

    val hostNameOnlyUris: Map[String, URI] = rootDir.listFiles(_.isDirectory).toSeq.map { dir =>
      assert(dir.isDirectory)
      val host = dir.getName
      val scheme = if (httpsHosts.contains(host)) "https" else "http"
      val hostOnlyUri = s"${scheme}://${dir.getName}/".pipe(u => URI.create(u))
      host -> hostOnlyUri
    }.toMap

    val relativeUri: URI = rootDir.toURI.relativize(f.toURI).ensuring(!_.isAbsolute)
    val host = relativeUri.toString.substring(0, relativeUri.toString.indexOf('/')).ensuring(_.trim.nonEmpty)
    val relativeUriWithoutHost: URI = relativeUri.toString.drop(host.length + 1).pipe(u => URI.create(u))
    hostNameOnlyUris.get(host).map(baseUri => baseUri.resolve(relativeUriWithoutHost)).getOrElse(relativeUri)
  }

  def main(args: Array[String]): Unit = {
    require(args.sizeIs >= 2, s"Usage: MultipleDtsInserter <root dir> <entrypoint doc URI regex> ...")

    val rootDir = new File(args(0)).ensuring(_.isDirectory)

    val httpsHosts: Set[String] = System.getProperty("httpsHosts", "").pipe(_.split(',').map(_.trim).filter(_.nonEmpty).toSet)

    insertOrUpdateDtses(rootDir, httpsHosts, args.drop(1).map(u => Pattern.compile(u)).toSet)
  }
}

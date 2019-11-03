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
import eu.cdevreeze.tqadb.repo.DefaultDtsRepo
import eu.cdevreeze.tqadb.repo.DtsRepo
import eu.cdevreeze.tqadb.wiring.DefaultAppConf
import eu.cdevreeze.tqadb.wiring.DefaultDataSourceProvider
import net.sf.saxon.s9api.Processor
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.xml.sax.InputSource

/**
 * Program that inserts a DTS into the database. Documents that have already been added are not added again (so they
 * are not overwritten in the database). Both the entrypoint-related tables and the taxonomy document table are filled
 * with the DTS.
 *
 * @author Chris de Vreeze
 */
object DtsInserter {

  private val logger: Logger = LoggerFactory.getLogger("DtsInserter")

  def insertOrUpdateDts(taxoRootDir: File, entrypointDocUris: Set[URI]): Unit = {
    logger.info(s"Loading taxonomy")
    val dts: BasicTaxonomy = getTaxonomy(taxoRootDir, entrypointDocUris)

    val entrypointName = entrypointDocUris.head.toString
    val entrypoint = Entrypoint(entrypointName, entrypointDocUris)

    val ds = DefaultDataSourceProvider.getInstanceFromSysProps().dataSource
    val appConf = new DefaultAppConf(ds)

    logger.info(s"Storing taxonomy")
    val dtsRepo: DtsRepo = new DefaultDtsRepo(appConf.transactionManager, new JdbcTemplate(ds))

    dtsRepo.insertOrUpdateTaxo(entrypoint, dts)
    logger.info(s"Stored taxonomy")
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

  def main(args: Array[String]): Unit = {
    require(args.sizeIs >= 2, s"Usage: DtsInserter <root dir> <entrypoint doc URI> ...")

    val rootDir = new File(args(0)).ensuring(_.isDirectory)

    insertOrUpdateDts(rootDir, args.drop(1).map(u => URI.create(u)).toSet)
  }
}

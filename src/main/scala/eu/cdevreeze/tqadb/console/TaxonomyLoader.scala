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

import eu.cdevreeze.tqadb.repo.DefaultDtsRepo
import eu.cdevreeze.tqadb.repo.DtsRepo
import eu.cdevreeze.tqadb.wiring.DefaultAppConf
import eu.cdevreeze.tqadb.wiring.DefaultDataSourceProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate

/**
 * Program that loads a taxonomy from the database, and shows some statistics.
 *
 * @author Chris de Vreeze
 */
object TaxonomyLoader {

  private val logger: Logger = LoggerFactory.getLogger("TaxonomyLoader")

  def loadTaxonomy(entrypointName: String): Unit = {
    logger.info(s"Loading taxonomy from the database for entrypoint $entrypointName")

    val ds = DefaultDataSourceProvider.getInstance().dataSource
    val appConf = new DefaultAppConf(ds)

    val dtsRepo: DtsRepo = new DefaultDtsRepo(appConf.transactionManager, new JdbcTemplate(ds))

    val dts = dtsRepo.getTaxonomy(entrypointName)

    logger.info(s"DTS (entrypoint $entrypointName) has ${dts.taxonomyDocs.size} documents and ${dts.relationships.size} relationships")
  }

  def main(args: Array[String]): Unit = {
    require(args.sizeIs == 1, s"Usage: TaxonomyLoader <entrypoint name>")

    loadTaxonomy(args(0))
  }
}

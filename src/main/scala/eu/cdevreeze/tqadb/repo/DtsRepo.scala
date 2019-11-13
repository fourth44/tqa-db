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

import eu.cdevreeze.tqa.base.taxonomy.BasicTaxonomy
import eu.cdevreeze.tqadb.data.Entrypoint

/**
 * Abstract DTS repository API, touching all tables of the database.
 *
 * @author Chris de Vreeze
 */
trait DtsRepoF[F[_]] {

  /**
   * Inserts a DTS into the database. The given entrypoint must belong to the given taxonomy.
   */
  def insertTaxo(entrypoint: Entrypoint, taxo: BasicTaxonomy): F[Unit]

  /**
   * Inserts a DTS into the database, or updates it if it is already in the database. The given entrypoint must belong to the given taxonomy.
   * Updating here means, within the same transaction: first delete, then insert.
   */
  def insertOrUpdateTaxo(entrypoint: Entrypoint, taxo: BasicTaxonomy): F[Unit]

  /**
   * Removes a DTS from the database. If the entrypoint does not occur in the database, this is a no-op.
   */
  def deleteTaxo(entrypoint: Entrypoint): F[Unit]

  /**
   * Loads the taxonomy with the given entrypoint from the database. The taxonomy is returned as a TQA taxonomy.
   * An exception is thrown if the entrypoint does not occur in the database.
   */
  def getTaxonomy(entrypointName: String): F[BasicTaxonomy]
}

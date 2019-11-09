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

package eu.cdevreeze.tqadb.wiring

import eu.cdevreeze.tqadb.repo.DtsRepo
import eu.cdevreeze.tqadb.repo.EntrypointRepo

/**
 * Application configuration without any magic, and fully understood by the Scala compiler.
 *
 * It offers the data repositories needed by application code, whether mocked or real ones (using a DataSource and
 * PlatformTransactionManager).
 *
 * Note that indeed the repositories are very easy to mock.
 *
 * @author Chris de Vreeze
 */
trait AppConf {

  def dtsRepo: DtsRepo

  def entrypointRepo: EntrypointRepo
}

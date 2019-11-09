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

package eu.cdevreeze.tqadb.internal

import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.TransactionTemplate

/**
 * "Extension methods" for Spring TransactionTemplate, offering a fluent TransactionTemplate configuration API.
 *
 * Note that TransactionTemplate objects may be created as often as desired. It is the PlatformTransactionManager that
 * needs to be created only once, and used all the time during the life of the application.
 *
 * @author Chris de Vreeze
 */
object TransactionTemplates {

  def apply(transactionManager: PlatformTransactionManager): TransactionTemplate = {
    new TransactionTemplate(transactionManager)
  }

  implicit class TransactionTemplateExtensions(val transactionTemplate: TransactionTemplate) {

    def withReadOnly(readOnly: Boolean): TransactionTemplate = {
      transactionTemplate.setReadOnly(readOnly)
      transactionTemplate
    }

    def withName(name: String): TransactionTemplate = {
      transactionTemplate.setName(name)
      transactionTemplate
    }

    def withTimeoutInSeconds(timeout: Int): TransactionTemplate = {
      transactionTemplate.setTimeout(timeout)
      transactionTemplate
    }

    def withIsolationLevel(isolationLevel: IsolationLevel): TransactionTemplate = {
      transactionTemplate.setIsolationLevel(isolationLevel.toInt)
      transactionTemplate
    }

    def withPropagationBehavior(propagationBehavior: PropagationBehavior): TransactionTemplate = {
      transactionTemplate.setPropagationBehavior(propagationBehavior.toInt)
      transactionTemplate
    }
  }

  sealed trait IsolationLevel {
    def toInt: Int
  }

  case object IsolationDefault extends IsolationLevel {
    def toInt: Int = TransactionDefinition.ISOLATION_DEFAULT
  }
  case object IsolationReadUncommited extends IsolationLevel {
    def toInt: Int = TransactionDefinition.ISOLATION_READ_UNCOMMITTED
  }
  case object IsolationReadCommitted extends IsolationLevel {
    def toInt: Int = TransactionDefinition.ISOLATION_READ_COMMITTED
  }
  case object IsolationRepeatableRead extends IsolationLevel {
    def toInt: Int = TransactionDefinition.ISOLATION_REPEATABLE_READ
  }
  case object IsolationSerializable extends IsolationLevel {
    def toInt: Int = TransactionDefinition.ISOLATION_SERIALIZABLE
  }

  sealed trait PropagationBehavior {
    def toInt: Int
  }

  /**
   * Supports a current transaction if it exists, but executes non-transactionally if none exists.
   */
  case object PropagationSupports extends PropagationBehavior {
    def toInt: Int = TransactionDefinition.PROPAGATION_SUPPORTS
  }

  /**
   * Do not support a current transaction, and always execute non-transactionally.
   */
  case object PropagationNotSupported extends PropagationBehavior {
    def toInt: Int = TransactionDefinition.PROPAGATION_NOT_SUPPORTED
  }

  /**
   * Do not support a current transaction, and throw an exception if one exists.
   */
  case object PropagationNever extends PropagationBehavior {
    def toInt: Int = TransactionDefinition.PROPAGATION_NEVER
  }

  /**
   * Support a current transaction, and throw an exception if none exists.
   */
  case object PropagationMandatory extends PropagationBehavior {
    def toInt: Int = TransactionDefinition.PROPAGATION_MANDATORY
  }

  /**
   * Execute within a nested transaction if a current transaction exists, and behave like PropagationRequired otherwise.
   */
  case object PropagationNested extends PropagationBehavior {
    def toInt: Int = TransactionDefinition.PROPAGATION_NESTED
  }

  /**
   * Support a current transaction, and create a new one if none exists.
   */
  case object PropagationRequired extends PropagationBehavior {
    def toInt: Int = TransactionDefinition.PROPAGATION_REQUIRED
  }

  /**
   * Create a new transaction, suspending the current transaction if one exists.
   */
  case object PropagationRequiresNew extends PropagationBehavior {
    def toInt: Int = TransactionDefinition.PROPAGATION_REQUIRES_NEW
  }
}

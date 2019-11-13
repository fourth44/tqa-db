package eu.cdevreeze.tqadb.repo

import java.net.URI

import cats.effect.IO
import doobie.`enum`.TransactionIsolation
import doobie.util.transactor.Transactor
import eu.cdevreeze.tqadb.data.Entrypoint
import doobie.implicits._
import doobie.hi._

class DoobieEntrypointRepo extends EntrypointRepoF[ConnectionIO] {

  import DoobieEntrypointRepo._

  override def findAllEntrypoints(): ConnectionIO[Seq[Entrypoint]] = {
    runReadOnly(findAllEntrypointQuery.to[List].map(_.groupBy(_.name).toSeq.map {
      case (name, rows) => Entrypoint(name, rows.map(_.docUri).toSet)
    }))
  }
}

object DoobieEntrypointRepo {

  case class EntrypointDocUri(name: String, docUri: URI)

  // convenience repo with transactions
  class TxEntrypointRepo(transactor: Transactor[IO]) extends EntrypointRepoF[IO] {
    val delegateRepo = new DoobieEntrypointRepo
    override def findAllEntrypoints(): IO[Seq[Entrypoint]] = delegateRepo.findAllEntrypoints().transact(transactor)
  }

  val findAllEntrypointQuery =
    sql"""select entrypoints.name, entrypoints_docuris.docuri
          from entrypoints join entrypoints_docuris on entrypoints.name = entrypoint_docuris.entrypoint_name"""
    .query[EntrypointDocUri]

  def runReadOnly[A](cio: => ConnectionIO[A]): ConnectionIO[A] = {
    for {
      _ <- HC.setTransactionIsolation(TransactionIsolation.TransactionSerializable)
      _ <- HC.setReadOnly(true)
      a <- cio
    } yield {
      a
    }
  }

}
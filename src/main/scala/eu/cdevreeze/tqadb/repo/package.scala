package eu.cdevreeze.tqadb

import java.net.URI

import cats.Id
import doobie.util.Meta

package object repo {
  type DtsRepo = DtsRepoF[Id]
  type EntrypointRepo = EntrypointRepoF[Id]

  implicit val URIMeta: Meta[URI] = Meta[String].imap(URI.create)(_.toString)
}

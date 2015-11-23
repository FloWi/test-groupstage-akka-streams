package de.flwi

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{FlowGraph, Keep, Sink, Source}
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._


case class ProductCode(code: String) extends Ordered[ProductCode] {
  override def compare(that: ProductCode): Int = code.compareToIgnoreCase(that.code)
}

/**
  * TBD
  */
case class ProductDetails()

/**
  * A localized product
  */
case class LocalizedProduct(articleCode: ProductCode, isoCode: String, articleDetails: ProductDetails)

/**
  * A product with all its localizations
  */
case class BaseProductWithLocalizations(articleCode: ProductCode, localizations: List[LocalizedProduct])

trait TestEnvironment {

  val deArticleNumbers: Seq[Int] = Seq(1, 2, 3, 4, 5, 6, 7, 8)
  val esArticleNumbers: Seq[Int] = Seq(1, 2, 4, 5, 6, 8)
  val czArticleNumbers: Seq[Int] = Seq(1, 2, 4, 6, 7, 8)

  val deStream = getMockedProductsStream("de", deArticleNumbers)
  val esStream = getMockedProductsStream("es", esArticleNumbers)
  val czStream = getMockedProductsStream("cz", czArticleNumbers)

  val denormalizedArticles: Seq[(Int, String)] = deArticleNumbers.map((_, "de")) ++ esArticleNumbers.map((_, "es")) ++ czArticleNumbers.map((_, "cz"))
  val expectedArticles: List[BaseProductWithLocalizations] = denormalizedArticles
    .groupBy(_._1)
    .map { case (articleNumber, tuples) => BaseProductWithLocalizations(
      ProductCode(articleNumber.toString),
      tuples.map { case (_, iso) => LocalizedProduct(ProductCode(articleNumber.toString), iso, ProductDetails()) }.toList
    ) }
    .toList
    .sortBy(_.articleCode)

  def getMockedProductsStream(iso: String, articleNumbers: Seq[Int]): Source[LocalizedProduct, Unit] = {

    val articleNumbersSource = Source(collection.immutable.Iterable(articleNumbers: _*))
    articleNumbersSource.map(n => toMockedLocalizedProduct(n, iso))
  }

  def toMockedLocalizedProduct(articleNumber: Int, isoCode: String): LocalizedProduct = {
    LocalizedProduct(ProductCode(articleNumber.toString), isoCode, ProductDetails())
  }
}

class GraphStageSpec extends FlatSpec with Matchers {
  "GraphStage" should "merge the graphs correctly into matching tuples" in new TestEnvironment {

    implicit val system = ActorSystem("Sys")

    implicit val materializer = ActorMaterializer()

    val sources: List[Source[LocalizedProduct, Unit]] = deStream :: esStream :: czStream :: Nil

    val sink = Sink.fold[Seq[BaseProductWithLocalizations], BaseProductWithLocalizations](Seq.empty)(_ :+ _)

    private val graph = getGroupSource(sources)
      .map(seqOfSameArticles => BaseProductWithLocalizations(seqOfSameArticles.head.articleCode, seqOfSameArticles.toList))

    val actualLocalizedArticles = Await.result(graph.toMat(sink)(Keep.right).run(), 1.minute)

    val actualList: List[BaseProductWithLocalizations] = actualLocalizedArticles.toList
    actualList.shouldBe(expectedArticles)
  }

  def getGroupSource(sources: Seq[Source[LocalizedProduct, Unit]]): Source[Seq[LocalizedProduct], Unit] = {

    Source.fromGraph(FlowGraph.create() { implicit builder =>
      import FlowGraph.Implicits._

      //create a group-stage with the right amount of input-inlets
      val group = builder.add(new GroupStage[ProductCode, LocalizedProduct](sources.size, l => l.articleCode))

      //connect each source to one inlet
      sources.foreach(_ ~> group)

      SourceShape(group.out)
    } )
  }
}

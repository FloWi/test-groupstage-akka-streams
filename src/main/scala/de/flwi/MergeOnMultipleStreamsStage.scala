package de.flwi

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.immutable

/**
  * A stage the groups multiple input-streams into a stream of matching tuples.
  * IMPORTANT: For the merge-logic to produce the correct results, all streams are required to have the same ordering on [[TKey]].
  * Since we are in a streaming environment, the sorting of the whole streams can't be handled here.
  *
  * A `MergeOnMultipleStreamsStage` has the given number of input-ports (`inputCount`), and one `out` port.
  *
  * '''Backpressures when''' the downstream backpressures
  *
  * '''Completes when''' all upstreams complete
  *
  * '''Cancels when''' downstream cancels of one of the input-ports cancels.
  *
  * @param inputCount the number of input-streams.
  * @param keyExtractor an extractor function to get the key out of an [[T]]
  * @tparam TKey the type of the key of the elements
  * @tparam T type of the data-elements
  *
  * @example
  * Take this input-stream of products (each tenant lies within its own index in elasticsearch)
  * {{{
         de 1 2 3 4 5 6 7 8 EOF
         es 1 2   4 5 6   8 EOF
         cz 1 2   4   6 7 8 EOF
  * }}}
  *
  * This stage merges the streams like that
  * {{{
        (de_1,es_1,cz_1)
        (de_2,es_2,cz_2)
        (de_3)
        (de_4,es_4,cz_4)
        (de_5,es_5)
        (de_6,es_6,cz_6)
        (de_7,cz_7)
        (de_8,es_8,cz_8)
        EOF
  * }}}
  */
class MergeOnMultipleStreamsStage[TKey: Ordering, T](inputCount: Int, keyExtractor: T => TKey) extends GraphStage[UniformFanInShape[T, Seq[T]]] {

  val inlets: immutable.IndexedSeq[Inlet[T]] = Vector.tabulate(inputCount)(i ⇒ Inlet[T]("GroupStage.in" + i))
  val out: Outlet[Seq[T]] = Outlet[Seq[T]]("GroupStage.out")
  override val shape = UniformFanInShape[T, Seq[T]](out, inlets: _*)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    val activeSources = collection.concurrent.TrieMap(inlets.zipWithIndex.map(tuple => tuple._2 -> tuple._1): _*)
    var buffer = collection.concurrent.TrieMap.empty[Int, T]

    //define handlers for input-streams
    activeSources.foreach { case (index, in) =>
      setHandler(in, new InHandler {

        //called when the input port has a new element available
        override def onPush(): Unit = {
          myPrintln(s"in #$index onPush called")
          pushIfPossible()
        }

        //called when the input port has reached EOF
        override def onUpstreamFinish(): Unit = {
          myPrintln(s"in #$index is done")
          activeSources.remove(index)
          pushIfPossible()
        }
      })
    }

    //define handler for output-stream
    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        myPrintln("out has demand")
        pushIfPossible()
      }
    })

    def pushIfPossible(): Unit = {
      fillEmptyBufferSlots()
      if (checkIfEnoughDataBufferedForPush()) {
        if (isAvailable(out)) {
          pushBufferValues()
        } //do nothing - the out-channel has no demand --> BackPressure
      } else {
        pullStreamsToFillUpEmptyBufferSlots()
      }
    }

    def fillEmptyBufferSlots(): Unit = {
      activeSources.foreach { case (index, inlet) =>
        if (isAvailable(inlet) && !buffer.isDefinedAt(index)) {
          buffer(index) = grab(inlet)
          myPrintln(s"filled buffer at slot #$index. current buffer: $buffer")
        }
      }
    }

    def checkIfEnoughDataBufferedForPush(): Boolean = {
      myPrintln(s"checkIfEnoughDataBufferedForPush. Buffer.keys: ${buffer.keys.mkString(", ")}. Active streams: ${activeSources.keys.mkString(", ")}")

      //check if the buffer-slot of every active-source is filled
      activeSources.keys.forall(buffer.contains)
    }

    def pushBufferValues(): Unit = {

      if(buffer.isEmpty && activeSources.isEmpty) {
        myPrintln("buffer is empty and no active input available. Finished")
        completeStage()
      } else {

        val values: List[(Int, T)] = buffer.toList
        val denormalized: List[(Int, TKey, T)] = values.map { case (bufferIndex, value) => (bufferIndex, keyExtractor(value), value) }

        val minValueKey: TKey = denormalized.map(_._2).min

        val pushable: List[(Int, TKey, T)] = denormalized.filter(_._2 == minValueKey)

        if (pushable.nonEmpty) {
          val pushableValues = pushable.map(_._3)
          pushable.foreach { case (bufferKey, _, _) => buffer.remove(bufferKey) }
          myPrintln(s"pushedBufferValues: $pushableValues")
          myPrintln()
          myPrintln()
          push(out, pushableValues)
        }
      }
    }

    def pullStreamsToFillUpEmptyBufferSlots(): Unit = {
      myPrintln(s"pullStreamsToFillUpEmptyBufferSlots")

      val notFilledSlots = activeSources.keys.toSet.diff(buffer.keys.toSet)
      notFilledSlots.foreach(slotIndex => {
        if (isClosed(activeSources(slotIndex))) {
          failStage(new IllegalStateException(s"there are not-filled slots which can't be filled from their corresponding streams. Slotindex: #$slotIndex"))
        } else {
          if (!hasBeenPulled(activeSources(slotIndex)))
            pull(activeSources(slotIndex))
        }
      })
    }
  }

  def myPrintln(any: => AnyRef): Unit = {
    // println(any)
  }

  def myPrintln(): Unit = {
    //println()
  }
}
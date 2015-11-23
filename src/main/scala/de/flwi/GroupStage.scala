package de.flwi

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.immutable

class GroupStage[TKey: Ordering, T](inputCount: Int, keyExtractor: T => TKey) extends GraphStage[UniformFanInShape[T, Seq[T]]] {

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
        //the actual element can be retrieved via grab
        override def onPush(): Unit = {
          myPrintln(s"in #$index onPush called")
          pushIfPossible()
        }

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
        } //do nothing, if out-channel has no demand
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
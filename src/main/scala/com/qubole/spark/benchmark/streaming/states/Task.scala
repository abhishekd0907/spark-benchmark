package com.qubole.spark.benchmark.streaming.states

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

/**
  * A class which implements Scala Futures lazily.
  *
  * @param run function which takes Execution Context as argument and
  *            returns typed object A after execution
  * @tparam A
  */
class Task[A](val run: ExecutionContext ⇒ A) {

  /**
    * API to chain a Task using Map
    *
    * @param f function
    * @return new Task
    */
  def map[B](f: A ⇒ B): Task[B] = {
    new Task[B](executionContext => {
      f(run(executionContext))
    })
  }

  /**
    * API to chain a Task using flatMap
    *
    * @param f function
    * @return new Task
    */
  def flatMap[B](f: A ⇒ Task[B]): Task[B] = {
    new Task[B](executionContext => {
      f(run(executionContext)).run(executionContext)
    })
  }

  /**
    * API to convert a Task to Future
    *
    * @param ec Execution Context to be used by future
    * @return Future
    */
  def toFuture(implicit ec: ExecutionContext): Future[A] = {
    Future {
      run(ec)
    }
  }

  /**
    * Helper method to return result of a Task
    *
    * @param ec Execution Context to be used by task
    * @return Object of the
    */
  def runBlocking(implicit ec: ExecutionContext): A = Await.result(toFuture, Duration.Inf)
}


object Task {

  def apply[A](run: ExecutionContext ⇒ A): Task[A] = new Task(run)

  def apply[A](run: ⇒ A): Task[A] = new Task(_ ⇒ run)

  /**
    * API to create a Task from a Future
    *
    * @param f method which results in a future when invoked
    * @return new Task which executes future lazily
    */
  def fromFuture[A](f: ⇒ Future[A]): Task[A] = {
    Task(_ => {
      Await.result(f, Duration.Inf)
    })
  }

  /**
    * API to create a Task which runs a set of Tasks sequentially
    *
    * @param taskA first Task
    * @param taskB second Task
    * @return new Task which gives a tuple containing results of taskA
    *         and taskB
    */
  def zipParallel[A, B](taskA: Task[A], taskB: Task[B]): Task[(A, B)] = {
    new Task[(A, B)](implicit ec => {

      val futureA = taskA.toFuture
      val futureB = taskB.toFuture

      val res = for {
        x <- futureA
        y <- futureB
      } yield (x, y)

      Await.result(res, Duration.Inf)

    })
  }

  /**
    * API to create a Task which runs a set of Tasks sequentially
    *
    * @param tasks sequence of Tasks
    * @return new Task which returns a sequence containing results of
    *         all tasks passed in input sequence
    */
  def sequence[A](tasks: Seq[Task[A]]): Task[Seq[A]] = {
    new Task[Seq[A]](implicit ec => {
      tasks.map(_.run(ec))
    })
  }

  /**
    * API to create a Task which runs a set of Tasks parallelly
    *
    * @param tasks sequence of Tasks
    * @return new Task which returns a sequence containing results of
    *         all tasks passed in input sequence
    */
  def parallel[A](tasks: Seq[Task[A]]): Task[Seq[A]] = {
    new Task[Seq[A]](implicit ec => {

      val futureResults = tasks.map(task =>
        task.toFuture
      )
      Await.result(Future.sequence(futureResults), Duration.Inf)

    })
  }

  /**
    * Method to Test Creating Simple IO Tasks
    */
  def simpleIOTask: Task[Unit] = for {
    _ ← Task(println("First Name?"))
    fn ← Task(scala.io.StdIn.readLine())
    _ ← Task(println("Last Name?"))
    ln ← Task(scala.io.StdIn.readLine())
    _ ← Task(println(s"Hello $fn $ln"))
  } yield ()

  /**
    * Method to Test API to chain Tasks using map
    */
  def chainMapCalls(num: Int = 100): Task[Int] = {
    var t: Task[Int] = Task(1)
    var counter = 0
    while (counter < num) {
      t = t.map(_ + 1)
      counter += 1
    }
    t
  }

  /**
    * Method to Test API to chain Tasks using flatMap
    */
  def chainFlatMapCalls(num: Int = 100): Task[Int] = {
    var t: Task[Int] = Task(1)
    var counter = 0
    while (counter < num) {
      t = t.flatMap(x ⇒ Task(x + 1))
      counter += 1
    }
    t
  }

  /**
    * Method to Test API to create Task from Future
    */
  def testFromFuture: Task[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    Task.fromFuture({
      Future {
        Thread.sleep(1000)
        print("Task created from Future")
      }
    })
  }

  /**
    * Method to Test Zipped Parallel Runs of Two Tasks
    */
  def testZipParallelRuns: Task[(Unit, Int)] = {
    println("Testing Zip Parallel Runs")
    val taskA: Task[Unit] = Task(_ => {
      runTask("A", 1000, Unit)
    })
    val taskB: Task[Int] = Task(_ => {
      runTask("B", 500, 2)
    })
    Task.zipParallel(taskA, taskB)
  }

  /**
    * Helper method to run a Task
    *
    * @param taskName    identifier of Task
    * @param sleepTime   time for which task waits or sleeps
    * @param returnValue value returned by task
    * @return A generic typed object returned by task
    */
  def runTask[A](taskName: String, sleepTime: Long, returnValue: A): A = {
    val startTime = System.currentTimeMillis()
    Thread.sleep(sleepTime)
    val endTime = System.currentTimeMillis()
    println(s"Task $taskName started at: $startTime and took ${endTime - startTime} ms")
    returnValue
  }

  /**
    * Method to Test Sequential Runs of Tasks
    */
  def testSeqRuns: Task[Seq[Unit]] = {
    println("Testing Seq Runs")
    val seq: Array[Task[Unit]] = Array(
      Task(_ => {
        runTask("A", 1000, Unit)
      }),
      Task(_ => {
        runTask("B", 200, Unit)
      }),
      Task(_ => {
        runTask("C", 500, Unit)
      })
    )
    Task.sequence(seq)
  }

  /**
    * Method to Test Parallel Runs of Tasks
    */
  def testParallelRuns: Task[Seq[Unit]] = {
    println("Testing Parallel Runs")
    val seq: Array[Task[Unit]] = Array(
      Task(_ => {
        runTask("A", 1000, Unit)
      }),
      Task(_ => {
        runTask("B", 200, Unit)
      }),
      Task(_ => {
        runTask("C", 500, Unit)
      })
    )
    Task.parallel(seq)
  }

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    // test 1
    //simpleIOTask.runBlocking

    // test 2
    testFromFuture.runBlocking

    // test 3
    chainMapCalls().runBlocking

    // test 4
    chainFlatMapCalls().runBlocking

    // test 5
    testZipParallelRuns.runBlocking

    // test 6
    testSeqRuns.runBlocking

    // test 7
    testParallelRuns.runBlocking

  }


}
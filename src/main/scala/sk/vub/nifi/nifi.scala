package sk.vub

package object nifi {

  def withThrowableAsEither[A, R](a: A)(f: A => R): Either[Throwable, R] = {
    try {
      Right(f(a))
    } catch {
      case e: Throwable => Left(e)
    }
  }

  def withResource[A <: AutoCloseable, R](a: A)(f: A => R): R = {
    try {
      f(a)
    } finally {
      a.close()
    }
  }

  def assertNotNull[A <: AnyRef](a: A): A = {
    assert(a != null, "value was null")
    a
  }

  def unfoldNotNull[A <: AnyRef](next: => A): LazyList[A] = {
    val n = next
    if (n == null) LazyList.empty
    else n #:: unfoldNotNull(next)
  }

  def unfoldWhile[A](hasNext: () => Boolean, getNext: () => A): LazyList[A] = {
    if (hasNext()) getNext() #:: unfoldWhile(hasNext, getNext)
    else LazyList.empty
  }

  def resolveOptions[A](opts: Option[A]*): Option[A] = opts.find(_.isDefined).flatten

  def contains[A](item: A, opts: Option[A]*): Option[Boolean] = {
    val flatten = opts.flatten
    if (flatten.isEmpty) None
    else Some(flatten.contains(item))
  }
}

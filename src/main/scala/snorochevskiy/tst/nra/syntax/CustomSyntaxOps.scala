package snorochevskiy.tst.nra.syntax

object CustomSyntaxOps {

  implicit class TapWrapper[A](val self: A) extends AnyVal {
    def tap[B](f: A=>B): B = f(self)
    def |>[B](f: A=>B): B = f(self)
  }
}

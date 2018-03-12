package auxlib.spark

import com.google.common.base.Optional

package object nlp {

  implicit class OptionConverter[T](val optional: Optional[T]) extends AnyVal {
    def asScala: Option[T] = Option(optional.orNull())
  }

  implicit class OptionalConverter[T](val option: Option[T]) extends AnyVal {
    def asJava: Optional[T] = option.map(Optional.of[T]).getOrElse(Optional.absent[T]())
  }
}

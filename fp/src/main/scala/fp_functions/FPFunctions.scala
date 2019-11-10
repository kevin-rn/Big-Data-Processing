package fp_functions

import scala.collection.immutable

/**
  * This part is about implementing several functions that are very common in functional programming.
  * For the exercises in this part you are _not_ allowed to use library functions.
  * Do not use iteration, write recursive functions instead.
  * Hint: read about the functions in the lecture slides.
  *
  * This part is worth 30 points, 5 points per function.
  */
object FPFunctions {

    /** Q7 (5p)
      * Applies `f` to all elements and returns a new list.
      * @param xs list to map.
      * @param f mapping function.
      * @tparam A type of items in xs.
      * @tparam B result type of mapping function.
      * @return a list of all items in `xs` mapped with `f`.
      */
    def map[A, B](xs: List[A], f: A => B): List[B] = {
        xs match {
            case i::t if xs.size > 1 => f(i)::map(t,f)
            case i if xs.size == 1 => List(f(i.head))
            case Nil => Nil
        }
    }

    /** Q8 (5p)
      * Takes a function that returns a boolean and returns all elements that satisfy it.
      * @param xs the list to filter.
      * @param f the filter function.
      * @tparam A the type of the items in `xs`.
      * @return a list of all items in `xs` that satisfy `f`.
      */
    def filter[A](xs: List[A], f: A => Boolean): List[A] = {
        xs match {
            case h::t if f(h) => h::filter(t,f)
            case h::t if !f(h) => filter(t,f)
            case Nil => Nil
        }
    }

    /** Q9 (5p)
      * Recursively flattens a list that may contain more lists into 1 list.
      * Example:
      *     List(List(1), List(2, 3), List(4)) -> List(1, 2, 3, 4)
      * @param xs the list to flatten.
      * @return one list containing all items in `xs`.
      */
    def recFlat(xs: List[Any]): List[Any] = {
        xs match {
            case Nil => Nil
            case (h:List[Any])::t => recFlat(h):::recFlat(t)
            case h::t => h::recFlat(t)
        }
    }

    /** Q10 (5p)
      * Takes `f` of 2 arguments and an `init` value and combines the elements by applying `f` on the result of each previous application.
      * @param xs the list to fold.
      * @param f the fold function.
      * @param init the initial value.
      * @tparam A the type of the items in `xs`.
      * @tparam B the result type of the fold function.
      * @return the result of folding `xs` with `f`.
      */
    def foldL[A, B](xs: List[A], f: (B, A) => B, init: B): B = {
        xs match {
            case Nil => init
            case h :: t => foldL(t, f, f(init, h))


        }
    }

    /** Q11 (5p)
      * Reuse `foldL` to define `foldR`.
      * If you do not reuse `foldL`, points will be subtracted.
      *
      * @param xs the list to fold.
      * @param f the fold function.
      * @param init the initial value.
      * @tparam A the type of the items in `xs`.
      * @tparam B the result type of the fold function.
      * @return the result of folding `xs` with `f`.
      */
    def foldR[A, B](xs: List[A], f: (A, B) => B, init: B): B = {
        foldL(xs.reverse, (b:B, a:A) => f(a, b), init)
    }

    /** Q12 (5p)
      * Returns a iterable collection formed by iterating over the corresponding items of `xs` and `ys`.
      * @param xs the first list.
      * @param ys the second list.
      * @tparam A the type of the items in `xs`.
      * @tparam B the type of the items in `ys`.
      * @return a list of tuples of items in `xs` and `ys`.
      */
    def zip[A, B](xs: List[A], ys: List[B]): List[(A, B)] = {
        (xs,ys) match {
            case (Nil, _) => Nil
            case (_, Nil) => Nil
            case (h1 :: t1, h2 :: t2) => (h1, h2) :: zip(t1, t2)
        }
    }
}

package intro

/**
  * This part has some exercises for you to practice with the recursive lists and functions.
  * For the exercises in this part you are _not_ allowed to use library functions,
  * you should implement everything yourself.
  * Use recursion to process lists, iteration is not allowed.
  *
  * This part is worth 5 points.
  */
object Practice {

    /** Q5 (2p)
      * Implement the function that returns the first n elements from the list.
      * Note that `n` is an upper bound, the list might not have `n` elements.
      *
      * @param xs list to take items from.
      * @param n amount of items to take.
      * @return the first n items of xs.
      */
    def firstN(xs: List[Int], n: Int): List[Int] = {
        xs match {
            case i::t if n>1 && xs.size>1  =>  i :: firstN(t, n-1)
            case i if n==1 || xs.size==1  =>  List(i.head)
            case Nil => Nil
        }
    }


    /** Q6 (3p)
      * Implement the function that returns the maximum value in the list.
      *
      * @param xs list to process.
      * @return the maximum value in the list.
      */
    def maxValue(xs: List[Int]): Int = {
        xs match {
            case i if xs.size==1 => i.head
            case i::t if i>t.head && xs.size > 1 => maxValue(i::t.tail)
            case i::t if i<t.head && xs.size > 1 => maxValue(t)

            case Nil => 0
        }
    }
}

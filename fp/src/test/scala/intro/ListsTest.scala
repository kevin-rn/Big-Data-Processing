package intro

import intro.Lists.customAverage
import org.scalatest.FunSuite

class ListsTest extends FunSuite {

    test("Example") {
        assertResult(5) {
            // 4 + 3 + 6 + 8 + 4 = 25, 25/5 = 5
            customAverage(List(4, 1, 3, 2, 6, 2, 8, 2, 1, 4), 2, 5)
        }
    }
}

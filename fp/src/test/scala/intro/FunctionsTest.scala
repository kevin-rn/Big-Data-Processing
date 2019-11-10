package intro

import intro.Functions._
import org.scalatest.FunSuite

class FunctionsTest extends FunSuite {

    test("Simple math") {
        // Using `assert` you can make assertions in tests.
        assert(simpleMath(3) == 17, "3 * 5 + 2 = 17")
    }

    test("More math") {
        // Alternatively you can expect a certain result from an operation.
        // This gives better error messages and looks cleaner.
        assertResult(42, "8 * 5 + 2 = 42") {
            simpleMath(8)
        }
    }

    test("Fizz") {
        assertResult("Fizz") {
            fizzBuzz(3)
//            fail("To activate the test, uncomment the previous line and comment this line")
        }
    }

    test("Buzz") {
        assertResult("Buzz") {
            fizzBuzz(5)
//            fail("To activate the test, uncomment the previous line and comment this line")
        }
    }

    test("FizzBuzz") {
        assertResult("FizzBuzz") {
            fizzBuzz(15)
            //            fail("To activate the test, uncomment the previous line and comment this line")
        }
    }

}

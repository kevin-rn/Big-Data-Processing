# Assignment 2: Functional Programming
This is the Functional Programming assignment of Big Data Processing, CSE2520.

## Setup of the assignment
The assignment consists of 4 parts, worth 100 points in total:


1. [An introduction to the Scala language](<src/main/scala/intro/readme.md>) (15 points)
    
    In this part you will get a brief introduction to Scala.
    It teaches you the basics only, you are expected to learn the rest as you go through the assignment.
     
2. [Implement several core FP functions](<src/main/scala/fp_functions/readme.md>) (30 points)

    With functional programming there are some functions that are used very often.
    In this part you will implement several of these yourself to get a better idea of how they work.

3. [Practice with functional programming](<src/main/scala/fp_practice/readme.md>) (15 points)

    Now that you know how to program functionally, solve these small exercises in an FP way.

4. [Process a dataset](<src/main/scala/dataset/readme.md>) (40 points)

    Process a dataset of Github commits using everything you have learned in the previous parts.
    
You are strongly advised to complete the parts in this order.

## About functional programming.
In the lectures you learned about functional programming.
The most important characteristic is the absence of side-effects.
For all exercises in this assignment you are expected to program functionally.

## Grading
### Grade calculation
All questions together add up to 100 points. Your grade for the assignment is `score/10`.

### Handing in your solutions
You can hand in your solutions on [CPM](https://cpm.ewi.tudelft.nl).
Create a ZIP archive of the `src` folder and upload this to CPM.

### Automatic grading
After handing in your solutions, you can see the results of the automatic grading.
This only shows you an overall grade and which question(s) is/are incorrect,
but will not give details of the failures.\
Note that the autograder contains more tests than the ones provided to you;
passing all provided tests does not mean you will get a 10.
You are encouraged to write more unit tests yourself to test your solutions.
You can find some help on writing tests in [test/intro/FunctionsTest.scala](<src/test/scala/intro/FunctionsTest.scala>)

**Warning**: the automatic grader may fail if you do any of the following actions:
- change the names of template files
- change function signatures
- use external dependencies that were not in the original `pom.xml` file
- hand in your solutions in another format than the one specified earlier.

If you are in doubt why the grader fails, ask the TAs for more details during the lab sessions.

### Programming style
The autograder only tests your solutions for correctness, it does not check your programming style.
Your solutions will be graded manually to verify you programmed functionally.
If not, points will be subtracted per question that violates the principles of FP.

Furthermore points may be deducted based on requirements set for the exercises, for example using library functions instead of implementing functions yourself.
These rules are specified in the template files, when applicable.

Hint: in previous editions of the course the most common mistake was to use mutation (i.e. variables).
Mutation is a side effect, therefore it is not good practice when programming functionally.\
Note that `for` and `while` loops use mutation.

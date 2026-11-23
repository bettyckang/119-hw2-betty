"""
Part 1: MapReduce

In our first part, we will practice using MapReduce
to create several pipelines.
This part has 20 questions.

As you complete your code, you can run the code with

    python3 part1.py
    pytest part1.py

and you can view the output so far in:

    output/part1-answers.txt

In general, follow the same guidelines as in HW1!
Make sure that the output in part1-answers.txt looks correct.
See "Grading notes" here:
https://github.com/DavisPL-Teaching/119-hw1/blob/main/part1.py

For Q5-Q7, make sure your answer uses general_map and general_reduce as much as possible.
You will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

If you aren't sure of the type of the output, please post a question on Piazza.
"""

# Spark boilerplate (remember to always add this at the top of any Spark file)
import pyspark
from pyspark.sql import SparkSession

import sys
if __name__ == "__main__" or "pytest" in sys.modules:
    spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
    sc = spark.sparkContext

# Additional imports
import pytest

"""
===== Questions 1-3: Generalized Map and Reduce =====

We will first implement the generalized version of MapReduce.
It works on (key, value) pairs:

- During the map stage, for each (key1, value1) pairs we
  create a list of (key2, value2) pairs.
  All of the values are output as the result of the map stage.

- During the reduce stage, we will apply a reduce_by_key
  function (value2, value2) -> value2
  that describes how to combine two values.
  The values (key2, value2) will be grouped
  by key, then values of each key key2
  will be combined (in some order) until there
  are no values of that key left. It should end up with a single
  (key2, value2) pair for each key.

1. Fill in the general_map function
using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q1() answer. It should fill out automatically.
"""

def general_map(rdd, f):
    """
    rdd: an RDD with values of type (k1, v1)
    f: a function (k1, v1) -> List[(k2, v2)]
    output: an RDD with values of type (k2, v2)
    """
    rdd = rdd.flatMap(lambda pair: f(pair[0], pair[1]))
    return rdd

def test_general_map():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Map returning no values
    rdd2 = general_map(rdd1, lambda k, v: [])

    # Map returning length
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = rdd3.map(lambda pair: pair[1])

    # Map returnning odd or even length
    rdd5 = general_map(rdd1, lambda k, v: [(len(v) % 2, ())])

    assert rdd2.collect() == []
    assert sum(rdd4.collect()) == 14
    assert set(rdd5.collect()) == set([(1, ())])

def q1():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_map(rdd1, lambda k, v: [(1, v[-1])])
    return sorted(rdd2.collect())

"""
2. Fill in the reduce function using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q2() answer. It should fill out automatically.
"""

def general_reduce(rdd, f):
    """
    rdd: an RDD with values of type (k2, v2)
    f: a function (v2, v2) -> v2
    output: an RDD with values of type (k2, v2),
        and just one single value per key
    """
    rdd = rdd.reduceByKey(f)
    return rdd

def test_general_reduce():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Reduce, concatenating strings of the same key
    rdd2 = general_reduce(rdd1, lambda x, y: x + y)
    res2 = set(rdd2.collect())

    # Reduce, adding lengths
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = general_reduce(rdd3, lambda x, y: x + y)
    res4 = sorted(rdd4.collect())

    assert (
        res2 == set([('c', "catcow"), ('d', "dog"), ('z', "zebra")])
        or res2 == set([('c', "cowcat"), ('d', "dog"), ('z', "zebra")])
    )
    assert res4 == [('c', 6), ('d', 3), ('z', 5)]

def q2():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_reduce(rdd1, lambda x, y: "hello")
    return sorted(rdd2.collect())

"""
3. Name one scenario where having the keys for Map
and keys for Reduce be different might be useful.

=== ANSWER Q3 BELOW ===

If we need to apply some function to individual observations in the map stage, 
but then want to aggregate the results by a different key in the reduce stage, 
it would be useful to have different keys. 

For example, we could use individual keys for each observation in the map stage
and then aggregate using another key (e.g., a category or group) in the reduce stage to compute summary statistics.

=== END OF Q3 ANSWER ===
"""

"""
===== Questions 4-10: MapReduce Pipelines =====

Now that we have our generalized MapReduce function,
let's do a few exercises.
For the first set of exercises, we will use a simple dataset that is the
set of integers between 1 and 1 million (inclusive).

4. First, we need a function that loads the input.
"""

def load_input():
    # Return a parallelized RDD with the integers between 1 and 1,000,000
    rdd = sc.parallelize(range(1, 1_000_001))
    return rdd

def q4(rdd):
    # Input: the RDD from load_input
    # Output: the length of the dataset.
    return rdd.count()

"""
Now use the general_map and general_reduce functions to answer the following questions.

For Q5-Q7, your answers should use general_map and general_reduce as much as possible (wherever possible): you will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

5. Among the numbers from 1 to 1 million, what is the average value?
"""

def q5(rdd):
    # Input: the RDD from Q4
    # Output: the average value

    rdd = rdd.map(lambda x: (0, x)) # Map numbers to key value pair for general_map 
    rdd = general_map(rdd, lambda k, v: [(k, (v, 1))]) # Map to (sum, count) pairs
    rdd = general_reduce(rdd, lambda x, y: (x[0] + y[0], x[1] + y[1])) # Reduce to get sum and count
    rdd = general_map(rdd, lambda k, v: [(k, v[0]/v[1])]) # Map to get average

    return rdd.collect()[0][1]

"""
6. Among the numbers from 1 to 1 million, when written out,
which digit is most common, with what frequency?
And which is the least common, with what frequency?

In case of ties, we will accept any letter that is one of the tied digits.

The digit should be either an integer 0-9 or a character '0'-'9'.
Frequency is the number of occurences of each value.

Your answer should use the general_map and general_reduce functions as much as possible.
"""

def q6(rdd):
    # Input: the RDD from Q4
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)

    rdd = rdd.map(lambda x: (0, x)) # Map numbers to key value pair for general_map
    rdd = general_map(rdd, lambda k, v: [(int(d), 1) for d in str(v)]) # Map to (digit, 1) pairs
    rdd = general_reduce(rdd, lambda k, v: k + v) # Reduce to get frequency of each digit

    collected = rdd.collect()

    most_common_digit, most_common_frequency = max(collected, key=lambda x: x[1])
    least_common_digit, least_common_frequency = min(collected, key=lambda x: x[1])

    return (most_common_digit, most_common_frequency, least_common_digit, least_common_frequency)

"""
7. Among the numbers from 1 to 1 million, written out in English, which letter is most common?
With what frequency?
The least common?
With what frequency?

(If there are ties, you may answer any of the tied characters.)

For this part, you will need a helper function that computes
the English name for a number.

Please implement this without using an external library!
You should write this from scratch in Python.

Examples:

    0 = zero
    71 = seventy one
    513 = five hundred and thirteen
    801 = eight hundred and one
    999 = nine hundred and ninety nine
    1001 = one thousand one
    500,501 = five hundred thousand five hundred and one
    555,555 = five hundred and fifty five thousand five hundred and fifty five
    1,000,000 = one million

Notes:
- For "least frequent", count only letters which occur,
  not letters which don't occur.
- Please ignore spaces and hyphens.
- Use all lowercase letters.
- The word "and" should only appear after the "hundred" part, and nowhere else.
  It should appear after the hundreds if there are tens or ones in the same block.
  (Note the 1001 case above which differs from some other implementations!)
"""

# Start with dictionaries for ones and tens
ones = {
    0: "",
    1: "one",
    2: "two",
    3: "three",
    4: "four",
    5: "five",
    6: "six",
    7: "seven",
    8: "eight",
    9: "nine",
    10: "ten",
    11: "eleven",
    12: "twelve",
    13: "thirteen",
    14: "fourteen",
    15: "fifteen",
    16: "sixteen",
    17: "seventeen",
    18: "eighteen",
    19: "nineteen"
}

tens = {
    20: "twenty",
    30: "thirty",
    40: "forty",
    50: "fifty",
    60: "sixty",
    70: "seventy",
    80: "eighty",
    90: "ninety"
}

# Helper function
def number_to_words(n):
    if n < 20: 
    # Automatically return ones dictionary value for numbers less than 20
        return ones[n]
    elif n < 100: 
    # For numbers between 20 and 99, return the tens dictionary value and the ones dictionary value for the remainder (if not divisible by 10)
        return tens[n - n % 10] + ('' if n % 10 == 0 else ' ' + ones[n % 10])
    elif n < 1000: 
    # For numbers between 100 and 999, return ones dictionary for hundreds + "hundred and" and recursively call number_to_words on remainder
        return ones[n // 100] + ' hundred' + ('' if n % 100 == 0 else ' and ' + number_to_words(n % 100))
    elif n < 1000000:
    # For numbers between 1000 and 999999, split number by thousands and hundreds and recursively call number_to_words on each part
        return number_to_words(n//1000) + ' thousand ' + (number_to_words(n % 1000) if n % 1000 !=0 else '' )
    elif n < 10000000:
    # Expand up to 10 million for question 8
    # For numbers between 1000000 and 9999999 split number by millions and thousands, use ones dictionary for millions and recursively call number_to_words on remainder
        return ones[n//1000000] + ' million ' + (number_to_words(n % 1000000) if n % 1000000 !=0 else '' )
    elif n == 10000000:
        return 'ten million'

def q7(rdd):
    # Input: the RDD from Q4
    # Output: a tuple (most common char, most common frequency, least common char, least common frequency)

    rdd = rdd.map(lambda x: (0, x)) # Map numbers to key value pair for general_map
    rdd = general_map(rdd, lambda k, v: [(letter, 1) for letter in number_to_words(v) if letter.isalpha()]) # Map to (letter, 1) pairs, excluding spaces
    rdd = general_reduce(rdd, lambda k, v: k + v) # Reduce to get frequency of each letter

    collected = rdd.collect()

    most_common_letter, most_common_frequency = max(collected, key=lambda x: x[1])
    least_common_letter, least_common_frequency = min(collected, key=lambda x: x[1])

    return (most_common_letter, most_common_frequency, least_common_letter, least_common_frequency)    

"""
8. Does the answer change if we have the numbers from 1 to 10,000,000?

Make a version of both pipelines from Q6 and Q7 for this case.
You will need a new load_input function.

Notes:
- The functions q8_a and q8_b don't have input parameters; they should call
  load_input_bigger directly.
- Please ensure that each of q8a and q8b runs in a time limit of at most 3 minutes.
- If your code is taking too long to run,
  you may need to change the number of partitions.
  For example, one student found that setting number of partitions to 100
  helped speed it up.
"""

def load_input_bigger():
    rdd = sc.parallelize(range(1, 10_000_001))
    return rdd

def q8_a():
    # Version of Q6 calling q6() with the new RDD!
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    return q6(load_input_bigger())

def q8_b():
    # Version of Q7 calling q7() with the new RDD!
    # Output: a tuple (most common char, most common frequency, least common char, least common frequency)
    return q7(load_input_bigger())

"""
Discussion questions

9. State what types you used for k1, v1, k2, and v2 for your Q6 and Q7 pipelines.

=== ANSWER Q9 BELOW ===

Q6
k1: int (initial key 0)
v1: int (the number itself)
k2: int (the digit)
v2: int (the frequency count)

Q7
k1: int (initial key 0)
v1: int (the number itself)
k2: str (the character for each letter in the English representation of number)
v2: int (the frequency count)

=== END OF Q9 ANSWER ===

10. Do you think it would be possible to compute the above using only the
"simplified" MapReduce we saw in class? Why or why not?

=== ANSWER Q10 BELOW ===

No, it would not be possible to computer using the simplified MapReduce model from class.
The simplified map function produces exactly one output for each input
Q6 and Q7 require multiple outputs for each number (one for each digit or letter)
The simplified reduce function also only produces one final result and wouldn't support aggregating separately by category or key
while Q6 and Q7 require separate frequency counts for each digit or letter

=== END OF Q10 ANSWER ===
"""

"""
===== Questions 11-18: MapReduce Edge Cases =====

For the remaining questions, we will explore two interesting edge cases in MapReduce.

11. One edge case occurs when there is no output for the reduce stage.
This can happen if the map stage returns an empty list (for all keys).

Demonstrate this edge case by creating a specific pipeline which uses
our data set from Q4. It should use the general_map and general_reduce functions.

For Q11, Q14, and Q16:
your answer should return a Python set of (key, value) pairs after the reduce stage.
"""

def q11(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    rdd = rdd.map(lambda x: (0, x))
    rdd = general_map(rdd, lambda k, v: [])
    rdd = general_reduce(rdd, lambda x, y: x + y)
    return set(rdd.collect())

"""
12. What happened? Explain below.
Does this depend on anything specific about how
we chose to define general_reduce?

=== ANSWER Q12 BELOW ===

The pipeline returns an empty set because the map stage produced no key value pairs, so there were no values to reduce.
This depends on how we define general_reduce. Since we use reduceByKey, we get an empty result from an empty RDD without error.
If we used reduce() instead we would get an error.  

=== END OF Q12 ANSWER ===

13. Lastly, we will explore a second edge case, where the reduce stage can
output different values depending on the order of the input.
This leads to something called "nondeterminism", where the output of the
pipeline can even change between runs!

First, take a look at the definition of your general_reduce function.
Why do you imagine it could be the case that the output of the reduce stage
is different depending on the order of the input?

=== ANSWER Q13 BELOW ===

Since my general_reduce uses reduceByKey, the order of the input can affect the output. 
If the reduction operation is not associative or commutative, changing the order or grouping of values can produce different results.

=== END OF Q13 ANSWER ===

14.
Now demonstrate this edge case concretely by writing a specific example below.
As before, you should use the same dataset from Q4.

Important: Please create an example where the output of the reduce stage is a set of (integer, integer) pairs.
(So k2 and v2 are both integers.)
"""

def q14(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    rdd = rdd.map(lambda x: (0, x))
    rdd = general_map(rdd, lambda k, v: [(k, v)])
    rdd = general_reduce(rdd, lambda x, y: x - y) # Use subtraction to create a non-associative operation
    return set(rdd.collect())

"""
15.
Run your pipeline. What happens?
Does it exhibit nondeterministic behavior on different runs?
(It may or may not! This depends on the Spark scheduler and implementation,
including partitioning.

=== ANSWER Q15 BELOW ===

First run: {(0, 484368374988)}
Second run: {(0, 484368374988)}
Third run: {(0, 484368374988)}

My runs produced the same output and did not exhibit nondeterministic behavior, but this is not guaranteed.
With more runs this could change

=== END OF Q15 ANSWER ===

16.
Lastly, try the same pipeline as in Q14
with at least 3 different levels of parallelism.

Write three functions, a, b, and c that use different levels of parallelism.
"""

def q16_a():
    # For this one, create the RDD yourself. Choose the number of partitions.
    rdd = sc.parallelize(range(1, 1_000_001), 1)
    return q14(rdd)

def q16_b():
    # For this one, create the RDD yourself. Choose the number of partitions.
    rdd = sc.parallelize(range(1, 1_000_001), 4)
    return q14(rdd)

def q16_c():
    # For this one, create the RDD yourself. Choose the number of partitions.
    rdd = sc.parallelize(range(1, 1_000_001), 16)
    return q14(rdd)

"""
Discussion questions

17. Was the answer different for the different levels of parallelism?

=== ANSWER Q17 BELOW ===

Yes, I observed different outputs for each level of parallelism
q16a answer: {(0, -500000499998)}
q16b answer: {(0, 437497249996)}
q16c answer: {(0, 496079187472)}

=== END OF Q17 ANSWER ===

18. Do you think this would be a serious problem if this occured on a real-world pipeline?
Explain why or why not.

=== ANSWER Q18 BELOW ===

Yes. In a real-world pipeline, nondeterministic behavior causes issues with reproducibility, debugging, and reliability.
Nondeterministic outputs are also sometimes rare, so they may not be detected during testing, but could cause problems in production.
When pipeline outputs are nondeterministic, the outputs cannot be trusted to make decisions or be used for further analysis.

=== END OF Q18 ANSWER ===

===== Q19-20: Further reading =====

19.
The following is a very nice paper
which explores this in more detail in the context of real-world MapReduce jobs.
https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/icsecomp14seip-seipid15-p.pdf

Take a look at the paper. What is one sentence you found interesting?

=== ANSWER Q19 BELOW ===

"For some patterns, non-commutative
reducers lead to nondeterministic results, but the nondeterminism
appears known and tolerated by programmers."

I would have thought that nondeterministic behavior would be a serious concern for programmers,
but this paper indicates that it is known and tolerated in some cases. 
I had figured that programmers would want to run robust checks for nondeterministic behavior and avoid it at all costs.

=== END OF Q19 ANSWER ===

20.
Take one example from the paper, and try to implement it using our
general_map and general_reduce functions.
For this part, just return the answer "True" at the end if you found
it possible to implement the example, and "False" if it was not.
"""

def q20():
    rdd = sc.parallelize(range(1, 6))
    rdd = rdd.map(lambda x: (0, x)) # Pre-mapping using 0 key
    rdd = general_map(rdd, lambda k, v: [(k, v)])
    rdd = general_reduce(rdd, lambda x, y: y) # Keep second value whenever two values are combined

    print(rdd.collect()) # Print results to show impelmentation is possible
    return True

"""
That's it for Part 1!

===== Wrapping things up =====

**Don't modify this part.**

To wrap things up, we have collected
everything together in a pipeline for you below.

Check out the output in output/part1-answers.txt.
"""

ANSWER_FILE = "output/part1-answers.txt"
UNFINISHED = 0

def log_answer(name, func, *args):
    try:
        answer = func(*args)
        print(f"{name} answer: {answer}")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},{answer}\n')
            print(f"Answer saved to {ANSWER_FILE}")
    except NotImplementedError:
        print(f"Warning: {name} not implemented.")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},Not Implemented\n')
        global UNFINISHED
        UNFINISHED += 1

def PART_1_PIPELINE():
    open(ANSWER_FILE, 'w').close()

    try:
        dfs = load_input()
    except NotImplementedError:
        print("Welcome to Part 1! Implement load_input() to get started.")
        dfs = sc.parallelize([])

    # Questions 1-3
    log_answer("q1", q1)
    log_answer("q2", q2)
    # 3: commentary

    # Questions 4-10
    log_answer("q4", q4, dfs)
    log_answer("q5", q5, dfs)
    log_answer("q6", q6, dfs)
    log_answer("q7", q7, dfs)
    log_answer("q8a", q8_a)
    log_answer("q8b", q8_b)
    # 9: commentary
    # 10: commentary

    # Questions 11-18
    log_answer("q11", q11, dfs)
    # 12: commentary
    # 13: commentary
    log_answer("q14", q14, dfs)
    # 15: commentary
    log_answer("q16a", q16_a)
    log_answer("q16b", q16_b)
    log_answer("q16c", q16_c)
    # 17: commentary
    # 18: commentary

    # Questions 19-20
    # 19: commentary
    log_answer("q20", q20)

    # Answer: return the number of questions that are not implemented
    if UNFINISHED > 0:
        print("Warning: there are unfinished questions.")

    return f"{UNFINISHED} unfinished questions"

if __name__ == '__main__':
    log_answer("PART 1", PART_1_PIPELINE)

"""
=== END OF PART 1 ===
"""

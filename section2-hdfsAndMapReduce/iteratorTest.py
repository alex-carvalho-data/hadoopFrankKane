# Build and return a list
def firstnList(n):
    num, nums = 0, []

    while num < n:
        nums.append(num)
        num += 1

    return nums


sum_of_first_n = sum(firstnList(1000))

print('sum_of_first_n: {}'.format(sum_of_first_n))
print('firstnList(100): ' + str(firstnList(1000)))


# Generator as an Iterable Object
# Using the generator pattern (an iterable)
class FirstNIterableClass(object):
    def __init__(self, n):
        self.n = n
        self.num = 0

    def __iter__(self):
        return self

    # Python 3 compatibility
    def __next__(self):
        return self.next()

    def next(self):
        if self.num < self.n:
            cur, self.num = self.num, self.num + 1
            return cur
        else:
            raise StopIteration()


sum_of_first_n = sum(FirstNIterableClass(1000))

print('sum_of_first_n: {}'.format(sum_of_first_n))
print('firstnList(100): ' + str(FirstNIterableClass(1000)))


# a genereator functions with the sames behavior as the Generator Class
def firstn_gen_fuction(n):
    num = 0
    while num < n:
        yield num
        num += 1

sum_of_first_n = sum(firstn_gen_fuction(1000))

print('sum_of_first_n: {}'.format(sum_of_first_n))
print('firstnList(100): ' + str(firstn_gen_fuction(1000)))


# Generator Expression
# It's declared as a list of comprehension
# but it uses parentheses instead of square brackets.
# The list of comprehension calculate all values and generates a list
# The Generator expression returns a Generator and yields the values
# one by one when requested
doubles = [2 * n for n in range(50)]
print('doubles as a list of comprehension: ' + str(doubles))
doubles = (2 * n for n in range(50))
print('doubles as a generator expression:' + str(doubles))
for i in range(10):
    print('yielding {}: {}'.format(i, next(doubles)))
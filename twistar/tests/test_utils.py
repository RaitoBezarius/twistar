from twisted.trial import unittest
from twisted.enterprise import adbapi
from twisted.internet.defer import inlineCallbacks

from twistar import utils
from twistar.registry import Registry

from utils import *
from collections import OrderedDict

class UtilsTest(unittest.TestCase):

    @inlineCallbacks
    def setUp(self):
        yield initDB(self)
        self.user = yield User(first_name="First", last_name="Last", age=10).save()


    @inlineCallbacks
    def test_joinWheres_precedence(self):
        yield User(first_name="Second").save()

        first = ['first_name = ?', "First"]
        last = ['last_name = ?', "Last"]
        second = ['first_name = ?', "Second"]

        last_or_second = utils.joinWheres(last, second, joiner='OR')
        where = utils.joinWheres(first, last_or_second, joiner='AND')

        results = yield User.count(where=where)
        self.assertEqual(1, results)


    def test_joinMultipleWheres_empty_arg(self):
        where = utils.joinMultipleWheres([], joiner='AND')
        self.assertEqual(where, [])


    def test_joinMultipleWheres_single_where(self):
        where = ['first_name = ?', "First"]
        joined_where = utils.joinMultipleWheres([where], joiner='AND')
        self.assertEqual(where, joined_where)


    @inlineCallbacks
    def test_joinMultipleWheres(self):
        yield User(first_name="First", last_name="Last", age=20).save()

        first = ['first_name = ?', "First"]
        last = ['last_name = ?', "Last"]
        age = ['age <> ?', 20]

        where = utils.joinMultipleWheres([first, last, age], joiner='AND')

        results = yield User.count(where=where)
        self.assertEqual(1, results)


    def test_dictToWhere(self):
        self.assertEqual(utils.dictToWhere({}), None)

        result = utils.dictToWhere(OrderedDict([('one', 'two')]), "BLAH")
        self.assertEqual(result, ["(one = ?)", "two"])

        result = utils.dictToWhere(OrderedDict([('one', None) ]), "BLAH")
        self.assertEqual(result, ["(one is ?)", None])

        result = utils.dictToWhere(OrderedDict([('one', 'two'), ('three', 'four')]))
        self.assertEqual(result, ["(one = ?) AND (three = ?)", "two", "four"])

        result = utils.dictToWhere(OrderedDict([('one', 'two'), ('three', 'four'), ('five', 'six')]), "BLAH")
        self.assertEqual(result, ["(one = ?) BLAH (three = ?) BLAH (five = ?)", "two", "four", "six"])

        result = utils.dictToWhere(OrderedDict([('one', 'two'), ('three', None)]))
        self.assertEqual(result, ["(one = ?) AND (three is ?)", "two", None])

        result = utils.dictToWhere(OrderedDict([('id', [1, 2, 3]), ('age', slice(1, 18))]))
        self.assertEqual(result, ["(id IN (?, ?, ?)) AND (age BETWEEN ? AND ?)", 1, 2, 3, 1, 18])

        result = utils.dictToWhere(OrderedDict([('id', [1, 2]), ('age', utils.RawQuery("age > ?", 1))]))
        self.assertEqual(result, ["(id IN (?, ?)) AND (age > ?)", 1, 2, 1])

        result = utils.dictToWhere(OrderedDict([('first_name', "First"), ('last_name', "Last"), ('age', 11)]))
        self.assertEqual(result, ["(first_name = ?) AND (last_name = ?) AND (age = ?)", "First", "Last", 11])


    @inlineCallbacks
    def tearDown(self):
        yield tearDownDB(self)


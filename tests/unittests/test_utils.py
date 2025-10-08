import unittest
from include.utils import add23


class TestAdd23(unittest.TestCase):

    def test_add23(self):
        self.assertEqual(add23(10), 33)
        self.assertEqual(add23(0), 23)
        self.assertEqual(add23(-10), 13)
        self.assertEqual(add23(10.5), 33.5)
        self.assertEqual(add23(-10.5), 12.5)
        self.assertEqual(add23(0.0), 23.0)
        with self.assertRaises(TypeError):
            add23("23")

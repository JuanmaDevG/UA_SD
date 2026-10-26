import unittest

from WM_Central import get_netparams


class TestParams(unittest.TestCase):
    def test_noparams(self):
        self.assertRaises(SystemExit, get_netparams, ())

    def test_wrongparams(self):
        self.assertRaises(SystemExit, get_netparams, ("not_here",))
        self.assertRaises(SystemExit, get_netparams, ("not_a_port", "localhost:8000"))
        self.assertRaises(SystemExit, get_netparams, ("5000", "not_a_broker"))
        self.assertRaises(SystemExit, get_netparams, ("5000", "broker.domain:"))
        self.assertRaises(SystemExit, get_netparams, ("5000", ":8000"))
        self.assertRaises(SystemExit, get_netparams, ("5000", ":"))
        self.assertRaises(
            SystemExit, get_netparams, ("5000", "broker.domain:not_a_port")
        )

    def test_lessparams(self):
        self.assertRaises(SystemExit, get_netparams, ("5000"))
        self.assertRaises(SystemExit, get_netparams, ("localhost:8000"))

    def test_toomanyparams(self):
        self.assertRaises(
            SystemExit, get_netparams, ("5000", "localhost:8000", "unrelated")
        )

    def test_correctparams(self):
        self.assertEqual(
            get_netparams(("2000", "localhost:3000")), (2000, "localhost:3000")
        )
        self.assertEqual(get_netparams(("here",)), (5000, "localhost:8000"))


if __name__ == "__main__":
    unittest.main()

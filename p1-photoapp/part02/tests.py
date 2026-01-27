#
# Unit tests for photoapp API functions
#
# Initial tests:
#   Prof. Joe Hummel
#   Northwestern University
#

import photoapp
import unittest


############################################################
#
# Unit tests
#
class PhotoappTests(unittest.TestCase):
    #
    # NOTE: a unit test must start with "test" in order to be
    # discovered by Python's unit testing framework.
    #

  def test_01(self):
    print()
    print("** test_01: initialize **")

    success = photoapp.initialize('photoapp-config.ini', 's3readwrite', 'photoapp-read-write')
    self.assertEqual(success, True)

    print("test passed!")

  def test_02(self):
    print()
    print("** test_02: get_ping **")

    (M, N) = photoapp.get_ping()

    self.assertEqual(M, 0)
    self.assertEqual(N, 3)

    print("test passed!")

  def test_03(self):
    print()
    print("** test_03: get_users **")

    correct = [(80001, 'p_sarkar', 'Pooja', 'Sarkar'), 
               (80002, 'e_ricci', 'Emanuele', 'Ricci'),
               (80003, 'l_chen', 'Li', 'Chen')]

    users = photoapp.get_users()

    self.assertEqual(users, correct)

    print("test passed!")


############################################################
#
# main
#
if __name__ == '__main__':
  unittest.main()

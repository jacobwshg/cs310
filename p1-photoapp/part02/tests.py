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

  def test_post_image(self):
    print()
    print("** test_post_image **")

    uid = 80003
    locname = "01degu.jpg"

    assetid = photoapp.post_image( uid, locname )
    print( f"userid: {uid}" )
    print( f"localname: { locname }" )
    print( f"assetid: {assetid}" )

  def test_get_image(self):
    print()
    print("** test_get_image **")

    aid = 1001
    _locname = "download.jpg"

    localname = photoapp.get_image( aid, _locname )
    #localname = photoapp.get_image( aid )
    print( f"assetid: {aid}" )
    print( f"request localname: {_locname}" )
    print( f"result localname: {localname}" )

  def test_delete_images( self ):
    print()
    print("** test_delete_images **")

    ##success = photoapp.delete_images()
    ##print( f"result: { success }" )


  def test_get_image_labels( self ):
    print()
    print("** test_get_image_labels **")

    aid = 1001

    print( photoapp.get_image_labels( aid ) )

  def test_get_images_with_label( self ):
    print()
    print("** test_get_images_with_label **")

    label = "o"

    print( photoapp.get_images_with_label( label ) )



############################################################
#
# main
#
if __name__ == '__main__':
  unittest.main()


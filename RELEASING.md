# Releasing datasette-pytables

Author: Javier Sancho, Francesc Alted
Contact: jsf@jsancho.org, francesc@blosc.org
Date: 2018-06-01


## Preliminaries

* Update the version number at ``VERSION`` and ``ANNOUNCE.md``.

* Make sure that ``RELEASE_NOTES.md`` and ``ANNOUNCE.md`` are up to
  date with the latest news in the release.

* Commit your changes:

```
    $ git commit -a -m"Getting ready for X.Y.Z final"
```

* Once a year: check that the copyright year in `LICENSE` file.


## Tagging

* Create a tag ``vX.Y.Z`` from ``master``.  Use the next message:

```
    $ git tag -a vX.Y.Z -m "Tagging version X.Y.Z"
```

  Note: For release candidates, just add a rcN suffix to tag ("vX.Y.ZrcN").

* Or, alternatively, make a signed tag (requires gpg correctly configured):

```
    $ git tag -s vX.Y.Z -m "Tagging version X.Y.Z"
```

* Push the tag to the Github repo:

```
    $ git push
    $ git push --tags
```


## Testing

* Go to the root directory and run:

```
    $ pytest
```


## Packaging

* Make sure that you are in a clean directory.  The best way is to
  re-clone and re-build:

```
  $ cd /tmp
  $ git clone https://github.com/PyTables/datasette-pytables
  $ cd datasette-pytables
  $ python setup.py build
  $ pytest
```

* Make the tarball with the command:

```
  $ python setup.py sdist
```

Do a quick check that the tarball is sane.


## Uploading

* Upload it also in the PyPi repository:

```
    $ python setup.py sdist upload
```


## Announcing

* Send an announcement to the python-announce, python-users and pydata
  lists.  Use the ``ANNOUNCE.md`` file as skeleton (or possibly as
  the definitive version).

* Tweet about the new release and rejoice!


## Post-release actions

* Create new headers for adding new features in ``RELEASE_NOTES.rst``
  and add this place-holder instead:

  #XXX version-specific blurb XXX#

* Commit your changes with:

```
  $ git commit -a -m"Post X.Y.Z release actions done"
```


That's all folks!

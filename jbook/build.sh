#!/bin/sh
# Delete prior build
# Commenting out the removal of prior artifacts for faster builds.
# rm -r jbook/_build/
# Prepare notebook display customizations
python3 jbook/prep_notebooks.py
# Rebuilds the book
jb build jbook/
# Commit book to gh-pages
ghp-import -o -n -p -f jbook/_build/html
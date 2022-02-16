#!/bin/sh
# Prepare notebook display customizations
python3 jbook/prep_notebooks.py
# Rebuilds the book
jb build jbook/
# Commit book to gh-pages
ghp-import -o -n -p -f jbook/_build/html
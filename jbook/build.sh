#!/bin/sh
# Delete prior build
echo "Removing prior jupyter-book build artifacts..."
rm -r jbook/_build/
#Prepare notebook display customizations
echo "Preparing notebook tags..."
python3 jbook/prep_notebooks.py
# Rebuilds the book
echo "Building book..."
jb build jbook/
# Commit book to gh-pages
echo "Committing changes to github pages..."
ghp-import -o -n -p -f jbook/_build/html

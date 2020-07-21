# iiif deviation checker

Spider that walks the public /article API, extracts image references, downloads the article data from the S3 bucket and
iiif and compares the two using ImageMagick.

It's intended to be a regression checking tool for IIIF that can be run on an adhoc IIIF instance.

## dependencies

* [babashka](https://github.com/borkdude/babashka/releases)
* [ImageMagick](https://imagemagick.org/index.php)

## run

    ./check.clj

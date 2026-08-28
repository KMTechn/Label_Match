"""Keep Label_Match on charset-normalizer's source-only analysis path.

The release builder places a verified Python-source-only package copy first on
pathex.  This higher-priority hook prevents the contrib hook from adding the
installed mypyc accelerator extensions as hidden imports.
"""

hiddenimports: list[str] = []

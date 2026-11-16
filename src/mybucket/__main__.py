"""``python -m mybucket``: the same as the ``mybucket`` command."""

import sys

from .cli import main

sys.exit(main())

"""Allow ``python -m s2s`` to run the writer (multi-process supervisor or solo)."""

from s2s.multiprocess import main

raise SystemExit(main())

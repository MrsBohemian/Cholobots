# ABOUTME: Atomic private JSON writer shared by merge_train.py and remediate_pr.py.
# ABOUTME: State and action files live in /tmp and carry PR titles, so they stay 0600.
import json
import os
import tempfile


def write_json_private(path, data):
    """Write ``data`` as JSON to ``path`` atomically, leaving the file mode 0600.

    A plain ``open(tmp, "w")`` creates the temp file with 0666 & ~umask, and
    renaming it over an existing 0600 file would widen that file's mode.
    mkstemp creates the temp file 0600 and uniquely named, in the destination
    directory so the rename stays on one filesystem.
    """
    directory = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(
        prefix=os.path.basename(path) + ".tmp.", dir=directory
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
        os.rename(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise

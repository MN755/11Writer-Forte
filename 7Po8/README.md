# 7Po8 Backend Reference

7Po8 is no longer treated as a standalone product inside 11Writer Forte.

What remains here is the backend reference material that is being folded into the main 11Writer Forte runtime:

- wave and connector models
- scheduler and run-history patterns
- discovered-source and domain-trust logic
- backend tests and Alembic history

Removed from the supported runtime:

- `apps/frontend`

Use this tree as migration input, not as a second app to run beside `app/server`.

## Local Reference Use

```bash
cd apps/backend
python -m pip install -e .[dev]
python -m alembic upgrade head
python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8001
```

That local run path is for backend comparison and migration work only. The target operational surface is the main `11writer` CLI and `app/server` backend.

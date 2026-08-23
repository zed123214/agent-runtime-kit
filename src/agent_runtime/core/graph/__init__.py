"""Optional graph-engine package.

The package initializer deliberately imports nothing, so the default runtime,
loop engine, CLI, and TUI do not load LangGraph.  The engine router lazily loads
the framework-backed ``engine`` and ``runtime`` modules only for ``engine=graph``.
"""

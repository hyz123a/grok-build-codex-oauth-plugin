# TDD guide

Development follows Red → Green → Refactor and treats tests as executable protocol documentation.

Prioritize observable contracts in this order:

1. security boundaries and credential non-disclosure;
2. public HTTP status, headers, JSON, and SSE bytes;
3. OAuth and refresh state transitions;
4. idempotent local configuration;
5. account model discovery, filtering, aliases, and cache fallback;
6. internal helper behavior.

Integration tests must start the real router on a random loopback port and inject external behavior
through the public `CredentialProvider` seam or the loopback-only test upstream constructor. They
must not call private handler functions or a real ChatGPT service.

Time-dependent behavior should use controlled expiry values or bounded condition polling. Avoid
fixed sleeps except for a short polling interval in shared test infrastructure.

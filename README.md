\# Enterprise AI \& API Gateway



One Gateway. Two Workloads. One Control Plane.



Enterprise AI \& API Gateway provides two first-class Data Plane engines:



\- Normal API Gateway

\- LLM Gateway



with a shared:



\- Authentication

\- Authorization

\- Rate Limiting

\- Concurrency Control

\- Circuit Breaking

\- Observability

\- Control Plane

\- Audit

\- Secret Management



\## V1 Status



Design Contract: FROZEN



Implementation: IN PROGRESS



Release Candidate: NOT YET EVIDENCED



\## V1 Architecture



Clients

&#x20; |

Gateway Core

&#x20; |

&#x20; +-- Normal API Engine

&#x20; |

&#x20; +-- LLM Gateway



Control Plane:

Admin API / Registry / Config / Audit / Secrets



\## Technology



\- Python 3.12+

\- FastAPI

\- SQLAlchemy 2

\- PostgreSQL 16+

\- Redis 7+

\- HTTPX

\- Alembic

\- React + TypeScript

\- Docker / Docker Compose

\- uv

\- pytest



\## Development



Implementation follows milestone execution packs M0 through M9.



See `docs/`.


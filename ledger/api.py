"""HTTP API.

    uvicorn ledger.api:app

There is no authentication: this is the ledger core, meant to sit behind a
service that owns auth. In particular, anyone who can call POST /accounts can
create an account with allow_negative, which is a money printer. Do not expose
this directly.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from . import core, db

# strict: 1.5, "100" and true are all rejected rather than coerced.
Amount = Annotated[int, Field(strict=True, gt=0, le=core.MAX_AMOUNT)]
IdempotencyKey = Annotated[str, Header(min_length=1, max_length=255)]


class AccountIn(BaseModel):
    name: Annotated[str, Field(min_length=1, max_length=200)]
    currency: Annotated[str, Field(pattern=r"^[A-Z]{3}$")]
    allow_negative: bool = False


class TransferIn(BaseModel):
    from_account: int
    to_account: int
    amount: Amount
    memo: Annotated[str | None, Field(max_length=500)] = None
    external_ref: Annotated[str | None, Field(max_length=200)] = None


def create_app(pool: ConnectionPool | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        owned = pool is None
        app.state.pool = pool or db.make_pool(db.database_url())
        if owned:
            with app.state.pool.connection() as conn:
                db.apply_schema(conn)
        yield
        if owned:
            app.state.pool.close()

    app = FastAPI(title="ledger", version="0.1.0", lifespan=lifespan)

    def conn(request: Request):
        with request.app.state.pool.connection() as c:
            yield c

    @app.exception_handler(core.LedgerError)
    async def ledger_error(request: Request, exc: core.LedgerError):
        return JSONResponse(status_code=exc.status, content={"error": exc.code, "detail": str(exc)})

    def posted(c, result: core.Posted, response: Response) -> dict:
        if result.replayed:
            response.status_code = 200
            response.headers["Idempotent-Replayed"] = "true"
        return core.get_transaction(c, result.transaction_id)

    @app.post("/accounts", status_code=201)
    def create_account(body: AccountIn, c=Depends(conn)):
        return core.create_account(c, body.name, body.currency, body.allow_negative)

    @app.get("/accounts/{account_id}")
    def get_account(account_id: int, c=Depends(conn)):
        return core.get_account(c, account_id)

    @app.get("/accounts/{account_id}/statement")
    def get_statement(account_id: int, limit: Annotated[int, Query(ge=1, le=1000)] = 100, c=Depends(conn)):
        return core.statement(c, account_id, limit)

    @app.post("/transfers", status_code=201)
    def create_transfer(body: TransferIn, response: Response, idempotency_key: IdempotencyKey,
                        c=Depends(conn)):
        result = core.transfer(c, key=idempotency_key, from_account=body.from_account,
                               to_account=body.to_account, amount=body.amount,
                               memo=body.memo, external_ref=body.external_ref)
        return posted(c, result, response)

    @app.get("/transactions/{transaction_id}")
    def get_transaction(transaction_id: int, c=Depends(conn)):
        return core.get_transaction(c, transaction_id)

    @app.post("/transactions/{transaction_id}/reverse", status_code=201)
    def reverse_transaction(transaction_id: int, response: Response, idempotency_key: IdempotencyKey,
                            c=Depends(conn)):
        result = core.reverse(c, key=idempotency_key, transaction_id=transaction_id)
        return posted(c, result, response)

    @app.get("/health")
    def health(c=Depends(conn)):
        report = core.check_invariants(c)
        return JSONResponse(status_code=200 if report["ok"] else 500, content=jsonable_encoder(report))

    return app


app = create_app()

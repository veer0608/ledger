"""HTTP API.

    uvicorn ledger.api:app

There is no authentication: this is the ledger core, meant to sit behind a
service that owns auth and sets the Client-Id header from it. Here the header
is trusted as given, so it scopes idempotency keys between honest clients; it
does not stop one client from claiming to be another.

The service connects as ledger_app, which cannot create an account that may
go negative. Settlement accounts and mints are made by the owner with
`python -m ledger.migrate --system-account NAME CURRENCY`.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Annotated

from fastapi import Depends, FastAPI, Header, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, ConfigDict, Field

from . import core, db

# strict: 1.5, "100" and true are all rejected rather than coerced.
Amount = Annotated[int, Field(strict=True, gt=0, le=core.MAX_AMOUNT)]
IdempotencyKey = Annotated[str, Header(min_length=1, max_length=255)]
ClientId = Annotated[str, Header(min_length=1, max_length=100)]


class AccountIn(BaseModel):
    # forbid: a request asking for allow_negative gets a 422, not a quiet
    # normal account the caller then believes is a mint.
    model_config = ConfigDict(extra="forbid")

    name: Annotated[str, Field(min_length=1, max_length=200)]
    currency: Annotated[str, Field(pattern=r"^[A-Z]{3}$")]


class TransferIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    from_account: int
    to_account: int
    amount: Amount
    memo: Annotated[str | None, Field(max_length=500)] = None
    external_ref: Annotated[str | None, Field(max_length=200)] = None


class HoldIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account: int
    payee: int
    amount: Amount
    expires_in_seconds: Annotated[int, Field(strict=True, ge=60, le=int(core.MAX_HOLD_TTL.total_seconds()))] = \
        int(core.DEFAULT_HOLD_TTL.total_seconds())


class CaptureIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Amount | None = None  # omit to capture the whole hold


def create_app(pool: ConnectionPool | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        owned = pool is None
        app.state.pool = pool or db.make_pool(db.app_url())
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
        return core.create_account(c, body.name, body.currency)

    @app.get("/accounts/{account_id}")
    def get_account(account_id: int, c=Depends(conn)):
        return core.get_account(c, account_id)

    @app.get("/accounts/{account_id}/statement")
    def get_statement(account_id: int, limit: Annotated[int, Query(ge=1, le=1000)] = 100, c=Depends(conn)):
        return core.statement(c, account_id, limit)

    @app.post("/transfers", status_code=201)
    def create_transfer(body: TransferIn, response: Response, idempotency_key: IdempotencyKey,
                        client_id: ClientId, c=Depends(conn)):
        result = core.transfer(c, client=client_id, key=idempotency_key, from_account=body.from_account,
                               to_account=body.to_account, amount=body.amount,
                               memo=body.memo, external_ref=body.external_ref)
        return posted(c, result, response)

    @app.get("/transactions/{transaction_id}")
    def get_transaction(transaction_id: int, c=Depends(conn)):
        return core.get_transaction(c, transaction_id)

    @app.post("/transactions/{transaction_id}/reverse", status_code=201)
    def reverse_transaction(transaction_id: int, response: Response, idempotency_key: IdempotencyKey,
                            client_id: ClientId, c=Depends(conn)):
        result = core.reverse(c, client=client_id, key=idempotency_key, transaction_id=transaction_id)
        return posted(c, result, response)

    @app.post("/holds", status_code=201)
    def create_hold(body: HoldIn, response: Response, idempotency_key: IdempotencyKey,
                    client_id: ClientId, c=Depends(conn)):
        result = core.authorize(c, client=client_id, key=idempotency_key, account=body.account,
                                payee=body.payee, amount=body.amount,
                                expires_in=timedelta(seconds=body.expires_in_seconds))
        if result.replayed:
            response.status_code = 200
            response.headers["Idempotent-Replayed"] = "true"
        return core.get_hold(c, result.hold_id)

    @app.get("/holds/{hold_id}")
    def get_hold(hold_id: int, c=Depends(conn)):
        return core.get_hold(c, hold_id)

    @app.post("/holds/{hold_id}/capture", status_code=201)
    def capture_hold(hold_id: int, response: Response, idempotency_key: IdempotencyKey,
                     client_id: ClientId, body: CaptureIn | None = None, c=Depends(conn)):
        result = core.capture(c, client=client_id, key=idempotency_key, hold_id=hold_id,
                              amount=body.amount if body else None)
        return posted(c, result, response)

    @app.post("/holds/{hold_id}/void")
    def void_hold(hold_id: int, c=Depends(conn)):
        return core.void(c, hold_id=hold_id)

    @app.get("/health")
    def health(c=Depends(conn)):
        report = core.check_invariants(c)
        return JSONResponse(status_code=200 if report["ok"] else 500, content=jsonable_encoder(report))

    return app


app = create_app()

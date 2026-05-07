from __future__ import annotations

import json
from typing import Any, Sequence
from urllib.parse import unquote, urlparse

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
from harlequin import (
    HarlequinAdapter,
    HarlequinConnection,
    HarlequinCursor,
)
from harlequin.autocomplete.completion import HarlequinCompletion
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from textual_fastdatatable.backend import AutoBackendType

from harlequin_clickhouse.cli_options import CLICKHOUSE_OPTIONS

_CONNECT_OPTION_NAMES = {
    "host",
    "port",
    "username",
    "password",
    "database",
    "secure",
    "verify",
    "connect_timeout",
    "send_receive_timeout",
}


def _connect_options_from_dsn(dsn: str) -> dict[str, Any]:
    parsed = urlparse(dsn)
    options: dict[str, Any] = {}
    if parsed.hostname:
        options["host"] = parsed.hostname
    if parsed.port:
        options["port"] = parsed.port
    if parsed.username:
        options["username"] = unquote(parsed.username)
    if parsed.password:
        options["password"] = unquote(parsed.password)
    if parsed.path and parsed.path != "/":
        options["database"] = unquote(parsed.path.lstrip("/"))
    if parsed.scheme == "clickhouses":
        options["secure"] = True
    return options


class HarlequinClickHouseCursor(HarlequinCursor):
    def __init__(self, result: Any) -> None:
        self.result = result
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        return [
            (name, getattr(type_, "name", str(type_)))
            for name, type_ in zip(self.result.column_names, self.result.column_types)
        ]

    def set_limit(self, limit: int) -> HarlequinClickHouseCursor:
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType:
        try:
            results = self.result.result_rows
            if self._limit is not None:
                results = results[: self._limit]
            return [tuple(self._serialize_json_like_value(value) for value in row) for row in results]
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e

    @staticmethod
    def _serialize_json_like_value(value: Any) -> Any:
        if isinstance(value, (dict, list)):
            return json.dumps(value, separators=(",", ":"), default=str)
        return value


class HarlequinClickHouseConnection(HarlequinConnection):
    def __init__(
        self,
        conn_str: Sequence[str],
        *args: Any,
        init_message: str = "Welcome to ClickHouse with harlequin",
        options: dict[str, Any],
    ) -> None:
        self.init_message = init_message
        self.conn_str = conn_str
        try:
            self.conn = self._connect(conn_str, options)
            self.conn.command("SELECT 1")
        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e),
                title="Harlequin could not connect to your ClickHouse with clickhouse-connect.",
            ) from e

    @staticmethod
    def _connect(conn_str: Sequence[str], options: dict[str, Any]) -> Client:
        connect_options = {**options}
        if "user" in connect_options:
            connect_options["username"] = connect_options.pop("user")
        if "port" in connect_options and connect_options["port"] is not None:
            connect_options["port"] = int(connect_options["port"])
        if "connect_timeout" in connect_options and connect_options["connect_timeout"] is not None:
            connect_options["connect_timeout"] = int(connect_options["connect_timeout"])
        if "send_receive_timeout" in connect_options and connect_options["send_receive_timeout"] is not None:
            connect_options["send_receive_timeout"] = int(connect_options["send_receive_timeout"])
        if "secure" in connect_options and isinstance(connect_options["secure"], str):
            connect_options["secure"] = connect_options["secure"].lower() == "true"
        if "verify" in connect_options and isinstance(connect_options["verify"], str):
            connect_options["verify"] = connect_options["verify"].lower() == "true"

        if len(conn_str) == 1:
            connect_options = {**_connect_options_from_dsn(conn_str[0]), **connect_options}

        connect_options = {
            key: value for key, value in connect_options.items() if key in _CONNECT_OPTION_NAMES and value is not None
        }
        return clickhouse_connect.get_client(**connect_options)

    def execute(self, query: str) -> HarlequinCursor | None:
        query = query.strip().rstrip(";")
        if not query.lower().startswith(("select", "show", "describe", "desc", "with", "explain")):
            try:
                self.conn.command(query)
            except Exception as e:
                raise HarlequinQueryError(
                    msg=str(e),
                    title="Harlequin encountered an error while executing your query.",
                ) from e
            return None

        try:
            result = self.conn.query(query)
        except ClickHouseError:
            try:
                self.conn.command(query)
            except Exception as e:
                raise HarlequinQueryError(
                    msg=str(e),
                    title="Harlequin encountered an error while executing your query.",
                ) from e
            return None
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e
        else:
            if not result.column_names:
                return None
            return HarlequinClickHouseCursor(result)

    def get_catalog(self) -> Catalog:
        # This is a small hack to overcome the fact that clickhouse doesn't
        # have the concept of schemas
        databases = self._list_databases()
        database_items: list[CatalogItem] = []
        for (db,) in databases:
            relations = self._list_relations_in_database(db)
            rel_items: list[CatalogItem] = []
            for rel, rel_type in relations:
                cols = self._list_columns_in_relation(db, rel)
                col_items = [
                    CatalogItem(
                        qualified_identifier=f'"{db}"."{rel}"."{col}"',
                        query_name=f'"{col}"',
                        label=col,
                        type_label=self._get_short_type(col_type),
                    )
                    for col, col_type in cols
                ]
                rel_items.append(
                    CatalogItem(
                        qualified_identifier=f'"{db}"."{rel}"',
                        query_name=f'"{db}"."{rel}"',
                        label=rel,
                        type_label="v" if rel_type == "VIEW" else "t",
                        children=col_items,
                    ),
                )
            database_items.append(
                CatalogItem(
                    qualified_identifier=f'"{db}"',
                    query_name=f'"{db}"',
                    label=db,
                    type_label="s",
                    children=rel_items,
                ),
            )
        return Catalog(items=database_items)

    def get_completions(self) -> list[HarlequinCompletion]:
        extra_keywords = ["foo", "bar", "baz"]
        return [
            HarlequinCompletion(
                label=item,
                type_label="kw",
                value=item,
                priority=1000,
                context=None,
            )
            for item in extra_keywords
        ]

    def _list_databases(self) -> list[tuple[str]]:
        return self.conn.query(
            """
            SELECT
                name
            FROM system.databases
            where name not in
                ('INFORMATION_SCHEMA', 'system', 'information_schema')
        """,
        ).result_rows

    def _list_relations_in_database(self, db: str) -> list[tuple[str, str]]:
        return self.conn.query(
            f"""
            SELECT
                table_name,
                table_type
            FROM information_schema.tables
            WHERE
                table_schema = '{db}'
            ORDER BY table_name asc
                """,
        ).result_rows

    def _list_columns_in_relation(
        self,
        db: str,
        relation: str,
    ) -> list[tuple[str, str]]:
        return self.conn.query(
            f"""
            select
                column_name, data_type
            from information_schema.columns
            where
                table_schema = '{db}'
                and table_name = '{relation}'
                order by ordinal_position asc
                """,
        ).result_rows

    @staticmethod
    def _get_short_type(type_name: str) -> str:
        MAPPING = {
            "UInt8": "#",
            "UInt16": "#",
            "UInt32": "#",
            "UInt64": "##",
            "UInt128": "##",
            "UInt256": "##",
            "Int8": "#",
            "Int16": "#",
            "Int32": "#",
            "Int64": "##",
            "Int128": "##",
            "Int256": "##",
            "Float32": "#.#",
            "Float64": "#.#",
            "Decimal": "#.#",
            "Boolean": "t/f",
            "String": "s",
            "FixedString": "s",
            "Date": "d",
            "Date32": "d",
            "DateTime": "ts",
            "DateTime64": "ts",
            "JSON": "{}",
            "UUID": "uid",
            "Enum": "e",
            "LowCardinality": "lc",
            "Array": "[]",
            "Map": "{}->{}",
            "SimpleAggregateFunction": "saf",
            "AggregateFunction": "af",
            "Nested": "tbl",
            "Tuple": "()",
            "Nullable": "?",
            "IPv4": "ip",
            "IPv6": "ip",
            "Point": "*",
            "Ring": "o",
            "Polygon": "v",
            "MultiPolygon": "vv",
            "Expression": "expr",
            "Set": "set",
            "Nothing": "nil",
            "Interval": "|-|",
        }
        return MAPPING.get(type_name.split("(")[0].split(" ")[0], "?")


class HarlequinClickHouseAdapter(HarlequinAdapter):
    ADAPTER_OPTIONS = CLICKHOUSE_OPTIONS

    def __init__(self, conn_str: Sequence[str], **options: Any) -> None:
        self.conn_str = conn_str
        self.options = options

    def connect(self) -> HarlequinClickHouseConnection:
        conn = HarlequinClickHouseConnection(
            conn_str=self.conn_str,
            options=self.options,
        )
        return conn

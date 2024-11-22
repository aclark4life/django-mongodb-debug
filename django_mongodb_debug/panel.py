import contextlib
import contextvars
import datetime
import django
import json


from collections import defaultdict
from copy import copy

from asgiref.sync import sync_to_async
from django.db import connections
from django.urls import path
from django.utils.translation import gettext_lazy as _, ngettext
from django.utils.encoding import force_str

from debug_toolbar import settings as dt_settings
from debug_toolbar.forms import SignedDataForm
from debug_toolbar.panels import Panel
from debug_toolbar.panels.sql import views
from debug_toolbar.panels.sql.forms import SQLSelectForm
from debug_toolbar.panels.sql.utils import (
    contrasting_color_generator,
    is_select_query,
    reformat_sql,
)
from debug_toolbar.utils import render_stacktrace
from debug_toolbar.utils import get_stack_trace, get_template_info
from time import perf_counter

# Prevents SQL queries from being sent to the DB. It's used
# by the TemplatePanel to prevent the toolbar from issuing
# additional queries.
allow_sql = contextvars.ContextVar("debug-toolbar-allow-sql", default=True)


def _similar_query_key(query):
    return query["raw_sql"]


def _duplicate_query_key(query):
    raw_params = () if query["raw_params"] is None else tuple(query["raw_params"])
    # repr() avoids problems because of unhashable types
    # (e.g. lists) when used as dictionary keys.
    # https://github.com/django-commons/django-debug-toolbar/issues/1091
    return (query["raw_sql"], repr(raw_params))


def _process_query_groups(query_groups, databases, colors, name):
    counts = defaultdict(int)
    for (alias, _key), query_group in query_groups.items():
        count = len(query_group)
        # Queries are similar / duplicates only if there are at least 2 of them.
        if count > 1:
            color = next(colors)
            for query in query_group:
                query[f"{name}_count"] = count
                query[f"{name}_color"] = color
            counts[alias] += count
    for alias, db_info in databases.items():
        db_info[f"{name}_count"] = counts[alias]


def wrap_cursor(connection):
    # (Pdb) connection
    # <DatabaseWrapper vendor='mongodb' alias='default'>

    # When running a SimpleTestCase, Django monkey patches some DatabaseWrapper
    # methods, including .cursor() and .chunked_cursor(), to raise an exception
    # if the test code tries to access the database, and then undoes the monkey
    # patching when the test case is finished.  If we monkey patch those methods
    # also, Django's process of undoing those monkey patches will fail.  To
    # avoid this failure, and because database access is not allowed during a
    # SimpleTestCase anyway, skip applying our instrumentation monkey patches if
    # we detect that Django has already monkey patched DatabaseWrapper.cursor().
    if isinstance(connection.cursor, django.test.testcases._DatabaseFailure):
        return
    if not hasattr(connection, "_djdt_cursor"):
        connection._djdt_cursor = connection.cursor
        connection._djdt_chunked_cursor = connection.chunked_cursor
        connection._djdt_logger = None

        def cursor(*args, **kwargs):
            # Per the DB API cursor() does not accept any arguments. There's
            # some code in the wild which does not follow that convention,
            # so we pass on the arguments even though it's not clean.
            # See:
            # https://github.com/django-commons/django-debug-toolbar/pull/615
            # https://github.com/django-commons/django-debug-toolbar/pull/896
            logger = connection._djdt_logger
            cursor = connection._djdt_cursor(*args, **kwargs)
            if logger is None:
                return cursor
            mixin = NormalCursorMixin if allow_sql.get() else ExceptionCursorMixin
            return patch_cursor_wrapper_with_mixin(cursor.__class__, mixin)(
                cursor.cursor, connection, logger
            )

        def chunked_cursor(*args, **kwargs):
            # prevent double wrapping
            # solves https://github.com/django-commons/django-debug-toolbar/issues/1239
            logger = connection._djdt_logger
            cursor = connection._djdt_chunked_cursor(*args, **kwargs)
            if logger is not None and not isinstance(cursor, DjDTCursorWrapperMixin):
                mixin = NormalCursorMixin if allow_sql.get() else ExceptionCursorMixin
                return patch_cursor_wrapper_with_mixin(cursor.__class__, mixin)(
                    cursor.cursor, connection, logger
                )
            return cursor

        connection.cursor = cursor
        connection.chunked_cursor = chunked_cursor


def patch_cursor_wrapper_with_mixin(base_wrapper, mixin):
    class DjDTCursorWrapper(mixin, base_wrapper):
        pass

    return DjDTCursorWrapper


class DjDTCursorWrapperMixin:
    def __init__(self, cursor, db, logger):
        super().__init__(cursor, db)
        # logger must implement a ``record`` method
        self.logger = logger


class ExceptionCursorMixin(DjDTCursorWrapperMixin):
    """
    Wraps a cursor and raises an exception on any operation.
    Used in Templates panel.
    """

    def __getattr__(self, attr):
        raise SQLQueryTriggered()


class NormalCursorMixin(DjDTCursorWrapperMixin):
    """
    Wraps a cursor and logs queries.
    """

    def _decode(self, param):
        # If a sequence type, decode each element separately
        if isinstance(param, (tuple, list)):
            return [self._decode(element) for element in param]

        # If a dictionary type, decode each value separately
        if isinstance(param, dict):
            return {key: self._decode(value) for key, value in param.items()}

        # make sure datetime, date and time are converted to string by force_str
        CONVERT_TYPES = (datetime.datetime, datetime.date, datetime.time)
        try:
            return force_str(param, strings_only=not isinstance(param, CONVERT_TYPES))
        except UnicodeDecodeError:
            return "(encoded string)"

    def _record(self, method, sql, params):
        alias = self.db.alias
        vendor = self.db.vendor

        start_time = perf_counter()
        try:
            return method(sql, params)
        finally:
            stop_time = perf_counter()
            duration = (stop_time - start_time) * 1000
            _params = ""
            with contextlib.suppress(TypeError):
                # object JSON serializable?
                _params = json.dumps(self._decode(params))
            template_info = get_template_info()

            sql = str(sql)

            kwargs = {
                "vendor": vendor,
                "alias": alias,
                "sql": self.last_executed_query(sql, params),
                "duration": duration,
                "raw_sql": sql,
                "params": _params,
                "raw_params": params,
                "stacktrace": get_stack_trace(skip=2),
                "template_info": template_info,
            }

            # We keep `sql` to maintain backwards compatibility
            self.logger.record(**kwargs)

    def callproc(self, procname, params=None):
        return self._record(super().callproc, procname, params)

    def execute(self, sql, params=None):
        return self._record(super().execute, sql, params)

    def executemany(self, sql, param_list):
        return self._record(super().executemany, sql, param_list)


class SQLQueryTriggered(Exception):
    """Thrown when template panel triggers a query"""


class MongoPanel(Panel):
    """
    Panel that displays information about the MongoDB queries run while processing
    the request.
    """

    is_async = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sql_time = 0
        self._queries = []
        self._databases = {}

    def record(self, **kwargs):
        self._queries.append(kwargs)
        alias = kwargs["alias"]
        if alias not in self._databases:
            self._databases[alias] = {
                "time_spent": kwargs["duration"],
                "num_queries": 1,
            }
        else:
            self._databases[alias]["time_spent"] += kwargs["duration"]
            self._databases[alias]["num_queries"] += 1
        self._sql_time += kwargs["duration"]

    # Implement the Panel API

    nav_title = _("MongoDB")

    @property
    def nav_subtitle(self):
        query_count = len(self._queries)
        return ngettext(
            "%(query_count)d query in %(sql_time).2fms",
            "%(query_count)d queries in %(sql_time).2fms",
            query_count,
        ) % {
            "query_count": query_count,
            "sql_time": self._sql_time,
        }

    @property
    def title(self):
        count = len(self._databases)
        return ngettext(
            "MongoDB queries from %(count)d connection",
            "MongoDB queries from %(count)d connections",
            count,
        ) % {"count": count}

    template = "mql.html"

    @classmethod
    def get_urls(cls):
        return [
            path("sql_select/", views.sql_select, name="sql_select"),
            path("sql_explain/", views.sql_explain, name="sql_explain"),
            path("sql_profile/", views.sql_profile, name="sql_profile"),
        ]

    async def aenable_instrumentation(self):
        """
        Async version of enable instrumentation.
        For async capable panels having async logic for instrumentation.
        """
        await sync_to_async(self.enable_instrumentation)()

    def enable_instrumentation(self):
        # This is thread-safe because database connections are thread-local.
        for connection in connections.all():
            wrap_cursor(connection)
            connection._djdt_logger = self

    def disable_instrumentation(self):
        for connection in connections.all():
            connection._djdt_logger = None

    def generate_stats(self, request, response):
        colors = contrasting_color_generator()
        trace_colors = defaultdict(lambda: next(colors))
        similar_query_groups = defaultdict(list)
        duplicate_query_groups = defaultdict(list)

        if self._queries:
            sql_warning_threshold = dt_settings.get_config()["SQL_WARNING_THRESHOLD"]

            width_ratio_tally = 0
            factor = int(256.0 / (len(self._databases) * 2.5))
            for n, db in enumerate(self._databases.values()):
                rgb = [0, 0, 0]
                color = n % 3
                rgb[color] = 256 - n // 3 * factor
                nn = color
                # XXX: pretty sure this is horrible after so many aliases
                while rgb[color] < factor:
                    nc = min(256 - rgb[color], 256)
                    rgb[color] += nc
                    nn += 1
                    if nn > 2:
                        nn = 0
                    rgb[nn] = nc
                db["rgb_color"] = rgb

            # the last query recorded for each DB alias
            last_by_alias = {}
            for query in self._queries:
                alias = query["alias"]

                similar_query_groups[(alias, _similar_query_key(query))].append(query)
                duplicate_query_groups[(alias, _duplicate_query_key(query))].append(
                    query
                )

                query["form"] = SignedDataForm(
                    auto_id=None, initial=SQLSelectForm(initial=copy(query)).initial
                )

                if query["sql"]:
                    query["sql"] = reformat_sql(query["sql"], with_toggle=True)

                query["is_slow"] = query["duration"] > sql_warning_threshold
                query["is_select"] = is_select_query(query["raw_sql"])

                query["rgb_color"] = self._databases[alias]["rgb_color"]
                try:
                    query["width_ratio"] = (query["duration"] / self._sql_time) * 100
                except ZeroDivisionError:
                    query["width_ratio"] = 0
                query["start_offset"] = width_ratio_tally
                query["end_offset"] = query["width_ratio"] + query["start_offset"]
                width_ratio_tally += query["width_ratio"]
                query["stacktrace"] = render_stacktrace(query["stacktrace"])

                query["trace_color"] = trace_colors[query["stacktrace"]]

                last_by_alias[alias] = query

        group_colors = contrasting_color_generator()
        _process_query_groups(
            similar_query_groups, self._databases, group_colors, "similar"
        )
        _process_query_groups(
            duplicate_query_groups, self._databases, group_colors, "duplicate"
        )

        self.record_stats(
            {
                "databases": sorted(
                    self._databases.items(), key=lambda x: -x[1]["time_spent"]
                ),
                "queries": self._queries,
                "sql_time": self._sql_time,
            }
        )

    def generate_server_timing(self, request, response):
        stats = self.get_stats()
        title = "MongoDB {} queries".format(len(stats.get("queries", [])))
        value = stats.get("sql_time", 0)
        self.record_server_timing("sql_time", title, value)

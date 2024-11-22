from collections import defaultdict
from copy import copy

from asgiref.sync import sync_to_async
from django.db import connections
from django.urls import path
from django.utils.translation import gettext_lazy as _, ngettext

from debug_toolbar import settings as dt_settings
from debug_toolbar.forms import SignedDataForm
from debug_toolbar.panels import Panel
from debug_toolbar.panels.sql import views
from debug_toolbar.panels.sql.forms import SQLSelectForm
from debug_toolbar.panels.sql.tracking import wrap_cursor
from debug_toolbar.panels.sql.utils import (
    contrasting_color_generator,
    is_select_query,
    reformat_sql,
)
from debug_toolbar.utils import render_stacktrace


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


class MongoPanel(Panel):
    """
    Panel that displays information about the SQL queries run while processing
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
            "SQL queries from %(count)d connection",
            "SQL queries from %(count)d connections",
            count,
        ) % {"count": count}

    template = "debug_toolbar/panels/sql.html"

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

                trans_id = query.get("trans_id")
                prev_query = last_by_alias.get(alias, {})
                prev_trans_id = prev_query.get("trans_id")

                # annotate the queries as appropriate.
                if trans_id != prev_trans_id:
                    if prev_trans_id is not None:
                        prev_query["ends_trans"] = True
                    if trans_id is not None:
                        query["starts_trans"] = True
                if trans_id is not None:
                    query["in_trans"] = True

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
        title = "SQL {} queries".format(len(stats.get("queries", [])))
        value = stats.get("sql_time", 0)
        self.record_server_timing("sql_time", title, value)

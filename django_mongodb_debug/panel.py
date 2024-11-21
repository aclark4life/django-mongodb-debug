from debug_toolbar.panels import Panel

class MongoPanel(Panel):
    """"""
    title = "MongoDB"
    template = "django_mongodb_debug.html"

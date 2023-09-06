from data_diff.sqeleton.databases import mysql
from .base import DatadiffDialect


# TODO: Rebind the Dialects & Databases from Sqeleton and avoid unused code/class duplication.
class Dialect(mysql.Dialect, mysql.Mixin_MD5, mysql.Mixin_NormalizeValue, DatadiffDialect):
    pass


class MySQL(mysql.MySQL):
    dialect = Dialect()

""" The exceptions used by All Packages """

from __future__ import annotations
# from typing import TYPE_CHECKING
# import attr


"""
========================================================
============== Database Exceptions
========================================================
"""
class MongoDBConnectionError(Exception):
    """MongoDB Connection Error"""
    pass


"""
========================================================
============== Network Exceptions
========================================================
"""
class NetworkConnectionError(Exception):
    """Other Network Unreachable"""
    pass

class UrlNotFound(Exception):
    """Other Network Unreachable"""
    pass

class UrlBadGateway(Exception):
    """When an invalid formatted is encountered."""
    pass

class NameError(Exception):
    """When no name is specified."""
    pass

class FalseDivisionError(Exception):
    """Zero Division Error"""
    pass

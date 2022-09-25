#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Bernhard Hofer  -   Mail@Bernhard-hofer.at
#
# Baseclass for all kinds of database connectors
# ---------------------------------------------------------------------------
import json
from abc import ABC, abstractmethod


class Database(ABC):

    m_LastInsertedID: int = 0  # always stores the last inserted id

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def Execute(self):
        pass

    @abstractmethod
    def Insert(self, table: str, data: dict):
        pass

    @abstractmethod
    def GetLastInsertedID(self):
        pass

    @abstractmethod
    def Select(self):
        pass

    @abstractmethod
    def Update(self, table: str, data: dict, condition):
        pass

    @ classmethod
    def GetLastInsertedID(self):
        """
        Always returns the last inserted id of an INSERT
        :return: int: Last inserted id
        """
        if hasattr(self, "m_LastInsertedID"):
            return int(self.m_LastInsertedID)
        else:
            return 0

    @staticmethod
    def JsonToString(data: dict):
        """ prepare a dict to write it as json into the database """
        if type(data) != dict:
            raise ValueError("Value 'data' for 'JsonToString' method must be a 'dict'")

        return json.dumps(data)

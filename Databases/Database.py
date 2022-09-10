#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Bernhard Hofer  -   Mail@Bernhard-hofer.at
#
# Baseclass for our database connectors
# ---------------------------------------------------------------------------
from abc import ABC, abstractmethod

class Database(ABC):

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def Execute(self):
        pass

    @abstractmethod
    def Insert(self):
        pass

    @abstractmethod
    def GetLastInsertedID(self):
        pass

    @abstractmethod
    def Select(self):
        pass

    @abstractmethod
    def Update(self):
        pass

    @abstractmethod
    def JsonToString(self):
        pass
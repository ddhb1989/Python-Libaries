#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Bernhard Hofer  -   Mail@Bernhard-hofer.at
#
# MSSQL Database connector
# ---------------------------------------------------------------------------
import pymssql
import pymssql._mssql # needed for py2exe
from .Database import Database
from Libaries.Configuration import Configuration

class MSSQL(Database, Configuration):

    def __init__(self,
                 host: str = Configuration.MSSQL_Server,
                 user: str = Configuration.MSSQL_User,
                 password: str = Configuration.MSSQL_Password,
                 database: str = Configuration.MSSQL_Name,
                 as_dict: bool = True
                 ):
        """
        Try to connect to a MySQL Database
        :param host: DB Host
        :param user: DB User
        :param password: DB Password
        :param database: DB Name
        """
        self.m_Host: str = host
        self.m_User: str = user
        self.m_Password: str = password
        self.m_Database: str = database
        self.m_AsDict: bool = as_dict

        self.m_Connection: None = None
        self.m_Cursor: None = None

        self._TestConnection()

    def _TestConnection(self):
        """ Test if we can establish a connection """
        try:
            self._EstablishConnection()
        except Exception as err:
            print(err)
        else:
            self._CloseConnection()

    def _EstablishConnection(self):
        """ establish a connection to the database """
        self.m_Connection = pymssql.connect(server=self.m_Host,
                                            user=self.m_User,
                                            password=self.m_Password,
                                            database=self.m_Database,
                                            as_dict=self.m_AsDict)
        self.m_Cursor = self.m_Connection.cursor()

    def _CloseConnection(self):
        """ close connection """
        self.m_Cursor.close()
        self.m_Connection.close()

    def Execute(self, *args, **kwargs):
        """

        :param args: *args
        :param kwargs: **kwargs
        :return: cursor data
        """
        self._EstablishConnection()
        self.m_Cursor.execute(*args, **kwargs)
        _Data = self.m_Cursor
        self._CloseConnection()

        return _Data

    def Insert(self, table: str, data: dict) -> int:
        """
        INSERT INTO statement
        :param table: tablename
        :param data: dict[key] = value
        :return: last inserted id as integer
        """
        if type(data) != dict:
            raise ValueError("Value 'data' for INSERT INTO '{}' statement must be a 'dict'".format(table))

        # create insert into  statement
        _ColumnList, _ValueList = '', ''
        for i, key in enumerate(data):
            _ColumnList += "{}, ".format(str(key))
            _ValueList += "%({})s, ".format(key)

        _SQL = "INSERT INTO {} ({}) VALUES ({})".format(table, _ColumnList[:-2], _ValueList[:-2])
        print(_SQL)
        self._EstablishConnection()
        try:
            self.m_Cursor.execute(_SQL, data)
            self.m_Connection.commit()
            self.m_LastInsertedID = int(self.m_Cursor.lastrowid)
        except Exception as e:
            print(e)
        finally:
            self._CloseConnection()

        return self.m_LastInsertedID

    def Select(self, *args, **kwargs):
        """ simple select query that returns all selected rows """
        self._EstablishConnection()
        self.m_Cursor.execute(*args, **kwargs)
        _Data = self.m_Cursor.fetchall()
        self._CloseConnection()

        return _Data

    def Update(self, table: str, data: dict, condition):
        """
        UPDATE statement
        Args:
            table: tablename
            data: 'dict' key is columnname and value is the value
            condition: can be a 'dict' or a simple string condition
        """
        if type(data) != dict:
            raise ValueError("Value 'data' for the UPDATE '{}' statement must be a 'dict'".format(table))

        _UpdateList, _Condition = '', ''
        for key in data:
            print(key)
            _UpdateList += "[{}] = %s,".format(key)

        # condition can be a dict or a simple string statement
        if type(condition) == dict:
            for key in condition:
                _Condition += "[{}] = %s AND ".format(key)
            _Condition = _Condition[:-5]

            _Values = list(data.values())

            _Values.extend(list(condition.values()))
        else:
            _Condition = condition
            _Values = list(data.values())

        _SQL = "UPDATE [{}] SET {} WHERE ({})".format(table, _UpdateList[:-1], _Condition)
        print(_SQL)

        self._EstablishConnection()
        self.m_Cursor.execute(_SQL, tuple(_Values))
        self.m_Connection.commit()
        self._CloseConnection()

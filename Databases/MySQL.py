#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Bernhard Hofer  -   Mail@Bernhard-hofer.at
#
# MySQL Database connector
# ---------------------------------------------------------------------------
import mysql.connector
from .Database import Database


class MySQL(Database):

    def __init__(self, host: str, user: str, password: str, database: str):
        """
        Try to connect to a MySQL Database
        :param host: DB Host
        :param user: DB User
        :param password: DB Password
        :param database: DB Name
        """
        self.m_Host = host
        self.m_User = user
        self.m_Password = password
        self.m_Database = database

        self.m_Connection = None
        self.m_Cursor = None

        self._TestConnection()

    def _TestConnection(self):
        """ Test if we can establish a connection """
        try:
            self._EstablishConnection()
        except mysql.connector.Error as err:
            print(err)
        else:
            self._CloseConnection()

    def _EstablishConnection(self):
        """ establish a connection to the database """
        self.m_Connection = mysql.connector.connect(host=self.m_Host,
                                                    user=self.m_User,
                                                    password=self.m_Password,
                                                    database=self.m_Database)
        self.m_Cursor = self.m_Connection.cursor(buffered=True, dictionary=True)

    def _CloseConnection(self):
        """ close connection """
        self.m_Cursor.close()
        self.m_Connection.close()

    def Execute(self, *args, **kwargs):
        """
        simple execute statement
        Args:
            *args:
            **kwargs:

        Returns: cursor data
        """
        self._EstablishConnection()
        self.m_Cursor.execute(*args, **kwargs)
        _Data = self.m_Cursor
        self._CloseConnection()

        return _Data

    def Insert(self, table: str, data: dict):
        """
        INSERT INTO statement
        :param table: tablename
        :param data: dict[key] = value
        :return: last inserted id as integer
        """
        if type(data) != dict:
            raise ValueError("Value 'data' for the INSERT INTO '{}' statement must be a 'dict'".format(table))

        # create insert into  statement
        _ColumnList, _ValueList = '', ''
        for i, key in enumerate(data):
            _ColumnList += "`{}`, ".format(str(key))
            _ValueList += "%({})s, ".format(key)

        _SQL = "INSERT INTO `{}`.`{}` ({}) VALUES ({})".format(self.m_Database, table, _ColumnList[:-2], _ValueList[:-2])

        self._EstablishConnection()
        self.m_Cursor.execute(_SQL, data)
        self.m_Connection.commit()
        self._CloseConnection()

        # save last inserted id
        self.m_LastInsertedID = int(self.m_Cursor.lastrowid)

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
            _UpdateList += "`{}` = %s, ".format(key)

        # condition can be a dict or a simple string statement
        if type(condition) == dict:
            for key in condition:
                _Condition += "`{}` = %s, ".format(key)
            _Condition = _Condition[:-2]

            _Values = list(data.values())
            _Values.extend(list(condition.values()))
        else:
            _Condition = condition
            _Values = list(data.values())

        _SQL = "UPDATE `{}`.`{}` SET {} WHERE ({})".format(self.m_Database, table, _UpdateList[:-2], _Condition)

        self._EstablishConnection()
        self.m_Cursor.execute(_SQL, _Values)
        self.m_Connection.commit()
        self._CloseConnection()

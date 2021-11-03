#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import (
    Column,
    DATE,
    FLOAT,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    create_engine,
    delete,
    exc,
    select,
    Identity,
    insert,
    sql,
    text,
)
import datetime
import re


class DataBase:
    __dict_months = {
        'январь': 1, 'февраль': 2, 'март': 3,
        'апрель': 4, 'май': 5, 'июнь': 6,
        'июль': 7, 'август': 8, 'сентябрь': 9,
        'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
    }

    def __init__(self, dialect, driver, user, password, host, port, service):
        self.__db_path = (
            f'{dialect}+{driver}://{user}:{password}@{host}:{port}/{service}'
        )
        try:
            self.__engine = create_engine(
                self.__db_path, pool_size=15, pool_pre_ping=True
            )
            self.conn = self.__engine.connect()
            self.__metadata = MetaData(self.__engine)
        except exc.SQLAlchemyError as err:
            print(err)
        else:
            self.init_tables()

    def __check_date(self, date_chk, mask: str = '%d.%m.%Y'):
        date_chk = '.'.join(re.findall(r'\d+', date_chk))
        return datetime.datetime.strptime(date_chk, mask)

    def __check_group(self, group_chk):
        return re.findall(r'\w+', group_chk)[0].lower()

    def init_tables(self):
        self.tbl_costs = Table(
            'costs',
            self.__metadata,
            Column('id', Integer, Identity(start=1),
                   primary_key=True, nullable=False),
            Column('col_date', DATE, nullable=False),
            Column('col_group', String(100), nullable=False),
            Column('col_summ', FLOAT, nullable=False)
        )

        self.tbl_costs_limits = Table(
            'costs_limits',
            self.__metadata,
            Column('col_date', DATE),
            Column('col_group', String(100)),
            Column('col_limit', FLOAT),
            UniqueConstraint('col_date', 'col_group', 'col_limit')
        )

        self.__metadata.create_all(self.__engine)

    def __drop_tables(self):
        try:
            self.tbl_costs.drop()
            self.tbl_costs_limits.drop()
        except exc.SQLAlchemyError as err:
            print(err)

    def insert_data(self, tbl_name: str, value: list, chk_lmt=True):
        try:
            self.__metadata.reflect()
            tbl = self.__metadata.tables[tbl_name]

            transaction = self.conn.begin()
            query = insert(tbl)
            self.conn.execute(query, value)
        except exc.SQLAlchemyError as err:
            transaction.rollback()
            print(err)
        else:
            transaction.commit()
        finally:
            transaction.close()
        if chk_lmt and not transaction.is_active:
            try:
                if isinstance(value, dict):
                    self.check_limit(value['col_date'], value['col_group'])
                elif isinstance(value[0], dict):
                    for i in value:
                        self.check_limit(i['col_date'], i['col_group'])
            except TypeError as err:
                print(err)

    def insert_tbl_costs(self, value: list):
        if isinstance(value[0], (list)):
            value = [
                {
                    'col_date': self.__check_date(i[0]),
                    'col_group': self.__check_group(i[1]),
                    'col_summ': i[2]
                } for i in value
            ]
            self.insert_data('costs', value)
        else:
            self.insert_data(
                'costs',
                {
                    'col_date': self.__check_date(value[0]),
                    'col_group': self.__check_group(value[1]),
                    'col_summ': value[2]
                }
            )

    def select_data(self, tbl_name: str, col_name: list = None):
        try:
            self.__metadata.reflect()
            tbl = self.__metadata.tables[tbl_name]

            transaction = self.conn.begin()
            if col_name:
                col_lst = [
                    i for i in tbl.c
                    if re.findall(r'\w+.(\w+)', i.__str__())[0] in col_name
                ]
                res = select(col_lst)
            else:
                res = tbl.select()
            return self.conn.execute(res).fetchall()
        except exc.SQLAlchemyError as err:
            print(err)
            transaction.rollback()
        finally:
            transaction.close()

    def calc_costs(self, date: str):
        try:
            self.__metadata.reflect()
            tbl = self.__metadata.tables['costs']
            date = self.__check_date(date)

            transaction = self.conn.begin()
            responce = select(
                tbl.c.col_date,
                tbl.c.col_group,
                sql.func.sum(tbl.c.col_summ)
            ).where(tbl.c.col_date == date)\
            .group_by(tbl.c.col_group, tbl.c.col_date)

            responce = self.conn.execute(responce).fetchall()
        except exc.SQLAlchemyError as err:
            transaction.rollback()
            print(err)
        else:
            return [
                (
                    i[0].strftime('%d.%m.%Y'),
                    i[1],
                    f'{i[2]:,g}'.replace(',', ' ')
                ) for i in responce
            ]
        finally:
            transaction.close()

    def calc_costs_between(self, date_first: str, date_last: str):
        try:
            self.__metadata.reflect()
            tbl = self.__metadata.tables['costs']
            date_first = self.__check_date(date_first)
            date_last = self.__check_date(date_last)

            transaction = self.conn.begin()
            responce = select(
                tbl.c.col_group,
                sql.func.sum(tbl.c.col_summ)
            ).where(tbl.c.col_date.between(date_first, date_last))\
            .group_by(tbl.c.col_group)
            responce = self.conn.execute(responce).fetchall()
        except exc.SQLAlchemyError as err:
            transaction.rollback()
            print(err)
        else:
            return [
                (
                    i[0],
                    f'{i[1]:,g}'.replace(',', ' ')
                ) for i in responce
            ]
        finally:
            transaction.close()

    def del_last_rec(self):
        try:
            transaction = self.conn.begin()
            subquery = select(sql.func.max(self.tbl_costs.c.id))
            delete_ = delete(self.tbl_costs).where(
                self.tbl_costs.c.id == subquery.scalar_subquery()
            )
            self.conn.execute(delete_)
            print('Последняя запись удалена')
        except exc.SQLAlchemyError as err:
            print(err)
            transaction.rollback()
        else:
            transaction.commit()
        finally:
            transaction.close()

    def set_limit(self, month_year: str, group: str, summ: int or float):
        """month_year передать в виде 'январь 2000'"""
        month_year = re.findall(r'[а-яА-Я0-9]+', month_year, re.UNICODE)
        month_year = (
            f'1.{self.__dict_months[month_year[0].lower()]}.'
            f'{month_year[1]}'
        )
        format_month_year = datetime.datetime.strptime(month_year, '%d.%m.%Y')
        group = self.__check_group(group)
        self.insert_data(
            'costs_limits',
            {
                'col_date': format_month_year,
                'col_group': group,
                'col_limit': summ
            }, chk_lmt=False
        )

    def check_limit(self, month: 'datetime', group: str):
        try:
            transaction = self.conn.begin()

            query = select(sql.func.sum(self.tbl_costs.c.col_summ)).where(
                text(
                    " TRUNC(col_date, 'MONTH') ="
                    " TRUNC(:col_date, 'MONTH')"
                    " AND col_group = :col_group"
                )
            ).group_by(text("TRUNC(col_date, 'MONTH'), col_group"))
            sum_costs = self.conn.execute(
                query,
                col_date=month,
                col_group=group
            ).fetchone()[0]

            query2 = select([self.tbl_costs_limits.c.col_limit]).where(
                text(
                    "col_date = TRUNC(:col_date, 'MONTH')"
                    "AND col_group = :col_group"
                )
            )

            query3 = query.union(query2)
            responce = self.conn.execute(
                query3,
                col_date=month,
                col_group=group
            ).fetchall()

            if responce[0][0] and (responce[1][0] > responce[0][0]):
                print(
                    f"Вы вышли за пределы лимита в {responce[0][0]:,g} рублей"
                    .replace(',', ' ')
            )
        except exc.SQLAlchemyError as er:
            print(er)
            transaction.rollback()
        except (IndexError, TypeError):
            pass
        finally:
            transaction.close()


if __name__ == '__main__':
    from config_db import config_db
    db = DataBase(**config_db)

import mysql.connector
import logging
import os
import pandas as pd
import csv


logging.basicConfig(filename='pz.log', filemode='w', level=logging.INFO)

db_config = {
        'user': 'j30084097_13418',
        'password': 'pPS090207/()',
        'host': 'srv221-h-st.jino.ru',
        'database': 'j30084097_13418'
    }

class SQLTable:
    def __init__(self, db_config, table_name):
        self.db_config = db_config
        self.table_name = table_name
        self.connection = None
        self.cursor = None
        self.columns = None
        self.connect()

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**db_config)
            self.cursor = self.connection.cursor()
            if self.connection.is_connected():
                logging.info(f"Подключение к таблице {self.table_name} установлено")
                return  self.cursor
        except Exception as e:
            logging.error(f"Ошибка подключения: {e}")

    def update_columns(self):
        query = f"SHOW COLUMNS FROM {self.table_name}"
        self.cursor.execute(query)
        self.columns = [row[0] for row in self.cursor.fetchall()]

    def create_table(self, columns):
        column_definition = ', '.join(f"`{name}` {type}" for name, type in columns.items())
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            {column_definition}
        )
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
        except Exception as e:
            logging.error(f"Не удалось создать таблицу: {e}")
        finally:
            cursor.close()
        self.update_columns()
        logging.info(f"Создана таблица '{self.table_name}' со столбцами {self.columns}.")
        print(f"Создана таблица '{self.table_name}'")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logging.info("Соединение закрыто")

    def insert(self, values, columns_name):
        query = f"INSERT INTO {self.table_name} {columns_name} VALUES {values}"
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
        except Exception as e:
            logging.error(f"Не добавить строку: {e}")
        finally:
            cursor.close()
        logging.info(f'Добавлена строка в таблицу {self.table_name} с данными: {values}')
        print(f'Добавлена строка в таблицу {self.table_name} с данными: {values}')

    def delete(self, id):
        primary_key = self.find_primary_key()
        if not primary_key: return False
        if self.check_PK(primary_key, id) == False: return False
        query = f"DELETE FROM {self.table_name} WHERE `{primary_key}` = %s"
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, (id,))
            self.connection.commit()
        except Exception as e:
            logging.error(f"Не удалить строку: {e}")
        finally:
            cursor.close()
        logging.info(f'Удалена строка с id {id}')
        print(f'Удалена строка с id {id}')

    def check_PK(self, primary_key, id):
        cursor = self.connection.cursor()
        try:
            check_query = f"SELECT COUNT(*) FROM {self.table_name} WHERE `{primary_key}` = %s"
            cursor.execute(check_query, (id,))
            count = cursor.fetchone()[0]
            if count == 0:
                logging.error(f"Запись с ID {id} не существует в таблице {self.table_name}")
                return False
        finally:
            cursor.close()

    def update_column(self, id, column_name, new_value):
        primary_key = self.find_primary_key()
        if not primary_key: return False
        if self.check_PK(primary_key, id) == False: return False
        query = f"UPDATE {self.table_name} SET `{column_name}` = %s WHERE `{primary_key}` = %s"
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, (new_value, id))
            self.connection.commit()
        except Exception as e:
            print(f"Не удалось обновить строку: {e}")
            return False
        finally:
            cursor.close()
        logging.info(f'Изменено значение {column_name} c id {id} на {new_value}')
        print(f'Изменено значение {column_name} c id {id} на {new_value}')

    def drop_table(self):
        cursor = self.connection.cursor()
        try:
            query = f"DROP TABLE IF EXISTS {self.table_name}"
            cursor.execute(query)
            self.connection.commit()

        except Exception as e:
            logging.error(f"Не удалось удалить таблицу: {e}")

        finally:
            cursor.close()
        logging.info(f"Таблица '{self.table_name}' была удалена.")
        print(f"Таблица '{self.table_name}' была удалена.")

    def find_primary_key(self):
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"SHOW KEYS FROM {self.table_name} WHERE Key_name = 'PRIMARY'")
            result = cursor.fetchone()
            if result:
                return result[4]
        finally:
            cursor.close()
        return None

    def column_sorted(self, column_name, order):
        cursor = self.connection.cursor()
        try:
            ord = "DESC" if order else "ASC"
            query = f"SELECT {column_name} FROM {self.table_name} ORDER BY {column_name} {ord}"
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            print(f"Столбец {column_name} отсортирован:")
            for row in result:
                print(row[0])
        except Exception as e:
            logging.error(f"Не удалось вывести отсортированный столбец: {e}")
            return False
        finally:
            cursor.close()
        logging.info(f"Столбец {column_name} отсортирован")

    def get_rows(self, start_id, end_id):
        cursor = self.connection.cursor()
        primary_key = self.find_primary_key()
        if not primary_key: return False
        if self.check_PK(primary_key, start_id) == False: return False
        if self.check_PK(primary_key, end_id) == False: return False
        try:
            query = f"SELECT * FROM {self.table_name} WHERE id BETWEEN %s AND %s"
            self.cursor.execute(query, (start_id, end_id))
            result = self.cursor.fetchall()
            print(f"Строки с ID от {start_id} до {end_id}:")
            for r in result:
                print(r)
        except Exception as e:
            logging.error(f"Не удалось вывести строки: {e}")
            return False
        finally:
            cursor.close()
        logging.info(f"Выведены строки с ID от {start_id} до {end_id}")

    def delete_rows(self, start_id, end_id):
        cursor = self.connection.cursor()
        primary_key = self.find_primary_key()
        if not primary_key: return False
        if self.check_PK(primary_key, start_id) == False: return False
        if self.check_PK(primary_key, end_id) == False: return False
        try:
            query = f"DELETE FROM {self.table_name} WHERE id BETWEEN %s AND %s"
            self.cursor.execute(query, (start_id, end_id))
            self.connection.commit()
            print(f"Удалены строки с ID от {start_id} до {end_id}")
        except Exception as e:
            logging.error(f"Не удалось удалить таблицу: {e}")
            return False
        finally:
            cursor.close()
        logging.info(f"Удалены строки с ID от {start_id} до {end_id}")

    def structure(self):
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"DESCRIBE {self.table_name}")
            columns = cursor.fetchall()
            for c in columns:
                print(f"{c[0]} ({c[1]})")
        except Exception as e:
            logging.error(f"Не удалось вывести описание таблицы: {e}")
            return False
        finally:
            cursor.close()
        logging.info(f'Выведено описание таблицы {self.table_name}')

    def find_value(self, column_name, value):
        cursor = self.connection.cursor()
        try:
            query = f"SELECT * FROM {self.table_name} WHERE {column_name} = %s"
            self.cursor.execute(query, (value,))
            result = self.cursor.fetchall()
            print(f"Строки со значением '{value}' в столбце {column_name}:")
            for r in result:
                print(r)
        except Exception as e:
            logging.error(f"Не удалось найти строки: {e}")
            return False
        finally:
            cursor.close()
        logging.info(f'Найдены строки со значением {value}')

    def add_column(self, column_name, type):
        cursor = self.connection.cursor()
        try:
            query = f"ALTER TABLE `{self.table_name}` ADD COLUMN `{column_name}` {type}"
            logging.info(query)
            self.cursor.execute(query)
            self.connection.commit()
            print(f"Столбец {column_name} типа {type} добавлен в таблицу {self.table_name}")
        except Exception as e:
            logging.error(f"Не удалось добавить столбец: {e}")
            return False
        finally:
            cursor.close()
        logging.info(f"Столбец {column_name} типа {type} добавлен в таблицу {self.table_name}")

    def delete_column(self, column_name):
        cursor = self.connection.cursor()
        try:
            query = f"ALTER TABLE {self.table_name} DROP COLUMN `{column_name}`"
            cursor.execute(query)
            self.connection.commit()
            print(f"Столбец {column_name} удален из таблицы {self.table_name}")
        except Exception as e:
            logging.error(f"Не удалось удалить столбец: {e}")
            return False
        finally:
            cursor.close()
        logging.info(f"Столбец {column_name} удален из таблицы {self.table_name}")

    def export_to_csv(self, fname):
        df = self.fetch_all()
        downloads_path = os.path.join(os.path.expanduser('~'), 'Downloads')
        if not os.path.exists(downloads_path):
            os.makedirs(downloads_path)
        file_name = f"{fname}.csv"
        file_path = os.path.join(downloads_path, file_name)
        df.to_csv(file_path, index=False)
        print(f"Таблица экспортирована в {file_path}")
        logging.info(f"Таблица экспортирована в {file_path}")

    def fetch_all(self):
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"SELECT * FROM {self.table_name}")
            rows = cursor.fetchall()
            column_names = [c[0] for c in cursor.description]
        finally:
            cursor.close()
        return pd.DataFrame(rows, columns=column_names)

    def import_csv(self, file_path, table_name):
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            columns = next(reader)
            data = [row for row in reader if len(row) == len(columns)]

        cursor = self.connection.cursor()
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        cursor.executemany(sql, data)
        self.connection.commit()
        cursor.close()
        print(f"Импортировано {len(data)} строк")
        logging.info(f"Импортировано {len(data)} строк")


db = SQLTable(db_config, 'kair')
db.connect()
db.close()

import os
import re
import sqlite3
from pathlib import Path
from typing import List

from fire import Fire


class Cli:
    def process(
        self,
        path: str,
        save_path: str = "./sq.db",
        recursive_search: bool = False,
        force: bool = False,
    ):
        """Converts csv to sqlite database.

        Args:
            path (str): path where csv file stored or any csv in directory if
                recursive search enabled ('-r' key).
            save_path (str): path to database, default is './sq.db'.
            recursive_search (bool): Recursive search option. Search for csv in
                directory and children directories. Any table will be created
                in same sqlite database.
            force (bool): Forces database recreation.

        Raises:
            FileExistsError: Sqlite database already exists.

        """
        path = Path(path)

        db_path = Path(save_path)
        if db_path.exists():
            if force:
                os.remove(db_path)
            else:
                raise FileExistsError(
                    f"Database on {save_to} already exists. Try '-f' argument"
                )
        db_path.parent.mkdir(exist_ok=True, parents=True)

        con = sqlite3.connect(save_path)
        self._write_csv(path, con)
        con.commit()
        con.close()

    def _write_csv(self, csv_path, con):
        table_name = csv_path.name.replace(".", "_")
        sep = ", "
        cur = con.cursor()
        lines = []
        with open(csv_path, "r") as file:
            header = file.readline()
            header = self._csv_row(header)
            cur.execute(f"CREATE TABLE IF NOT EXISTS '{table_name}' ({sep.join(header)});")
            line = file.readline()
            while line:
                lines.append(self._csv_row(line))
                line = file.readline()
        cur.executemany(
            f"INSERT INTO '{table_name}' ({sep.join(header)}) VALUES ({sep.join(['?']*len(header))});",
            lines,
        )

    @staticmethod
    def _csv_row(row):
        item = row.strip() + ","
        matches = re.findall(r'("(.*?)",|(.*?),)', item)
        matches = [(m[1] or m[2]) for m in matches]
        return matches

    def interactive(self, database: str):
        """Launches an interactive shell for a quick view of the data.

        Args:
            database (str): Sqlite database path.
        """
        con = sqlite3.connect(database)
        print("Wellcome to interactive mode!")
        print("ctrl+C to exit")
        print("commands: list exit")
        while True:
            cur = con.cursor()
            code = ""
            while True:
                try:
                    code += input(".. " if code else ">> ")
                    if ";" in code:
                        break
                    elif code == "exit":
                        exit()
                    elif code == "list":
                        result = cur.execute("SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';").fetchall()
                        self._sql_print(result)
                        code = ""
                    else:
                        code += "\n"
                except KeyboardInterrupt:
                    print("Interrupted! (type 'exit' to exit interactive mode)")
                    code = ""
            result = cur.execute(code).fetchall()
            self._sql_print(result)
        con.commit()
        con.close()
    
    @staticmethod
    def _sql_print(data):
        sep = " | "
        cell_size = 80 // len(data[0])
        print("\n".join(["| " + sep.join([str(s).ljust(cell_size) for s in row]) + " |" for row in data]))


def main():
    Fire(Cli)

import pandas as pd
import sqlite3


class Conn():
	def __init__(self, db_path:str) -> None:
		self.db_path = db_path

	def 
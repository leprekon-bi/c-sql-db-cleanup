import pyodbc
import pandas as pd
import re
from datetime import datetime
from utils.config import settings
from dateutil.relativedelta import relativedelta
import warnings
import logging
import os

class Table:
    def __init__(self, database, name, columns):
        self.database = database
        self.name = name
        self.columns = columns

        if '_Version' in self.columns:
            self.columns.remove('_Version')
        self.columns_joined = ", ".join(self.columns)

        self.TEMP_PREFIX = "tmp_cleanup"

    def __repr__(self):
        return f"Table(name={self.name}, columns={self.columns})"
    
    @property
    def where_statement(self):
        return "WHERE 1=1"
    
    @property
    def record_count(self):
        query = f"SELECT COUNT(*) FROM [dbo].{self.name}"
        with self.database.connection.cursor() as cursor:
            cursor.execute(query)
            count = cursor.fetchone()[0]
            return count

    @property  
    def where_record_count(self):
        query = f"SELECT COUNT(*) FROM [dbo].{self.name} {self.where_statement}"
        with self.database.connection.cursor() as cursor:
            cursor.execute(query)
            count = cursor.fetchone()[0]
            return count
    
    @property
    def cleanup_query(self):
        return f"DROP TABLE IF EXISTS dbo.{self.name}_{self.TEMP_PREFIX}"

    @property
    def select_query(self):
        
        return f"""
            SELECT * INTO [dbo].{self.name}_{self.TEMP_PREFIX}
            FROM [dbo].{self.name}
            {self.where_statement}
        """

    @property
    def truncate_query(self):
        return f"TRUNCATE TABLE [dbo].{self.name}"
    
    @property
    def insert_query(self):
        return f"""
            INSERT INTO [dbo].{self.name} ({self.columns_joined}) 
            SELECT {self.columns_joined} FROM [dbo].{self.name}_{self.TEMP_PREFIX}
        """
    
    @property
    def start_date(self):
        date = datetime.strptime(settings.START_DATE, "%Y%m%d")
        date_shifted = date + relativedelta(years=self.database.offset)
        return date_shifted.strftime("%d.%m.%Y")

    
class Document(Table):
    def __init__(self, database, name, columns, subtables = None, is_subtable = False):
        super().__init__(database, name, columns)
        self.subtables = subtables if subtables else []
        self.is_subtable = is_subtable

    def __repr__(self):
        if self.is_subtable:
            return f"Subtable(name={self.name})"
        return f"Document(name={self.name}, subtables={self.subtables})"
    
    @property
    def where_statement(self):
        
        base_date_filter = f"WHERE _Date_Time >= '{self.start_date}' AND _Marked = 0"

        if self.is_subtable:
            parent_doc = self.name.split('_VT')[0]
            return f"WHERE {parent_doc}_IDRRef IN (SELECT _IDRRef FROM [dbo].{parent_doc} {base_date_filter})"
        else:
            return base_date_filter
    

class RegisterTotal(Table):
    @property
    def where_statement(self):
        date = datetime.now().date()
        date_shifted = date + relativedelta(years=self.database.offset + 100)
        return f"WHERE _Period > '{date_shifted.strftime('%d.%m.%Y')}'"

class Sequence(Table):
    @property
    def where_statement(self):
        return "WHERE 1<>1"  # No filtering, just a placeholder for consistency

class Register(Table):
    
    @property
    def registers_columns(self):
        options = [
                    ('_RecorderTRef', '_RecorderRRef'),
                    ('_Recorder_RTRef', '_Recorder_RRRef'),
                    ('_DocumentTRef', '_DocumentRRef'),
                  ]
        
        for option in options:
            if option[0] in self.columns and option[1] in self.columns:
                return option
        
        return None
    
    @staticmethod
    def get_reference_table(hex_string):
        num_repr = str(int(hex_string, 16))
        return f"_Document{num_repr}"
    
    @property
    def full_truncate(self):
        if self.registers_columns:
            return False
        return True
         
    @property
    def where_statement(self):
        if self.full_truncate or self.record_count == 0:
            return "WHERE 1=1"
        
        table_column, reference_column = self.registers_columns
        table_column_encoded = f"convert(varchar(16), {table_column}, 2)"

        df_reference_tables = pd.read_sql_query(f"""SELECT 
                                                        {table_column_encoded} as ref_tab
                                                        , count(1) as rows 
                                                    FROM [dbo].{self.name} 
                                                    GROUP BY {table_column_encoded}""", self.database.connection)
        
        reference_tables = [table for table in df_reference_tables['ref_tab']]

        query_start =f"WHERE concat({table_column_encoded},'|', convert(char(36), cast({reference_column} as uniqueidentifier))) IN ( \n\n"

        table_filters = []
        is_first_union = True

        for ref_table in reference_tables:

            document = self.get_reference_table(ref_table)
            document_query = f"SELECT concat('{ref_table}','|', convert(char(36), cast(_IDRRef as uniqueidentifier))) FROM [dbo].{document} WHERE _Date_Time >= '{self.start_date}' \n"

            if is_first_union:
                table_filters.append(f"{document_query} \n")
                is_first_union = False    
            else:
                table_filters.append(f"UNION ALL {document_query} \n")

        return query_start + "".join(table_filters) + ")"
    
    def __repr__(self):
        
        if self.full_truncate:
            return f"Register(name={self.name}, full_truncate={self.full_truncate})"

        return f"Register(name={self.name}, Refs={self.registers_columns})"


class Database:

    def __init__(self, settings = settings):

        try:
            self.connection = pyodbc.connect(
                'DRIVER=' + settings.DRIVER + ';SERVER=' + settings.DB_HOST +
                ';DATABASE=' + settings.DB_NAME +
                ';UID=' + settings.USERNAME +
                ';PWD=' + settings.PASSWORD
                , autocommit=True
            )

        except pyodbc.Error as e:
            print("Error in connection:", e)

    @staticmethod
    def is_document(table_name):
        if table_name.startswith('_Document'):
            if 'Journal' in table_name or 'Chng' in table_name:
                return False
            else:
                return True
        return False

    @staticmethod
    def is_register_total(table_name):
        prefixes = ['_AccRgAT', '_AccRgCT', '_AccumRgT', '_AccumRgTn']
        for prefix in prefixes:
            pattern = rf"^{re.escape(prefix)}[0-9]+$"
            if bool(re.fullmatch(pattern, table_name)):
                return True
        
        return False

    @staticmethod   
    def is_register(table_name):
        prefixes = ['_AccRg', '_AccRgED', '_AccumRg', '_CRg', '_CRgAct', '_CRgRecalc', '_InfoRg']
        for prefix in prefixes:
            pattern = rf"^{re.escape(prefix)}[0-9]+$"
            if bool(re.fullmatch(pattern, table_name)):
                return True
            
        if table_name.startswith('_DocumentJournal'):
            return True
        
        return False

    @staticmethod
    def is_sequence(table_name):
        prefixes = ['_Seq']
        for prefix in prefixes:
            pattern = rf"^{re.escape(prefix)}[0-9]+$"
            if bool(re.fullmatch(pattern, table_name)):
                return True
        
        return False

    
    def parse_configuration(self):
        
        df_tables = pd.read_sql_query("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME", self.connection)
        df_columns = pd.read_sql_query("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS", self.connection)

        table_list = df_tables['TABLE_NAME'].tolist()
        data_structure = df_columns[['TABLE_NAME', 'COLUMN_NAME']].groupby('TABLE_NAME')['COLUMN_NAME'].agg(list).to_dict()

        # Documents parsing as they have nested subtables
        self.documents = []
        document_list = [table for table in df_tables['TABLE_NAME'] if self.is_document(table)]

        docs = {}
        for document in document_list:
            if '_VT' not in document:
                docs[document] = {"name": document, "subtables": []}

        for document in document_list:
            if '_VT' in document:
                parts = document.split('_VT')
                docs[parts[0]]["subtables"].append(document)


        for parent in docs.keys():
            if docs[parent]["subtables"]:
                subdocuments = [Document(self, subtable, data_structure[subtable], is_subtable=True) for subtable in docs[parent]["subtables"]]
                self.documents.append(Document(self, parent, data_structure[parent], subtables=subdocuments, is_subtable=False))

            else:
                self.documents.append(Document(self, parent, data_structure[parent], is_subtable=False))
        
        # Rest objects parsing
        self.register_totals = [RegisterTotal(self, table, data_structure[table]) for table in table_list if self.is_register_total(table)]
        self.registers = [Register(self, table, data_structure[table]) for table in table_list if self.is_register(table)]
        self.sequences = [Sequence(self, table, data_structure[table]) for table in table_list if self.is_sequence(table)]

    @property
    def offset(self):
        df_offset = pd.read_sql_query(f"SELECT Offset FROM [dbo]._YearOffset ", self.connection)
        return df_offset['Offset'][0]


class QueryProcessor:

    def __init__(self, connection,  logger, dry_run=True):
        self.connection = connection
        self.logger = logger
        self.dry_run = dry_run

    def execute_query(self, query):

        if self.dry_run:
            self.logger.info(f"Running query (dry run) ...")
            self.logger.debug(f"{query}")

        elif not self.dry_run:
            self.logger.debug(f"Running query ...")
            self.logger.debug(f"{query}")
            cursor = self.connection.cursor()
            cursor.execute(query)
            cursor.close()
        
        else:
            self.logger.error("Unknown dry run mode, please check the settings.")

        pass
        
    def process_table(self, table):

        self.logger.info("-"*80)
        self.logger.info(f"Processing table: {table.name}")

        if isinstance(table, Document):
            if table.subtables:
                self.logger.info("Processing subtables ... ")
                for subtable in table.subtables:
                    self.process_table(subtable)
                self.logger.info("Subtables completed ... ")
                self.logger.info("-"*80)

        total_records, where_records = table.record_count, table.where_record_count
        self.logger.info(f"Total records: {total_records}, Filtered records: {where_records}")

        if total_records == 0:
                self.logger.info(f"Table {table.name} is empty, skipping ...")
                return

        if where_records == 0:

            self.logger.info(f"Table {table.name} has no filtered records, executing truncate cleanup only ...")
            self.execute_query(table.truncate_query)
            return

        self.logger.info(f"Cleanup for {table.name}_{table.TEMP_PREFIX} ...")
        self.execute_query(table.cleanup_query)

        self.logger.info(f"Selecting from {table.name} ---> {table.name}_{table.TEMP_PREFIX} ...")
        self.execute_query(table.select_query)

        self.logger.info(f"Truncating {table.name} ...")
        self.execute_query(table.truncate_query)

        self.logger.info(f"Inserting from {table.name}_{table.TEMP_PREFIX} ---> {table.name} ...")
        self.execute_query(table.insert_query)

        self.logger.info(f"Executing final cleanup for {table.name}_{table.TEMP_PREFIX} ...")
        self.execute_query(table.cleanup_query)


warnings.filterwarnings('ignore')
logger = logging.getLogger("PRICE")
logger.setLevel(logging.DEBUG)

current_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

if not os.path.exists("logs"):
    os.makedirs("logs")

file_handler = logging.FileHandler(f"logs//{current_ts}.log", encoding="UTF-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logger.info("Starting cleanup process ...")

db = Database()
logger.info(f"Database connection established. DB = {db.connection.getinfo(pyodbc.SQL_DATABASE_NAME)}")

db.parse_configuration()
logger.info("Database configuration parsed.")
logger.debug("-"*80)
logger.debug(f"Registry list: {[reg.name for reg in db.registers]}")
logger.debug("-"*80)
logger.debug(f"Registry totas list: {[total.name for total in db.register_totals]}")
logger.debug("-"*80)
logger.debug(f"Documents: {[doc.name for doc in db.documents]}")
logger.debug("-"*80)
logger.debug(f"Sequences: {[seq.name for seq in db.sequences]}")
logger.debug("-"*80)

print("All is set up to clean up the database.")
print(settings.DRY_RUN)

if settings.DRY_RUN:
    print("Dry run mode is enabled. NO changes will be made to the database.")
else:
    print("!!! Dry run mode is disabled !!! Changes will be made to the database.")

print("Press 'Y' to continue or any other key to exit.")
if input().strip().upper() != 'Y':
    print("Exiting without changes.")
    exit(0)

processor = QueryProcessor(db.connection, logger, dry_run=settings.DRY_RUN)

# Processing registers
logger.info("Processing registers ...")
for reg in db.registers:
    processor.process_table(reg)
logger.info("Processing registers completed.")

# Processing registers totals
logger.info("Processing registers totals ...")
for total in db.register_totals:
    processor.process_table(total)
logger.info("Processing registers totals completed.")

# Processing documents
logger.info("Processing documents ...")
for doc in db.documents:
    processor.process_table(doc)
logger.info("Processing documents completed.")

# Processing sequences
for seq in db.sequences:
    processor.process_table(seq)
logger.info("Processing sequences completed.")

logger.info("CLEANUP PROCESS COMPLETED !!!")

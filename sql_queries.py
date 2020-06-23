from pyspark.sql import SQLContext


class SQLQueries:
    def __init__(self, sparkContext, data):
        self.sqlContext = SQLContext(sparkContext)
        self.dataFrame = self.sqlContext.createDataFrame(data)
        self.dataFrame.createOrReplaceTempView('invoices')

    def get_lines(self):
        lines = self.sqlContext.sql('SELECT lines FROM invoices').collect()
        return lines

    def get_incomes(self):
        incomes = self.sqlContext.sql("SELECT lines FROM invoices WHERE issuer LIKE '%BRICOMAN%'").collect()
        return incomes

    def get_invoices_by_date(self, date):
        invoices = self.sqlContext.sql("SELECT * FROM invoices WHERE date LIKE '%{}%'".format(date)).collect()
        return invoices

    def get_invoices(self):
        invoices = self.sqlContext.sql("SELECT * FROM invoices").collect()
        return invoices




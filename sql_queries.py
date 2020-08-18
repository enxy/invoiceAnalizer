from pyspark.sql import SQLContext
import collections

class SQLQueries:
    def __init__(self, sparkContext, data):
        self.sqlContext = SQLContext(sparkContext)
        self.dataFrame = self.sqlContext.createDataFrame(data)
        self.dataFrame.createOrReplaceTempView('invoices')
        self.linesFrame = self.sqlContext.createDataFrame(self.prepare_linesFrame_data())
        self.linesFrame.createOrReplaceTempView('lines')

    def prepare_linesFrame_data(self):
        data = []
        lines = self.sqlContext.sql('SELECT lines FROM invoices').collect()
        invoice_lines = [ row['lines'] for row in lines ]

        for line in invoice_lines:
            for pos in line:
                data.append(pos)
        return data

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

    def get_products_by_qty_sold(self):
        res = self.sqlContext.sql("SELECT description, SUM(qty) FROM (SELECT lines FROM invoices) GROUP BY description").collect()
        return res

    def get_products_by_qty(self):
        descriptions = []
        qties = []

        data = sqlContext.sql('SELECT description, SUM(qty) FROM lines GROUP BY description').collect()
        items = [ [item['description'], item['sum(CAST(qty AS DOUBLE))']] for item in data ]
        for item in items:
            descriptions.append(item[0])
            qties.append(int(item[1]))
        
        return descriptions, qties

    def get_products_by_unit_price(self):
        descriptions = []
        unit_prices = []
        total_net = 0

        data = sqlContext.sql('SELECT description, SUM(qty*unit_price) FROM lines GROUP BY description').collect()
        items = [ [item['description'], item['sum((CAST(qty AS DOUBLE) * CAST(unit_price AS DOUBLE)))']] for item in data ]
        
        for item in items:
            descriptions.append(item[0] + ' : '+ str(round(item[1], 2)))
            unit_prices.append(round(item[1], 2))
        
        total_net += round(sum(unit_prices), 2)
        percentage = list( map(lambda x: round(x / total_net, 4), unit_prices) )

        return descriptions, percentage, total_net

    def get_purchaser_statistics(self):
        info = collections.defaultdict(list)
        data = sqlContext.sql('SELECT purchaser, lines FROM invoices').collect()

        for purchaser, lines in data:
            suma = qty = 0
            purchasers = []
            values = []

            for line in lines:
                suma += float(line['net_value'])
                qty += float(line['qty'])

            if purchaser not in info:
                info[purchaser] = {'x': int(qty), 'y': int(suma), 'r': 10}
            else:
                info[purchaser]['x'] += int(qty)
                info[purchaser]['y'] += int(suma)
                
        for key, value in info.items():
            purchasers.append(key)
            values.append(value)

        return purchasers, values
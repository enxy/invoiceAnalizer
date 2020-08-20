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
        lines = self.sqlContext.sql('SELECT id, lines FROM invoices').collect()

        for row in lines:
            for line in row['lines']:
                line['id'] = row['id']
                data.append(line)

        return data

    def get_invoices_by_date(self, date):
        invoices = self.sqlContext.sql("SELECT * FROM invoices WHERE date LIKE '%{}%'".format(date)).collect()
        return invoices

    def get_invoices(self):
        invoices = self.sqlContext.sql("SELECT * FROM invoices").collect()
        return invoices


    def get_summarization(self, period, user):
        result = {}
        # {'incomes': 0, 'expenditures': 4386.66, 'tax_paid': 820.27, 'discount_received': 45.5, 'discount_given': 0}
        """ Summarize overall incomes/expenditures """
        if period:
            issuerQuery = "SELECT id FROM invoices WHERE issuer LIKE '%{}%' AND date LIKE '%{}%'".format(user, period)
            purchaserQuery = "SELECT id FROM invoices WHERE purchaser LIKE '%{}%' AND date LIKE '%{}%'".format(user, period)
        else:
            issuerQuery = "SELECT id FROM invoices WHERE issuer LIKE '%{}%'".format(user)
            purchaserQuery = "SELECT id FROM invoices WHERE purchaser LIKE '%{}%'".format(user)

        incomesQ = "SELECT SUM(net_value) FROM lines WHERE id IN ({})".format(issuerQuery)
        expendituresQ = "SELECT SUM(gross_value) FROM lines WHERE id IN ({})".format(purchaserQuery)
        income = self.sqlContext.sql(incomesQ).first()[0]
        expend = self.sqlContext.sql(expendituresQ).first()[0]

        result['incomes'] = round(income, 2) if income else 0
        result['expenditures'] = round(expend, 2) if expend else 0

        """ Summarize tax paid """
        taxQ = "SELECT SUM(tax_rate * net_value) FROM lines WHERE id IN ({})".format(purchaserQuery)
        tax_paid = self.sqlContext.sql(taxQ).first()[0]
        result['tax_paid'] = round(tax_paid, 2) if tax_paid else 0

        """ Summarize discounts received/given """
        received = "SELECT SUM(net_value * discount) FROM lines WHERE id IN ({}) AND discount IS NOT NULL".format(purchaserQuery)
        total = self.sqlContext.sql(received).first()[0]
        result['discount_received'] = round(total, 2) if total else 0

        given = "SELECT SUM(net_value * discount) FROM lines WHERE id IN ({}) AND discount IS NOT NULL".format(issuerQuery)
        total = self.sqlContext.sql(given).first()[0]
        result['discount_given'] = round(total, 2) if total else 0

        return result

    def get_products_by_qty(self, period):
        descriptions, qties = [], []
        values = collections.defaultdict(float)

        if period:
            subquery = "SELECT id FROM invoices WHERE date LIKE '%{}%'".format(period)
            query = "SELECT description, SUM(qty) FROM lines WHERE id IN ({}) GROUP BY description".format(subquery)
            data = sqlContext.sql(query).collect()
        else:
            data = sqlContext.sql('SELECT description, SUM(qty) FROM lines GROUP BY description').collect()

        items = [ [item['description'], item['sum(CAST(qty AS DOUBLE))']] for item in data ]

        for item in items:
            descriptions.append(item[0])
            qties.append(int(item[1]))
        
        return descriptions, qties

    def get_products_by_unit_price(self, period):
        descriptions, unit_prices = [], []

        if period:
            subquery = "SELECT id FROM invoices WHERE date LIKE '%{}%'".format(period)
            query = "SELECT description, SUM(qty*unit_price) FROM lines WHERE id IN ({}) GROUP BY description".format(subquery)
            data = sqlContext.sql(query).collect()
        else:
            data = sqlContext.sql('SELECT description, SUM(qty*unit_price) FROM lines GROUP BY description').collect()

        items = [ [item['description'], item['sum((CAST(qty AS DOUBLE) * CAST(unit_price AS DOUBLE)))']] for item in data ]

        for item in items:
            descriptions.append(item[0] + ' : '+ str(round(item[1], 2)))
            unit_prices.append(round(item[1], 2))
        
        total_net = round(sum(unit_prices), 2)
        percentage = list( map(lambda x: round(x / total_net, 4), unit_prices) )

        return descriptions, percentage, total_net

    def get_purchaser_statistics(self, period):
        info = collections.defaultdict(list)
        if period:
            query = "SELECT purchaser, lines FROM invoices WHERE date LIKE '%{}%'".format(period)
        else:
            query = "SELECT purchaser, lines FROM invoices"
        data = sqlContext.sql(query).collect()

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
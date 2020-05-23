#!/usr/bin/env python
# -*- coding: utf-8 -*-

def sum_gross(rows):
    """Returns total gross value from all invoice lines"""
    gross = 0
    for row in rows:
        for line in row['lines']:
            gross += float(line['gross_value'])
    return gross

def sum_net(rows):
    """Returns total net value from all invoice lines"""
    net = 0
    for row in rows:
        for line in row['lines']:
            net += float(line['net_value'])
    return net

def get_sold_products(rows):
    """Returns sold products with quantity"""
    products = {}
    for row in rows:
        for line in row['lines']:
            if line['description'] in products.keys():
                products[line['description']] += float(line['qty'])
            else:
                products[line['description']] = float(line['qty'])
    return products

def sum_discounts(rows):
    """Returns total value of discounts"""
    discounts = 0
    for row in rows:
        for line in row['lines']:
            if line['discount']:
                discounts = discounts + (float(line['discount']) * float(line['net_value']) * float(line['qty']))
    return discounts 

def get_lines_from_invoices(invoices):
    lines = []
    for invoice in invoices:
        lines.append(invoice['lines'])
    return lines

def get_lines_by_product(lines, name):
    product_lines = []
    for line in lines:
        if name == line['description']:
            product_line.append(line)
    return product_lines


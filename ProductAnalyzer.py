from database import Database
from bson.decimal128 import Decimal128

class ProductAnalyzer:
    def __init__(self, db: Database):
        self.db = db

    def total_de_vendas_por_dia(self):
        result = self.db.collection.aggregate([
            {"$group": {"_id": "$data_compra", "total_vendas": {"$sum": 1}}}
        ])
        return result

    def produto_mais_vendido(self):
        result = self.db.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$group": {"_id": "$produtos.descricao", "total_vendido": {"$sum": "$produtos.quantidade"}}},
            {"$sort": {"total_vendido": -1}},
            {"$limit": 1}
        ])
        return result

    def cliente_que_mais_gastou(self):
        result = self.db.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$group": {"_id": "$cliente_id", "total_spent": {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}}},
            {"$sort": {"total_spent": -1}},
            {"$limit": 1}
        ])
        return result

    def produtos_com_vendas_acima_de_um(self):
        result = self.db.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$match": {"produtos.quantidade": {"$gt": 1}}},
            {"$group": {"_id": "$produtos.descricao"}}
        ])
        return result

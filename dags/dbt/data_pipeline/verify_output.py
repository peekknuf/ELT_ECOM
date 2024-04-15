import duckdb


conn = duckdb.connect(database="~/ecom.db")

result = conn.execute(query="SELECT 1+1")
print(result.fetchdf())

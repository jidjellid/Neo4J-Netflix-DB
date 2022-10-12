from neo4j import GraphDatabase

uri = "neo4j://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

with driver.session(database="projet") as session:

    recordArray = session.run("MATCH (a:Person)-[:casted_in]->(f) RETURN a.name AS name LIMIT 25")
    for record in recordArray:
        print(record["name"])

driver.close()
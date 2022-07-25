import twint

c = twint.Config()
c.Search = "Anies Baswedan"
c.Limit = 1000
c.DatabasePostgres = {
    "host": "localhost",
    "port": 5432,
    "user":"postgres",
    "password":"postgres",
    "database":"twint"
}
c.PostgresAdditionalId = 2
c.Hide_output = True
twint.run.Search(c)
print("ASDASDASD")
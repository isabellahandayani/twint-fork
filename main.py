import twint

c = twint.Config()
c.Username = "jokowi"
c.DatabasePostgres = {
    "host": "localhost",
    "port": 5432,
    "user":"postgres",
    "password":"postgres",
    "database":"twint"
}

twint.run.Search(c)
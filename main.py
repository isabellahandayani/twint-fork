import twint

config = twint.Config()
config.Username = "ftrarviana"
config.Hide_output = True
config.Store_object = True


config.DatabasePostgres = {
    "host": "localhost",
    "port": 5432,
    "user":"postgres",
    "password":"postgres",
    "database":"app"
}
config.Hide_output = True
twint.run.Lookup(config)

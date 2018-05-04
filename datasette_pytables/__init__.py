from datasette.connectors import connector_method

@connector_method
def inspect(path):
    "Open file and return tables info"
    return [], []

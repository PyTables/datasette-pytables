from datasette.app import Datasette
import numpy as np
import os
from tables import *
import tempfile

def app_client(max_returned_rows=None):
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, 'test_tables.h5')
        populate_file(filepath)
        ds = Datasette(
            [filepath],
            config={
                'default_page_size': 50,
                'max_returned_rows': max_returned_rows or 1000,
            }
        )
        client = ds.app().test_client
        client.ds = ds
        yield client

def populate_file(filepath):
    class Particle(IsDescription):
        identity = StringCol(itemsize=22, dflt=' ', pos=0)
        idnumber = Int16Col(dflt=1, pos=1)
        speed = Float32Col(dflt=1, pos=2)

    h5file = open_file(filepath, mode='w')
    root = h5file.root

    group1 = h5file.create_group(root, 'group1')
    group2 = h5file.create_group(root, 'group2')

    array1 = h5file.create_array(root, 'array1', ['string', 'array'])

    table1 = h5file.create_table(group1, 'table1', Particle)
    table2 = h5file.create_table(group2, 'table2', Particle)

    array2 = h5file.create_array(group1, 'array2', [x for x in range(10000)])

    multiarray = h5file.create_array(group2, 'multi', np.arange(1000).reshape(10, 50, 2))

    for table in (table1, table2):
        row = table.row

        for i in range(10000):
            row['identity'] = 'This is particle: %2d' % (i)
            row['idnumber'] = i
            row['speed'] = i * 2.
            row.append()

        table.flush()

    h5file.close()

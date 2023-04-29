#!/usr/bin/env python
from datetime import timedelta
import random
import string
from timeit import Timer
from pathlib import Path
from tempfile import TemporaryDirectory
from expiringsqlitedict import SqliteDict
population = string.ascii_letters + string.digits + string.punctuation

with TemporaryDirectory() as dir:
    db_file = Path(dir) / 'test.db'

    def test():
        with SqliteDict(str(db_file), lifespan=timedelta(seconds=1)) as db:
            k = ''.join(random.choices(population, k=10))
            v = ''.join(random.choices(population, k=10))
            db[k] = v
    

    timer = Timer('test()', globals={'test': test})

    runs = 0
    time = 0

    # Run for at least 5 seconds
    for second in range(5):
        for fraction in range(5):
            part_runs, part_time = timer.autorange()
            runs += part_runs
            time += part_time

    size = db_file.stat().st_size

    print(f'{runs=} / {time=} = {runs / time=}')
    print(f'{size=}')

